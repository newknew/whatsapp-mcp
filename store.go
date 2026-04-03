package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protojson"
	_ "modernc.org/sqlite"
)

// retryOnBusy retries a database operation if it fails with SQLITE_BUSY
func retryOnBusy(ctx context.Context, maxRetries int, op func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = op()
		if err == nil {
			return nil
		}
		// Check if it's a busy/locked error
		if !strings.Contains(err.Error(), "database is locked") &&
			!strings.Contains(err.Error(), "SQLITE_BUSY") {
			return err // Non-retryable error
		}
		// Wait before retry with exponential backoff
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(50*(i+1)) * time.Millisecond):
		}
	}
	return err
}

// MessageStore handles local message storage
type MessageStore struct {
	db *sql.DB
}

// Message represents a stored WhatsApp message
type Message struct {
	ID           string    `json:"id"`
	ChatJID      string    `json:"chat_jid"`      // Original JID as received from WhatsApp
	CanonicalJID string    `json:"canonical_jid"` // Normalized JID (LID resolved to phone)
	SenderJID    string    `json:"sender_jid"`
	Timestamp    time.Time `json:"timestamp"`
	Content      string    `json:"content"`
	Type         string    `json:"type"` // text, image, video, audio, document, etc.
	IsFromMe     bool      `json:"is_from_me"`
	PushName     string    `json:"push_name"` // sender's display name
	RawProto     string    `json:"-"`         // JSON-serialized protobuf (not exposed in API)
}

// Chat represents a WhatsApp chat/conversation
type Chat struct {
	JID             string    `json:"jid"`
	CanonicalJID    string    `json:"canonical_jid"` // Normalized JID (LID resolved to phone)
	Name            string    `json:"name"`
	IsGroup         bool      `json:"is_group"`
	LastMessageTime time.Time `json:"last_message_time"`
	MessageCount    int       `json:"message_count"`
	RawProto        string    `json:"-"` // JSON-serialized protobuf for future re-parsing
}

// Attachment represents downloadable media from a WhatsApp message
type Attachment struct {
	MessageID      string    `json:"message_id"`
	ChatJID        string    `json:"chat_jid"`
	CanonicalJID   string    `json:"canonical_jid"` // Normalized JID (LID resolved to phone)
	SenderJID      string    `json:"sender_jid"`
	SenderName     string    `json:"sender_name"`
	Timestamp      time.Time `json:"timestamp"`
	MediaType      string    `json:"media_type"` // image, video, audio, document
	MimeType       string    `json:"mime_type"`
	Filename       string    `json:"filename"`
	Caption        string    `json:"caption"`
	FileSize       int64     `json:"file_size"`
	MediaKey       []byte    `json:"-"` // Not exposed in API
	FileSHA256     []byte    `json:"-"`
	FileEncSHA256  []byte    `json:"-"`
	DirectPath     string    `json:"-"`
	DownloadedPath string    `json:"downloaded_path,omitempty"` // Set after download
}

// NewMessageStore creates or opens the message store
func NewMessageStore(dbPath string) (*MessageStore, error) {
	// busy_timeout=5000 waits up to 5 seconds for locks instead of failing immediately
	db, err := sql.Open("sqlite", dbPath+"?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	// Limit concurrent connections to reduce lock contention
	db.SetMaxOpenConns(1)

	store := &MessageStore{db: db}
	if err := store.init(); err != nil {
		db.Close()
		return nil, err
	}

	return store, nil
}

// init creates tables if they don't exist
func (s *MessageStore) init() error {
	schema := `
	CREATE TABLE IF NOT EXISTS messages (
		id TEXT PRIMARY KEY,
		chat_jid TEXT NOT NULL,
		sender_jid TEXT NOT NULL,
		timestamp INTEGER NOT NULL,
		content TEXT,
		type TEXT NOT NULL DEFAULT 'text',
		is_from_me INTEGER NOT NULL DEFAULT 0,
		push_name TEXT,
		raw_proto TEXT,
		created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
	);

	CREATE INDEX IF NOT EXISTS idx_messages_chat_time ON messages(chat_jid, timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp DESC);

	CREATE TABLE IF NOT EXISTS chats (
		jid TEXT PRIMARY KEY,
		name TEXT,
		is_group INTEGER NOT NULL DEFAULT 0,
		last_message_time INTEGER,
		raw_proto TEXT,
		updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
	);

	CREATE TABLE IF NOT EXISTS sync_state (
		key TEXT PRIMARY KEY,
		value TEXT,
		updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
	);

	CREATE TABLE IF NOT EXISTS attachments (
		message_id TEXT PRIMARY KEY,
		chat_jid TEXT NOT NULL,
		sender_jid TEXT NOT NULL,
		sender_name TEXT,
		timestamp INTEGER NOT NULL,
		media_type TEXT NOT NULL,
		mime_type TEXT,
		filename TEXT,
		caption TEXT,
		file_size INTEGER,
		media_key BLOB,
		file_sha256 BLOB,
		file_enc_sha256 BLOB,
		direct_path TEXT,
		downloaded_path TEXT,
		created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
	);

	CREATE INDEX IF NOT EXISTS idx_attachments_chat ON attachments(chat_jid, timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_attachments_type ON attachments(media_type, timestamp DESC);

	CREATE TABLE IF NOT EXISTS contacts (
		jid TEXT PRIMARY KEY,
		phone_number TEXT,
		full_name TEXT,
		first_name TEXT,
		push_name TEXT,
		lid_jid TEXT,
		username TEXT,
		is_business INTEGER DEFAULT 0,
		raw_proto TEXT,
		updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
	);

	CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(full_name);
	CREATE INDEX IF NOT EXISTS idx_contacts_push_name ON contacts(push_name);
	CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone_number);
	`
	if _, err := s.db.Exec(schema); err != nil {
		return err
	}

	// FTS5 full-text search index (external content, backed by messages table)
	s.db.Exec(`
		CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
		USING fts5(content, content=messages, content_rowid=rowid)
	`)

	// Triggers to keep FTS index in sync with messages table
	s.db.Exec(`
		CREATE TRIGGER IF NOT EXISTS messages_fts_insert
		AFTER INSERT ON messages BEGIN
			INSERT INTO messages_fts(rowid, content)
				SELECT rowid, content FROM messages WHERE id = new.id;
		END
	`)
	s.db.Exec(`
		CREATE TRIGGER IF NOT EXISTS messages_fts_update
		AFTER UPDATE OF content ON messages BEGIN
			INSERT INTO messages_fts(messages_fts, rowid, content)
				VALUES('delete', old.rowid, old.content);
			INSERT INTO messages_fts(rowid, content)
				SELECT rowid, content FROM messages WHERE id = new.id;
		END
	`)
	s.db.Exec(`
		CREATE TRIGGER IF NOT EXISTS messages_fts_delete
		AFTER DELETE ON messages BEGIN
			INSERT INTO messages_fts(messages_fts, rowid, content)
				VALUES('delete', old.rowid, old.content);
		END
	`)

	// Migration: add raw_proto column if it doesn't exist
	s.db.Exec("ALTER TABLE messages ADD COLUMN raw_proto TEXT")
	// Ignore error - column may already exist

	// Migration: add last_read_at column to chats table
	s.db.Exec("ALTER TABLE chats ADD COLUMN last_read_at INTEGER")
	// Ignore error - column may already exist

	// Migration: add canonical_jid columns for JID normalization
	// This allows us to resolve LID JIDs to phone JIDs while keeping the original
	s.db.Exec("ALTER TABLE messages ADD COLUMN canonical_jid TEXT")
	s.db.Exec("ALTER TABLE chats ADD COLUMN canonical_jid TEXT")
	s.db.Exec("ALTER TABLE attachments ADD COLUMN canonical_jid TEXT")
	// Create indexes on canonical_jid for efficient queries
	s.db.Exec("CREATE INDEX IF NOT EXISTS idx_messages_canonical_time ON messages(canonical_jid, timestamp DESC)")
	s.db.Exec("CREATE INDEX IF NOT EXISTS idx_attachments_canonical ON attachments(canonical_jid, timestamp DESC)")
	// Ignore errors - columns/indexes may already exist

	// Backfill: for existing data, set canonical_jid = chat_jid where canonical_jid is NULL
	// This ensures existing data works with new queries (will be properly normalized on next sync)
	s.db.Exec("UPDATE messages SET canonical_jid = chat_jid WHERE canonical_jid IS NULL")
	s.db.Exec("UPDATE chats SET canonical_jid = jid WHERE canonical_jid IS NULL")
	s.db.Exec("UPDATE attachments SET canonical_jid = chat_jid WHERE canonical_jid IS NULL")

	// Migration: add raw_proto column to chats for future re-parsing of conversation metadata
	s.db.Exec("ALTER TABLE chats ADD COLUMN raw_proto TEXT")
	// Ignore error - column may already exist

	// Migration: add dismiss tracking columns for triage semantics
	// user_handled_at is independent of last_read_at (read receipts)
	// This decouples "I've triaged this" from "sender sees blue checkmarks"
	s.db.Exec("ALTER TABLE chats ADD COLUMN user_handled_at INTEGER")
	s.db.Exec("ALTER TABLE chats ADD COLUMN dismiss_notes TEXT")
	// Ignore errors - columns may already exist

	return nil
}

// RunMigrations runs one-time data migrations that require a context
// Called after store creation when a context is available
func (s *MessageStore) RunMigrations(ctx context.Context) {
	// One-time backfill: seed user_handled_at from last_read_at for chats
	// that were previously marked as read (Option B: conservative)
	val, _ := s.GetSyncState(ctx, "migration_backfill_user_handled_at")
	if val == "" {
		s.db.ExecContext(ctx, `
			UPDATE chats SET user_handled_at = last_read_at
			WHERE last_read_at IS NOT NULL AND user_handled_at IS NULL
		`)
		s.SetSyncState(ctx, "migration_backfill_user_handled_at",
			time.Now().Format(time.RFC3339))
	}

	// One-time backfill: populate FTS5 index from existing messages
	val, _ = s.GetSyncState(ctx, "migration_fts5_backfill")
	if val == "" {
		s.db.ExecContext(ctx, `
			INSERT INTO messages_fts(rowid, content)
			SELECT rowid, content FROM messages
			WHERE content IS NOT NULL AND content != ''
		`)
		s.SetSyncState(ctx, "migration_fts5_backfill",
			time.Now().Format(time.RFC3339))
	}
}

// BackfillCanonicalJID updates messages, chats, and attachments that have
// oldJID as their canonical_jid to use newJID instead. This fixes data
// stored before the LID→phone mapping was available.
// Returns the total number of rows updated across all tables.
func (s *MessageStore) BackfillCanonicalJID(ctx context.Context, oldJID, newJID string) (int, error) {
	if oldJID == "" || newJID == "" || oldJID == newJID {
		return 0, nil
	}

	var total int

	// Update messages
	res, err := s.db.ExecContext(ctx, `
		UPDATE messages SET canonical_jid = ? WHERE canonical_jid = ?
	`, newJID, oldJID)
	if err != nil {
		return 0, fmt.Errorf("backfill messages: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		total += int(n)
	}

	// Update chats
	res, err = s.db.ExecContext(ctx, `
		UPDATE chats SET canonical_jid = ? WHERE canonical_jid = ?
	`, newJID, oldJID)
	if err != nil {
		return total, fmt.Errorf("backfill chats: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		total += int(n)
	}

	// Update attachments
	res, err = s.db.ExecContext(ctx, `
		UPDATE attachments SET canonical_jid = ? WHERE canonical_jid = ?
	`, newJID, oldJID)
	if err != nil {
		return total, fmt.Errorf("backfill attachments: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		total += int(n)
	}

	return total, nil
}

// GetLIDCanonicalJIDs returns all distinct canonical_jid values that are
// LID JIDs (ending in @lid). These are candidates for backfill when
// the LID→phone mapping becomes available.
func (s *MessageStore) GetLIDCanonicalJIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT canonical_jid FROM messages
		WHERE canonical_jid LIKE '%@lid'
		UNION
		SELECT DISTINCT canonical_jid FROM chats
		WHERE canonical_jid LIKE '%@lid'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jids []string
	for rows.Next() {
		var jid string
		if err := rows.Scan(&jid); err != nil {
			continue
		}
		jids = append(jids, jid)
	}
	return jids, rows.Err()
}

// StoreMessage stores a message (upsert - idempotent)
func (s *MessageStore) StoreMessage(ctx context.Context, msg *Message) error {
	// Ensure canonical_jid is set (fallback to chat_jid if not set by caller)
	canonicalJID := msg.CanonicalJID
	if canonicalJID == "" {
		canonicalJID = msg.ChatJID
	}

	err := retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO messages (id, chat_jid, canonical_jid, sender_jid, timestamp, content, type, is_from_me, push_name, raw_proto)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET
				content = excluded.content,
				type = excluded.type,
				push_name = excluded.push_name,
				raw_proto = COALESCE(excluded.raw_proto, raw_proto),
				canonical_jid = COALESCE(excluded.canonical_jid, canonical_jid)
		`, msg.ID, msg.ChatJID, canonicalJID, msg.SenderJID, msg.Timestamp.Unix(), msg.Content, msg.Type, msg.IsFromMe, msg.PushName, msg.RawProto)
		return err
	})

	if err != nil {
		return fmt.Errorf("store message: %w", err)
	}

	// Update chat's last message time (use canonical_jid for the chat)
	err = retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO chats (jid, canonical_jid, last_message_time)
			VALUES (?, ?, ?)
			ON CONFLICT(jid) DO UPDATE SET
				last_message_time = MAX(last_message_time, excluded.last_message_time),
				canonical_jid = COALESCE(excluded.canonical_jid, canonical_jid),
				updated_at = strftime('%s', 'now')
		`, canonicalJID, canonicalJID, msg.Timestamp.Unix())
		return err
	})

	return err
}

// UpdateChat updates chat metadata
func (s *MessageStore) UpdateChat(ctx context.Context, chat *Chat) error {
	// Use canonical_jid if provided, otherwise fall back to JID
	canonicalJID := chat.CanonicalJID
	if canonicalJID == "" {
		canonicalJID = chat.JID
	}

	return retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO chats (jid, canonical_jid, name, is_group, last_message_time, raw_proto)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(jid) DO UPDATE SET
				name = COALESCE(NULLIF(excluded.name, ''), name),
				is_group = excluded.is_group,
				canonical_jid = COALESCE(NULLIF(excluded.canonical_jid, ''), canonical_jid),
				last_message_time = MAX(COALESCE(last_message_time, 0), COALESCE(excluded.last_message_time, 0)),
				raw_proto = COALESCE(NULLIF(excluded.raw_proto, ''), raw_proto),
				updated_at = strftime('%s', 'now')
		`, chat.JID, canonicalJID, chat.Name, chat.IsGroup, chat.LastMessageTime.Unix(), chat.RawProto)
		return err
	})
}

// ChatWithMessages represents a chat with its recent messages
type ChatWithMessages struct {
	JID             string    `json:"jid"`
	Name            string    `json:"name"`
	IsGroup         bool      `json:"is_group"`
	LastMessageTime time.Time `json:"last_message_time"`
	MessageCount    int       `json:"message_count"`
	UnreadCount     int       `json:"unread_count"`
	UserHandledAt   *int64    `json:"-"` // dismiss timestamp (internal, used for pending filter)
	Messages        []Message `json:"messages"`
}

// GetChats returns all chats ordered by last message time
// For 1:1 chats, returns contact name if available
// Uses canonical_jid for grouping to merge LID and phone JID conversations
func (s *MessageStore) GetChats(ctx context.Context, limit int) ([]Chat, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT c.jid, c.canonical_jid,
			COALESCE(
				NULLIF(c.name, ''),
				NULLIF(cont.full_name, ''),
				NULLIF(cont.first_name, ''),
				NULLIF(cont.push_name, ''),
				''
			) as name,
			c.is_group, c.last_message_time,
			(SELECT COUNT(*) FROM messages m WHERE m.canonical_jid = c.canonical_jid) as message_count
		FROM chats c
		LEFT JOIN contacts cont ON c.canonical_jid = cont.jid
		WHERE c.last_message_time IS NOT NULL
		ORDER BY c.last_message_time DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chats []Chat
	for rows.Next() {
		var c Chat
		var canonicalJID sql.NullString
		var lastTime sql.NullInt64
		if err := rows.Scan(&c.JID, &canonicalJID, &c.Name, &c.IsGroup, &lastTime, &c.MessageCount); err != nil {
			return nil, err
		}
		c.CanonicalJID = canonicalJID.String
		if c.CanonicalJID == "" {
			c.CanonicalJID = c.JID // Fallback for old data
		}
		if lastTime.Valid {
			c.LastMessageTime = time.Unix(lastTime.Int64, 0)
		}
		chats = append(chats, c)
	}
	return chats, rows.Err()
}

// GetChatsWithMessages returns chats with their last N messages each
// Uses canonical_jid for grouping to correctly count messages and unread across JID aliases
func (s *MessageStore) GetChatsWithMessages(ctx context.Context, limit int, messagesPerChat int) ([]ChatWithMessages, error) {
	if limit <= 0 {
		limit = 50
	}
	if messagesPerChat <= 0 {
		messagesPerChat = 1
	}
	if messagesPerChat > 10 {
		messagesPerChat = 10 // cap to prevent abuse
	}

	// First get the chats - using canonical_jid for consistent counting
	chatRows, err := s.db.QueryContext(ctx, `
		SELECT c.jid, c.canonical_jid,
			COALESCE(
				NULLIF(c.name, ''),
				NULLIF(cont.full_name, ''),
				NULLIF(cont.first_name, ''),
				NULLIF(cont.push_name, ''),
				''
			) as name,
			c.is_group, c.last_message_time,
			(SELECT COUNT(*) FROM messages m WHERE m.canonical_jid = c.canonical_jid) as message_count,
			(SELECT COUNT(*) FROM messages m WHERE m.canonical_jid = c.canonical_jid
				AND m.is_from_me = 0
				AND m.timestamp > COALESCE(c.last_read_at, 0)) as unread_count,
			c.user_handled_at
		FROM chats c
		LEFT JOIN contacts cont ON c.canonical_jid = cont.jid
		WHERE c.last_message_time IS NOT NULL
		ORDER BY c.last_message_time DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer chatRows.Close()

	var chats []ChatWithMessages
	var canonicalJIDs []string
	for chatRows.Next() {
		var c ChatWithMessages
		var canonicalJID sql.NullString
		var lastTime, userHandledAt sql.NullInt64
		if err := chatRows.Scan(&c.JID, &canonicalJID, &c.Name, &c.IsGroup, &lastTime, &c.MessageCount, &c.UnreadCount, &userHandledAt); err != nil {
			return nil, err
		}
		// Use canonical_jid as the primary JID for MCP interface
		if canonicalJID.String != "" {
			c.JID = canonicalJID.String
		}
		if lastTime.Valid {
			c.LastMessageTime = time.Unix(lastTime.Int64, 0)
		}
		if userHandledAt.Valid {
			v := userHandledAt.Int64
			c.UserHandledAt = &v
		}
		c.Messages = []Message{} // initialize empty
		chats = append(chats, c)
		canonicalJIDs = append(canonicalJIDs, c.JID)
	}
	if err := chatRows.Err(); err != nil {
		return nil, err
	}

	if len(chats) == 0 {
		return chats, nil
	}

	// Build a map for quick lookup by canonical_jid
	chatIndex := make(map[string]int)
	for i, c := range chats {
		chatIndex[c.JID] = i
	}

	// Now get the last N messages for each chat using window function
	// Query by canonical_jid to get all messages regardless of original JID
	placeholders := make([]string, len(canonicalJIDs))
	args := make([]interface{}, len(canonicalJIDs)+1)
	for i, jid := range canonicalJIDs {
		placeholders[i] = "?"
		args[i] = jid
	}
	args[len(canonicalJIDs)] = messagesPerChat

	query := `
		WITH ranked AS (
			SELECT id, chat_jid, canonical_jid, sender_jid, timestamp, content, type, is_from_me, push_name,
				ROW_NUMBER() OVER (PARTITION BY canonical_jid ORDER BY timestamp DESC) as rn
			FROM messages
			WHERE canonical_jid IN (` + strings.Join(placeholders, ",") + `)
		)
		SELECT id, chat_jid, canonical_jid, sender_jid, timestamp, content, type, is_from_me, push_name
		FROM ranked
		WHERE rn <= ?
		ORDER BY canonical_jid, timestamp DESC
	`

	msgRows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query messages: %w", err)
	}
	defer msgRows.Close()

	for msgRows.Next() {
		var m Message
		var ts int64
		var pushName, canonicalJID sql.NullString
		if err := msgRows.Scan(&m.ID, &m.ChatJID, &canonicalJID, &m.SenderJID, &ts, &m.Content, &m.Type, &m.IsFromMe, &pushName); err != nil {
			return nil, fmt.Errorf("scan message: %w", err)
		}
		m.Timestamp = time.Unix(ts, 0)
		m.PushName = pushName.String
		m.CanonicalJID = canonicalJID.String

		// Look up by canonical_jid
		lookupJID := m.CanonicalJID
		if lookupJID == "" {
			lookupJID = m.ChatJID
		}
		if idx, ok := chatIndex[lookupJID]; ok {
			chats[idx].Messages = append(chats[idx].Messages, m)
		}
	}

	return chats, msgRows.Err()
}

// GetMessages returns messages for a chat
// Queries by canonical_jid to find all messages regardless of original JID variant
func (s *MessageStore) GetMessages(ctx context.Context, chatJID string, limit int, beforeTime *time.Time) ([]Message, error) {
	if limit <= 0 {
		limit = 50
	}

	var rows *sql.Rows
	var err error

	// Query by canonical_jid to get messages from all JID aliases
	if beforeTime != nil {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, chat_jid, canonical_jid, sender_jid, timestamp, content, type, is_from_me, push_name
			FROM messages
			WHERE canonical_jid = ? AND timestamp < ?
			ORDER BY timestamp DESC
			LIMIT ?
		`, chatJID, beforeTime.Unix(), limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, chat_jid, canonical_jid, sender_jid, timestamp, content, type, is_from_me, push_name
			FROM messages
			WHERE canonical_jid = ?
			ORDER BY timestamp DESC
			LIMIT ?
		`, chatJID, limit)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var m Message
		var ts int64
		var pushName, canonicalJID sql.NullString
		if err := rows.Scan(&m.ID, &m.ChatJID, &canonicalJID, &m.SenderJID, &ts, &m.Content, &m.Type, &m.IsFromMe, &pushName); err != nil {
			return nil, err
		}
		m.Timestamp = time.Unix(ts, 0)
		m.PushName = pushName.String
		m.CanonicalJID = canonicalJID.String
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

// GetMessagesByJIDs returns messages for multiple JID variants (handles legacy aliased data)
// Queries by both canonical_jid and chat_jid columns to catch all messages
func (s *MessageStore) GetMessagesByJIDs(ctx context.Context, jids []string, limit int) ([]Message, error) {
	if len(jids) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}

	// Build query with OR conditions for both canonical_jid and chat_jid
	placeholders := make([]string, len(jids))
	args := make([]interface{}, 0, len(jids)*2+1)
	for i, jid := range jids {
		placeholders[i] = "?"
		args = append(args, jid)
	}
	// Duplicate args for chat_jid check
	for _, jid := range jids {
		args = append(args, jid)
	}
	args = append(args, limit)

	query := `
		SELECT id, chat_jid, canonical_jid, sender_jid, timestamp, content, type, is_from_me, push_name
		FROM messages
		WHERE canonical_jid IN (` + strings.Join(placeholders, ",") + `)
		   OR chat_jid IN (` + strings.Join(placeholders, ",") + `)
		ORDER BY timestamp DESC
		LIMIT ?
	`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var m Message
		var ts int64
		var pushName, canonicalJID sql.NullString
		if err := rows.Scan(&m.ID, &m.ChatJID, &canonicalJID, &m.SenderJID, &ts, &m.Content, &m.Type, &m.IsFromMe, &pushName); err != nil {
			return nil, err
		}
		m.Timestamp = time.Unix(ts, 0)
		m.PushName = pushName.String
		m.CanonicalJID = canonicalJID.String
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

// GetMessage retrieves a single message by ID
func (s *MessageStore) GetMessage(ctx context.Context, messageID string) (*Message, error) {
	var m Message
	var ts int64
	var pushName sql.NullString

	err := s.db.QueryRowContext(ctx, `
		SELECT id, chat_jid, sender_jid, timestamp, content, type, is_from_me, push_name
		FROM messages WHERE id = ?
	`, messageID).Scan(&m.ID, &m.ChatJID, &m.SenderJID, &ts, &m.Content, &m.Type, &m.IsFromMe, &pushName)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	m.Timestamp = time.Unix(ts, 0)
	m.PushName = pushName.String
	return &m, nil
}

// GetMessageRawProto retrieves the raw_proto for a message by ID
func (s *MessageStore) GetMessageRawProto(ctx context.Context, messageID string) (string, error) {
	var rawProto sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT raw_proto FROM messages WHERE id = ? AND raw_proto IS NOT NULL AND raw_proto != ''
	`, messageID).Scan(&rawProto)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return rawProto.String, nil
}

// MessageQuery holds parameters for QueryMessages
type MessageQuery struct {
	ChatJID    string     // optional - omit for cross-chat query
	Limit      int        // max messages to return
	BeforeTime *time.Time // pagination: messages before this time
	SinceTime  *time.Time // filter: messages after this time
	Search     string     // full-text search in content
	Type       string     // filter by message type
}

// QueryMessages returns messages with flexible filtering
// Supports cross-chat queries when ChatJID is empty
// Queries by canonical_jid to find all messages regardless of original JID variant
func (s *MessageStore) QueryMessages(ctx context.Context, q MessageQuery) ([]Message, error) {
	if q.Limit <= 0 {
		q.Limit = 50
	}
	if q.Limit > 500 {
		q.Limit = 500 // cap for cross-chat queries
	}

	// Build dynamic query
	var conditions []string
	var args []interface{}
	useFTS := q.Search != ""

	// Column references need table prefix when FTS join is active
	col := func(name string) string {
		if useFTS {
			return "messages." + name
		}
		return name
	}

	if q.ChatJID != "" {
		// Query by canonical_jid to get messages from all JID aliases
		conditions = append(conditions, col("canonical_jid")+" = ?")
		args = append(args, q.ChatJID)
	}

	if q.BeforeTime != nil {
		conditions = append(conditions, col("timestamp")+" < ?")
		args = append(args, q.BeforeTime.Unix())
	}

	if q.SinceTime != nil {
		conditions = append(conditions, col("timestamp")+" > ?")
		args = append(args, q.SinceTime.Unix())
	}

	if useFTS {
		conditions = append(conditions, "messages_fts MATCH ?")
		args = append(args, q.Search)
	}

	if q.Type != "" {
		conditions = append(conditions, col("type")+" = ?")
		args = append(args, q.Type)
	}

	fromClause := "messages"
	if useFTS {
		fromClause = "messages JOIN messages_fts ON messages.rowid = messages_fts.rowid"
	}

	query := fmt.Sprintf(`
		SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s
		FROM %s
	`, col("id"), col("chat_jid"), col("canonical_jid"), col("sender_jid"),
		col("timestamp"), col("content"), col("type"), col("is_from_me"), col("push_name"),
		fromClause)
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY %s DESC LIMIT ?", col("timestamp"))
	args = append(args, q.Limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var m Message
		var ts int64
		var pushName, canonicalJID sql.NullString
		if err := rows.Scan(&m.ID, &m.ChatJID, &canonicalJID, &m.SenderJID, &ts, &m.Content, &m.Type, &m.IsFromMe, &pushName); err != nil {
			return nil, err
		}
		m.Timestamp = time.Unix(ts, 0)
		m.PushName = pushName.String
		m.CanonicalJID = canonicalJID.String
		messages = append(messages, m)
	}
	return messages, rows.Err()
}

// DismissChat marks a chat as handled for triage purposes
// This is independent of read receipts — it only affects pending_only filtering
// Uses canonical_jid for lookup to handle JID aliasing
func (s *MessageStore) DismissChat(ctx context.Context, chatJID string, notes string) (int64, error) {
	now := time.Now().Unix()
	err := retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE chats SET user_handled_at = ?, dismiss_notes = ?, updated_at = strftime('%s', 'now')
			WHERE canonical_jid = ?
		`, now, notes, chatJID)
		return err
	})
	return now, err
}

// UndismissChat clears the dismiss state so the chat reappears in pending
// Uses canonical_jid for lookup to handle JID aliasing
func (s *MessageStore) UndismissChat(ctx context.Context, chatJID string) error {
	return retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE chats SET user_handled_at = NULL, dismiss_notes = NULL, updated_at = strftime('%s', 'now')
			WHERE canonical_jid = ?
		`, chatJID)
		return err
	})
}

// SetChatLastReadAt updates the last_read_at timestamp for a chat
// This is called when marking messages as read to track local unread state
// Uses canonical_jid for lookup to handle JID aliasing
func (s *MessageStore) SetChatLastReadAt(ctx context.Context, chatJID string, t time.Time) error {
	return retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE chats SET last_read_at = ?, updated_at = strftime('%s', 'now')
			WHERE canonical_jid = ?
		`, t.Unix(), chatJID)
		return err
	})
}

// GetMessageCount returns total messages stored
func (s *MessageStore) GetMessageCount(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&count)
	return count, err
}

// SetSyncState stores sync state
func (s *MessageStore) SetSyncState(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sync_state (key, value, updated_at)
		VALUES (?, ?, strftime('%s', 'now'))
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = strftime('%s', 'now')
	`, key, value)
	return err
}

// GetSyncState retrieves sync state
func (s *MessageStore) GetSyncState(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM sync_state WHERE key = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// ReparseMessages re-parses messages using stored raw_proto
// Returns count of updated messages
func (s *MessageStore) ReparseMessages(ctx context.Context, typeFilter string) (int, error) {
	// Query messages with raw_proto that match the type filter
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, raw_proto FROM messages
		WHERE type = ? AND raw_proto IS NOT NULL AND raw_proto != ''
	`, typeFilter)
	if err != nil {
		return 0, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var updates []struct {
		id      string
		content string
		msgType string
	}

	for rows.Next() {
		var id, rawProto string
		if err := rows.Scan(&id, &rawProto); err != nil {
			continue
		}

		// Deserialize raw proto
		var msg waE2E.Message
		if err := protojson.Unmarshal([]byte(rawProto), &msg); err != nil {
			continue
		}

		// Re-extract content using current parser
		content, msgType := extractContent(&msg)

		// Only update if type changed
		if msgType != typeFilter {
			updates = append(updates, struct {
				id      string
				content string
				msgType string
			}{id, content, msgType})
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate rows: %w", err)
	}

	// Apply updates
	for _, u := range updates {
		err := retryOnBusy(ctx, 5, func() error {
			_, err := s.db.ExecContext(ctx, `
				UPDATE messages SET content = ?, type = ? WHERE id = ?
			`, u.content, u.msgType, u.id)
			return err
		})
		if err != nil {
			return 0, fmt.Errorf("update message %s: %w", u.id, err)
		}
	}

	return len(updates), nil
}

// ReparseAttachments backfills missing attachment records from raw_proto.
// Finds messages with media types (image, video, audio, document, sticker)
// that have raw_proto but no corresponding row in the attachments table.
func (s *MessageStore) ReparseAttachments(ctx context.Context) (int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT m.id, m.chat_jid, m.canonical_jid, m.sender_jid, m.timestamp, m.push_name, m.raw_proto
		FROM messages m
		LEFT JOIN attachments a ON m.id = a.message_id
		WHERE m.type IN ('image', 'video', 'audio', 'document', 'sticker')
			AND m.raw_proto IS NOT NULL AND m.raw_proto != ''
			AND a.message_id IS NULL
	`)
	if err != nil {
		return 0, fmt.Errorf("query messages: %w", err)
	}
	defer rows.Close()

	var toStore []*Attachment
	for rows.Next() {
		var id, chatJID, senderJID, rawProto string
		var canonicalJID, pushName sql.NullString
		var ts int64
		if err := rows.Scan(&id, &chatJID, &canonicalJID, &senderJID, &ts, &pushName, &rawProto); err != nil {
			continue
		}

		var msg waE2E.Message
		if err := protojson.Unmarshal([]byte(rawProto), &msg); err != nil {
			continue
		}

		att := extractAttachmentFromProto(id, chatJID, canonicalJID.String, senderJID, pushName.String, time.Unix(ts, 0), &msg)
		if att != nil {
			toStore = append(toStore, att)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate rows: %w", err)
	}

	for _, att := range toStore {
		if err := s.StoreAttachment(ctx, att); err != nil {
			return 0, fmt.Errorf("store attachment %s: %w", att.MessageID, err)
		}
	}

	return len(toStore), nil
}

// extractAttachmentFromProto extracts attachment metadata from a deserialized protobuf message.
// Shared logic used by both ReparseAttachments and event handlers.
func extractAttachmentFromProto(messageID, chatJID, canonicalJID, senderJID, senderName string, ts time.Time, msg *waE2E.Message) *Attachment {
	if msg == nil {
		return nil
	}

	att := &Attachment{
		MessageID:    messageID,
		ChatJID:      chatJID,
		CanonicalJID: canonicalJID,
		SenderJID:    senderJID,
		SenderName:   senderName,
		Timestamp:    ts,
	}

	if img := msg.ImageMessage; img != nil {
		att.MediaType = "image"
		att.MimeType = img.GetMimetype()
		att.Caption = img.GetCaption()
		att.FileSize = int64(img.GetFileLength())
		att.MediaKey = img.GetMediaKey()
		att.FileSHA256 = img.GetFileSHA256()
		att.FileEncSHA256 = img.GetFileEncSHA256()
		att.DirectPath = img.GetDirectPath()
		return att
	}
	if vid := msg.VideoMessage; vid != nil {
		att.MediaType = "video"
		att.MimeType = vid.GetMimetype()
		att.Caption = vid.GetCaption()
		att.FileSize = int64(vid.GetFileLength())
		att.MediaKey = vid.GetMediaKey()
		att.FileSHA256 = vid.GetFileSHA256()
		att.FileEncSHA256 = vid.GetFileEncSHA256()
		att.DirectPath = vid.GetDirectPath()
		return att
	}
	if aud := msg.AudioMessage; aud != nil {
		att.MediaType = "audio"
		att.MimeType = aud.GetMimetype()
		att.FileSize = int64(aud.GetFileLength())
		att.MediaKey = aud.GetMediaKey()
		att.FileSHA256 = aud.GetFileSHA256()
		att.FileEncSHA256 = aud.GetFileEncSHA256()
		att.DirectPath = aud.GetDirectPath()
		return att
	}
	if doc := msg.DocumentMessage; doc != nil {
		att.MediaType = "document"
		att.MimeType = doc.GetMimetype()
		att.Filename = doc.GetFileName()
		att.Caption = doc.GetCaption()
		att.FileSize = int64(doc.GetFileLength())
		att.MediaKey = doc.GetMediaKey()
		att.FileSHA256 = doc.GetFileSHA256()
		att.FileEncSHA256 = doc.GetFileEncSHA256()
		att.DirectPath = doc.GetDirectPath()
		return att
	}
	if stk := msg.StickerMessage; stk != nil {
		att.MediaType = "sticker"
		att.MimeType = stk.GetMimetype()
		att.FileSize = int64(stk.GetFileLength())
		att.MediaKey = stk.GetMediaKey()
		att.FileSHA256 = stk.GetFileSHA256()
		att.FileEncSHA256 = stk.GetFileEncSHA256()
		att.DirectPath = stk.GetDirectPath()
		return att
	}

	return nil
}

// Close closes the database
func (s *MessageStore) Close() error {
	return s.db.Close()
}

// StoreAttachment stores attachment metadata (upsert)
func (s *MessageStore) StoreAttachment(ctx context.Context, att *Attachment) error {
	// Ensure canonical_jid is set (fallback to chat_jid if not set by caller)
	canonicalJID := att.CanonicalJID
	if canonicalJID == "" {
		canonicalJID = att.ChatJID
	}

	return retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO attachments (message_id, chat_jid, canonical_jid, sender_jid, sender_name, timestamp,
				media_type, mime_type, filename, caption, file_size,
				media_key, file_sha256, file_enc_sha256, direct_path)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(message_id) DO UPDATE SET
				mime_type = COALESCE(excluded.mime_type, mime_type),
				filename = COALESCE(excluded.filename, filename),
				caption = COALESCE(excluded.caption, caption),
				file_size = COALESCE(excluded.file_size, file_size),
				canonical_jid = COALESCE(excluded.canonical_jid, canonical_jid)
		`, att.MessageID, att.ChatJID, canonicalJID, att.SenderJID, att.SenderName, att.Timestamp.Unix(),
			att.MediaType, att.MimeType, att.Filename, att.Caption, att.FileSize,
			att.MediaKey, att.FileSHA256, att.FileEncSHA256, att.DirectPath)
		return err
	})
}

// GetAttachment retrieves attachment metadata by message ID
func (s *MessageStore) GetAttachment(ctx context.Context, messageID string) (*Attachment, error) {
	var att Attachment
	var ts int64
	var senderName, mimeType, filename, caption, directPath, downloadedPath, canonicalJID sql.NullString
	var fileSize sql.NullInt64

	err := s.db.QueryRowContext(ctx, `
		SELECT message_id, chat_jid, canonical_jid, sender_jid, sender_name, timestamp,
			media_type, mime_type, filename, caption, file_size,
			media_key, file_sha256, file_enc_sha256, direct_path, downloaded_path
		FROM attachments WHERE message_id = ?
	`, messageID).Scan(&att.MessageID, &att.ChatJID, &canonicalJID, &att.SenderJID, &senderName, &ts,
		&att.MediaType, &mimeType, &filename, &caption, &fileSize,
		&att.MediaKey, &att.FileSHA256, &att.FileEncSHA256, &directPath, &downloadedPath)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	att.Timestamp = time.Unix(ts, 0)
	att.SenderName = senderName.String
	att.MimeType = mimeType.String
	att.Filename = filename.String
	att.Caption = caption.String
	att.FileSize = fileSize.Int64
	att.DirectPath = directPath.String
	att.DownloadedPath = downloadedPath.String
	att.CanonicalJID = canonicalJID.String

	return &att, nil
}

// ListAttachments returns attachments with optional filters
// Uses canonical_jid for chat filtering to handle JID aliasing
func (s *MessageStore) ListAttachments(ctx context.Context, chatJID string, mediaType string, limit int) ([]Attachment, error) {
	if limit <= 0 {
		limit = 50
	}

	var args []interface{}
	query := `
		SELECT message_id, chat_jid, canonical_jid, sender_jid, sender_name, timestamp,
			media_type, mime_type, filename, caption, file_size, downloaded_path
		FROM attachments WHERE 1=1
	`

	if chatJID != "" {
		// Query by canonical_jid to get attachments from all JID aliases
		query += " AND canonical_jid = ?"
		args = append(args, chatJID)
	}
	if mediaType != "" {
		query += " AND media_type = ?"
		args = append(args, mediaType)
	}

	query += " ORDER BY timestamp DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attachments []Attachment
	for rows.Next() {
		var att Attachment
		var ts int64
		var senderName, mimeType, filename, caption, downloadedPath, canonicalJID sql.NullString
		var fileSize sql.NullInt64

		if err := rows.Scan(&att.MessageID, &att.ChatJID, &canonicalJID, &att.SenderJID, &senderName, &ts,
			&att.MediaType, &mimeType, &filename, &caption, &fileSize, &downloadedPath); err != nil {
			return nil, err
		}

		att.Timestamp = time.Unix(ts, 0)
		att.SenderName = senderName.String
		att.MimeType = mimeType.String
		att.Filename = filename.String
		att.Caption = caption.String
		att.FileSize = fileSize.Int64
		att.DownloadedPath = downloadedPath.String
		att.CanonicalJID = canonicalJID.String

		attachments = append(attachments, att)
	}

	return attachments, rows.Err()
}

// UpdateAttachmentDownloadPath updates the downloaded path for an attachment
func (s *MessageStore) UpdateAttachmentDownloadPath(ctx context.Context, messageID, downloadedPath string) error {
	return retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE attachments SET downloaded_path = ? WHERE message_id = ?
		`, downloadedPath, messageID)
		return err
	})
}

// GetAttachmentCount returns the number of attachments
func (s *MessageStore) GetAttachmentCount(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attachments`).Scan(&count)
	return count, err
}

// Contact represents a WhatsApp contact
type Contact struct {
	JID         string `json:"jid"`
	PhoneNumber string `json:"phone_number,omitempty"`
	FullName    string `json:"full_name,omitempty"`
	FirstName   string `json:"first_name,omitempty"`
	PushName    string `json:"push_name,omitempty"`
	LidJID      string `json:"lid_jid,omitempty"`
	Username    string `json:"username,omitempty"`
	IsBusiness  bool   `json:"is_business"`
	RawProto    string `json:"-"` // JSON-serialized protobuf for future re-parsing
}

// StoreContact stores or updates a contact (upsert)
func (s *MessageStore) StoreContact(ctx context.Context, c *Contact) error {
	return retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO contacts (jid, phone_number, full_name, first_name, push_name, lid_jid, username, is_business, raw_proto, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%s', 'now'))
			ON CONFLICT(jid) DO UPDATE SET
				phone_number = COALESCE(excluded.phone_number, phone_number),
				full_name = COALESCE(excluded.full_name, full_name),
				first_name = COALESCE(excluded.first_name, first_name),
				push_name = COALESCE(excluded.push_name, push_name),
				lid_jid = COALESCE(excluded.lid_jid, lid_jid),
				username = COALESCE(excluded.username, username),
				is_business = excluded.is_business,
				raw_proto = COALESCE(excluded.raw_proto, raw_proto),
				updated_at = strftime('%s', 'now')
		`, c.JID, c.PhoneNumber, c.FullName, c.FirstName, c.PushName, c.LidJID, c.Username, c.IsBusiness, c.RawProto)
		return err
	})
}

// UpdateContactPushName updates just the push_name for a contact
func (s *MessageStore) UpdateContactPushName(ctx context.Context, jid, pushName string) error {
	return retryOnBusy(ctx, 5, func() error {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO contacts (jid, push_name, updated_at)
			VALUES (?, ?, strftime('%s', 'now'))
			ON CONFLICT(jid) DO UPDATE SET
				push_name = excluded.push_name,
				updated_at = strftime('%s', 'now')
		`, jid, pushName)
		return err
	})
}

// GetContact retrieves a contact by JID
func (s *MessageStore) GetContact(ctx context.Context, jid string) (*Contact, error) {
	var c Contact
	var phoneNumber, fullName, firstName, pushName, lidJID, username sql.NullString

	err := s.db.QueryRowContext(ctx, `
		SELECT jid, phone_number, full_name, first_name, push_name, lid_jid, username, is_business
		FROM contacts WHERE jid = ?
	`, jid).Scan(&c.JID, &phoneNumber, &fullName, &firstName, &pushName, &lidJID, &username, &c.IsBusiness)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	c.PhoneNumber = phoneNumber.String
	c.FullName = fullName.String
	c.FirstName = firstName.String
	c.PushName = pushName.String
	c.LidJID = lidJID.String
	c.Username = username.String

	return &c, nil
}

// SearchContacts searches contacts by name (full_name, first_name, or push_name)
func (s *MessageStore) SearchContacts(ctx context.Context, query string, limit int) ([]Contact, error) {
	if limit <= 0 {
		limit = 20
	}

	// Use LIKE for simple substring matching
	pattern := "%" + query + "%"

	rows, err := s.db.QueryContext(ctx, `
		SELECT jid, phone_number, full_name, first_name, push_name, lid_jid, username, is_business
		FROM contacts
		WHERE full_name LIKE ? OR first_name LIKE ? OR push_name LIKE ? OR phone_number LIKE ?
		ORDER BY
			CASE WHEN full_name LIKE ? THEN 0 ELSE 1 END,
			COALESCE(full_name, push_name, phone_number)
		LIMIT ?
	`, pattern, pattern, pattern, pattern, pattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var contacts []Contact
	for rows.Next() {
		var c Contact
		var phoneNumber, fullName, firstName, pushName, lidJID, username sql.NullString

		if err := rows.Scan(&c.JID, &phoneNumber, &fullName, &firstName, &pushName, &lidJID, &username, &c.IsBusiness); err != nil {
			return nil, err
		}

		c.PhoneNumber = phoneNumber.String
		c.FullName = fullName.String
		c.FirstName = firstName.String
		c.PushName = pushName.String
		c.LidJID = lidJID.String
		c.Username = username.String

		contacts = append(contacts, c)
	}

	return contacts, rows.Err()
}

// GetDistinctChatJIDs returns all unique chat_jid values from messages
// Used to find LID aliases that might not be in the chats table
func (s *MessageStore) GetDistinctChatJIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT chat_jid FROM messages
		UNION
		SELECT DISTINCT jid FROM chats
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jids []string
	for rows.Next() {
		var jid string
		if err := rows.Scan(&jid); err != nil {
			continue
		}
		jids = append(jids, jid)
	}
	return jids, rows.Err()
}

// GetContactName returns the best display name for a JID
func (s *MessageStore) GetContactName(ctx context.Context, jid string) string {
	var fullName, firstName, pushName sql.NullString

	err := s.db.QueryRowContext(ctx, `
		SELECT full_name, first_name, push_name FROM contacts WHERE jid = ?
	`, jid).Scan(&fullName, &firstName, &pushName)

	if err != nil {
		return ""
	}

	// Prefer full_name > first_name > push_name
	if fullName.String != "" {
		return fullName.String
	}
	if firstName.String != "" {
		return firstName.String
	}
	return pushName.String
}

// GetContactCount returns the number of contacts
func (s *MessageStore) GetContactCount(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM contacts`).Scan(&count)
	return count, err
}

// GetChatUnreadCount returns the unread count for a specific chat
// Uses canonical_jid for lookup to handle JID aliasing
func (s *MessageStore) GetChatUnreadCount(ctx context.Context, chatJID string) (int, error) {
	var unreadCount int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM messages m
		JOIN chats c ON m.canonical_jid = c.canonical_jid
		WHERE c.canonical_jid = ?
			AND m.is_from_me = 0
			AND m.timestamp > COALESCE(c.last_read_at, 0)
	`, chatJID).Scan(&unreadCount)
	if err != nil {
		return 0, err
	}
	return unreadCount, nil
}
