package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mdp/qrterminal/v3"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	_ "modernc.org/sqlite"
)

// Debug log file path
var DebugLogPath = getLogPath()

func getLogPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/whatsapp-mcp.log" // fallback
	}
	logDir := filepath.Join(home, ".local", "log", "newknew-mcp")
	os.MkdirAll(logDir, 0755) // ensure directory exists
	return filepath.Join(logDir, "whatsapp.log")
}

// fileLogger implements waLog.Logger interface
type fileLogger struct {
	file   *os.File
	module string
}

func newFileLogger(module string) waLog.Logger {
	f, err := os.OpenFile(DebugLogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return waLog.Noop
	}
	return &fileLogger{file: f, module: module}
}

func (l *fileLogger) Debugf(msg string, args ...interface{}) {
	l.log("DEBUG", msg, args...)
}

func (l *fileLogger) Infof(msg string, args ...interface{}) {
	l.log("INFO", msg, args...)
}

func (l *fileLogger) Warnf(msg string, args ...interface{}) {
	l.log("WARN", msg, args...)
}

func (l *fileLogger) Errorf(msg string, args ...interface{}) {
	l.log("ERROR", msg, args...)
}

func (l *fileLogger) Sub(module string) waLog.Logger {
	return newFileLogger(l.module + "/" + module)
}

func (l *fileLogger) log(level, msg string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	fmt.Fprintf(l.file, "%s [%s] %s: %s\n", timestamp, level, l.module, fmt.Sprintf(msg, args...))
	l.file.Sync()
}

// Client wraps whatsmeow client with our configuration
type Client struct {
	client         *whatsmeow.Client
	container      *sqlstore.Container
	msgStore       *MessageStore
	lock           *os.File
	configDir      string
	mu             sync.Mutex
	authInProgress bool
	qrFilePath     string
}

var (
	globalClient *Client
	clientOnce   sync.Once
)

// GetClient returns the singleton WhatsApp client
func GetClient() (*Client, error) {
	var initErr error
	clientOnce.Do(func() {
		globalClient, initErr = newClient()
	})
	if initErr != nil {
		return nil, initErr
	}
	return globalClient, nil
}

// newClient creates a new WhatsApp client
func newClient() (*Client, error) {
	ctx := context.Background()

	// Determine config directory
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("get home dir: %w", err)
	}
	configDir := filepath.Join(home, ".local", "share", "newknew-mcp", "whatsapp")

	// Create config directory with restrictive permissions
	if err := os.MkdirAll(configDir, 0700); err != nil {
		return nil, fmt.Errorf("create config dir: %w", err)
	}

	// Acquire file lock (single instance enforcement)
	lock, err := acquireLock(configDir)
	if err != nil {
		return nil, err
	}

	// Initialize SQLite store (whatsmeow session store)
	dbPath := filepath.Join(configDir, "store.db")
	container, err := sqlstore.New(
		ctx,
		"sqlite",
		fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)", dbPath),
		newFileLogger("store"),
	)
	if err != nil {
		lock.Close()
		return nil, fmt.Errorf("init store: %w", err)
	}

	// Initialize message store (our local message cache)
	msgDBPath := filepath.Join(configDir, "messages.db")
	msgStore, err := NewMessageStore(msgDBPath)
	if err != nil {
		lock.Close()
		return nil, fmt.Errorf("init message store: %w", err)
	}

	// Run one-time data migrations (e.g., backfill user_handled_at)
	msgStore.RunMigrations(ctx)

	return &Client{
		container: container,
		msgStore:  msgStore,
		lock:      lock,
		configDir: configDir,
	}, nil
}

// acquireLock gets an exclusive file lock
func acquireLock(configDir string) (*os.File, error) {
	lockPath := filepath.Join(configDir, "whatsapp.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}

	// Non-blocking exclusive lock
	err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		lock.Close()
		return nil, fmt.Errorf("another whatsapp-mcp instance is running")
	}
	return lock, nil
}

// IsAuthenticated checks if we have a valid session
func (c *Client) IsAuthenticated(ctx context.Context) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	device, err := c.container.GetFirstDevice(ctx)
	if err != nil || device == nil {
		return false
	}
	return device.ID != nil
}

// GetPhoneNumber returns the authenticated phone number
func (c *Client) GetPhoneNumber(ctx context.Context) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	device, err := c.container.GetFirstDevice(ctx)
	if err != nil || device == nil || device.ID == nil {
		return ""
	}
	return device.ID.User
}

// ClearSession removes the local session, forcing re-authentication
func (c *Client) ClearSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Disconnect if connected
	if c.client != nil {
		c.client.Disconnect()
		c.client = nil
	}

	// Delete all devices from the container
	devices, err := c.container.GetAllDevices(ctx)
	if err != nil {
		return fmt.Errorf("get devices: %w", err)
	}

	for _, device := range devices {
		if err := device.Delete(ctx); err != nil {
			return fmt.Errorf("delete device: %w", err)
		}
	}

	c.authInProgress = false
	return nil
}

// Connect establishes connection to WhatsApp
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil && c.client.IsConnected() {
		return nil
	}

	device, err := c.container.GetFirstDevice(ctx)
	if err != nil {
		return fmt.Errorf("get device: %w", err)
	}

	if device == nil {
		// New device - will need QR code
		device = c.container.NewDevice()
	}

	// Create client with debug logging
	c.client = whatsmeow.NewClient(device, newFileLogger("client"))
	c.client.EnableAutoReconnect = true

	// Register event handler to capture messages
	if c.msgStore != nil {
		handler := NewEventHandler(c.client, c.msgStore)
		handler.Register()
	}

	// Connect
	if err := c.client.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	// Run LID canonical_jid backfill in background after connecting.
	// This fixes messages/chats/attachments stored before the LID→phone
	// mapping was available in whatsmeow's LID store.
	go c.backfillLIDCanonicalJIDs()

	return nil
}

// RunInteractiveAuth displays QR code to terminal and waits for authentication
func (c *Client) RunInteractiveAuth(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create new device
	device := c.container.NewDevice()
	c.client = whatsmeow.NewClient(device, newFileLogger("client"))

	qrChan, err := c.client.GetQRChannel(ctx)
	if err != nil {
		return fmt.Errorf("get QR channel: %w", err)
	}

	if err := c.client.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	// Process QR events
	for evt := range qrChan {
		switch evt.Event {
		case "code":
			// Display QR code directly to terminal
			qrterminal.GenerateWithConfig(evt.Code, qrterminal.Config{
				Level:     qrterminal.L,
				Writer:    os.Stdout,
				BlackChar: qrterminal.BLACK,
				WhiteChar: qrterminal.WHITE,
				QuietZone: 1,
			})
			fmt.Println("\nWaiting for scan...")
		case "success":
			return nil
		case "timeout":
			return fmt.Errorf("QR code timeout - please try again")
		}
	}

	return fmt.Errorf("QR channel closed unexpectedly")
}

// QRFilePath is where the QR code is written for MCP-based auth
const QRFilePath = "/tmp/whatsapp-qr.txt"

// StartBackgroundAuth starts auth and writes QR to file (for MCP use)
func (c *Client) StartBackgroundAuth(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.authInProgress {
		// Already in progress - just return the file path
		return c.qrFilePath, nil
	}

	// Create new device
	device := c.container.NewDevice()
	c.client = whatsmeow.NewClient(device, newFileLogger("client"))
	c.client.EnableAutoReconnect = true

	// Register event handler to capture messages (including history sync)
	if c.msgStore != nil {
		handler := NewEventHandler(c.client, c.msgStore)
		handler.Register()
	}

	// Use background context so auth continues after MCP request returns
	bgCtx := context.Background()

	qrChan, err := c.client.GetQRChannel(bgCtx)
	if err != nil {
		return "", fmt.Errorf("get QR channel: %w", err)
	}

	if err := c.client.Connect(); err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}

	c.authInProgress = true
	c.qrFilePath = QRFilePath

	// Wait for first QR code
	evt := <-qrChan
	if evt.Event != "code" {
		c.authInProgress = false
		return "", fmt.Errorf("unexpected event: %s", evt.Event)
	}

	// Write QR to file
	if err := c.writeQRToFile(evt.Code); err != nil {
		c.authInProgress = false
		return "", err
	}

	// Start background goroutine to handle subsequent events
	go c.handleAuthEvents(qrChan)

	return c.qrFilePath, nil
}

// writeQRToFile writes ASCII QR code to the temp file
func (c *Client) writeQRToFile(code string) error {
	f, err := os.Create(c.qrFilePath)
	if err != nil {
		return fmt.Errorf("create QR file: %w", err)
	}
	defer f.Close()

	f.WriteString("WhatsApp QR Code - Scan with your phone\n")
	f.WriteString("========================================\n\n")

	qrterminal.GenerateWithConfig(code, qrterminal.Config{
		Level:     qrterminal.L,
		Writer:    f,
		BlackChar: qrterminal.BLACK,
		WhiteChar: qrterminal.WHITE,
		QuietZone: 1,
	})

	f.WriteString("\n(QR code refreshes automatically if it expires)\n")
	return nil
}

// handleAuthEvents processes QR channel events in background
func (c *Client) handleAuthEvents(qrChan <-chan whatsmeow.QRChannelItem) {
	for evt := range qrChan {
		switch evt.Event {
		case "code":
			// QR code refreshed - update file
			c.mu.Lock()
			c.writeQRToFile(evt.Code)
			c.mu.Unlock()
		case "success":
			c.mu.Lock()
			c.authInProgress = false
			// Clean up QR file
			os.Remove(c.qrFilePath)
			c.mu.Unlock()
			return
		case "timeout":
			c.mu.Lock()
			c.authInProgress = false
			// Write timeout message to file
			os.WriteFile(c.qrFilePath, []byte("QR code expired. Call check_auth again to get a new one.\n"), 0644)
			c.mu.Unlock()
			return
		}
	}
}

// IsAuthInProgress returns whether background auth is in progress
func (c *Client) IsAuthInProgress() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.authInProgress
}

// GetQRFilePath returns the path to the QR code file
func (c *Client) GetQRFilePath() string {
	return QRFilePath
}

// PairWithPhone generates a pairing code for phone number authentication
// This is an alternative to QR code scanning
func (c *Client) PairWithPhone(ctx context.Context, phoneNumber string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clean up any existing auth state
	if c.authInProgress {
		c.authInProgress = false
	}

	// Create new device
	device := c.container.NewDevice()
	c.client = whatsmeow.NewClient(device, newFileLogger("client"))

	// Use background context so pairing continues after MCP request returns
	// The MCP request context would cancel when the tool call completes
	bgCtx := context.Background()

	// Get QR channel (required to establish connection properly)
	qrChan, err := c.client.GetQRChannel(bgCtx)
	if err != nil {
		return "", fmt.Errorf("get QR channel: %w", err)
	}

	// Connect to WhatsApp
	if err := c.client.Connect(); err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}

	// Wait for first QR event to ensure connection is established
	select {
	case evt := <-qrChan:
		if evt.Event != "code" {
			return "", fmt.Errorf("unexpected event: %s", evt.Event)
		}
		// QR code received, connection is ready
	case <-ctx.Done():
		return "", ctx.Err()
	}

	// Generate pairing code
	// Using Chrome as client type with standard display name
	code, err := c.client.PairPhone(bgCtx, phoneNumber, true, whatsmeow.PairClientChrome, "Chrome (Linux)")
	if err != nil {
		return "", fmt.Errorf("pair phone: %w", err)
	}

	// Start background handler to wait for pairing completion
	c.authInProgress = true
	go c.handlePairingEvents(qrChan)

	return code, nil
}

// handlePairingEvents waits for pairing to complete
func (c *Client) handlePairingEvents(qrChan <-chan whatsmeow.QRChannelItem) {
	for evt := range qrChan {
		switch evt.Event {
		case "code":
			// Ignore QR code refreshes during phone pairing
			continue
		case "success":
			c.mu.Lock()
			c.authInProgress = false
			c.mu.Unlock()
			return
		case "timeout":
			c.mu.Lock()
			c.authInProgress = false
			c.mu.Unlock()
			return
		}
	}
}

// Disconnect closes the WhatsApp connection
func (c *Client) Disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		c.client.Disconnect()
	}
}

// Close cleans up all resources
func (c *Client) Close() error {
	c.Disconnect()

	if c.msgStore != nil {
		c.msgStore.Close()
	}

	if c.lock != nil {
		c.lock.Close()
	}

	return nil
}

// GetMessageStore returns the message store for MCP tools
func (c *Client) GetMessageStore() *MessageStore {
	return c.msgStore
}

// GetWhatsmeowClient returns the underlying whatsmeow client for media download
func (c *Client) GetWhatsmeowClient() *whatsmeow.Client {
	return c.client
}

// SyncContactsFromWhatsmeow copies contacts from whatsmeow's store to our message store
func (c *Client) SyncContactsFromWhatsmeow(ctx context.Context) (int, error) {
	if c.client == nil || c.client.Store == nil {
		return 0, fmt.Errorf("client not connected")
	}
	if c.msgStore == nil {
		return 0, fmt.Errorf("message store not initialized")
	}

	contacts, err := c.client.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		return 0, fmt.Errorf("get contacts: %w", err)
	}

	var synced int
	for jid, info := range contacts {
		// Skip LID JIDs (internal WhatsApp IDs)
		if jid.Server == "lid" {
			continue
		}

		contact := &Contact{
			JID:       jid.String(),
			FullName:  info.FullName,
			FirstName: info.FirstName,
			PushName:  info.PushName,
		}
		if info.BusinessName != "" {
			contact.IsBusiness = true
			if contact.FullName == "" {
				contact.FullName = info.BusinessName
			}
		}

		// Extract phone number from JID
		if jid.Server == "s.whatsapp.net" && jid.User != "" {
			contact.PhoneNumber = "+" + jid.User
		}

		if err := c.msgStore.StoreContact(ctx, contact); err == nil {
			synced++
		}
	}

	return synced, nil
}

// ResolveToPhoneJID converts a LID JID to its phone-based JID if possible.
// Returns the original JID if no mapping exists or if it's already phone-based.
func (c *Client) ResolveToPhoneJID(ctx context.Context, jid string) string {
	if c.client == nil || c.client.Store == nil || c.client.Store.LIDs == nil {
		return jid
	}

	// Parse the JID
	parsed, err := types.ParseJID(jid)
	if err != nil {
		return jid
	}

	// Only resolve LID JIDs
	if parsed.Server != "lid" {
		return jid // Already phone-based or other type
	}

	// Try to get phone JID from LID store
	phoneJID, err := c.client.Store.LIDs.GetPNForLID(ctx, parsed)
	if err != nil || phoneJID.IsEmpty() {
		return jid // No mapping found
	}

	return phoneJID.String()
}

// GetAllLIDMappings returns all known LID→phone mappings for a set of JIDs.
// Returns a map from LID JID string to phone JID string.
func (c *Client) GetAllLIDMappings(ctx context.Context, jids []string) map[string]string {
	result := make(map[string]string)

	if c.client == nil || c.client.Store == nil || c.client.Store.LIDs == nil {
		return result
	}

	for _, jid := range jids {
		parsed, err := types.ParseJID(jid)
		if err != nil || parsed.Server != "lid" {
			continue
		}

		phoneJID, err := c.client.Store.LIDs.GetPNForLID(ctx, parsed)
		if err == nil && !phoneJID.IsEmpty() {
			result[jid] = phoneJID.String()
		}
	}

	return result
}

// GetAllJIDVariants returns all JID variants for a chat (phone JID + any LIDs that map to it)
// This handles legacy data where messages may be stored under LID JIDs
func (c *Client) GetAllJIDVariants(ctx context.Context, phoneJID string) []string {
	variants := []string{phoneJID}

	if c.client == nil || c.client.Store == nil || c.client.Store.LIDs == nil || c.msgStore == nil {
		return variants
	}

	// Get all distinct JIDs from messages and chats to find LID aliases
	// (chats table may be missing LID entries due to earlier bug)
	allJIDs, err := c.msgStore.GetDistinctChatJIDs(ctx)
	if err != nil {
		return variants
	}

	// Check each JID - if it's a LID that resolves to our phone JID, include it
	for _, jid := range allJIDs {
		parsed, err := types.ParseJID(jid)
		if err != nil || parsed.Server != "lid" {
			continue
		}

		// Try to resolve this LID to a phone JID
		resolvedPhone, err := c.client.Store.LIDs.GetPNForLID(ctx, parsed)
		if err != nil || resolvedPhone.IsEmpty() {
			continue
		}

		// If this LID resolves to our target phone JID, include it
		if resolvedPhone.String() == phoneJID {
			variants = append(variants, jid)
		}
	}

	return variants
}

// backfillLIDCanonicalJIDs resolves all LID canonical_jids in the message store
// to phone JIDs using whatsmeow's LID store. This fixes data stored before
// the LID→phone mapping was available (e.g., due to a race condition at startup
// or when a new contact's LID is seen before the contact event arrives).
func (c *Client) backfillLIDCanonicalJIDs() {
	ctx := context.Background()
	logger := newFileLogger("backfill")

	if c.client == nil || c.client.Store == nil || c.client.Store.LIDs == nil || c.msgStore == nil {
		return
	}

	lidJIDs, err := c.msgStore.GetLIDCanonicalJIDs(ctx)
	if err != nil {
		logger.Infof("Error getting LID canonical JIDs: %v", err)
		return
	}

	if len(lidJIDs) == 0 {
		return
	}

	logger.Infof("Found %d LID canonical_jids to backfill", len(lidJIDs))

	var totalFixed int
	for _, lidJID := range lidJIDs {
		parsed, err := types.ParseJID(lidJID)
		if err != nil {
			continue
		}

		phoneJID, err := c.client.Store.LIDs.GetPNForLID(ctx, parsed)
		if err != nil || phoneJID.IsEmpty() {
			logger.Infof("No phone mapping for LID %s (will retry on next message)", lidJID)
			continue
		}

		n, err := c.msgStore.BackfillCanonicalJID(ctx, lidJID, phoneJID.String())
		if err != nil {
			logger.Infof("Error backfilling %s -> %s: %v", lidJID, phoneJID, err)
			continue
		}
		if n > 0 {
			totalFixed += n
			logger.Infof("Backfilled %d rows: %s -> %s", n, lidJID, phoneJID)
		}
	}

	if totalFixed > 0 {
		logger.Infof("LID backfill complete: %d total rows fixed", totalFixed)
	}
}

// SendMessage sends a text message to a chat
// If replyTo is provided, the message is sent as a reply to that message
func (c *Client) SendMessage(ctx context.Context, chatJID string, text string, replyTo string) (*SendResult, error) {
	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	// Parse target JID
	targetJID, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat JID: %w", err)
	}

	// Build the message
	var msg *waE2E.Message

	if replyTo != "" {
		// Reply to existing message - need to fetch the original message for context
		var quotedMsg *Message
		if c.msgStore != nil {
			quotedMsg, _ = c.msgStore.GetMessage(ctx, replyTo)
		}

		contextInfo := &waE2E.ContextInfo{
			StanzaID: proto.String(replyTo),
		}

		// Set participant (sender of quoted message) for proper reply display
		if quotedMsg != nil {
			contextInfo.Participant = proto.String(quotedMsg.SenderJID)
			// Include quoted message content if we have it
			if quotedMsg.Content != "" {
				contextInfo.QuotedMessage = &waE2E.Message{
					Conversation: proto.String(quotedMsg.Content),
				}
			}
		}

		msg = &waE2E.Message{
			ExtendedTextMessage: &waE2E.ExtendedTextMessage{
				Text:        proto.String(text),
				ContextInfo: contextInfo,
			},
		}
	} else {
		// Simple text message
		msg = &waE2E.Message{
			Conversation: proto.String(text),
		}
	}

	// Send the message
	resp, err := c.client.SendMessage(ctx, targetJID, msg)
	if err != nil {
		return nil, fmt.Errorf("send failed: %w", err)
	}

	// Store the sent message in our message store
	if c.msgStore != nil {
		// Resolve LID JID to phone JID for canonical storage
		canonicalJID := c.ResolveToPhoneJID(ctx, chatJID)

		// Serialize protobuf for raw_proto storage
		var rawProto string
		if jsonBytes, err := protojson.Marshal(msg); err == nil {
			rawProto = string(jsonBytes)
		}

		sentMsg := &Message{
			ID:           resp.ID,
			ChatJID:      chatJID,
			CanonicalJID: canonicalJID,
			SenderJID:    "me",
			Content:      text,
			Timestamp:    resp.Timestamp,
			IsFromMe:     true,
			Type:         "text",
			RawProto:     rawProto,
		}
		c.msgStore.StoreMessage(ctx, sentMsg)
	}

	return &SendResult{
		ID:        resp.ID,
		Timestamp: resp.Timestamp,
	}, nil
}

// SendMediaMessage sends a media message (image, video, audio, or document) to a chat
// mimeType determines the media type. caption is optional text shown with the media.
// If replyTo is provided, the message is sent as a reply.
func (c *Client) SendMediaMessage(ctx context.Context, chatJID string, data []byte, mimeType string, fileName string, caption string, replyTo string) (*SendResult, error) {
	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	// Parse target JID
	targetJID, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("invalid chat JID: %w", err)
	}

	// Determine whatsmeow media type from MIME
	var mediaType whatsmeow.MediaType
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		mediaType = whatsmeow.MediaImage
	case strings.HasPrefix(mimeType, "video/"):
		mediaType = whatsmeow.MediaVideo
	case strings.HasPrefix(mimeType, "audio/"):
		mediaType = whatsmeow.MediaAudio
	default:
		mediaType = whatsmeow.MediaDocument
	}

	// Upload media to WhatsApp servers
	uploadResp, err := c.client.Upload(ctx, data, mediaType)
	if err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}

	// Build context info for replies
	var contextInfo *waE2E.ContextInfo
	if replyTo != "" {
		contextInfo = &waE2E.ContextInfo{
			StanzaID: proto.String(replyTo),
		}
		if c.msgStore != nil {
			if quotedMsg, _ := c.msgStore.GetMessage(ctx, replyTo); quotedMsg != nil {
				contextInfo.Participant = proto.String(quotedMsg.SenderJID)
				if quotedMsg.Content != "" {
					contextInfo.QuotedMessage = &waE2E.Message{
						Conversation: proto.String(quotedMsg.Content),
					}
				}
			}
		}
	}

	// Build media-specific protobuf message
	var msg *waE2E.Message
	switch mediaType {
	case whatsmeow.MediaImage:
		imgMsg := &waE2E.ImageMessage{
			URL:           proto.String(uploadResp.URL),
			DirectPath:    proto.String(uploadResp.DirectPath),
			MediaKey:      uploadResp.MediaKey,
			FileEncSHA256: uploadResp.FileEncSHA256,
			FileSHA256:    uploadResp.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(data))),
			Mimetype:      proto.String(mimeType),
		}
		if caption != "" {
			imgMsg.Caption = proto.String(caption)
		}
		if contextInfo != nil {
			imgMsg.ContextInfo = contextInfo
		}
		msg = &waE2E.Message{ImageMessage: imgMsg}

	case whatsmeow.MediaVideo:
		vidMsg := &waE2E.VideoMessage{
			URL:           proto.String(uploadResp.URL),
			DirectPath:    proto.String(uploadResp.DirectPath),
			MediaKey:      uploadResp.MediaKey,
			FileEncSHA256: uploadResp.FileEncSHA256,
			FileSHA256:    uploadResp.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(data))),
			Mimetype:      proto.String(mimeType),
		}
		if caption != "" {
			vidMsg.Caption = proto.String(caption)
		}
		if contextInfo != nil {
			vidMsg.ContextInfo = contextInfo
		}
		msg = &waE2E.Message{VideoMessage: vidMsg}

	case whatsmeow.MediaAudio:
		audioMsg := &waE2E.AudioMessage{
			URL:           proto.String(uploadResp.URL),
			DirectPath:    proto.String(uploadResp.DirectPath),
			MediaKey:      uploadResp.MediaKey,
			FileEncSHA256: uploadResp.FileEncSHA256,
			FileSHA256:    uploadResp.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(data))),
			Mimetype:      proto.String(mimeType),
		}
		if contextInfo != nil {
			audioMsg.ContextInfo = contextInfo
		}
		msg = &waE2E.Message{AudioMessage: audioMsg}

	default: // Document
		docMsg := &waE2E.DocumentMessage{
			URL:           proto.String(uploadResp.URL),
			DirectPath:    proto.String(uploadResp.DirectPath),
			MediaKey:      uploadResp.MediaKey,
			FileEncSHA256: uploadResp.FileEncSHA256,
			FileSHA256:    uploadResp.FileSHA256,
			FileLength:    proto.Uint64(uint64(len(data))),
			Mimetype:      proto.String(mimeType),
		}
		if fileName != "" {
			docMsg.FileName = proto.String(fileName)
		}
		if caption != "" {
			docMsg.Title = proto.String(caption)
		}
		if contextInfo != nil {
			docMsg.ContextInfo = contextInfo
		}
		msg = &waE2E.Message{DocumentMessage: docMsg}
	}

	// Send the message
	resp, err := c.client.SendMessage(ctx, targetJID, msg)
	if err != nil {
		return nil, fmt.Errorf("send failed: %w", err)
	}

	// Store the sent message and attachment in our message store
	if c.msgStore != nil {
		canonicalJID := c.ResolveToPhoneJID(ctx, chatJID)
		msgType := "document"
		switch mediaType {
		case whatsmeow.MediaImage:
			msgType = "image"
		case whatsmeow.MediaVideo:
			msgType = "video"
		case whatsmeow.MediaAudio:
			msgType = "audio"
		}
		content := caption
		if content == "" {
			content = fmt.Sprintf("[%s: %s]", msgType, fileName)
		}

		// Serialize protobuf for raw_proto storage
		var rawProto string
		if jsonBytes, err := protojson.Marshal(msg); err == nil {
			rawProto = string(jsonBytes)
		}

		sentMsg := &Message{
			ID:           resp.ID,
			ChatJID:      chatJID,
			CanonicalJID: canonicalJID,
			SenderJID:    "me",
			Content:      content,
			Timestamp:    resp.Timestamp,
			IsFromMe:     true,
			Type:         msgType,
			RawProto:     rawProto,
		}
		c.msgStore.StoreMessage(ctx, sentMsg)

		// Store attachment metadata so list/download work for sent media
		att := &Attachment{
			MessageID:     resp.ID,
			ChatJID:       chatJID,
			CanonicalJID:  canonicalJID,
			SenderJID:     "me",
			Timestamp:     resp.Timestamp,
			MediaType:     msgType,
			MimeType:      mimeType,
			Filename:      fileName,
			Caption:       caption,
			FileSize:      int64(len(data)),
			MediaKey:      uploadResp.MediaKey,
			FileSHA256:    uploadResp.FileSHA256,
			FileEncSHA256: uploadResp.FileEncSHA256,
			DirectPath:    uploadResp.DirectPath,
		}
		c.msgStore.StoreAttachment(ctx, att)
	}

	return &SendResult{
		ID:        resp.ID,
		Timestamp: resp.Timestamp,
	}, nil
}

// SendResult contains the result of sending a message
type SendResult struct {
	ID        string
	Timestamp time.Time
}

// MarkRead sends read receipts for recent messages in a chat
// This sends blue checkmarks to senders and clears unread status
func (c *Client) MarkRead(ctx context.Context, chatJID string) (int, error) {
	if c.client == nil {
		return 0, fmt.Errorf("client not connected")
	}

	// Parse chat JID
	targetJID, err := types.ParseJID(chatJID)
	if err != nil {
		return 0, fmt.Errorf("invalid chat JID: %w", err)
	}

	// Get recent messages to find what needs to be marked as read
	if c.msgStore == nil {
		return 0, fmt.Errorf("message store not initialized")
	}

	// Resolve LID JID to phone JID, then get all JID variants (handles legacy data)
	canonicalJID := c.ResolveToPhoneJID(ctx, chatJID)
	jidVariants := c.GetAllJIDVariants(ctx, canonicalJID)

	// Query messages by all JID variants (catches legacy data under LID JIDs)
	messages, err := c.msgStore.GetMessagesByJIDs(ctx, jidVariants, 50)
	if err != nil {
		return 0, fmt.Errorf("get messages: %w", err)
	}

	// Collect incoming message IDs (not from me)
	var toMarkRead []types.MessageID
	var latestSender types.JID
	for _, msg := range messages {
		if !msg.IsFromMe {
			toMarkRead = append(toMarkRead, types.MessageID(msg.ID))
			// Track most recent sender for read receipt
			if latestSender.IsEmpty() && msg.SenderJID != "" {
				latestSender, _ = types.ParseJID(msg.SenderJID)
			}
		}
	}

	if len(toMarkRead) == 0 {
		return 0, nil // Nothing to mark as read
	}

	// For 1:1 chats, sender is the chat JID
	// For groups, use the actual sender of the messages
	sender := targetJID
	if !latestSender.IsEmpty() && latestSender.Server != "" {
		sender = latestSender
	}

	now := time.Now()
	if err := c.client.MarkRead(ctx, toMarkRead, now, targetJID, sender); err != nil {
		return 0, fmt.Errorf("mark read: %w", err)
	}

	// Update local last_read_at for all JID variants to track unread count
	for _, jid := range jidVariants {
		c.msgStore.SetChatLastReadAt(ctx, jid, now)
		// Ignore errors - read receipts were sent successfully
	}

	return len(toMarkRead), nil
}

// Dismiss marks a chat as handled for triage purposes
// This does NOT send read receipts — it only updates local triage state
func (c *Client) Dismiss(ctx context.Context, chatJID string, notes string) (int64, error) {
	if c.msgStore == nil {
		return 0, fmt.Errorf("message store not initialized")
	}

	canonicalJID := c.ResolveToPhoneJID(ctx, chatJID)
	return c.msgStore.DismissChat(ctx, canonicalJID, notes)
}

// Undismiss clears dismiss state so a chat reappears in pending triage
func (c *Client) Undismiss(ctx context.Context, chatJID string) error {
	if c.msgStore == nil {
		return fmt.Errorf("message store not initialized")
	}

	canonicalJID := c.ResolveToPhoneJID(ctx, chatJID)
	return c.msgStore.UndismissChat(ctx, canonicalJID)
}

// GroupInfoResult contains group metadata and participants
type GroupInfoResult struct {
	JID              string            `json:"jid"`
	Name             string            `json:"name"`
	Topic            string            `json:"topic"`
	Created          time.Time         `json:"created"`
	ParticipantCount int               `json:"participant_count"`
	Participants     []ParticipantInfo `json:"participants"`
	Found            bool              `json:"found"`
}

// ParticipantInfo contains info about a single group participant
type ParticipantInfo struct {
	JID          string `json:"jid"`
	PhoneNumber  string `json:"phone_number,omitempty"`
	Name         string `json:"name,omitempty"`
	IsAdmin      bool   `json:"is_admin"`
	IsSuperAdmin bool   `json:"is_super_admin"`
}

// GetGroupInfo fetches live group metadata and participants from WhatsApp servers
func (c *Client) GetGroupInfo(ctx context.Context, jid string) (*GroupInfoResult, error) {
	if c.client == nil {
		return nil, fmt.Errorf("client not connected")
	}

	// Parse and validate JID
	parsedJID, err := types.ParseJID(jid)
	if err != nil {
		return nil, fmt.Errorf("invalid JID: %w", err)
	}

	if parsedJID.Server != types.GroupServer {
		return nil, fmt.Errorf("not a group JID (must end with @g.us): %s", jid)
	}

	// Fetch live group info from WhatsApp servers
	groupInfo, err := c.client.GetGroupInfo(ctx, parsedJID)
	if err != nil {
		return nil, fmt.Errorf("get group info: %w", err)
	}

	// Build participant list with resolved names
	participants := make([]ParticipantInfo, 0, len(groupInfo.Participants))
	for _, p := range groupInfo.Participants {
		participantJID := p.JID.String()

		// Resolve LID JID to phone JID if possible
		phoneJID := c.ResolveToPhoneJID(ctx, participantJID)

		info := ParticipantInfo{
			JID:          phoneJID,
			IsAdmin:      p.IsAdmin,
			IsSuperAdmin: p.IsSuperAdmin,
		}

		// Extract phone number from resolved JID
		parsedPhone, err := types.ParseJID(phoneJID)
		if err == nil && parsedPhone.Server == "s.whatsapp.net" && parsedPhone.User != "" {
			info.PhoneNumber = "+" + parsedPhone.User
		}

		// Resolve name from contacts table (try both original and resolved JID)
		if c.msgStore != nil {
			info.Name = c.msgStore.GetContactName(ctx, phoneJID)
			if info.Name == "" && phoneJID != participantJID {
				info.Name = c.msgStore.GetContactName(ctx, participantJID)
			}
		}

		// Fall back to display name from group metadata
		if info.Name == "" && p.DisplayName != "" {
			info.Name = p.DisplayName
		}

		participants = append(participants, info)
	}

	return &GroupInfoResult{
		JID:              groupInfo.JID.String(),
		Name:             groupInfo.GroupName.Name,
		Topic:            groupInfo.GroupTopic.Topic,
		Created:          groupInfo.GroupCreated,
		ParticipantCount: len(participants),
		Participants:     participants,
		Found:            true,
	}, nil
}

func init() {
	// Disable zerolog output (whatsmeow uses it internally)
	zerolog.SetGlobalLevel(zerolog.Disabled)
}
