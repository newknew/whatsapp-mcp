// WhatsApp MCP Server
// A Model Context Protocol server for WhatsApp messaging via whatsmeow.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"log"
	"math/rand"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "image/gif"
	_ "image/png"

	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/tiff"
	_ "golang.org/x/image/webp"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protojson"
)

const version = "0.1.0"

// =============================================================================
// Schema-free tool registration
// =============================================================================
//
// The MCP Go SDK's generic mcp.AddTool validates incoming arguments against an
// auto-generated JSON schema. Unfortunately, Claude Code sends ALL tool
// parameters as JSON strings (e.g. "2" instead of 2, "true" instead of true).
// The SDK's strict validation rejects these type mismatches.
//
// Workaround: for tools with int/bool parameters, we use server.AddTool (the
// raw handler form) with flexTool. This bypasses schema validation: incoming
// arguments are normalized (int/bool → string) before unmarshaling into the
// typed Go struct, so both native and string-encoded values work.
//
// Tools with only string parameters still use the generic mcp.AddTool.

// normalizeArgs converts all JSON values to strings so they match string-typed
// struct fields. This lets us accept {"limit": 2}, {"limit": "2"}, and
// {"pending_only": true} / {"pending_only": "true"} uniformly.
func normalizeArgs(raw json.RawMessage) json.RawMessage {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return raw
	}
	for k, v := range m {
		switch v := v.(type) {
		case float64:
			m[k] = strconv.FormatFloat(v, 'f', -1, 64)
		case bool:
			m[k] = strconv.FormatBool(v)
		}
	}
	b, _ := json.Marshal(m)
	return b
}

// flexTool wraps a typed handler to bypass the SDK's schema validation.
// It normalizes arguments, unmarshals into the typed input struct, calls the
// handler, and packs the output into a CallToolResult.
func flexTool[In, Out any](fn func(context.Context, *mcp.CallToolRequest, In) (*mcp.CallToolResult, Out, error)) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		normalized := normalizeArgs(req.Params.Arguments)
		var input In
		if err := json.Unmarshal(normalized, &input); err != nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("invalid arguments: %v", err)}},
				IsError: true,
			}, nil
		}
		result, output, err := fn(ctx, req, input)
		if err != nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{&mcp.TextContent{Text: err.Error()}},
				IsError: true,
			}, nil
		}
		if result != nil {
			return result, nil
		}
		data, _ := json.Marshal(output)
		return &mcp.CallToolResult{
			Content:           []mcp.Content{&mcp.TextContent{Text: string(data)}},
			StructuredContent: output,
		}, nil
	}
}

// parseIntParam parses a string parameter as an integer with a default value.
func parseIntParam(s string, defaultVal int) int {
	if s == "" {
		return defaultVal
	}
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		return n
	}
	return defaultVal
}

// parseBoolParam parses a string parameter as a boolean (default false).
func parseBoolParam(s string) bool {
	switch strings.ToLower(s) {
	case "true", "1":
		return true
	default:
		return false
	}
}

// =============================================================================
// File Helpers
// =============================================================================

var sanitizeUnsafe = regexp.MustCompile(`[^A-Za-z0-9._-]`)
var sanitizeMultiUnderscore = regexp.MustCompile(`_+`)

func sanitizeFilename(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = regexp.MustCompile(`\s+`).ReplaceAllString(name, "_")
	name = sanitizeUnsafe.ReplaceAllString(name, "_")
	name = sanitizeMultiUnderscore.ReplaceAllString(name, "_")
	return strings.Trim(name, "_.")
}

func tmpPath(service, originalName string) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	slug := make([]byte, 4)
	for i := range slug {
		slug[i] = chars[rand.Intn(len(chars))]
	}
	sanitized := sanitizeFilename(originalName)
	return fmt.Sprintf("/tmp/newknew_%s_%s_%s", service, string(slug), sanitized)
}

// convertToJPEG converts any decodable image to JPEG bytes.
// Tries Go's native image decoders first (PNG, GIF, BMP, TIFF, WebP),
// then falls back to Python/Pillow for formats like HEIC.
func convertToJPEG(data []byte) ([]byte, error) {
	// Try native Go decode first
	img, _, err := image.Decode(bytes.NewReader(data))
	if err == nil {
		var buf bytes.Buffer
		if encErr := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90}); encErr != nil {
			return nil, encErr
		}
		return buf.Bytes(), nil
	}

	// Fallback: use Python/Pillow (handles HEIC via pillow_heif)
	return convertToJPEGViaPillow(data)
}

// convertToJPEGViaPillow shells out to scripts/vm/heic-to-jpeg.py.
func convertToJPEGViaPillow(data []byte) ([]byte, error) {
	// Write input to temp file
	inFile, err := os.CreateTemp("", "convert_in_*")
	if err != nil {
		return nil, fmt.Errorf("create temp input: %w", err)
	}
	defer os.Remove(inFile.Name())

	if _, err := inFile.Write(data); err != nil {
		inFile.Close()
		return nil, fmt.Errorf("write temp input: %w", err)
	}
	inFile.Close()

	outPath := inFile.Name() + ".jpg"
	defer os.Remove(outPath)

	// Find repo root (two levels up from go/whatsapp-mcp/)
	execPath, _ := os.Executable()
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(execPath)))
	scriptPath := filepath.Join(repoRoot, "scripts", "vm", "heic-to-jpeg.py")

	cmd := exec.Command("uv", "run", "python3", scriptPath,
		inFile.Name(), outPath)
	cmd.Dir = repoRoot

	if output, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("heic-to-jpeg failed: %s: %w", string(output), err)
	}

	return os.ReadFile(outPath)
}

// handleAuthEndpoint handles the /auth HTTP endpoint for authentication
// Query params:
//   - force=true: Clear existing session and re-authenticate
func handleAuthEndpoint(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	client, err := GetClient()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "failed to get client: %s"}`, err)
		return
	}

	// Check for force parameter
	force := r.URL.Query().Get("force") == "true"

	if force {
		log.Println("Force auth requested, clearing session...")
		if err := client.ClearSession(ctx); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"error": "failed to clear session: %s"}`, err)
			return
		}
	}

	// Check if already authenticated (and not forcing)
	if !force && client.IsAuthenticated(ctx) {
		if err := client.Connect(ctx); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"error": "connection failed: %s"}`, err)
			return
		}
		phone := client.GetPhoneNumber(ctx)
		fmt.Fprintf(w, `{"authenticated": true, "phone_number": "%s"}`, phone)
		return
	}

	// Start background auth - returns QR file path
	qrPath, err := client.StartBackgroundAuth(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "auth failed: %s"}`, err)
		return
	}

	fmt.Fprintf(w, `{"authenticated": false, "qr_file": "%s", "message": "Scan QR code with WhatsApp (Settings > Linked Devices > Link a Device)"}`, qrPath)
}

// handleHealthEndpoint returns server health and status
func handleHealthEndpoint(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	client, err := GetClient()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"status": "error", "error": "%s"}`, err)
		return
	}

	ctx := r.Context()
	authenticated := client.IsAuthenticated(ctx)
	phone := ""
	if authenticated {
		phone = client.GetPhoneNumber(ctx)
	}

	store := client.GetMessageStore()
	msgCount := 0
	chatCount := 0
	if store != nil {
		msgCount, _ = store.GetMessageCount(ctx)
		chats, _ := store.GetChats(ctx, 1000)
		chatCount = len(chats)
	}

	fmt.Fprintf(w, `{"status": "ok", "authenticated": %t, "phone": "%s", "messages": %d, "chats": %d}`,
		authenticated, phone, msgCount, chatCount)
}

// handleReparseEndpoint re-parses messages using stored raw_proto
// Query params:
//   - type: only reparse messages of this type (e.g., "other")
//   - limit: max messages to reparse (default 1000)
func handleReparseEndpoint(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	client, err := GetClient()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "%s"}`, err)
		return
	}

	store := client.GetMessageStore()
	if store == nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "message store not initialized"}`)
		return
	}

	// Check mode: "attachments" backfills missing attachment records
	mode := r.URL.Query().Get("mode")
	if mode == "attachments" {
		backfilled, err := store.ReparseAttachments(ctx)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"error": "%s"}`, err)
			return
		}
		fmt.Fprintf(w, `{"backfilled_attachments": %d}`, backfilled)
		return
	}

	// Default: reparse message content/type from raw_proto
	typeFilter := r.URL.Query().Get("type")
	if typeFilter == "" {
		typeFilter = "other" // Default to reparsing "other" type
	}

	updated, err := store.ReparseMessages(ctx, typeFilter)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "%s"}`, err)
		return
	}

	fmt.Fprintf(w, `{"reparsed": %d, "type_filter": "%s"}`, updated, typeFilter)
}

// runServeCommand runs the HTTP server for MCP connections
func runServeCommand() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime | log.Lshortfile)

	// Auto-connect to WhatsApp on startup if authenticated
	go func() {
		ctx := context.Background()
		client, err := GetClient()
		if err != nil {
			log.Printf("Failed to get client: %v", err)
			return
		}
		if client.IsAuthenticated(ctx) {
			log.Println("Auto-connecting to WhatsApp...")
			if err := client.Connect(ctx); err != nil {
				log.Printf("Auto-connect failed: %v", err)
			} else {
				log.Printf("Connected to WhatsApp as %s", client.GetPhoneNumber(ctx))
				// Sync contacts from whatsmeow's store to our message store
				if synced, err := client.SyncContactsFromWhatsmeow(ctx); err != nil {
					log.Printf("Contact sync failed: %v", err)
				} else {
					log.Printf("Synced %d contacts from WhatsApp", synced)
				}
			}
		} else {
			log.Println("Not authenticated - use /auth endpoint to authenticate")
		}
	}()

	// Create MCP server
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "whatsapp-mcp",
		Version: version,
	}, nil)

	// Register all tools
	registerTools(server)

	// Create HTTP handler for MCP (stateless mode for simpler client compatibility)
	mcpHandler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{
		Stateless: true,
	})

	// Set up HTTP routes
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpHandler)
	mux.HandleFunc("/health", handleHealthEndpoint)
	mux.HandleFunc("/admin/auth", handleAuthEndpoint)
	mux.HandleFunc("/admin/reparse", handleReparseEndpoint)

	addr := fmt.Sprintf("127.0.0.1:%d", TCPPort)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	log.Printf("WhatsApp MCP server listening on http://%s/mcp", addr)

	// Handle graceful shutdown via signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutdown signal received...")
		// Clean up WhatsApp client
		if client, err := GetClient(); err == nil {
			client.Close()
		}
		httpServer.Close()
	}()

	// Start HTTP server
	if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}

	log.Println("Server stopped")
}

// runSetupCommand pairs with WhatsApp via QR code, then exits.
func runSetupCommand() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime | log.Lshortfile)

	ctx := context.Background()

	client, err := GetClient()
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}

	if client.IsAuthenticated(ctx) {
		if err := client.Connect(ctx); err != nil {
			log.Fatalf("Connection failed: %v", err)
		}
		phone := client.GetPhoneNumber(ctx)
		fmt.Printf("\nAlready paired as %s\n", phone)
		fmt.Println("\nTo re-pair, delete the session directory and run setup again:")
		fmt.Printf("  rm -rf ~/.local/share/newknew-mcp/whatsapp/\n")
		fmt.Printf("  whatsapp-mcp setup\n")
		return
	}

	fmt.Println("WhatsApp MCP Setup")
	fmt.Println("==================")
	fmt.Println()
	fmt.Println("Scan this QR code with WhatsApp:")
	fmt.Println("  Phone > Settings > Linked Devices > Link a Device")
	fmt.Println()

	if err := client.RunInteractiveAuth(ctx); err != nil {
		log.Fatalf("Pairing failed: %v", err)
	}

	phone := client.GetPhoneNumber(ctx)
	fmt.Printf("\nPaired successfully as %s\n", phone)
	fmt.Println()
	fmt.Println("Start the server:")
	fmt.Println("  whatsapp-mcp serve")
}

// registerTools adds all MCP tools to the server
func registerTools(server *mcp.Server) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_ping",
		Description: "Check if WhatsApp MCP server is running",
	}, Ping)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_check_auth",
		Description: "Check WhatsApp authentication status. Returns QR code if not authenticated.",
	}, CheckAuth)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_pair_phone",
		Description: "Authenticate via phone number pairing (alternative to QR code). Returns a code to enter on your phone.",
	}, PairPhone)

	server.AddTool(&mcp.Tool{
		Name:        "whatsapp_list_chats",
		Description: "List WhatsApp chats ordered by most recent message. Returns chat JID, name, and message count. Use messages_per_chat (1-10) to include last N messages per chat. Use pending_only=true to only show chats with new messages since last dismiss.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"limit":{"description":"Max chats to return (default 50)"},"messages_per_chat":{"description":"Include last N messages per chat (0-10, default 0)"},"pending_only":{"description":"Only return chats with new messages since last dismiss (true/false)"}}}`),
	}, flexTool(ListChats))

	server.AddTool(&mcp.Tool{
		Name:        "whatsapp_get_messages",
		Description: "Get messages from a specific WhatsApp chat. Use chat_jid from list_chats. Supports pagination via before_time. Omit chat_jid for cross-chat query. Filter by since_time, search (text), type (text/image/video/etc).",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"chat_jid":{"description":"Chat JID from list_chats"},"limit":{"description":"Max messages (default 50)"},"before_time":{"description":"Pagination cursor (RFC3339)"},"since_time":{"description":"Only messages after this time (RFC3339)"},"search":{"description":"Text search filter"},"type":{"description":"Message type filter (text/image/video/etc)"}}}`),
	}, flexTool(GetMessages))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_sync_status",
		Description: "Get message sync status: total messages stored, chat count, and last sync time.",
	}, GetSyncStatus)

	server.AddTool(&mcp.Tool{
		Name:        "whatsapp_list_attachments",
		Description: "List WhatsApp attachments (images, videos, audio, documents). Filter by chat_jid or media_type.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"chat_jid":{"description":"Filter by chat JID"},"media_type":{"description":"Filter by type (image/video/audio/document)"},"limit":{"description":"Max results (default 50)"}}}`),
	}, flexTool(ListAttachments))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_download_attachment",
		Description: "Download a WhatsApp attachment by message_id. Returns the /tmp/ file path.",
	}, DownloadAttachment)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_get_contact",
		Description: "Get contact info by JID. Returns name, phone number, and other details.",
	}, GetContact)

	server.AddTool(&mcp.Tool{
		Name:        "whatsapp_search_contacts",
		Description: "Search contacts by name or phone number. Returns matching contacts.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"query":{"description":"Name or phone number to search"},"limit":{"description":"Max results (default 20)"}},"required":["query"]}`),
	}, flexTool(SearchContacts))

	server.AddTool(&mcp.Tool{
		Name:        "whatsapp_send",
		Description: "Send a WhatsApp message with optional media attachment. Use mode='new' for new messages (requires chat_jid), mode='reply' to reply (requires reply_to message_id). Use 'attachment' with a file path to send media. At least one of 'text' or 'attachment' required. Non-JPEG/PNG images are auto-converted to JPEG before sending. Text becomes caption when sent with media.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"mode":{"description":"'new' or 'reply'"},"chat_jid":{"description":"Required for mode=new"},"text":{"description":"Message content"},"reply_to":{"description":"Message ID for mode=reply"},"mark_read":{"description":"Auto-mark as read after send (default true)"},"attachment":{"description":"File path to send as media"}},"required":["mode"]}`),
	}, flexTool(SendMessage))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_mark_read",
		Description: "Mark messages in a chat as read. Sends read receipts (blue checkmarks) and clears unread count. Does NOT dismiss from triage — use whatsapp_dismiss for that.",
	}, MarkRead)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_dismiss",
		Description: "Dismiss a chat from triage (mark as handled). Does NOT send read receipts. Chat will reappear in pending_only if new messages arrive. Use whatsapp_undismiss to undo.",
	}, DismissChat)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_undismiss",
		Description: "Undismiss a chat, making it reappear in pending_only queries even without new messages. Undoes a previous whatsapp_dismiss.",
	}, UndismissChat)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "whatsapp_get_group_info",
		Description: "Get group metadata and participant list. Returns group name, topic, creation time, and all participants with phone numbers, names, and admin status. Requires a group JID (ending in @g.us).",
	}, GetGroupInfo)
}

// PingInput is empty - no parameters needed
type PingInput struct{}

// PingOutput returns server status
type PingOutput struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

// Ping is a simple health check tool
func Ping(ctx context.Context, req *mcp.CallToolRequest, input PingInput) (*mcp.CallToolResult, PingOutput, error) {
	return nil, PingOutput{
		Status:  "pong",
		Version: version,
	}, nil
}

// CheckAuthInput is empty - no parameters needed
type CheckAuthInput struct{}

// CheckAuthOutput returns authentication status
type CheckAuthOutput struct {
	Authenticated bool   `json:"authenticated"`
	PhoneNumber   string `json:"phone_number,omitempty"`
	QRFile        string `json:"qr_file,omitempty"`
	Message       string `json:"message,omitempty"`
}

// CheckAuth checks authentication status and starts background auth if needed
func CheckAuth(ctx context.Context, req *mcp.CallToolRequest, input CheckAuthInput) (*mcp.CallToolResult, CheckAuthOutput, error) {
	client, err := GetClient()
	if err != nil {
		return nil, CheckAuthOutput{
			Authenticated: false,
			Message:       err.Error(),
		}, nil
	}

	// Check if already authenticated
	if client.IsAuthenticated(ctx) {
		phone := client.GetPhoneNumber(ctx)
		// Try to connect
		if err := client.Connect(ctx); err != nil {
			return nil, CheckAuthOutput{
				Authenticated: false,
				Message:       "Session exists but connection failed: " + err.Error(),
			}, nil
		}
		return nil, CheckAuthOutput{
			Authenticated: true,
			PhoneNumber:   phone,
			Message:       "Connected to WhatsApp",
		}, nil
	}

	// Check if auth already in progress
	if client.IsAuthInProgress() {
		// Check if it completed
		if client.IsAuthenticated(ctx) {
			return nil, CheckAuthOutput{
				Authenticated: true,
				PhoneNumber:   client.GetPhoneNumber(ctx),
				Message:       "Authentication successful",
			}, nil
		}
		return nil, CheckAuthOutput{
			Authenticated: false,
			QRFile:        client.GetQRFilePath(),
			Message:       "Scan QR code, then call check_auth again. View QR: cat " + client.GetQRFilePath(),
		}, nil
	}

	// Not authenticated - start background auth
	qrFile, err := client.StartBackgroundAuth(ctx)
	if err != nil {
		return nil, CheckAuthOutput{
			Authenticated: false,
			Message:       "Failed to start auth: " + err.Error(),
		}, nil
	}

	return nil, CheckAuthOutput{
		Authenticated: false,
		QRFile:        qrFile,
		Message:       "QR code ready. View with: cat " + qrFile,
	}, nil
}

// ListChatsInput for listing chats
type ListChatsInput struct {
	Limit           string `json:"limit,omitempty"`
	MessagesPerChat string `json:"messages_per_chat,omitempty"` // 0 = no messages, 1-10 = include last N messages per chat
	PendingOnly     string `json:"pending_only,omitempty"`      // only return chats with new messages since last dismiss
}

// ListChatsOutput returns chats (with optional messages)
type ListChatsOutput struct {
	Chats []ChatWithMessages `json:"chats"`
	Count int                `json:"count"`
}

// ListChats returns recent chats with message counts and optional messages
// With canonical_jid normalization, chats are already properly grouped.
// mergeAliasedChats handles legacy duplicate entries if any exist.
func ListChats(ctx context.Context, req *mcp.CallToolRequest, input ListChatsInput) (*mcp.CallToolResult, ListChatsOutput, error) {
	client, err := GetClient()
	if err != nil {
		return nil, ListChatsOutput{}, fmt.Errorf("get client: %w", err)
	}

	store := client.GetMessageStore()
	if store == nil {
		return nil, ListChatsOutput{}, fmt.Errorf("message store not initialized")
	}

	limit := 50
	if input.Limit != "" {
		if n, err := strconv.Atoi(input.Limit); err == nil && n > 0 {
			limit = n
		}
	}

	// Use GetChatsWithMessages which returns canonical_jid as JID
	chats, err := store.GetChatsWithMessages(ctx, limit, parseIntParam(input.MessagesPerChat, 0))
	if err != nil {
		return nil, ListChatsOutput{}, fmt.Errorf("get chats: %w", err)
	}

	// Merge any duplicate chat entries (handles legacy data)
	chats = mergeAliasedChats(ctx, client, chats)

	// Filter to pending only if requested
	// Uses dismiss-based semantics: a chat is pending if last_message_time > user_handled_at
	// This is independent of read receipts (last_read_at / unread_count)
	if parseBoolParam(input.PendingOnly) {
		pending := make([]ChatWithMessages, 0)
		for _, chat := range chats {
			if chat.UserHandledAt == nil || chat.LastMessageTime.Unix() > *chat.UserHandledAt {
				pending = append(pending, chat)
			}
		}
		chats = pending
	}

	// Ensure non-nil slice for JSON serialization
	if chats == nil {
		chats = []ChatWithMessages{}
	}

	return nil, ListChatsOutput{
		Chats: chats,
		Count: len(chats),
	}, nil
}

// mergeAliasedChats combines chat threads that belong to the same contact
// (e.g., LID and phone number JIDs for the same person)
// NOTE: With canonical_jid normalization in store.go, NEW messages should already
// be properly grouped. This function handles LEGACY data where canonical_jid
// was backfilled to the original JID (not normalized).
func mergeAliasedChats(ctx context.Context, client *Client, chats []ChatWithMessages) []ChatWithMessages {
	if len(chats) == 0 {
		return chats
	}

	// Collect all JIDs to resolve LIDs for legacy data
	jids := make([]string, len(chats))
	for i, chat := range chats {
		jids[i] = chat.JID
	}

	// Get LID→phone mappings for any LID JIDs (handles legacy data)
	lidMappings := client.GetAllLIDMappings(ctx, jids)

	// Group chats by their canonical JID (resolved phone JID if available)
	groups := make(map[string][]ChatWithMessages)
	order := []string{} // Preserve order

	for _, chat := range chats {
		// Try to resolve LID to phone JID for legacy data
		canonical := chat.JID
		if phoneJID, ok := lidMappings[chat.JID]; ok {
			canonical = phoneJID
		}

		if _, exists := groups[canonical]; !exists {
			order = append(order, canonical)
		}
		groups[canonical] = append(groups[canonical], chat)
	}

	// If no duplicates found, return as-is (fast path)
	if len(groups) == len(chats) {
		return chats
	}

	// Merge each group into a single chat
	result := make([]ChatWithMessages, 0, len(groups))
	for _, canonical := range order {
		merged := mergeChatGroup(groups[canonical], canonical)
		result = append(result, merged)
	}

	// Re-sort by last message time (merging may have changed order)
	sort.Slice(result, func(i, j int) bool {
		return result[i].LastMessageTime.After(result[j].LastMessageTime)
	})

	return result
}

// mergeChatGroup combines multiple chat threads into one
func mergeChatGroup(chats []ChatWithMessages, canonicalJID string) ChatWithMessages {
	if len(chats) == 1 {
		return chats[0]
	}

	merged := ChatWithMessages{
		JID:      canonicalJID,
		Messages: []Message{},
	}

	// Collect all alias JIDs
	aliasJIDs := []string{}

	for _, chat := range chats {
		// Take the best name (non-empty, prefer from phone-based JID)
		if merged.Name == "" || (chat.Name != "" && !strings.Contains(chat.JID, "@lid")) {
			if chat.Name != "" {
				merged.Name = chat.Name
			}
		}

		// Take latest message time
		if chat.LastMessageTime.After(merged.LastMessageTime) {
			merged.LastMessageTime = chat.LastMessageTime
		}

		// Sum message counts
		merged.MessageCount += chat.MessageCount

		// Sum unread counts
		merged.UnreadCount += chat.UnreadCount

		// Preserve is_group (should be same for all)
		merged.IsGroup = chat.IsGroup

		// Collect messages
		merged.Messages = append(merged.Messages, chat.Messages...)

		// Track aliases
		if chat.JID != canonicalJID {
			aliasJIDs = append(aliasJIDs, chat.JID)
		}
	}

	// Sort messages by timestamp descending
	sort.Slice(merged.Messages, func(i, j int) bool {
		return merged.Messages[i].Timestamp.After(merged.Messages[j].Timestamp)
	})

	return merged
}

// GetMessagesInput for getting messages from a chat
type GetMessagesInput struct {
	ChatJID    string `json:"chat_jid,omitempty"`    // optional - omit for cross-chat query
	Limit      string `json:"limit,omitempty"`       // max messages (default 50, max 500 for cross-chat)
	BeforeTime string  `json:"before_time,omitempty"` // RFC3339 - pagination: messages before this time
	SinceTime  string  `json:"since_time,omitempty"`  // RFC3339 - filter: messages after this time
	Search     string  `json:"search,omitempty"`      // full-text search in content
	Type       string  `json:"type,omitempty"`        // filter by message type (text, image, etc.)
}

// GetMessagesOutput returns messages
type GetMessagesOutput struct {
	Messages    []Message `json:"messages"`
	Count       int       `json:"count"`
	ChatJID     string    `json:"chat_jid,omitempty"`     // only set if filtering by chat
	UnreadCount int       `json:"unread_count,omitempty"` // only set if filtering by chat
}

// GetMessages returns messages with flexible filtering
// Supports cross-chat queries when chat_jid is omitted
func GetMessages(ctx context.Context, req *mcp.CallToolRequest, input GetMessagesInput) (*mcp.CallToolResult, GetMessagesOutput, error) {
	client, err := GetClient()
	if err != nil {
		return nil, GetMessagesOutput{}, fmt.Errorf("get client: %w", err)
	}

	store := client.GetMessageStore()
	if store == nil {
		return nil, GetMessagesOutput{}, fmt.Errorf("message store not initialized")
	}

	// Build query
	query := MessageQuery{
		ChatJID: input.ChatJID,
		Limit:   parseIntParam(input.Limit, 50),
		Search:  input.Search,
		Type:    input.Type,
	}

	// Parse time filters
	if input.BeforeTime != "" {
		t, err := time.Parse(time.RFC3339, input.BeforeTime)
		if err != nil {
			return nil, GetMessagesOutput{}, fmt.Errorf("invalid before_time format (use RFC3339): %w", err)
		}
		query.BeforeTime = &t
	}

	if input.SinceTime != "" {
		t, err := time.Parse(time.RFC3339, input.SinceTime)
		if err != nil {
			return nil, GetMessagesOutput{}, fmt.Errorf("invalid since_time format (use RFC3339): %w", err)
		}
		query.SinceTime = &t
	}

	messages, err := store.QueryMessages(ctx, query)
	if err != nil {
		return nil, GetMessagesOutput{}, fmt.Errorf("query messages: %w", err)
	}

	// Ensure non-nil slice for JSON serialization
	if messages == nil {
		messages = []Message{}
	}

	// Get unread count if filtering by chat
	var unreadCount int
	if input.ChatJID != "" {
		unreadCount, _ = store.GetChatUnreadCount(ctx, input.ChatJID)
	}

	return nil, GetMessagesOutput{
		Messages:    messages,
		Count:       len(messages),
		ChatJID:     input.ChatJID,
		UnreadCount: unreadCount,
	}, nil
}

// GetSyncStatusInput is empty
type GetSyncStatusInput struct{}

// GetSyncStatusOutput returns sync status
type GetSyncStatusOutput struct {
	MessageCount    int    `json:"message_count"`
	ChatCount       int    `json:"chat_count"`
	LastHistorySync string `json:"last_history_sync,omitempty"`
}

// GetSyncStatus returns message store statistics
func GetSyncStatus(ctx context.Context, req *mcp.CallToolRequest, input GetSyncStatusInput) (*mcp.CallToolResult, GetSyncStatusOutput, error) {
	client, err := GetClient()
	if err != nil {
		return nil, GetSyncStatusOutput{}, fmt.Errorf("get client: %w", err)
	}

	store := client.GetMessageStore()
	if store == nil {
		return nil, GetSyncStatusOutput{}, fmt.Errorf("message store not initialized")
	}

	msgCount, err := store.GetMessageCount(ctx)
	if err != nil {
		return nil, GetSyncStatusOutput{}, fmt.Errorf("get message count: %w", err)
	}

	chats, err := store.GetChats(ctx, 1000)
	if err != nil {
		return nil, GetSyncStatusOutput{}, fmt.Errorf("get chats: %w", err)
	}

	lastSync, _ := store.GetSyncState(ctx, "last_history_sync")

	return nil, GetSyncStatusOutput{
		MessageCount:    msgCount,
		ChatCount:       len(chats),
		LastHistorySync: lastSync,
	}, nil
}

// PairPhoneInput takes a phone number
type PairPhoneInput struct {
	PhoneNumber string `json:"phone_number"`
}

// PairPhoneOutput returns the pairing code
type PairPhoneOutput struct {
	Success     bool   `json:"success"`
	PairingCode string `json:"pairing_code,omitempty"`
	Message     string `json:"message"`
}

// PairPhone generates a pairing code for phone-based authentication (alternative to QR)
func PairPhone(ctx context.Context, req *mcp.CallToolRequest, input PairPhoneInput) (*mcp.CallToolResult, PairPhoneOutput, error) {
	if input.PhoneNumber == "" {
		return nil, PairPhoneOutput{
			Success: false,
			Message: "Phone number is required (e.g. +33612345678)",
		}, nil
	}

	client, err := GetClient()
	if err != nil {
		return nil, PairPhoneOutput{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	// Check if already authenticated
	if client.IsAuthenticated(ctx) {
		phone := client.GetPhoneNumber(ctx)
		return nil, PairPhoneOutput{
			Success: false,
			Message: "Already authenticated as " + phone + ". Use check_auth to verify.",
		}, nil
	}

	// Generate pairing code
	code, err := client.PairWithPhone(ctx, input.PhoneNumber)
	if err != nil {
		return nil, PairPhoneOutput{
			Success: false,
			Message: "Pairing failed: " + err.Error(),
		}, nil
	}

	// Format code with dashes for readability (XXXX-XXXX)
	formattedCode := code
	if len(code) == 8 {
		formattedCode = code[:4] + "-" + code[4:]
	}

	return nil, PairPhoneOutput{
		Success:     true,
		PairingCode: formattedCode,
		Message:     "Enter this code on your phone: WhatsApp > Linked Devices > Link a Device > Link with phone number instead",
	}, nil
}

// ListAttachmentsInput for listing attachments
type ListAttachmentsInput struct {
	ChatJID   string  `json:"chat_jid,omitempty"`
	MediaType string  `json:"media_type,omitempty"` // image, video, audio, document
	Limit     string `json:"limit,omitempty"`
}

// ListAttachmentsOutput returns attachments
type ListAttachmentsOutput struct {
	Attachments []Attachment `json:"attachments"`
	Count       int          `json:"count"`
}

// ListAttachments returns attachments with optional filters
func ListAttachments(ctx context.Context, req *mcp.CallToolRequest, input ListAttachmentsInput) (*mcp.CallToolResult, ListAttachmentsOutput, error) {
	client, err := GetClient()
	if err != nil {
		return nil, ListAttachmentsOutput{}, fmt.Errorf("get client: %w", err)
	}

	store := client.GetMessageStore()
	if store == nil {
		return nil, ListAttachmentsOutput{}, fmt.Errorf("message store not initialized")
	}

	limit := parseIntParam(input.Limit, 50)

	attachments, err := store.ListAttachments(ctx, input.ChatJID, input.MediaType, limit)
	if err != nil {
		return nil, ListAttachmentsOutput{}, fmt.Errorf("list attachments: %w", err)
	}

	if attachments == nil {
		attachments = []Attachment{}
	}

	return nil, ListAttachmentsOutput{
		Attachments: attachments,
		Count:       len(attachments),
	}, nil
}

// DownloadAttachmentInput for downloading an attachment
type DownloadAttachmentInput struct {
	MessageID string `json:"message_id"`
}

// DownloadAttachmentOutput returns the downloaded file info
type DownloadAttachmentOutput struct {
	Path     string `json:"path"`
	Filename string `json:"filename"`
	MimeType string `json:"mime_type"`
	Size     int64  `json:"size"`
	Message  string `json:"message,omitempty"`
}

// DownloadAttachment downloads media and saves it to /tmp/
func DownloadAttachment(ctx context.Context, req *mcp.CallToolRequest, input DownloadAttachmentInput) (*mcp.CallToolResult, DownloadAttachmentOutput, error) {
	if input.MessageID == "" {
		return nil, DownloadAttachmentOutput{Message: "message_id is required"}, nil
	}

	client, err := GetClient()
	if err != nil {
		return nil, DownloadAttachmentOutput{Message: err.Error()}, nil
	}

	store := client.GetMessageStore()
	if store == nil {
		return nil, DownloadAttachmentOutput{Message: "message store not initialized"}, nil
	}

	// Get attachment metadata
	att, err := store.GetAttachment(ctx, input.MessageID)
	if err != nil {
		return nil, DownloadAttachmentOutput{Message: "error getting attachment: " + err.Error()}, nil
	}
	if att == nil {
		return nil, DownloadAttachmentOutput{Message: "attachment not found for message_id: " + input.MessageID}, nil
	}

	// Need whatsmeow client for download
	wmClient := client.GetWhatsmeowClient()
	if wmClient == nil {
		return nil, DownloadAttachmentOutput{Message: "WhatsApp client not connected"}, nil
	}

	// Download based on media type
	var data []byte
	var filename string

	switch att.MediaType {
	case "image":
		data, err = wmClient.DownloadMediaWithPath(ctx, att.DirectPath, att.FileEncSHA256, att.FileSHA256, att.MediaKey, int(att.FileSize), whatsmeow.MediaImage, att.MimeType)
		filename = fmt.Sprintf("image_%s.%s", input.MessageID[:8], getExtFromMime(att.MimeType, "jpg"))
	case "video":
		data, err = wmClient.DownloadMediaWithPath(ctx, att.DirectPath, att.FileEncSHA256, att.FileSHA256, att.MediaKey, int(att.FileSize), whatsmeow.MediaVideo, att.MimeType)
		filename = fmt.Sprintf("video_%s.%s", input.MessageID[:8], getExtFromMime(att.MimeType, "mp4"))
	case "audio":
		data, err = wmClient.DownloadMediaWithPath(ctx, att.DirectPath, att.FileEncSHA256, att.FileSHA256, att.MediaKey, int(att.FileSize), whatsmeow.MediaAudio, att.MimeType)
		filename = fmt.Sprintf("audio_%s.%s", input.MessageID[:8], getExtFromMime(att.MimeType, "ogg"))
	case "document":
		data, err = wmClient.DownloadMediaWithPath(ctx, att.DirectPath, att.FileEncSHA256, att.FileSHA256, att.MediaKey, int(att.FileSize), whatsmeow.MediaDocument, att.MimeType)
		filename = att.Filename
		if filename == "" {
			filename = fmt.Sprintf("document_%s.%s", input.MessageID[:8], getExtFromMime(att.MimeType, "bin"))
		}
	case "sticker":
		data, err = wmClient.DownloadMediaWithPath(ctx, att.DirectPath, att.FileEncSHA256, att.FileSHA256, att.MediaKey, int(att.FileSize), whatsmeow.MediaImage, att.MimeType)
		filename = fmt.Sprintf("sticker_%s.%s", input.MessageID[:8], getExtFromMime(att.MimeType, "webp"))
	default:
		return nil, DownloadAttachmentOutput{Message: "unsupported media type: " + att.MediaType}, nil
	}

	// If direct-path download failed, try re-downloading from raw protobuf
	// The raw proto may contain a URL field that DownloadMediaWithPath doesn't use
	if err != nil {
		rawProto, rawErr := store.GetMessageRawProto(ctx, input.MessageID)
		if rawErr == nil && rawProto != "" {
			var msg waE2E.Message
			if protojson.Unmarshal([]byte(rawProto), &msg) == nil {
				if retryData, retryErr := wmClient.DownloadAny(ctx, &msg); retryErr == nil {
					data = retryData
					err = nil
				}
			}
		}
	}

	if err != nil {
		return nil, DownloadAttachmentOutput{Message: "download failed: " + err.Error()}, nil
	}

	// Save to /tmp with naming convention
	path := tmpPath("whatsapp", filename)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return nil, DownloadAttachmentOutput{Message: "write failed: " + err.Error()}, nil
	}

	return nil, DownloadAttachmentOutput{
		Path:     path,
		Filename: filename,
		MimeType: att.MimeType,
		Size:     int64(len(data)),
		Message:  "Downloaded successfully",
	}, nil
}

// getExtFromMime extracts extension from MIME type or returns default
func getExtFromMime(mimeType, defaultExt string) string {
	if mimeType == "" {
		return defaultExt
	}
	exts, err := mime.ExtensionsByType(mimeType)
	if err == nil && len(exts) > 0 {
		return strings.TrimPrefix(exts[0], ".")
	}
	// Fallback: extract from mime type (e.g., "image/jpeg" -> "jpeg")
	parts := strings.Split(mimeType, "/")
	if len(parts) == 2 {
		return parts[1]
	}
	return defaultExt
}

// GetContactInput for getting contact info
type GetContactInput struct {
	JID string `json:"jid"`
}

// GetContactOutput returns contact info
type GetContactOutput struct {
	JID         string `json:"jid"`
	PhoneNumber string `json:"phone_number,omitempty"`
	FullName    string `json:"full_name,omitempty"`
	FirstName   string `json:"first_name,omitempty"`
	PushName    string `json:"push_name,omitempty"`
	Username    string `json:"username,omitempty"`
	IsBusiness  bool   `json:"is_business"`
	Found       bool   `json:"found"`
}

// GetContact retrieves contact information by JID
func GetContact(ctx context.Context, req *mcp.CallToolRequest, input GetContactInput) (*mcp.CallToolResult, GetContactOutput, error) {
	if input.JID == "" {
		return nil, GetContactOutput{Found: false}, nil
	}

	client, err := GetClient()
	if err != nil {
		return nil, GetContactOutput{}, fmt.Errorf("get client: %w", err)
	}

	store := client.GetMessageStore()
	if store == nil {
		return nil, GetContactOutput{}, fmt.Errorf("message store not initialized")
	}

	contact, err := store.GetContact(ctx, input.JID)
	if err != nil {
		return nil, GetContactOutput{}, fmt.Errorf("get contact: %w", err)
	}

	if contact == nil {
		return nil, GetContactOutput{JID: input.JID, Found: false}, nil
	}

	return nil, GetContactOutput{
		JID:         contact.JID,
		PhoneNumber: contact.PhoneNumber,
		FullName:    contact.FullName,
		FirstName:   contact.FirstName,
		PushName:    contact.PushName,
		Username:    contact.Username,
		IsBusiness:  contact.IsBusiness,
		Found:       true,
	}, nil
}

// SearchContactsInput for searching contacts
type SearchContactsInput struct {
	Query string  `json:"query"`
	Limit string `json:"limit,omitempty"`
}

// SearchContactsOutput returns matching contacts
type SearchContactsOutput struct {
	Contacts []Contact `json:"contacts"`
	Count    int       `json:"count"`
	Query    string    `json:"query"`
}

// SearchContacts searches contacts by name or phone number
func SearchContacts(ctx context.Context, req *mcp.CallToolRequest, input SearchContactsInput) (*mcp.CallToolResult, SearchContactsOutput, error) {
	if input.Query == "" {
		return nil, SearchContactsOutput{Contacts: []Contact{}, Count: 0, Query: ""}, nil
	}

	client, err := GetClient()
	if err != nil {
		return nil, SearchContactsOutput{}, fmt.Errorf("get client: %w", err)
	}

	store := client.GetMessageStore()
	if store == nil {
		return nil, SearchContactsOutput{}, fmt.Errorf("message store not initialized")
	}

	limit := parseIntParam(input.Limit, 20)

	contacts, err := store.SearchContacts(ctx, input.Query, limit)
	if err != nil {
		return nil, SearchContactsOutput{}, fmt.Errorf("search contacts: %w", err)
	}

	if contacts == nil {
		contacts = []Contact{}
	}

	return nil, SearchContactsOutput{
		Contacts: contacts,
		Count:    len(contacts),
		Query:    input.Query,
	}, nil
}

// MIME type allowlists for WhatsApp media sending
var allowedImageMIME = map[string]bool{
	"image/jpeg": true,
	"image/png":  true,
}

var allowedVideoMIME = map[string]bool{
	"video/mp4":  true,
	"video/3gpp": true,
}

var allowedAudioMIME = map[string]bool{
	"audio/aac":  true,
	"audio/amr":  true,
	"audio/mpeg": true,
	"audio/mp4":  true,
	"audio/ogg":  true,
}

// Size limits per media type
const (
	maxImageSize    = 5 * 1024 * 1024   // 5 MB
	maxVideoSize    = 16 * 1024 * 1024  // 16 MB
	maxAudioSize    = 16 * 1024 * 1024  // 16 MB
	maxDocumentSize = 100 * 1024 * 1024 // 100 MB
)

// SendMessageInput for sending a message
type SendMessageInput struct {
	Mode       string `json:"mode"`                  // "new" or "reply"
	ChatJID    string `json:"chat_jid,omitempty"`    // required for mode=new
	Text       string `json:"text,omitempty"`        // message content (required if no attachment)
	ReplyTo    string `json:"reply_to,omitempty"`    // message_id for mode=reply
	MarkRead   *string `json:"mark_read,omitempty"`   // auto-mark chat as read after send (default: true)
	Attachment string `json:"attachment,omitempty"`   // file path (optional)
}

// SendMessageOutput returns the sent message info
type SendMessageOutput struct {
	Success   bool   `json:"success"`
	MessageID string `json:"message_id,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Message   string `json:"message,omitempty"`
}

// SendMessage sends a WhatsApp message (text and/or media)
func SendMessage(ctx context.Context, req *mcp.CallToolRequest, input SendMessageInput) (*mcp.CallToolResult, SendMessageOutput, error) {
	// Validate input: at least one of text or attachment required
	if input.Text == "" && input.Attachment == "" {
		return nil, SendMessageOutput{
			Success: false,
			Message: "at least one of 'text' or 'attachment' is required",
		}, nil
	}

	if input.Mode == "" {
		input.Mode = "new" // default to new message
	}

	var chatJID string
	var replyTo string

	switch input.Mode {
	case "new":
		if input.ChatJID == "" {
			return nil, SendMessageOutput{
				Success: false,
				Message: "chat_jid is required for mode=new",
			}, nil
		}
		chatJID = input.ChatJID
	case "reply":
		if input.ReplyTo == "" {
			return nil, SendMessageOutput{
				Success: false,
				Message: "reply_to is required for mode=reply",
			}, nil
		}
		replyTo = input.ReplyTo
		// Get the chat JID from the original message
		client, err := GetClient()
		if err != nil {
			return nil, SendMessageOutput{Success: false, Message: err.Error()}, nil
		}
		store := client.GetMessageStore()
		if store == nil {
			return nil, SendMessageOutput{Success: false, Message: "message store not initialized"}, nil
		}
		origMsg, err := store.GetMessage(ctx, input.ReplyTo)
		if err != nil {
			return nil, SendMessageOutput{Success: false, Message: "error getting original message: " + err.Error()}, nil
		}
		if origMsg == nil {
			return nil, SendMessageOutput{Success: false, Message: "original message not found: " + input.ReplyTo}, nil
		}
		chatJID = origMsg.ChatJID
	default:
		return nil, SendMessageOutput{
			Success: false,
			Message: "mode must be 'new' or 'reply'",
		}, nil
	}

	// Get client
	client, err := GetClient()
	if err != nil {
		return nil, SendMessageOutput{Success: false, Message: err.Error()}, nil
	}

	var result *SendResult

	if input.Attachment != "" {
		// Media message path: read file, auto-convert if needed, send
		data, err := os.ReadFile(input.Attachment)
		if err != nil {
			return nil, SendMessageOutput{
				Success: false,
				Message: fmt.Sprintf("attachment not found or unreadable: %s: %s", input.Attachment, err.Error()),
			}, nil
		}

		mimeType := mime.TypeByExtension(filepath.Ext(input.Attachment))
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		fileName := filepath.Base(input.Attachment)

		// Auto-convert non-JPEG/PNG images to JPEG
		if strings.HasPrefix(mimeType, "image/") && !allowedImageMIME[mimeType] {
			converted, convErr := convertToJPEG(data)
			if convErr != nil {
				return nil, SendMessageOutput{
					Success: false,
					Message: fmt.Sprintf("auto-convert to JPEG failed for %s: %s", mimeType, convErr.Error()),
				}, nil
			}
			data = converted
			mimeType = "image/jpeg"
			// Update filename extension
			ext := filepath.Ext(fileName)
			if ext != "" {
				fileName = strings.TrimSuffix(fileName, ext) + ".jpg"
			}
		}

		fileSize := int64(len(data))

		// Validate MIME type and size
		if errMsg := validateMediaForWhatsApp(mimeType, fileSize); errMsg != "" {
			return nil, SendMessageOutput{
				Success: false,
				Message: errMsg,
			}, nil
		}

		result, err = client.SendMediaMessage(ctx, chatJID, data, mimeType, fileName, input.Text, replyTo)
		if err != nil {
			return nil, SendMessageOutput{Success: false, Message: err.Error()}, nil
		}
	} else {
		// Text-only message
		result, err = client.SendMessage(ctx, chatJID, input.Text, replyTo)
		if err != nil {
			return nil, SendMessageOutput{Success: false, Message: err.Error()}, nil
		}
	}

	// Auto-mark as read after successful send (default: true)
	shouldMarkRead := input.MarkRead == nil || parseBoolParam(*input.MarkRead)
	if shouldMarkRead {
		_, _ = client.MarkRead(ctx, chatJID)
	}

	// Auto-dismiss from triage after sending
	_, _ = client.Dismiss(ctx, chatJID, "replied")

	return nil, SendMessageOutput{
		Success:   true,
		MessageID: result.ID,
		Timestamp: result.Timestamp.Format(time.RFC3339),
		Message:   "Message sent successfully",
	}, nil
}

// validateMediaForWhatsApp checks MIME type and file size against WhatsApp limits.
// Returns an error message string, or "" if valid.
func validateMediaForWhatsApp(mimeType string, fileSize int64) string {
	// Check image types
	if strings.HasPrefix(mimeType, "image/") {
		if !allowedImageMIME[mimeType] {
			return fmt.Sprintf(
				"WhatsApp does not support %s for image sending. "+
					"Supported image formats: JPEG, PNG.",
				mimeType,
			)
		}
		if fileSize > maxImageSize {
			return fmt.Sprintf(
				"Image too large: %d bytes (limit: %d bytes / 5 MB)",
				fileSize, maxImageSize,
			)
		}
		return ""
	}

	// Check video types
	if strings.HasPrefix(mimeType, "video/") {
		if !allowedVideoMIME[mimeType] {
			return fmt.Sprintf(
				"WhatsApp does not support %s for video sending. "+
					"Supported video formats: MP4, 3GPP.",
				mimeType,
			)
		}
		if fileSize > maxVideoSize {
			return fmt.Sprintf(
				"Video too large: %d bytes (limit: %d bytes / 16 MB)",
				fileSize, maxVideoSize,
			)
		}
		return ""
	}

	// Check audio types
	if strings.HasPrefix(mimeType, "audio/") {
		if !allowedAudioMIME[mimeType] {
			return fmt.Sprintf(
				"WhatsApp does not support %s for audio sending. "+
					"Supported audio formats: AAC, AMR, MP3, MP4, OGG.",
				mimeType,
			)
		}
		if fileSize > maxAudioSize {
			return fmt.Sprintf(
				"Audio too large: %d bytes (limit: %d bytes / 16 MB)",
				fileSize, maxAudioSize,
			)
		}
		return ""
	}

	// Everything else -> document (100 MB limit)
	if fileSize > maxDocumentSize {
		return fmt.Sprintf(
			"Document too large: %d bytes (limit: %d bytes / 100 MB)",
			fileSize, maxDocumentSize,
		)
	}
	return ""
}

// MarkReadInput for marking messages as read
type MarkReadInput struct {
	ChatJID string `json:"chat_jid"` // required: the chat to mark as read
}

// MarkReadOutput returns the result of marking messages as read
type MarkReadOutput struct {
	Success     bool   `json:"success"`
	MarkedCount int    `json:"marked_count"` // number of messages marked as read
	Message     string `json:"message,omitempty"`
}

// MarkRead marks messages in a chat as read (sends read receipts)
func MarkRead(ctx context.Context, req *mcp.CallToolRequest, input MarkReadInput) (*mcp.CallToolResult, MarkReadOutput, error) {
	if input.ChatJID == "" {
		return nil, MarkReadOutput{
			Success: false,
			Message: "chat_jid is required",
		}, nil
	}

	client, err := GetClient()
	if err != nil {
		return nil, MarkReadOutput{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	count, err := client.MarkRead(ctx, input.ChatJID)
	if err != nil {
		return nil, MarkReadOutput{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	msg := "Marked as read"
	if count == 0 {
		msg = "No unread messages to mark"
	}

	return nil, MarkReadOutput{
		Success:     true,
		MarkedCount: count,
		Message:     msg,
	}, nil
}

// DismissChatInput for dismissing a chat from triage
type DismissChatInput struct {
	ChatJID string `json:"chat_jid"`        // required: the chat to dismiss
	Notes   string `json:"notes,omitempty"` // optional: why it was dismissed (e.g. "replied", "not urgent")
}

// DismissChatOutput returns the result of dismissing a chat
type DismissChatOutput struct {
	Success   bool   `json:"success"`
	Timestamp int64  `json:"timestamp,omitempty"` // unix timestamp when dismissed
	Message   string `json:"message,omitempty"`
}

// DismissChat marks a chat as handled for triage (does NOT send read receipts)
func DismissChat(ctx context.Context, req *mcp.CallToolRequest, input DismissChatInput) (*mcp.CallToolResult, DismissChatOutput, error) {
	if input.ChatJID == "" {
		return nil, DismissChatOutput{
			Success: false,
			Message: "chat_jid is required",
		}, nil
	}

	client, err := GetClient()
	if err != nil {
		return nil, DismissChatOutput{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	ts, err := client.Dismiss(ctx, input.ChatJID, input.Notes)
	if err != nil {
		return nil, DismissChatOutput{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return nil, DismissChatOutput{
		Success:   true,
		Timestamp: ts,
		Message:   "Chat dismissed from triage",
	}, nil
}

// UndismissChatInput for undismissing a chat
type UndismissChatInput struct {
	ChatJID string `json:"chat_jid"` // required: the chat to undismiss
}

// UndismissChatOutput returns the result of undismissing a chat
type UndismissChatOutput struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// UndismissChat clears dismiss state so a chat reappears in pending triage
func UndismissChat(ctx context.Context, req *mcp.CallToolRequest, input UndismissChatInput) (*mcp.CallToolResult, UndismissChatOutput, error) {
	if input.ChatJID == "" {
		return nil, UndismissChatOutput{
			Success: false,
			Message: "chat_jid is required",
		}, nil
	}

	client, err := GetClient()
	if err != nil {
		return nil, UndismissChatOutput{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	if err := client.Undismiss(ctx, input.ChatJID); err != nil {
		return nil, UndismissChatOutput{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	return nil, UndismissChatOutput{
		Success: true,
		Message: "Chat undismissed — will appear in pending triage",
	}, nil
}

// GetGroupInfoInput for getting group info
type GetGroupInfoInput struct {
	ChatJID string `json:"chat_jid"` // required: group JID (must end with @g.us)
}

// GetGroupInfoOutput returns group metadata and participants
type GetGroupInfoOutput struct {
	JID              string            `json:"jid"`
	Name             string            `json:"name,omitempty"`
	Topic            string            `json:"topic,omitempty"`
	Created          string            `json:"created,omitempty"`
	ParticipantCount int               `json:"participant_count"`
	Participants     []ParticipantInfo `json:"participants"`
	Found            bool              `json:"found"`
	Message          string            `json:"message,omitempty"`
}

// GetGroupInfo retrieves group metadata and participant list
func GetGroupInfo(ctx context.Context, req *mcp.CallToolRequest, input GetGroupInfoInput) (*mcp.CallToolResult, GetGroupInfoOutput, error) {
	if input.ChatJID == "" {
		return nil, GetGroupInfoOutput{
			Found:   false,
			Message: "chat_jid is required (must be a group JID ending with @g.us)",
		}, nil
	}

	client, err := GetClient()
	if err != nil {
		return nil, GetGroupInfoOutput{
			Found:   false,
			Message: err.Error(),
		}, nil
	}

	result, err := client.GetGroupInfo(ctx, input.ChatJID)
	if err != nil {
		return nil, GetGroupInfoOutput{
			Found:   false,
			Message: err.Error(),
		}, nil
	}

	// Ensure non-nil slice for JSON serialization
	participants := result.Participants
	if participants == nil {
		participants = []ParticipantInfo{}
	}

	return nil, GetGroupInfoOutput{
		JID:              result.JID,
		Name:             result.Name,
		Topic:            result.Topic,
		Created:          result.Created.Format(time.RFC3339),
		ParticipantCount: result.ParticipantCount,
		Participants:     participants,
		Found:            true,
	}, nil
}

// TCPPort is the port for TCP MCP connections (follows 205x convention)
const TCPPort = 2053

func main() {
	// Handle CLI subcommands
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			runServeCommand()
			return
		case "setup":
			runSetupCommand()
			return
		case "version":
			fmt.Printf("whatsapp-mcp %s\n", version)
			return
		case "help", "-h", "--help":
			fmt.Println("WhatsApp MCP Server")
			fmt.Println()
			fmt.Println("Usage:")
			fmt.Println("  whatsapp-mcp          Run as stdio MCP server")
			fmt.Println("  whatsapp-mcp serve    Run as HTTP server on port 2053")
			fmt.Println("  whatsapp-mcp setup    Pair with WhatsApp via QR code")
			fmt.Println("  whatsapp-mcp version  Show version")
			fmt.Println()
			fmt.Println("HTTP Endpoints (when running 'serve'):")
			fmt.Println("  /mcp      MCP protocol endpoint")
			fmt.Println("  /health   Health check")
			fmt.Println("  /auth     Trigger QR authentication")
			return
		}
	}

	// Set up logging to stderr (stdout is for MCP protocol)
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime | log.Lshortfile)

	// Create MCP server
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "whatsapp-mcp",
		Version: version,
	}, nil)

	// Register all tools
	registerTools(server)

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		// Clean up WhatsApp client
		if client, err := GetClient(); err == nil {
			client.Close()
		}
		cancel()
	}()

	// Run server over stdio
	log.Println("WhatsApp MCP server starting...")
	if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil {
		if ctx.Err() == nil {
			log.Fatalf("Server error: %v", err)
		}
	}
}
