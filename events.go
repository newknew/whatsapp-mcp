package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/encoding/protojson"
)

// resolveToPhoneJID converts a LID JID to its phone-based JID if possible.
// Returns the original JID if no mapping exists or if it's already phone-based.
func (h *EventHandler) resolveToPhoneJID(ctx context.Context, jid string) string {
	if h.client == nil || h.client.Store == nil || h.client.Store.LIDs == nil {
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
	phoneJID, err := h.client.Store.LIDs.GetPNForLID(ctx, parsed)
	if err != nil || phoneJID.IsEmpty() {
		return jid // No mapping found
	}

	return phoneJID.String()
}

// EventHandler processes WhatsApp events and stores messages
type EventHandler struct {
	client *whatsmeow.Client
	store  *MessageStore
	log    func(string, ...interface{})
}

// NewEventHandler creates a new event handler
func NewEventHandler(client *whatsmeow.Client, store *MessageStore) *EventHandler {
	return &EventHandler{
		client: client,
		store:  store,
		log: func(msg string, args ...interface{}) {
			logToFile("events", msg, args...)
		},
	}
}

// Register registers all event handlers
func (h *EventHandler) Register() {
	h.client.AddEventHandler(h.handleEvent)
}

// handleEvent dispatches events to appropriate handlers
func (h *EventHandler) handleEvent(evt interface{}) {
	switch v := evt.(type) {
	case *events.Message:
		h.handleMessage(v)
	case *events.HistorySync:
		h.handleHistorySync(v)
	case *events.Contact:
		h.handleContact(v)
	case *events.PushName:
		h.handlePushName(v)
	}
}

// handleMessage processes real-time incoming messages
func (h *EventHandler) handleMessage(evt *events.Message) {
	ctx := context.Background()

	msg := h.convertMessage(evt.Info, evt.Message)
	if msg == nil {
		return
	}

	// Resolve LID JID to phone JID for canonical storage
	msg.CanonicalJID = h.resolveToPhoneJID(ctx, msg.ChatJID)

	// Opportunistic backfill: if we resolved a LID to a phone JID,
	// fix any previously-stored messages that have the LID as canonical_jid
	if msg.CanonicalJID != msg.ChatJID {
		if n, err := h.store.BackfillCanonicalJID(ctx, msg.ChatJID, msg.CanonicalJID); err != nil {
			h.log("Error backfilling canonical JID %s -> %s: %v", msg.ChatJID, msg.CanonicalJID, err)
		} else if n > 0 {
			h.log("Backfilled %d rows: canonical_jid %s -> %s", n, msg.ChatJID, msg.CanonicalJID)
		}
	}

	if err := h.store.StoreMessage(ctx, msg); err != nil {
		h.log("Error storing message: %v", err)
	} else {
		h.log("Stored real-time message %s in chat %s (canonical: %s)", msg.ID, msg.ChatJID, msg.CanonicalJID)
	}

	// Update contact with push_name from message
	if evt.Info.PushName != "" && !evt.Info.IsFromMe {
		contact := &Contact{
			JID:         evt.Info.Sender.String(),
			PhoneNumber: extractPhoneNumber(evt.Info.Sender.String()),
			PushName:    evt.Info.PushName,
		}
		h.store.StoreContact(ctx, contact)
	}

	// Store attachment metadata if this is a media message
	att := h.extractAttachment(evt.Info, evt.Message)
	if att != nil {
		// Set canonical JID for attachment as well
		att.CanonicalJID = msg.CanonicalJID
		if err := h.store.StoreAttachment(ctx, att); err != nil {
			h.log("Error storing attachment: %v", err)
		} else {
			h.log("Stored attachment %s (%s)", att.MessageID, att.MediaType)
		}
	}
}

// handleHistorySync processes history sync batches
func (h *EventHandler) handleHistorySync(evt *events.HistorySync) {
	ctx := context.Background()
	data := evt.Data

	h.log("History sync received: %s, conversations=%d",
		data.GetSyncType().String(), len(data.GetConversations()))

	var stored, skipped, attachments, contacts, backfilled int

	for _, conv := range data.GetConversations() {
		chatJID := conv.GetID()

		// Resolve LID JID to phone JID for canonical storage
		canonicalJID := h.resolveToPhoneJID(ctx, chatJID)

		// Opportunistic backfill: fix old data with stale LID canonical_jid
		if canonicalJID != chatJID {
			if n, err := h.store.BackfillCanonicalJID(ctx, chatJID, canonicalJID); err != nil {
				h.log("Error backfilling canonical JID %s -> %s: %v", chatJID, canonicalJID, err)
			} else if n > 0 {
				backfilled += n
				h.log("Backfilled %d rows: canonical_jid %s -> %s", n, chatJID, canonicalJID)
			}
		}

		// Debug: log all available fields on the Conversation proto for groups
		if isGroupJID(chatJID) {
			h.log("Group conversation debug: jid=%s name=%q displayName=%q unreadCount=%d",
				chatJID, conv.GetName(), conv.GetDisplayName(), conv.GetUnreadCount())
		}

		// Serialize raw proto to JSON for future re-parsing
		var rawProto string
		if jsonBytes, err := protojson.Marshal(conv); err == nil {
			rawProto = string(jsonBytes)
		}

		// Update chat info with canonical JID
		chat := &Chat{
			JID:          chatJID,
			CanonicalJID: canonicalJID,
			Name:         conv.GetName(),
			IsGroup:      isGroupJID(chatJID),
			RawProto:     rawProto,
		}
		if err := h.store.UpdateChat(ctx, chat); err != nil {
			h.log("Error updating chat %s: %v", chatJID, err)
		}

		// Process messages in this conversation
		for _, historyMsg := range conv.GetMessages() {
			msg := h.convertHistorySyncMessage(chatJID, historyMsg)
			if msg == nil {
				skipped++
				continue
			}

			// Set canonical JID for message
			msg.CanonicalJID = canonicalJID

			if err := h.store.StoreMessage(ctx, msg); err != nil {
				h.log("Error storing history message: %v", err)
				skipped++
			} else {
				stored++
			}

			// Update contact with push_name from history message
			if msg.PushName != "" && !msg.IsFromMe && msg.SenderJID != "me" {
				contact := &Contact{
					JID:         msg.SenderJID,
					PhoneNumber: extractPhoneNumber(msg.SenderJID),
					PushName:    msg.PushName,
				}
				if h.store.StoreContact(ctx, contact) == nil {
					contacts++
				}
			}

			// Extract and store attachment metadata
			att := h.extractHistorySyncAttachment(chatJID, historyMsg)
			if att != nil {
				// Set canonical JID for attachment
				att.CanonicalJID = canonicalJID
				if err := h.store.StoreAttachment(ctx, att); err != nil {
					h.log("Error storing attachment: %v", err)
				} else {
					attachments++
				}
			}
		}
	}

	h.log("History sync complete: stored=%d, skipped=%d, attachments=%d, contacts=%d, backfilled=%d", stored, skipped, attachments, contacts, backfilled)

	// Update sync state
	h.store.SetSyncState(ctx, "last_history_sync", time.Now().Format(time.RFC3339))
}

// handleContact processes contact sync events
func (h *EventHandler) handleContact(evt *events.Contact) {
	ctx := context.Background()

	// Serialize raw ContactAction proto for future re-parsing
	var rawProto string
	if evt.Action != nil {
		if jsonBytes, err := protojson.Marshal(evt.Action); err == nil {
			rawProto = string(jsonBytes)
		}
	}

	contact := &Contact{
		JID:         evt.JID.String(),
		PhoneNumber: extractPhoneNumber(evt.JID.String()),
		RawProto:    rawProto,
	}

	// Extract fields from ContactAction if available
	if evt.Action != nil {
		contact.FullName = evt.Action.GetFullName()
		contact.FirstName = evt.Action.GetFirstName()
		contact.LidJID = evt.Action.GetLidJID()
		contact.Username = evt.Action.GetUsername()
	}

	if err := h.store.StoreContact(ctx, contact); err != nil {
		h.log("Error storing contact %s: %v", evt.JID, err)
	} else {
		h.log("Stored contact %s (name=%s)", evt.JID, contact.FullName)
	}
}

// handlePushName processes push name change events
func (h *EventHandler) handlePushName(evt *events.PushName) {
	ctx := context.Background()

	jid := evt.JID.String()
	if jid == "" {
		return
	}

	// Create/update contact with push name
	contact := &Contact{
		JID:         jid,
		PhoneNumber: extractPhoneNumber(jid),
		PushName:    evt.NewPushName,
	}

	if err := h.store.StoreContact(ctx, contact); err != nil {
		h.log("Error storing push name for %s: %v", jid, err)
	} else {
		h.log("Updated push name for %s: %s -> %s", jid, evt.OldPushName, evt.NewPushName)
	}
}

// extractPhoneNumber extracts phone number from a JID like "33612345678@s.whatsapp.net"
func extractPhoneNumber(jid string) string {
	// Remove @s.whatsapp.net or @lid suffix
	if idx := strings.Index(jid, "@"); idx > 0 {
		phone := jid[:idx]
		// Only return if it looks like a phone number (digits only)
		isPhone := true
		for _, c := range phone {
			if c < '0' || c > '9' {
				isPhone = false
				break
			}
		}
		if isPhone && len(phone) >= 7 {
			return "+" + phone
		}
	}
	return ""
}

// convertMessage converts a real-time message to our Message struct
func (h *EventHandler) convertMessage(info types.MessageInfo, msg *waE2E.Message) *Message {
	content, msgType := extractContent(msg)

	// Serialize raw proto to JSON for future re-parsing
	var rawProto string
	if msg != nil {
		if jsonBytes, err := protojson.Marshal(msg); err == nil {
			rawProto = string(jsonBytes)
		}
	}

	return &Message{
		ID:        info.ID,
		ChatJID:   info.Chat.String(),
		SenderJID: info.Sender.String(),
		Timestamp: info.Timestamp,
		Content:   content,
		Type:      msgType,
		IsFromMe:  info.IsFromMe,
		PushName:  info.PushName,
		RawProto:  rawProto,
	}
}

// convertHistorySyncMessage converts a history sync message
func (h *EventHandler) convertHistorySyncMessage(chatJID string, historyMsg *waHistorySync.HistorySyncMsg) *Message {
	webMsg := historyMsg.GetMessage()
	if webMsg == nil {
		return nil
	}

	msgInfo := webMsg.GetKey()
	if msgInfo == nil {
		return nil
	}

	// Parse timestamp
	ts := time.Unix(int64(webMsg.GetMessageTimestamp()), 0)

	// Determine sender
	var senderJID string
	if msgInfo.GetFromMe() {
		senderJID = "me"
	} else if webMsg.GetParticipant() != "" {
		senderJID = webMsg.GetParticipant()
	} else {
		senderJID = chatJID
	}

	msg := webMsg.GetMessage()
	content, msgType := extractContent(msg)

	// Serialize raw proto to JSON for future re-parsing
	var rawProto string
	if msg != nil {
		if jsonBytes, err := protojson.Marshal(msg); err == nil {
			rawProto = string(jsonBytes)
		}
	}

	return &Message{
		ID:        msgInfo.GetID(),
		ChatJID:   chatJID,
		SenderJID: senderJID,
		Timestamp: ts,
		Content:   content,
		Type:      msgType,
		IsFromMe:  msgInfo.GetFromMe(),
		PushName:  webMsg.GetPushName(),
		RawProto:  rawProto,
	}
}

// extractContent extracts text content and message type from a Message
func extractContent(msg *waE2E.Message) (content string, msgType string) {
	if msg == nil {
		return "", "unknown"
	}

	// Text message
	if msg.Conversation != nil {
		return msg.GetConversation(), "text"
	}

	// Extended text (with link preview etc)
	if msg.ExtendedTextMessage != nil {
		return msg.ExtendedTextMessage.GetText(), "text"
	}

	// Image
	if msg.ImageMessage != nil {
		return msg.ImageMessage.GetCaption(), "image"
	}

	// Video
	if msg.VideoMessage != nil {
		return msg.VideoMessage.GetCaption(), "video"
	}

	// Audio/voice
	if msg.AudioMessage != nil {
		return "", "audio"
	}

	// Document
	if msg.DocumentMessage != nil {
		return msg.DocumentMessage.GetFileName(), "document"
	}

	// Sticker
	if msg.StickerMessage != nil {
		return "", "sticker"
	}

	// Contact
	if msg.ContactMessage != nil {
		return msg.ContactMessage.GetDisplayName(), "contact"
	}

	// Location
	if msg.LocationMessage != nil {
		return fmt.Sprintf("%.6f,%.6f", msg.LocationMessage.GetDegreesLatitude(), msg.LocationMessage.GetDegreesLongitude()), "location"
	}

	// Reaction
	if msg.ReactionMessage != nil {
		return msg.ReactionMessage.GetText(), "reaction"
	}

	// Poll creation (all versions - check newer first)
	if poll := msg.PollCreationMessageV5; poll != nil {
		return formatPoll(poll), "poll"
	}
	if poll := msg.PollCreationMessageV3; poll != nil {
		return formatPoll(poll), "poll"
	}
	if poll := msg.PollCreationMessageV2; poll != nil {
		return formatPoll(poll), "poll"
	}
	if poll := msg.PollCreationMessage; poll != nil {
		return formatPoll(poll), "poll"
	}

	// Poll update (vote)
	if msg.PollUpdateMessage != nil {
		return "[poll vote]", "poll_vote"
	}

	// Event/calendar message
	if evt := msg.EventMessage; evt != nil {
		content := evt.GetName()
		if desc := evt.GetDescription(); desc != "" {
			content += ": " + desc
		}
		if loc := evt.GetLocation(); loc != nil {
			content += fmt.Sprintf(" @ %.6f,%.6f", loc.GetDegreesLatitude(), loc.GetDegreesLongitude())
		}
		return content, "event"
	}

	// View-once messages (photos/videos that disappear)
	if vo := msg.ViewOnceMessage; vo != nil {
		return extractContent(vo.GetMessage())
	}
	if vo := msg.ViewOnceMessageV2; vo != nil {
		return extractContent(vo.GetMessage())
	}

	// Ephemeral messages (disappearing messages wrapper)
	if eph := msg.EphemeralMessage; eph != nil {
		return extractContent(eph.GetMessage())
	}

	// Edited message - extract the edited content
	if edited := msg.EditedMessage; edited != nil {
		return extractContent(edited.GetMessage())
	}

	// Protocol messages (read receipts, typing, etc) - usually not stored
	if msg.ProtocolMessage != nil {
		return "", "protocol"
	}

	// Live location
	if msg.LiveLocationMessage != nil {
		ll := msg.LiveLocationMessage
		return fmt.Sprintf("%.6f,%.6f", ll.GetDegreesLatitude(), ll.GetDegreesLongitude()), "live_location"
	}

	// Contact array (multiple contacts)
	if msg.ContactsArrayMessage != nil {
		names := []string{}
		for _, c := range msg.ContactsArrayMessage.GetContacts() {
			names = append(names, c.GetDisplayName())
		}
		return fmt.Sprintf("%d contacts: %s", len(names), joinStrings(names, ", ")), "contacts"
	}

	// Group invite
	if msg.GroupInviteMessage != nil {
		return msg.GroupInviteMessage.GetGroupName(), "group_invite"
	}

	// Template/button messages
	if msg.TemplateMessage != nil {
		return "[template message]", "template"
	}
	if msg.ButtonsMessage != nil {
		return msg.ButtonsMessage.GetContentText(), "buttons"
	}
	if msg.ListMessage != nil {
		return msg.ListMessage.GetTitle(), "list"
	}

	// Interactive messages (buttons, lists)
	if msg.InteractiveMessage != nil {
		return "[interactive]", "interactive"
	}

	// Order/product messages
	if msg.OrderMessage != nil {
		return "[order]", "order"
	}
	if msg.ProductMessage != nil {
		return "[product]", "product"
	}

	// Call log
	if msg.CallLogMesssage != nil {
		return "[call]", "call"
	}

	return "", "other"
}

// formatPoll formats a poll creation message for display
func formatPoll(poll *waE2E.PollCreationMessage) string {
	if poll == nil {
		return ""
	}
	question := poll.GetName()
	options := []string{}
	for _, opt := range poll.GetOptions() {
		options = append(options, "- "+opt.GetOptionName())
	}
	if len(options) > 0 {
		return question + "\n" + joinStrings(options, "\n")
	}
	return question
}

// joinStrings joins strings with a separator (simple helper)
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

// isGroupJID checks if a JID is a group
func isGroupJID(jid string) bool {
	// Group JIDs end with @g.us, individual chats end with @s.whatsapp.net
	return len(jid) > 5 && jid[len(jid)-5:] == "@g.us"
}

// logToFile writes to the debug log file
func logToFile(module, msg string, args ...interface{}) {
	logger := newFileLogger(module)
	logger.Infof(msg, args...)
}

// extractAttachment extracts attachment metadata from a real-time message
func (h *EventHandler) extractAttachment(info types.MessageInfo, msg *waE2E.Message) *Attachment {
	if msg == nil {
		return nil
	}

	att := &Attachment{
		MessageID:  info.ID,
		ChatJID:    info.Chat.String(),
		SenderJID:  info.Sender.String(),
		SenderName: info.PushName,
		Timestamp:  info.Timestamp,
	}

	// Check each media type
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

// extractHistorySyncAttachment extracts attachment metadata from a history sync message
func (h *EventHandler) extractHistorySyncAttachment(chatJID string, historyMsg *waHistorySync.HistorySyncMsg) *Attachment {
	webMsg := historyMsg.GetMessage()
	if webMsg == nil {
		return nil
	}

	msgInfo := webMsg.GetKey()
	if msgInfo == nil {
		return nil
	}

	msg := webMsg.GetMessage()
	if msg == nil {
		return nil
	}

	ts := time.Unix(int64(webMsg.GetMessageTimestamp()), 0)

	// Determine sender
	var senderJID string
	if msgInfo.GetFromMe() {
		senderJID = "me"
	} else if webMsg.GetParticipant() != "" {
		senderJID = webMsg.GetParticipant()
	} else {
		senderJID = chatJID
	}

	att := &Attachment{
		MessageID:  msgInfo.GetID(),
		ChatJID:    chatJID,
		SenderJID:  senderJID,
		SenderName: webMsg.GetPushName(),
		Timestamp:  ts,
	}

	// Check each media type
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
