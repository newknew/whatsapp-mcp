package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// groupSyncInterval throttles the full group list refresh triggered on
// every (re)connect. Group events keep the cache fresh in between.
const groupSyncInterval = time.Hour

var groupSyncMu sync.Mutex

// resolvePhoneJID converts a LID JID to its phone JID if whatsmeow knows
// the mapping; otherwise returns the input unchanged.
func resolvePhoneJID(ctx context.Context, wa *whatsmeow.Client, jid string) string {
	if wa == nil || wa.Store == nil || wa.Store.LIDs == nil {
		return jid
	}
	parsed, err := types.ParseJID(jid)
	if err != nil || parsed.Server != types.HiddenUserServer {
		return jid
	}
	phoneJID, err := wa.Store.LIDs.GetPNForLID(ctx, parsed)
	if err != nil || phoneJID.IsEmpty() {
		return jid
	}
	return phoneJID.String()
}

// buildParticipants converts whatsmeow group participants into our
// representation, resolving LIDs to phone JIDs and names from contacts.
func buildParticipants(ctx context.Context, wa *whatsmeow.Client, store *MessageStore, info *types.GroupInfo) []ParticipantInfo {
	participants := make([]ParticipantInfo, 0, len(info.Participants))
	for _, p := range info.Participants {
		originalJID := p.JID.String()
		phoneJID := originalJID
		if !p.PhoneNumber.IsEmpty() {
			phoneJID = p.PhoneNumber.String()
		} else {
			phoneJID = resolvePhoneJID(ctx, wa, originalJID)
		}

		part := ParticipantInfo{
			JID:          phoneJID,
			IsAdmin:      p.IsAdmin,
			IsSuperAdmin: p.IsSuperAdmin,
		}
		if parsed, err := types.ParseJID(phoneJID); err == nil && parsed.Server == types.DefaultUserServer && parsed.User != "" {
			part.PhoneNumber = "+" + parsed.User
		}
		if store != nil {
			part.Name = store.GetContactName(ctx, phoneJID)
			if part.Name == "" && phoneJID != originalJID {
				part.Name = store.GetContactName(ctx, originalJID)
			}
			if part.Name == "" && !p.LID.IsEmpty() {
				part.Name = store.GetContactName(ctx, p.LID.String())
			}
		}
		if part.Name == "" && p.DisplayName != "" {
			part.Name = p.DisplayName
		}
		participants = append(participants, part)
	}
	return participants
}

// storeGroup caches a group's name and member list locally
func (h *EventHandler) storeGroup(ctx context.Context, info *types.GroupInfo) error {
	jid := info.JID.String()
	chat := &Chat{
		JID:          jid,
		CanonicalJID: jid,
		Name:         info.GroupName.Name,
		IsGroup:      true,
	}
	if err := h.store.UpdateChat(ctx, chat); err != nil {
		return fmt.Errorf("update chat: %w", err)
	}
	parts := buildParticipants(ctx, h.client, h.store, info)
	if err := h.store.ReplaceGroupParticipants(ctx, jid, parts); err != nil {
		return fmt.Errorf("store participants: %w", err)
	}
	return nil
}

// syncGroups refreshes names and members of every joined group in one
// server round trip. Skipped if a sync ran within groupSyncInterval
// unless force is set.
func (h *EventHandler) syncGroups(ctx context.Context, force bool) (int, error) {
	groupSyncMu.Lock()
	defer groupSyncMu.Unlock()

	if !force {
		last, _ := h.store.GetSyncState(ctx, "last_group_sync")
		if t, err := time.Parse(time.RFC3339, last); err == nil && time.Since(t) < groupSyncInterval {
			return 0, nil
		}
	}

	groups, err := h.client.GetJoinedGroups(ctx)
	if err != nil {
		h.log("Group sync failed: %v", err)
		return 0, err
	}

	var stored int
	for _, g := range groups {
		if err := h.storeGroup(ctx, g); err != nil {
			h.log("Error storing group %s: %v", g.JID, err)
			continue
		}
		stored++
	}
	h.log("Group sync complete: %d/%d groups stored", stored, len(groups))
	h.store.SetSyncState(ctx, "last_group_sync", time.Now().Format(time.RFC3339))
	return stored, nil
}

// handleJoinedGroup caches a group we just created or were added to
func (h *EventHandler) handleJoinedGroup(evt *events.JoinedGroup) {
	ctx := context.Background()
	if err := h.storeGroup(ctx, &evt.GroupInfo); err != nil {
		h.log("Error storing joined group %s: %v", evt.JID, err)
		return
	}
	h.log("Stored joined group %s (%q, %d participants)", evt.JID, evt.GroupName.Name, len(evt.Participants))
}

// handleGroupInfo applies group metadata changes: name edits are stored
// directly; membership changes trigger a single refetch of that group.
func (h *EventHandler) handleGroupInfo(evt *events.GroupInfo) {
	ctx := context.Background()
	jid := evt.JID.String()

	if evt.Name != nil {
		chat := &Chat{JID: jid, CanonicalJID: jid, Name: evt.Name.Name, IsGroup: true}
		if err := h.store.UpdateChat(ctx, chat); err != nil {
			h.log("Error renaming group %s: %v", jid, err)
		} else {
			h.log("Group %s renamed to %q", jid, evt.Name.Name)
		}
	}

	if len(evt.Join)+len(evt.Leave)+len(evt.Promote)+len(evt.Demote) == 0 {
		return
	}
	info, err := h.client.GetGroupInfo(ctx, evt.JID)
	if err != nil {
		h.log("Error refetching group %s after membership change: %v", jid, err)
		return
	}
	if err := h.storeGroup(ctx, info); err != nil {
		h.log("Error storing group %s: %v", jid, err)
		return
	}
	h.log("Refreshed group %s membership (%d participants)", jid, len(info.Participants))
}

// parseInviteCode extracts the invite code from a chat.whatsapp.com link
// or returns a bare code unchanged
func parseInviteCode(link string) (string, error) {
	code := strings.TrimSpace(link)
	for _, prefix := range []string{"https://", "http://", "chat.whatsapp.com/", "invite/"} {
		code = strings.TrimPrefix(code, prefix)
	}
	code = strings.TrimSuffix(code, "/")
	if i := strings.IndexAny(code, "?#"); i >= 0 {
		code = code[:i]
	}
	if code == "" || strings.ContainsAny(code, "/ ") {
		return "", fmt.Errorf("invalid invite link: %q", link)
	}
	return code, nil
}
