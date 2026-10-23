package main

import (
	"context"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// ParticipantResult reports the outcome of one membership change
type ParticipantResult struct {
	JID     string `json:"jid"`
	Name    string `json:"name,omitempty"`
	OK      bool   `json:"ok"`
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// participantErrorText explains WhatsApp's per-participant status codes
func participantErrorText(code int) string {
	switch code {
	case 0:
		return ""
	case 401:
		return "this person has blocked you"
	case 403:
		return "their privacy settings block being added; send them the invite link"
	case 404:
		return "not a WhatsApp user"
	case 408:
		return "recently left the group; they must rejoin via invite link"
	case 409:
		return "already a member"
	case 500:
		return "group is full"
	default:
		return fmt.Sprintf("WhatsApp error %d", code)
	}
}

// parseParticipantJIDs accepts a comma or newline separated list of phone
// numbers (any formatting) and JIDs (@s.whatsapp.net or @lid)
func parseParticipantJIDs(list string) ([]types.JID, error) {
	var jids []types.JID
	seen := map[string]bool{}
	for _, raw := range strings.FieldsFunc(list, func(r rune) bool { return r == ',' || r == '\n' || r == ';' }) {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		var jid types.JID
		if strings.Contains(item, "@") {
			parsed, err := types.ParseJID(item)
			if err != nil {
				return nil, fmt.Errorf("invalid JID %q: %w", item, err)
			}
			if parsed.Server != types.DefaultUserServer && parsed.Server != types.HiddenUserServer {
				return nil, fmt.Errorf("not a user JID: %q", item)
			}
			jid = parsed
		} else {
			digits := normalizeParticipantQuery(item)
			if !isDigits(digits) || len(digits) < 7 {
				return nil, fmt.Errorf("not a phone number or JID: %q", item)
			}
			jid = types.NewJID(digits, types.DefaultUserServer)
		}
		if !seen[jid.String()] {
			seen[jid.String()] = true
			jids = append(jids, jid)
		}
	}
	if len(jids) == 0 {
		return nil, fmt.Errorf("no participants given")
	}
	return jids, nil
}

func isDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return s != ""
}

// participantResults converts whatsmeow's per-participant response
func (c *Client) participantResults(ctx context.Context, parts []types.GroupParticipant) []ParticipantResult {
	results := make([]ParticipantResult, 0, len(parts))
	for _, p := range parts {
		jid := p.JID.String()
		if !p.PhoneNumber.IsEmpty() {
			jid = p.PhoneNumber.String()
		} else {
			jid = resolvePhoneJID(ctx, c.client, jid)
		}
		r := ParticipantResult{JID: jid, OK: p.Error == 0, Code: p.Error, Message: participantErrorText(p.Error)}
		if c.msgStore != nil {
			r.Name = c.msgStore.GetContactName(ctx, jid)
		}
		results = append(results, r)
	}
	return results
}

// refreshGroup re-fetches a group from the server and updates the cache
func (c *Client) refreshGroup(ctx context.Context, jid types.JID) *GroupInfoResult {
	info, err := c.client.GetGroupInfo(ctx, jid)
	if err != nil {
		logToFile("client", "Error refreshing group %s: %v", jid, err)
		return nil
	}
	if c.msgStore != nil {
		if err := NewEventHandler(c.client, c.msgStore).storeGroup(ctx, info); err != nil {
			logToFile("client", "Error caching group %s: %v", jid, err)
		}
	}
	return c.groupResult(ctx, info, "live")
}

// CreateGroup creates a group with the given name and members. The creator
// is added by the server. Members who could not be added are reported, not
// fatal.
func (c *Client) CreateGroup(ctx context.Context, name string, participants []types.JID) (*GroupInfoResult, []ParticipantResult, error) {
	if c.client == nil {
		return nil, nil, fmt.Errorf("client not connected")
	}
	if strings.TrimSpace(name) == "" {
		return nil, nil, fmt.Errorf("group name is required")
	}
	info, err := c.client.CreateGroup(ctx, whatsmeow.ReqCreateGroup{Name: name, Participants: participants})
	if err != nil {
		return nil, nil, fmt.Errorf("create group: %w", err)
	}
	if c.msgStore != nil {
		if err := NewEventHandler(c.client, c.msgStore).storeGroup(ctx, info); err != nil {
			logToFile("client", "Error caching new group %s: %v", info.JID, err)
		}
	}
	return c.groupResult(ctx, info, "live"), c.participantResults(ctx, info.Participants), nil
}

// UpdateGroupMembers adds, removes, promotes, or demotes members
func (c *Client) UpdateGroupMembers(ctx context.Context, jid string, action string, participants []types.JID) ([]ParticipantResult, *GroupInfoResult, error) {
	if c.client == nil {
		return nil, nil, fmt.Errorf("client not connected")
	}
	groupJID, err := parseGroupJID(jid)
	if err != nil {
		return nil, nil, err
	}
	var change whatsmeow.ParticipantChange
	switch action {
	case "add":
		change = whatsmeow.ParticipantChangeAdd
	case "remove":
		change = whatsmeow.ParticipantChangeRemove
	case "promote":
		change = whatsmeow.ParticipantChangePromote
	case "demote":
		change = whatsmeow.ParticipantChangeDemote
	default:
		return nil, nil, fmt.Errorf("action must be add, remove, promote, or demote (got %q)", action)
	}
	parts, err := c.client.UpdateGroupParticipants(ctx, groupJID, participants, change)
	if err != nil {
		return nil, nil, fmt.Errorf("%s participants: %w", action, err)
	}
	return c.participantResults(ctx, parts), c.refreshGroup(ctx, groupJID), nil
}

// GroupMetaUpdate lists which metadata fields to change
type GroupMetaUpdate struct {
	Name     string // new subject, "" = unchanged
	Topic    string // new description, "" = unchanged
	IconPath string // image file path, "" = unchanged, "none" = remove
}

// UpdateGroupMeta changes the group subject, description, and icon
func (c *Client) UpdateGroupMeta(ctx context.Context, jid string, upd GroupMetaUpdate) ([]string, *GroupInfoResult, error) {
	if c.client == nil {
		return nil, nil, fmt.Errorf("client not connected")
	}
	groupJID, err := parseGroupJID(jid)
	if err != nil {
		return nil, nil, err
	}
	var changed []string
	if upd.Name != "" {
		if err := c.client.SetGroupName(ctx, groupJID, upd.Name); err != nil {
			return changed, nil, fmt.Errorf("set name: %w", err)
		}
		changed = append(changed, "name")
	}
	if upd.Topic != "" {
		if err := c.client.SetGroupTopic(ctx, groupJID, "", "", upd.Topic); err != nil {
			return changed, nil, fmt.Errorf("set topic: %w", err)
		}
		changed = append(changed, "topic")
	}
	if upd.IconPath != "" {
		var data []byte
		if upd.IconPath != "none" {
			data, err = readIconJPEG(upd.IconPath)
			if err != nil {
				return changed, nil, err
			}
		}
		if _, err := c.client.SetGroupPhoto(ctx, groupJID, data); err != nil {
			return changed, nil, fmt.Errorf("set icon: %w", err)
		}
		changed = append(changed, "icon")
	}
	if len(changed) == 0 {
		return nil, nil, fmt.Errorf("nothing to update: give name, topic, or icon")
	}
	return changed, c.refreshGroup(ctx, groupJID), nil
}

// GetInviteLink returns the group's current invite link
func (c *Client) GetInviteLink(ctx context.Context, jid types.JID) string {
	if c.client == nil {
		return ""
	}
	link, err := c.client.GetGroupInviteLink(ctx, jid, false)
	if err != nil {
		logToFile("client", "Error getting invite link for %s: %v", jid, err)
		return ""
	}
	return link
}

// parseGroupJID validates a @g.us JID
func parseGroupJID(jid string) (types.JID, error) {
	parsed, err := types.ParseJID(strings.TrimSpace(jid))
	if err != nil {
		return types.JID{}, fmt.Errorf("invalid JID: %w", err)
	}
	if parsed.Server != types.GroupServer {
		return types.JID{}, fmt.Errorf("not a group JID (must end with @g.us): %s", jid)
	}
	return parsed, nil
}

// readIconJPEG loads an image file, converting to JPEG as WhatsApp requires
func readIconJPEG(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("icon not readable: %w", err)
	}
	mimeType := mime.TypeByExtension(filepath.Ext(path))
	if mimeType == "image/jpeg" {
		return data, nil
	}
	converted, err := convertToJPEG(data)
	if err != nil {
		return nil, fmt.Errorf("icon must be an image (convert to JPEG failed): %w", err)
	}
	return converted, nil
}
