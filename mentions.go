package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/encoding/protojson"
)

// Mention is a participant tagged in a message
type Mention struct {
	JID  string `json:"jid"`            // phone JID when known, else the wire JID
	Name string `json:"name,omitempty"` // contact name when known
}

// wireMention is a mention resolved for sending
type wireMention struct {
	Phone types.JID // phone JID if known (may be empty for bare LIDs)
	Wire  types.JID // JID placed in ContextInfo.MentionedJID
}

// mentionToken matches @<digits> or @+<digits> in message text
var mentionToken = regexp.MustCompile(`@\+?(\d{5,})`)

// applyMentions rewrites @phone/@LID tokens to the wire form and returns
// the MentionedJID list; every mention must appear in the text.
func applyMentions(text string, mentions []wireMention) (string, []string, error) {
	byUser := map[string]int{}
	for i, m := range mentions {
		byUser[m.Wire.User] = i
		if !m.Phone.IsEmpty() {
			byUser[m.Phone.User] = i
		}
	}
	used := make([]bool, len(mentions))
	out := mentionToken.ReplaceAllStringFunc(text, func(tok string) string {
		digits := strings.TrimLeft(tok, "@+")
		i, ok := byUser[digits]
		if !ok {
			return tok
		}
		used[i] = true
		return "@" + mentions[i].Wire.User
	})
	jids := make([]string, 0, len(mentions))
	for i, m := range mentions {
		if !used[i] {
			id := m.Phone.User
			if id == "" {
				id = m.Wire.User
			}
			return "", nil, fmt.Errorf("text has no @%s token for mention %s", id, m.Wire)
		}
		jids = append(jids, m.Wire.String())
	}
	return out, jids, nil
}

// contextInfoOf returns the ContextInfo of whichever message type is set
func contextInfoOf(msg *waE2E.Message) *waE2E.ContextInfo {
	switch {
	case msg == nil:
		return nil
	case msg.ExtendedTextMessage != nil:
		return msg.ExtendedTextMessage.GetContextInfo()
	case msg.ImageMessage != nil:
		return msg.ImageMessage.GetContextInfo()
	case msg.VideoMessage != nil:
		return msg.VideoMessage.GetContextInfo()
	case msg.DocumentMessage != nil:
		return msg.DocumentMessage.GetContextInfo()
	case msg.AudioMessage != nil:
		return msg.AudioMessage.GetContextInfo()
	case msg.StickerMessage != nil:
		return msg.StickerMessage.GetContextInfo()
	}
	return nil
}

// extractMentions returns the MentionedJID list from a stored proto JSON
func extractMentions(rawProto string) []string {
	if rawProto == "" {
		return nil
	}
	var msg waE2E.Message
	if err := protojson.Unmarshal([]byte(rawProto), &msg); err != nil {
		return nil
	}
	return contextInfoOf(&msg).GetMentionedJID()
}

// mentionList wraps raw mention JIDs for the Message struct
func mentionList(rawProto string) []Mention {
	jids := extractMentions(rawProto)
	if len(jids) == 0 {
		return nil
	}
	out := make([]Mention, len(jids))
	for i, j := range jids {
		out[i] = Mention{JID: j}
	}
	return out
}

// renderMentionNames replaces @<user> tokens with @Name where known
func renderMentionNames(content string, names map[string]string) string {
	if len(names) == 0 {
		return content
	}
	return mentionToken.ReplaceAllStringFunc(content, func(tok string) string {
		if name, ok := names[strings.TrimLeft(tok, "@+")]; ok && name != "" {
			return "@" + name
		}
		return tok
	})
}

// ResolveMentions maps participant JIDs to the form WhatsApp expects,
// preferring the LID when whatsmeow knows the phone-to-LID mapping.
func (c *Client) ResolveMentions(ctx context.Context, jids []types.JID) []wireMention {
	out := make([]wireMention, 0, len(jids))
	for _, jid := range jids {
		m := wireMention{Wire: jid}
		if jid.Server == types.HiddenUserServer {
			if pn, err := types.ParseJID(resolvePhoneJID(ctx, c.client, jid.String())); err == nil && pn.Server == types.DefaultUserServer {
				m.Phone = pn
			}
		} else {
			m.Phone = jid
			if c.client != nil && c.client.Store != nil && c.client.Store.LIDs != nil {
				if lid, err := c.client.Store.LIDs.GetLIDForPN(ctx, jid); err == nil && !lid.IsEmpty() {
					m.Wire = lid
				}
			}
		}
		out = append(out, m)
	}
	return out
}

// mentionDetails resolves wire JIDs to phone JIDs and contact names
func (c *Client) mentionDetails(ctx context.Context, jids []string) ([]Mention, map[string]string) {
	out := make([]Mention, 0, len(jids))
	names := map[string]string{}
	for _, raw := range jids {
		m := Mention{JID: resolvePhoneJID(ctx, c.client, raw)}
		if c.msgStore != nil {
			m.Name = c.msgStore.GetContactName(ctx, m.JID)
			if m.Name == "" && m.JID != raw {
				m.Name = c.msgStore.GetContactName(ctx, raw)
			}
		}
		if m.Name != "" {
			for _, j := range []string{raw, m.JID} {
				if p, err := types.ParseJID(j); err == nil {
					names[p.User] = m.Name
				}
			}
		}
		out = append(out, m)
	}
	return out, names
}

// DecorateMentions resolves mention names and renders @Name in content
func (c *Client) DecorateMentions(ctx context.Context, msgs []Message) {
	for i := range msgs {
		if len(msgs[i].Mentions) == 0 {
			continue
		}
		raw := make([]string, len(msgs[i].Mentions))
		for j, m := range msgs[i].Mentions {
			raw[j] = m.JID
		}
		var names map[string]string
		msgs[i].Mentions, names = c.mentionDetails(ctx, raw)
		msgs[i].Content = renderMentionNames(msgs[i].Content, names)
	}
}
