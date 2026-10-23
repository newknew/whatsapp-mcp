package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func lid(user string) types.JID   { return types.NewJID(user, types.HiddenUserServer) }
func phone(user string) types.JID { return types.NewJID(user, types.DefaultUserServer) }

// Phone and LID tokens in the text are rewritten to the wire form, and
// the MentionedJID list carries the matching full JIDs.
func TestApplyMentions(t *testing.T) {
	jeremy := wireMention{Phone: phone("33615741435"), Wire: lid("131516344594528")}
	rebekah := wireMention{Phone: phone("14086211426"), Wire: lid("260305183576222")}

	text := "hi @33615741435, @+33615741435, @131516344594528 and @14086211426 (not @336157414351)"
	got, jids, err := applyMentions(text, []wireMention{jeremy, rebekah})
	if err != nil {
		t.Fatal(err)
	}
	want := "hi @131516344594528, @131516344594528, @131516344594528 and @260305183576222 (not @336157414351)"
	if got != want {
		t.Errorf("text:\n got %q\nwant %q", got, want)
	}
	if strings.Join(jids, ",") != "131516344594528@lid,260305183576222@lid" {
		t.Errorf("mentioned JIDs: %v", jids)
	}
}

// Without a LID mapping the mention falls back to the phone JID.
func TestApplyMentionsPhoneFallback(t *testing.T) {
	m := wireMention{Phone: phone("33615741435"), Wire: phone("33615741435")}
	got, jids, err := applyMentions("yo @+33615741435", []wireMention{m})
	if err != nil {
		t.Fatal(err)
	}
	if got != "yo @33615741435" || len(jids) != 1 || jids[0] != "33615741435@s.whatsapp.net" {
		t.Errorf("got %q %v", got, jids)
	}
}

// A mention with no @token in the text is an error naming the number.
func TestApplyMentionsMissingToken(t *testing.T) {
	m := wireMention{Phone: phone("33615741435"), Wire: lid("131516344594528")}
	_, _, err := applyMentions("no tag here", []wireMention{m})
	if err == nil || !strings.Contains(err.Error(), "33615741435") {
		t.Errorf("expected error naming the number, got %v", err)
	}
}

// Mentions are read from ContextInfo on text and media messages.
func TestExtractMentions(t *testing.T) {
	cases := map[string][]string{
		`{"extendedTextMessage":{"text":"x","contextInfo":{"mentionedJID":["260305183576222@lid","131516344594528@lid"]}}}`: {"260305183576222@lid", "131516344594528@lid"},
		`{"imageMessage":{"caption":"x","contextInfo":{"mentionedJID":["260305183576222@lid"]}}}`:                           {"260305183576222@lid"},
		`{"conversation":"plain"}`: nil,
		`not json`:                 nil,
		``:                         nil,
	}
	for raw, want := range cases {
		got := extractMentions(raw)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: got %v want %v", raw, got, want)
		}
	}
}

// @<user> tokens are rendered as @Name when the name is known.
func TestRenderMentionNames(t *testing.T) {
	names := map[string]string{"260305183576222": "Rebekah Phillips", "131516344594528": "Jéremy Molins"}
	got := renderMentionNames("@260305183576222 : hi @131516344594528 and @999 (@2603051835762220)", names)
	want := "@Rebekah Phillips : hi @Jéremy Molins and @999 (@2603051835762220)"
	if got != want {
		t.Errorf("\n got %q\nwant %q", got, want)
	}
}

// Every message read path surfaces mentions from the stored proto.
func TestStoreReturnsMentions(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	group := "120363431575923885@g.us"
	raw := `{"extendedTextMessage":{"text":"@260305183576222 hi","contextInfo":{"mentionedJID":["260305183576222@lid"]}}}`
	msg := &Message{ID: "m1", ChatJID: group, CanonicalJID: group, SenderJID: "1@lid",
		Timestamp: time.Now(), Content: "@260305183576222 hi", Type: "text", RawProto: raw}
	if err := s.StoreMessage(ctx, msg); err != nil {
		t.Fatal(err)
	}

	check := func(name string, msgs []Message, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(msgs) != 1 || len(msgs[0].Mentions) != 1 || msgs[0].Mentions[0].JID != "260305183576222@lid" {
			t.Errorf("%s: mentions not returned: %+v", name, msgs)
		}
	}
	got, err := s.GetMessages(ctx, group, 10, nil)
	check("GetMessages", got, err)
	got, err = s.GetMessagesByJIDs(ctx, []string{group}, 10)
	check("GetMessagesByJIDs", got, err)
	got, err = s.QueryMessages(ctx, MessageQuery{ChatJID: group, Limit: 10})
	check("QueryMessages", got, err)
	chats, err := s.GetChatsWithMessages(ctx, 10, 5, ChatFilter{})
	if err != nil || len(chats) != 1 {
		t.Fatalf("GetChatsWithMessages: %v %d", err, len(chats))
	}
	check("GetChatsWithMessages", chats[0].Messages, nil)
}
