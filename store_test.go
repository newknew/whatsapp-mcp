package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *MessageStore {
	t.Helper()
	s, err := NewMessageStore(filepath.Join(t.TempDir(), "messages.db"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func chatFlag(t *testing.T, s *MessageStore, jid string) (bool, string) {
	t.Helper()
	var isGroup bool
	var name *string
	err := s.db.QueryRow(`SELECT is_group, name FROM chats WHERE jid = ?`, jid).Scan(&isGroup, &name)
	if err != nil {
		t.Fatalf("chat %s: %v", jid, err)
	}
	if name == nil {
		return isGroup, ""
	}
	return isGroup, *name
}

// A group discovered only via real-time messages (no history sync) must
// still be flagged as a group.
func TestStoreMessageFlagsGroupChat(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	msg := &Message{
		ID: "m1", ChatJID: "120363431575923885@g.us", SenderJID: "1@lid",
		Timestamp: time.Now(), Content: "hi", Type: "text",
	}
	if err := s.StoreMessage(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if isGroup, _ := chatFlag(t, s, msg.ChatJID); !isGroup {
		t.Errorf("chat %s not flagged as group", msg.ChatJID)
	}
}

// Rows created by older versions have is_group=0 for @g.us JIDs.
func TestMigrationRepairsGroupFlag(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	_, err := s.db.Exec(`INSERT INTO chats (jid, canonical_jid, is_group, last_message_time)
		VALUES ('123@g.us', '123@g.us', 0, 1)`)
	if err != nil {
		t.Fatal(err)
	}
	s.RunMigrations(ctx)
	if isGroup, _ := chatFlag(t, s, "123@g.us"); !isGroup {
		t.Error("migration left @g.us chat with is_group=0")
	}
}

// Groups are findable by name and by member phone number or name.
func TestChatFilterByNameAndParticipant(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	group := "120363431575923885@g.us"
	other := "120363000000000000@g.us"
	for _, jid := range []string{group, other} {
		if err := s.StoreMessage(ctx, &Message{ID: "m-" + jid, ChatJID: jid, SenderJID: "1@lid",
			Timestamp: time.Now(), Content: "x", Type: "text"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.UpdateChat(ctx, &Chat{JID: group, Name: "Monterey Freediving, Nov 7-8", IsGroup: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateChat(ctx, &Chat{JID: other, Name: "Unrelated", IsGroup: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.StoreContact(ctx, &Contact{JID: "14086211426@s.whatsapp.net", FullName: "Rebekah Phillips"}); err != nil {
		t.Fatal(err)
	}
	err := s.ReplaceGroupParticipants(ctx, group, []ParticipantInfo{
		{JID: "14086211426@s.whatsapp.net", PhoneNumber: "+14086211426"},
		{JID: "33615741435@s.whatsapp.net", Name: "Apnée Marseille", IsSuperAdmin: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		filter ChatFilter
		want   []string
	}{
		{"name substring", ChatFilter{Query: "monterey"}, []string{group}},
		{"jid substring", ChatFilter{Query: "431575923885"}, []string{group}},
		{"participant phone", ChatFilter{Participant: "+1 (408) 621-1426"}, []string{group}},
		{"participant contact name", ChatFilter{Participant: "rebekah"}, []string{group}},
		{"participant push name", ChatFilter{Participant: "Marseille"}, []string{group}},
		{"no match", ChatFilter{Participant: "nobody"}, nil},
		{"unfiltered", ChatFilter{}, []string{group, other}},
	}
	for _, tc := range cases {
		chats, err := s.GetChatsWithMessages(ctx, 50, 0, tc.filter)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got := map[string]bool{}
		for _, c := range chats {
			got[c.JID] = true
		}
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			continue
		}
		for _, w := range tc.want {
			if !got[w] {
				t.Errorf("%s: missing %s in %v", tc.name, w, got)
			}
		}
	}

	parts, err := s.GetGroupParticipants(ctx, group)
	if err != nil || len(parts) != 2 {
		t.Fatalf("participants: %v, %v", parts, err)
	}
	if parts[0].JID != "33615741435@s.whatsapp.net" || parts[1].PhoneNumber != "+14086211426" {
		t.Errorf("unexpected participants: %+v", parts)
	}
}

func TestParseInviteCode(t *testing.T) {
	cases := map[string]string{
		"https://chat.whatsapp.com/KisqWk0oOQsC05LB2sN9IR": "KisqWk0oOQsC05LB2sN9IR",
		"chat.whatsapp.com/KisqWk0oOQsC05LB2sN9IR/":        "KisqWk0oOQsC05LB2sN9IR",
		"https://chat.whatsapp.com/invite/Abc123?mode=r_t": "Abc123",
		"KisqWk0oOQsC05LB2sN9IR":                           "KisqWk0oOQsC05LB2sN9IR",
		"":                                                 "",
		"https://chat.whatsapp.com/a/b":                    "",
	}
	for in, want := range cases {
		got, err := parseInviteCode(in)
		if want == "" {
			if err == nil {
				t.Errorf("%q: expected error, got %q", in, got)
			}
			continue
		}
		if err != nil || got != want {
			t.Errorf("%q: got %q, %v; want %q", in, got, err, want)
		}
	}
}

func TestParseParticipantJIDs(t *testing.T) {
	got, err := parseParticipantJIDs("+1 (408) 621-1426, 33615741435@s.whatsapp.net\n260305183576222@lid")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"14086211426@s.whatsapp.net", "33615741435@s.whatsapp.net", "260305183576222@lid"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i].String() != want[i] {
			t.Errorf("[%d] got %s, want %s", i, got[i], want[i])
		}
	}
	for _, bad := range []string{"", "bob", "123", "x@g.us"} {
		if _, err := parseParticipantJIDs(bad); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}
}
