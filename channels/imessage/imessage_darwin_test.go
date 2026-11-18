//go:build darwin

package imessage

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

const (
	testMeHandle = "me@icloud.com"
	testMyPhone  = "+8613800000000"
	testFriend   = "+8613900000000"
	selfChatGUID = "iMessage;-;" + testMeHandle
	phoneGUID    = "iMessage;-;" + testMyPhone
	friendGUID   = "iMessage;-;" + testFriend
	groupGUID    = "iMessage;+;chat123456789"
)

var testCreds = imsdk.Credentials{
	"handle":  testMeHandle,
	"handles": testMeHandle + "," + testMyPhone,
	"since":   "0",
}

type fakeFileRow struct {
	Path string
	Name string
	Mime string
	Size int64
}

type addMsgOpts struct {
	GUID    string
	Text    *string
	FromMe  *bool
	AtMs    int64
	Tapback bool
	File    *fakeFileRow
	ToSet   bool
	To      *string
}

type fakeMessagesDB struct {
	mu    sync.Mutex
	path  string
	db    *sqliteDB
	chats map[string]int64
}

func newFakeMessagesDB(t *testing.T, path string) *fakeMessagesDB {
	t.Helper()
	db, err := openReadWriteSQLite(path)
	if err != nil {
		t.Fatalf("openReadWriteSQLite: %v", err)
	}
	schema := `
		PRAGMA journal_mode = WAL;
		CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT);
		CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, style INTEGER, chat_identifier TEXT, last_addressed_handle TEXT);
		CREATE TABLE message (ROWID INTEGER PRIMARY KEY, guid TEXT, text TEXT, attributedBody BLOB, date INTEGER,
			handle_id INTEGER, is_from_me INTEGER, associated_message_type INTEGER DEFAULT 0,
			item_type INTEGER DEFAULT 0, service TEXT, account TEXT, destination_caller_id TEXT);
		CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
		CREATE TABLE attachment (ROWID INTEGER PRIMARY KEY, filename TEXT, mime_type TEXT, transfer_name TEXT, total_bytes INTEGER);
		CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);
	`
	if err := db.exec(schema); err != nil {
		_ = db.close()
		t.Fatalf("create schema: %v", err)
	}
	fm := &fakeMessagesDB{
		path:  path,
		db:    db,
		chats: make(map[string]int64),
	}
	fm.insertChat(selfChatGUID, 45, testMeHandle)
	fm.insertChat(phoneGUID, 45, testMyPhone)
	fm.insertChat(friendGUID, 45, testFriend)
	fm.insertChat(groupGUID, 43, "chat123456789")
	t.Cleanup(func() {
		fm.close()
	})
	return fm
}

func (fm *fakeMessagesDB) close() {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.db != nil {
		_ = fm.db.close()
		fm.db = nil
	}
}

func (fm *fakeMessagesDB) insertChat(guid string, style int, identifier string) {
	stmt, err := fm.db.prepare("INSERT INTO chat (guid, style, chat_identifier) VALUES (?, ?, ?)", guid, style, identifier)
	if err != nil {
		panic(err)
	}
	_, _ = stmt.step()
	stmt.finalize()
	fm.chats[guid] = fm.db.lastInsertRowID()
}

func (fm *fakeMessagesDB) setAddressed(guid, handle string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	stmt, err := fm.db.prepare("UPDATE chat SET last_addressed_handle = ? WHERE guid = ?", handle, guid)
	if err != nil {
		panic(err)
	}
	_, _ = stmt.step()
	stmt.finalize()
}

func (fm *fakeMessagesDB) sendsFrom(guid string) string {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	stmt, err := fm.db.prepare("SELECT chat_identifier, last_addressed_handle FROM chat WHERE guid = ?", guid)
	if err != nil {
		return ""
	}
	defer stmt.finalize()
	if has, err := stmt.step(); err == nil && has {
		id, _ := stmt.colText(0)
		if from, ok := stmt.colText(1); ok && from != "" {
			return from
		}
		return id
	}
	return ""
}

func (fm *fakeMessagesDB) add(chat string, opts addMsgOpts) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	fromMe := true
	if opts.FromMe != nil {
		fromMe = *opts.FromMe
	}
	atMs := opts.AtMs
	if atMs == 0 {
		atMs = time.Now().UnixMilli()
	}
	dateNs := (atMs - AppleEpochMs) * 1_000_000

	var blob any
	if opts.Text != nil {
		blob = buildAttributedBody(*opts.Text)
	}
	fromMeInt := 0
	if fromMe {
		fromMeInt = 1
	}
	assocType := 0
	if opts.Tapback {
		assocType = 2000
	}
	var toVal any
	if opts.ToSet {
		if opts.To != nil {
			toVal = *opts.To
		}
	} else if fromMe {
		toVal = testMyPhone
	}

	stmt, err := fm.db.prepare(
		`INSERT INTO message (guid, text, attributedBody, date, handle_id, is_from_me, associated_message_type,
		 service, account, destination_caller_id) VALUES (?, NULL, ?, ?, 1, ?, ?, 'iMessage', ?, ?)`,
		opts.GUID, blob, dateNs, fromMeInt, assocType, "E:"+testMeHandle, toVal,
	)
	if err != nil {
		panic(err)
	}
	_, _ = stmt.step()
	stmt.finalize()
	rowID := fm.db.lastInsertRowID()

	jStmt, err := fm.db.prepare("INSERT INTO chat_message_join (chat_id, message_id) VALUES (?, ?)", fm.chats[chat], rowID)
	if err != nil {
		panic(err)
	}
	_, _ = jStmt.step()
	jStmt.finalize()

	if opts.File != nil {
		aStmt, err := fm.db.prepare(
			"INSERT INTO attachment (filename, mime_type, transfer_name, total_bytes) VALUES (?, ?, ?, ?)",
			opts.File.Path, opts.File.Mime, opts.File.Name, opts.File.Size,
		)
		if err != nil {
			panic(err)
		}
		_, _ = aStmt.step()
		aStmt.finalize()
		attID := fm.db.lastInsertRowID()

		majStmt, err := fm.db.prepare("INSERT INTO message_attachment_join (message_id, attachment_id) VALUES (?, ?)", rowID, attID)
		if err != nil {
			panic(err)
		}
		_, _ = majStmt.step()
		majStmt.finalize()
	}
}

type scriptCall struct {
	Script string
	Args   []string
}

type fakeScriptHarness struct {
	mu        sync.Mutex
	calls     []scriptCall
	failWith  error
	accounts  string
	keepMark  bool
	forceFrom string
	seq       int
	messages  *fakeMessagesDB
}

func newFakeScripts(messages *fakeMessagesDB, keepMark bool, forceFrom string) *fakeScriptHarness {
	return &fakeScriptHarness{
		accounts:  "E:" + testMeHandle + "\nE:" + testMeHandle + "\n",
		keepMark:  keepMark,
		forceFrom: forceFrom,
		messages:  messages,
	}
}

func (fs *fakeScriptHarness) runner() ScriptRunner {
	return func(_ context.Context, script string, args []string, _ time.Duration) (string, error) {
		fs.mu.Lock()
		fs.calls = append(fs.calls, scriptCall{Script: script, Args: slices.Clone(args)})
		fail := fs.failWith
		accts := fs.accounts
		keepMark := fs.keepMark
		forceFrom := fs.forceFrom
		fs.seq++
		n := fs.seq
		fs.mu.Unlock()

		if fail != nil {
			return "", fail
		}
		if strings.Contains(script, "description of") {
			return accts, nil
		}
		target := ""
		if len(args) > 1 {
			target = args[1]
		}
		chat := target
		if !strings.Contains(chat, ";") {
			chat = "iMessage;-;" + target
		}
		if strings.Contains(script, "POSIX file") {
			p := args[0]
			marker := "\ufffc"
			fs.messages.add(chat, addMsgOpts{
				GUID: "sent-" + string(rune('0'+n)),
				Text: &marker,
				File: &fakeFileRow{Path: p, Name: filepath.Base(p), Mime: "text/plain", Size: 1},
			})
			return "", nil
		}
		parts := strings.Split(chat, ";")
		to := parts[len(parts)-1]
		from := forceFrom
		if from == "" {
			from = fs.messages.sendsFrom(chat)
		}
		if from == "" {
			from = to
		}
		txt := args[0]
		if !keepMark {
			txt = strings.ReplaceAll(txt, AgentMark, "")
		}
		sentGUID := "sent-" + itoaTest(n)
		echoGUID := "echo-" + itoaTest(n)
		fFalse := false
		fs.messages.add(chat, addMsgOpts{
			GUID:  sentGUID,
			Text:  &txt,
			ToSet: true,
			To:    &from,
		})
		fs.messages.add("iMessage;-;"+from, addMsgOpts{
			GUID:   echoGUID,
			Text:   &txt,
			FromMe: &fFalse,
			ToSet:  true,
			To:     &to,
		})
		return "", nil
	}
}

func itoaTest(n int) string {
	const digits = "0123456789"
	if n < 10 {
		return string(digits[n])
	}
	return itoaTest(n/10) + string(digits[n%10])
}

func (fs *fakeScriptHarness) setFail(err error) {
	fs.mu.Lock()
	fs.failWith = err
	fs.mu.Unlock()
}

func (fs *fakeScriptHarness) signOut() {
	fs.mu.Lock()
	fs.accounts = ""
	fs.mu.Unlock()
}

func (fs *fakeScriptHarness) getCalls() []scriptCall {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return slices.Clone(fs.calls)
}

type imRecorder struct {
	mu       sync.Mutex
	messages []imsdk.InboundMessage
	states   []imsdk.ConnectionState
}

func newIMRecorder() (*imRecorder, imsdk.ChannelEvents) {
	r := &imRecorder{}
	return r, imsdk.ChannelEvents{
		OnMessage: func(m imsdk.InboundMessage) {
			r.mu.Lock()
			r.messages = append(r.messages, m)
			r.mu.Unlock()
		},
		OnState: func(s imsdk.ConnectionState) {
			r.mu.Lock()
			r.states = append(r.states, s)
			r.mu.Unlock()
		},
	}
}

func waitIM(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func strP(s string) *string { return &s }
func boolP(b bool) *bool    { return &b }

func TestOnboardingAndSetupChecks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "chat.db")
	msgs := newFakeMessagesDB(t, dbPath)
	msgs.add(selfChatGUID, addMsgOpts{GUID: "old", Text: strP("hi")})

	scripts := newFakeScripts(msgs, true, "")
	ch := New(Options{
		DBPath:    dbPath,
		RunScript: scripts.runner(),
		Now:       func() time.Time { return time.UnixMilli(1000) },
	})

	var onboardEvs []imsdk.OnboardingEvent
	for ev := range ch.Onboard(ctx, nil, imsdk.OnboardingOptions{}) {
		onboardEvs = append(onboardEvs, ev)
	}
	if len(onboardEvs) != 1 || onboardEvs[0].Kind != imsdk.EventSucceeded || onboardEvs[0].OwnerID != testMeHandle ||
		onboardEvs[0].Credentials["handles"] != testMeHandle+","+testMyPhone || onboardEvs[0].Credentials["since"] != "1000" {
		t.Fatalf("unexpected onboarding events: %+v", onboardEvs)
	}

	// Sign-out -> FailureNotSignedIn; -1743 -> FailurePermissionMissing
	scripts.signOut()
	var signedOutEvs []imsdk.OnboardingEvent
	for ev := range ch.Onboard(ctx, nil, imsdk.OnboardingOptions{}) {
		signedOutEvs = append(signedOutEvs, ev)
	}
	if signedOutEvs[0].Code != imsdk.FailureNotSignedIn {
		t.Fatalf("expected not_signed_in, got %+v", signedOutEvs)
	}

	scripts.setFail(&ScriptError{Message: "execution error: Not authorized to send Apple events to Messages. (-1743)"})
	var autoDeniedEvs []imsdk.OnboardingEvent
	for ev := range ch.Onboard(ctx, nil, imsdk.OnboardingOptions{}) {
		autoDeniedEvs = append(autoDeniedEvs, ev)
	}
	if autoDeniedEvs[0].Code != imsdk.FailurePermissionMissing {
		t.Fatalf("expected permission_missing on -1743, got %+v", autoDeniedEvs)
	}
	scripts.setFail(nil)

	// Setup checks with real temporary TCC.db via purego sqlite3
	appBundle := "com.example.KongTest"
	tccPath := filepath.Join(dir, "TCC.db")
	tccDB, err := openReadWriteSQLite(tccPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := tccDB.exec("CREATE TABLE access (service TEXT, client TEXT, client_type INTEGER, auth_value INTEGER, indirect_object_identifier TEXT)"); err != nil {
		t.Fatal(err)
	}
	ins, err := tccDB.prepare("INSERT INTO access VALUES ('kTCCServiceAppleEvents', ?, 0, ?, 'com.apple.MobileSMS')", appBundle, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = ins.step()
	ins.finalize()
	_ = tccDB.close()

	var opened [][]string
	probeScripts := newFakeScripts(msgs, true, "")
	setupCh := New(Options{
		DBPath:    dbPath,
		RunScript: probeScripts.runner(),
		Setup: SetupProbe{
			ActiveAccounts: func(context.Context) (*int, error) { return IntPtr(1), nil },
			TCCPath:        tccPath,
			BundleID:       appBundle,
			AppPath:        func(context.Context, string) (string, error) { return "/Applications/Kong.app", nil },
			Open: func(_ context.Context, args []string) error {
				opened = append(opened, slices.Clone(args))
				return nil
			},
		},
	})
	status, err := setupCh.Setup().Check(ctx)
	if err != nil {
		t.Fatalf("Setup.Check: %v", err)
	}
	if status.App != "Kong" {
		t.Fatalf("Setup.App = %q, want Kong", status.App)
	}
	wantChecks := []imsdk.SetupCheck{
		{ID: "signed_in", State: imsdk.SetupOK},
		{ID: "full_disk_access", State: imsdk.SetupOK},
		{ID: "automation", State: imsdk.SetupOK},
	}
	if !slices.Equal(status.Checks, wantChecks) {
		t.Fatalf("Setup.Checks = %+v, want %+v", status.Checks, wantChecks)
	}
	if len(probeScripts.getCalls()) != 0 {
		t.Fatal("Setup.Check must never invoke AppleScript")
	}

	// allow_automation triggers script and marks signed_in + automation ok
	allowScripts := newFakeScripts(msgs, true, "")
	allowCh := New(Options{
		DBPath:    dbPath,
		RunScript: allowScripts.runner(),
		Setup: SetupProbe{
			ActiveAccounts: func(context.Context) (*int, error) { return IntPtr(0), nil },
			TCCPath:        filepath.Join(dir, "missing-TCC.db"),
			BundleID:       appBundle,
			AppPath:        func(context.Context, string) (string, error) { return "/Applications/Kong.app", nil },
			Open: func(_ context.Context, args []string) error {
				opened = append(opened, slices.Clone(args))
				return nil
			},
		},
	})
	beforeAllow, _ := allowCh.Setup().Check(ctx)
	if beforeAllow.Checks[0].State != imsdk.SetupMissing || beforeAllow.Checks[2].State != imsdk.SetupMissing {
		t.Fatalf("unexpected pre-allow state: %+v", beforeAllow.Checks)
	}
	if err := allowCh.Setup().Act(ctx, "allow_automation"); err != nil {
		t.Fatalf("Act(allow_automation): %v", err)
	}
	afterAllow, _ := allowCh.Setup().Check(ctx)
	if afterAllow.Checks[0].State != imsdk.SetupOK || afterAllow.Checks[2].State != imsdk.SetupOK {
		t.Fatalf("unexpected post-allow state: %+v", afterAllow.Checks)
	}

	for _, act := range []string{"open_messages", "open_full_disk_access", "open_automation", "reveal_app"} {
		_ = allowCh.Setup().Act(ctx, act)
	}
	if len(opened) != 4 || opened[3][0] != "-R" || opened[3][1] != "/Applications/Kong.app" {
		t.Fatalf("unexpected open actions: %+v", opened)
	}
}

func TestSelfChatEchoTwinAndAttachments(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "chat.db")
	msgs := newFakeMessagesDB(t, dbPath)

	nowMs := time.Now().UnixMilli()
	msgs.add(selfChatGUID, addMsgOpts{GUID: "ancient", Text: strP("last week"), AtMs: nowMs - 7*86_400_000})
	msgs.add(selfChatGUID, addMsgOpts{GUID: "recent", Text: strP("recent self message"), AtMs: nowMs - 30_000})

	scripts := newFakeScripts(msgs, true, "")
	rec, events := newIMRecorder()
	conn, err := New(Options{
		DBPath:       dbPath,
		RunScript:    scripts.runner(),
		PollInterval: 15 * time.Millisecond,
	}).Connect(ctx, testCreds, events)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()

	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.messages) == 1
	})

	// Ignore friend DMs, group chats, and tapbacks; hear phone self-chat
	msgs.add(friendGUID, addMsgOpts{GUID: "friend-1", Text: strP("dinner?"), FromMe: boolP(false)})
	msgs.add(groupGUID, addMsgOpts{GUID: "group-1", Text: strP("hike?"), FromMe: boolP(false)})
	msgs.add(selfChatGUID, addMsgOpts{GUID: "tapback-1", Text: strP("Loved hi"), Tapback: true})
	msgs.add(phoneGUID, addMsgOpts{GUID: "by-phone", Text: strP("check calendar")})

	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.messages) == 2
	})

	// Outbound marked text + staged file attachment must NOT echo back as owner messages
	reportFile := filepath.Join(dir, "report.txt")
	if err := os.WriteFile(reportFile, []byte("MINGLE-FILE-OK"), 0o644); err != nil {
		t.Fatal(err)
	}
	sendRes, err := conn.Send(ctx, imsdk.OutboundMessage{
		Chat:    imsdk.ChatRef{PlatformID: selfChatGUID, Kind: imsdk.ChatDM},
		Text:    "**Done.** See `report.txt`",
		ReplyTo: "recent",
		Files:   []imsdk.OutboundFile{{Path: reportFile}},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if sendRes.Outcome != imsdk.OutcomeSent || !slices.Contains(sendRes.Degraded, imsdk.DegradedMarkdownFlattened) || !slices.Contains(sendRes.Degraded, imsdk.DegradedQuoteDropped) {
		t.Fatalf("unexpected SendResult: %+v", sendRes)
	}

	sc := scripts.getCalls()
	if len(sc) < 2 {
		t.Fatalf("expected text and file AppleScript calls, got %+v", sc)
	}
	if sc[0].Args[0] != "Done. See report.txt"+AgentMark {
		t.Fatalf("expected AgentMark on outbound text, got %q", sc[0].Args[0])
	}
	stagedPath := sc[1].Args[0]
	wantPrefix := filepath.Join(dir, "Attachments", "mingle")
	if !strings.HasPrefix(stagedPath, wantPrefix) || filepath.Base(stagedPath) != "report.txt" {
		t.Fatalf("staged path = %q, expected under %q", stagedPath, wantPrefix)
	}
	stagedBytes, err := os.ReadFile(stagedPath)
	if err != nil || string(stagedBytes) != "MINGLE-FILE-OK" {
		t.Fatalf("unexpected staged file content: %q (%v)", string(stagedBytes), err)
	}

	// Twin dedup: sent + received within 60s collapses to 1, third message still counts
	tBase := time.Now().UnixMilli()
	msgs.add(selfChatGUID, addMsgOpts{GUID: "twin-1", Text: strP("ping"), AtMs: tBase})
	msgs.add(selfChatGUID, addMsgOpts{GUID: "twin-2", Text: strP("ping"), AtMs: tBase + 400, FromMe: boolP(false)})
	msgs.add(selfChatGUID, addMsgOpts{GUID: "twin-3", Text: strP("ping"), AtMs: tBase + 1500})

	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.messages) == 4
	})
	time.Sleep(60 * time.Millisecond)

	rec.mu.Lock()
	var gotIDs []string
	for _, m := range rec.messages {
		gotIDs = append(gotIDs, m.PlatformID)
	}
	rec.mu.Unlock()

	wantIDs := []string{"recent", "by-phone", "twin-1", "twin-3"}
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("received message IDs = %v, want %v", gotIDs, wantIDs)
	}

	// Unmarked echo fallback via 10m sentTexts pair tracking
	_ = conn.Close()
	unmarkedScripts := newFakeScripts(msgs, false, "")
	rec2, ev2 := newIMRecorder()
	conn2, err := New(Options{
		DBPath:       dbPath,
		RunScript:    unmarkedScripts.runner(),
		PollInterval: 15 * time.Millisecond,
	}).Connect(ctx, imsdk.Credentials{
		"handle":  testMeHandle,
		"handles": testMeHandle + "," + testMyPhone,
		"since":   "9999999999999",
	}, ev2)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	_, _ = conn2.Send(ctx, imsdk.OutboundMessage{
		Chat: imsdk.ChatRef{PlatformID: selfChatGUID, Kind: imsdk.ChatDM},
		Text: "ok",
	})
	msgs.add(selfChatGUID, addMsgOpts{GUID: "owner-ok", Text: strP("ok")})
	waitIM(t, func() bool {
		rec2.mu.Lock()
		defer rec2.mu.Unlock()
		for _, m := range rec2.messages {
			if m.PlatformID == "owner-ok" {
				return true
			}
		}
		return false
	})
	rec2.mu.Lock()
	defer rec2.mu.Unlock()
	for _, m := range rec2.messages {
		if strings.HasPrefix(m.PlatformID, "sent-") || strings.HasPrefix(m.PlatformID, "echo-") {
			t.Fatalf("agent's own unmarked echo leaked as inbound: %+v", m)
		}
	}
}

func TestDualHandleReplyChatAndInodeReplacement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "chat.db")
	msgs := newFakeMessagesDB(t, dbPath)

	ownerWrites := func(guid, text string, receivedFirst bool) {
		at := time.Now().UnixMilli()
		phone := testMyPhone
		me := testMeHandle
		addSent := func() {
			msgs.add(selfChatGUID, addMsgOpts{GUID: guid + "-sent", Text: &text, AtMs: at, ToSet: true, To: &phone})
		}
		addRecv := func() {
			msgs.add(phoneGUID, addMsgOpts{GUID: guid + "-recv", Text: &text, AtMs: at + 200, FromMe: boolP(false), ToSet: true, To: &me})
		}
		if receivedFirst {
			addRecv()
			addSent()
		} else {
			addSent()
			addRecv()
		}
	}

	// Force sends from MY_PHONE to verify sendsFrom overrides stale last_addressed_handle
	scripts := newFakeScripts(msgs, true, testMyPhone)
	rec, events := newIMRecorder()
	conn, err := New(Options{
		DBPath:       dbPath,
		RunScript:    scripts.runner(),
		PollInterval: 15 * time.Millisecond,
	}).Connect(ctx, imsdk.Credentials{
		"handle":  testMeHandle,
		"handles": testMeHandle + "," + testMyPhone,
		"since":   "9999999999999",
	}, events)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 1) No last_addressed_handle yet -> answers in SELF
	ownerWrites("a", "hello", false)
	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.messages) == 1
	})
	if rec.messages[0].Chat.PlatformID != selfChatGUID {
		t.Fatalf("expected %q, got %q", selfChatGUID, rec.messages[0].Chat.PlatformID)
	}

	// 2) last_addressed_handle = ME -> answers in SELF_BY_PHONE (regardless of copy order)
	msgs.setAddressed(phoneGUID, testMeHandle)
	ownerWrites("b", "hello again", true)
	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.messages) == 2
	})
	if rec.messages[1].Chat.PlatformID != phoneGUID {
		t.Fatalf("expected %q, got %q", phoneGUID, rec.messages[1].Chat.PlatformID)
	}

	// 3) Actual send reveals Messages sends from MY_PHONE -> sendsFrom overrides last_addressed_handle -> back to SELF
	_, _ = conn.Send(ctx, imsdk.OutboundMessage{
		Chat: imsdk.ChatRef{PlatformID: phoneGUID, Kind: imsdk.ChatDM},
		Text: "Hi",
	})
	ownerWrites("c", "third", false)
	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.messages) == 3
	})
	if rec.messages[2].Chat.PlatformID != selfChatGUID {
		t.Fatalf("expected sendsFrom override to route to %q, got %q", selfChatGUID, rec.messages[2].Chat.PlatformID)
	}

	// 4) Move chat.db away -> reconnecting -> move back -> connected + catches up
	awayPath := filepath.Join(dir, "away.db")
	if err := os.Rename(dbPath, awayPath); err != nil {
		t.Fatal(err)
	}
	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		for _, s := range rec.states {
			if s.Kind == imsdk.StateReconnecting {
				return true
			}
		}
		return false
	})
	msgs.add(selfChatGUID, addMsgOpts{GUID: "during-away", Text: strP("are you there?")})
	if err := os.Rename(awayPath, dbPath); err != nil {
		t.Fatal(err)
	}
	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		hasConn := len(rec.states) > 0 && rec.states[len(rec.states)-1].Kind == imsdk.StateConnected
		hasMsg := false
		for _, m := range rec.messages {
			if m.PlatformID == "during-away" {
				hasMsg = true
			}
		}
		return hasConn && hasMsg
	})

	// 5) Permission revoked (chmod 0000) -> StateError(ConnErrPermissionMissing)
	gonePath := filepath.Join(dir, "gone.db")
	_ = os.Rename(dbPath, gonePath)
	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return rec.states[len(rec.states)-1].Kind == imsdk.StateReconnecting
	})
	_ = os.Rename(gonePath, dbPath)
	if err := os.Chmod(dbPath, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dbPath, 0o644)
	})
	waitIM(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		last := rec.states[len(rec.states)-1]
		return last.Kind == imsdk.StateError && last.Code == imsdk.ConnErrPermissionMissing
	})
}
