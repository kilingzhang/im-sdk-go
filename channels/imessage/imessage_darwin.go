//go:build darwin

package imessage

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

const (
	defaultPollInterval = 1 * time.Second
	defaultSentLookup   = 5 * time.Second
	backfillMs          = 10 * 60_000
	echoMs              = 10 * 60_000
	twinMs              = 60_000

	sendTextScript = `on run argv
  tell application "Messages" to send (item 1 of argv) to chat id (item 2 of argv)
end run`

	sendFileScript = `on run argv
  tell application "Messages" to send (POSIX file (item 1 of argv)) to chat id (item 2 of argv)
end run`

	sendToHandleScript = `on run argv
  tell application "Messages"
    set svc to 1st account whose service type = iMessage
    send (item 1 of argv) to participant (item 2 of argv) of svc
  end tell
end run`

	accountScript = `tell application "Messages"
  set out to ""
  repeat with a in (every account whose service type = iMessage)
    try
      set out to out & (description of a) & linefeed
    end try
  end repeat
  return out
end tell`
)

var (
	automationDeniedRe = regexp.MustCompile(`(?i)-1743|not authori[sz]ed`)
	chatGoneRe         = regexp.MustCompile(`Can.t get chat id|Invalid key form|-1728`)
	appleEventTimedRe  = regexp.MustCompile(`(?i)-1712|timed out`)

	settingsPanes = map[string]string{
		"open_full_disk_access": "x-apple.systempreferences:com.apple.preference.security?Privacy_AllFiles",
		"open_automation":       "x-apple.systempreferences:com.apple.preference.security?Privacy_Automation",
	}

	capabilities = imsdk.Capabilities{
		QuoteReply:      false,
		Reactions:       false,
		TypingIndicator: false,
		FileUpload:      true,
		FileDownload:    true,
		ListMembers:     true,
		Mentions:        false,
		MaxTextLength:   8000,
	}
)

type channel struct {
	imsdk.BaseChannel
	dbPath       string
	runScript    ScriptRunner
	pollInterval time.Duration
	sentLookup   time.Duration
	now          func() time.Time
}

// New creates a macOS iMessage self-chat channel adapter.
func New(opts Options) imsdk.Channel {
	dbPath := opts.DBPath
	if dbPath == "" {
		home, _ := os.UserHomeDir()
		dbPath = filepath.Join(home, "Library", "Messages", "chat.db")
	}
	runScript := opts.RunScript
	if runScript == nil {
		runScript = defaultOSAScript
	}
	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	sentLookup := opts.SentLookup
	if sentLookup <= 0 {
		sentLookup = defaultSentLookup
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	setup := newIMessageSetup(dbPath, runScript, opts.Setup)
	return &channel{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue: imsdk.PlatformIMessage,
			TrustValue:    imsdk.TrustOwner,
			OnboardingValue: imsdk.OnboardingMethod{
				Kind:   imsdk.OnboardingCredentials,
				Fields: []imsdk.CredentialField{},
			},
			SetupValue: setup,
		},
		dbPath:       dbPath,
		runScript:    runScript,
		pollInterval: pollInterval,
		sentLookup:   sentLookup,
		now:          nowFn,
	}
}

func (c *channel) Onboard(ctx context.Context, _ imsdk.Credentials, _ imsdk.OnboardingOptions) <-chan imsdk.OnboardingEvent {
	ch := make(chan imsdk.OnboardingEvent, 1)
	go func() {
		defer close(ch)
		db, err := openMessagesDB(c.dbPath)
		if err != nil {
			code := imsdk.FailureUnknown
			if isPermissionError(err) {
				code = imsdk.FailurePermissionMissing
			}
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
			return
		}
		fromDB := callerIDs(db)
		_ = db.close()

		out, err := c.runScript(ctx, accountScript, nil, 60*time.Second)
		if err != nil {
			code := imsdk.FailureUnknown
			if isAutomationDenied(err) {
				code = imsdk.FailurePermissionMissing
			}
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: code}
			return
		}
		accounts := ParseAccounts(out)
		if len(accounts) == 0 {
			ch <- imsdk.OnboardingEvent{Kind: imsdk.EventFailed, Code: imsdk.FailureNotSignedIn}
			return
		}
		handle := accounts[0]
		seen := make(map[string]struct{})
		var handles []string
		for _, h := range append(accounts, fromDB...) {
			if h == "" {
				continue
			}
			if _, dup := seen[h]; !dup {
				seen[h] = struct{}{}
				handles = append(handles, h)
			}
		}
		ch <- imsdk.OnboardingEvent{
			Kind: imsdk.EventSucceeded,
			Credentials: imsdk.Credentials{
				"handle":  handle,
				"handles": strings.Join(handles, ","),
				"since":   strconv.FormatInt(c.now().UnixMilli(), 10),
			},
			Bot:     imsdk.BotIdentity{ID: handle, Name: handle},
			OwnerID: handle,
		}
	}()
	return ch
}

func (c *channel) Connect(_ context.Context, creds imsdk.Credentials, events imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	handle := creds["handle"]
	sinceMs, _ := strconv.ParseInt(creds["since"], 10, 64)
	bot := imsdk.BotIdentity{ID: handle, Name: handle}
	owner := imsdk.Sender{ID: handle, Name: handle, IsBot: false}

	db, err := openMessagesDB(c.dbPath)
	if err != nil {
		if isPermissionError(err) && events.OnState != nil {
			events.OnState(imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrPermissionMissing})
		}
		return nil, err
	}
	ino, err := fileInode(c.dbPath)
	if err != nil {
		_ = db.close()
		return nil, err
	}

	handleSet := make(map[string]struct{})
	var handleList []string
	addHandle := func(raw string) {
		h := normalizeHandle(raw)
		if h == "" {
			return
		}
		if _, exists := handleSet[h]; !exists {
			handleSet[h] = struct{}{}
			handleList = append(handleList, h)
		}
	}
	addHandle(handle)
	for _, part := range strings.Split(creds["handles"], ",") {
		addHandle(part)
	}
	for _, cid := range callerIDs(db) {
		addHandle(cid)
	}

	fromMs := max(sinceMs, c.now().UnixMilli()-backfillMs)
	lastRowID := startRow(db, fromMs)

	stopCtx, cancel := context.WithCancel(context.Background())
	conn := &connection{
		BaseConnection: imsdk.BaseConnection{
			BotValue:          bot,
			OwnerOnlyValue:    true,
			CapabilitiesValue: capabilities,
		},
		dbPath:       c.dbPath,
		runScript:    c.runScript,
		pollInterval: c.pollInterval,
		sentLookup:   c.sentLookup,
		now:          c.now,
		owner:        owner,
		handles:      handleList,
		events:       events,
		stopCtx:      stopCtx,
		cancel:       cancel,
		done:         make(chan struct{}),
		db:           db,
		inode:        ino,
		lastRowID:    lastRowID,
		sendingFiles: make(map[string]int64),
		sendsFrom:    make(map[string]string),
	}
	go conn.poll()
	return conn, nil
}

type sentEntry struct {
	text     string
	at       int64
	sent     bool
	received bool
}

type seenEntry struct {
	text   string
	fromMe bool
	at     int64
}

type connection struct {
	imsdk.BaseConnection
	dbPath       string
	runScript    ScriptRunner
	pollInterval time.Duration
	sentLookup   time.Duration
	now          func() time.Time
	owner        imsdk.Sender
	handles      []string
	events       imsdk.ChannelEvents

	stopCtx   context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once

	mu           sync.Mutex
	db           *sqliteDB
	inode        uint64
	closed       bool
	lastRowID    int64
	sentTexts    []sentEntry
	sendingFiles map[string]int64
	seen         []seenEntry
	sendsFrom    map[string]string
	lastChat     string
}

func (c *connection) emitMessage(msg imsdk.InboundMessage) {
	if c.events.OnMessage != nil {
		c.events.OnMessage(msg)
	}
}

func (c *connection) emitState(st imsdk.ConnectionState) {
	if c.events.OnState != nil {
		c.events.OnState(st)
	}
}

func (c *connection) poll() {
	defer close(c.done)
	failures := 0

	for c.stopCtx.Err() == nil {
		var delivered []imsdk.InboundMessage
		var states []imsdk.ConnectionState
		var fatal bool

		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		stepErr := func() error {
			if failures > 0 || c.db == nil {
				if c.db != nil {
					_ = c.db.close()
					c.db = nil
				}
				reopened, err := openMessagesDB(c.dbPath)
				if err != nil {
					return err
				}
				ino, err := fileInode(c.dbPath)
				if err != nil {
					_ = reopened.close()
					return err
				}
				c.db = reopened
				c.inode = ino
			} else {
				ino, err := fileInode(c.dbPath)
				if err != nil {
					return err
				}
				if ino != c.inode {
					return errors.New("the Messages database was replaced")
				}
			}
			var err error
			delivered, err = c.readLocked()
			return err
		}()

		if stepErr == nil {
			if failures > 0 {
				states = append(states, imsdk.ConnectionState{Kind: imsdk.StateConnected})
				failures = 0
			}
		} else {
			if c.db != nil {
				_ = c.db.close()
				c.db = nil
			}
			if isPermissionError(stepErr) {
				states = append(states, imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrPermissionMissing})
				c.closed = true
				fatal = true
			} else {
				failures++
				states = append(states, imsdk.ConnectionState{Kind: imsdk.StateReconnecting, Attempt: failures})
			}
		}
		c.mu.Unlock()

		for _, st := range states {
			c.emitState(st)
		}
		for _, m := range delivered {
			c.emitMessage(m)
		}
		if fatal {
			return
		}

		wait := c.pollInterval
		if failures > 0 {
			shift := min(failures, 10)
			wait = min(30*time.Second, c.pollInterval*time.Duration(1<<shift))
		}
		if !sleepCtx(c.stopCtx, wait) {
			return
		}
	}
}

func (c *connection) readLocked() ([]imsdk.InboundMessage, error) {
	rows, err := selfMessages(c.db, c.lastRowID, c.handles)
	if err != nil {
		return nil, err
	}
	var out []imsdk.InboundMessage
	for _, row := range rows {
		c.lastRowID = max(c.lastRowID, row.RowID)
		raw := rowText(row)
		files, err := attachmentsOf(c.db, row.RowID)
		if err != nil {
			return out, err
		}
		if c.isAgentsLocked(row, raw, files) {
			continue
		}
		if c.isTwinLocked(row, raw) {
			continue
		}
		row.ChatGUID = c.replyChatLocked(row)
		inbound, ok := ToInbound(row, raw, files, c.owner)
		if !ok {
			continue
		}
		c.lastChat = inbound.Chat.PlatformID
		out = append(out, inbound)
	}
	return out, nil
}

func (c *connection) isAgentsLocked(row SelfRow, raw string, files []AttachmentRow) bool {
	t := c.now().UnixMilli()
	cutoff := t - echoMs
	for len(c.sentTexts) > 0 && c.sentTexts[0].at < cutoff {
		c.sentTexts = c.sentTexts[1:]
	}
	marked := strings.HasSuffix(raw, AgentMark)
	words := raw
	if marked {
		words = strings.TrimSuffix(raw, AgentMark)
	}
	words = strings.TrimSpace(words)
	at := AppleEpochMs + row.Date

	hitIdx := -1
	if words != "" {
		for i := range c.sentTexts {
			s := &c.sentTexts[i]
			if row.FromMe {
				if !s.sent && s.text == words {
					hitIdx = i
					break
				}
			} else {
				if !s.received && s.text == words && abs64(s.at-at) < twinMs {
					hitIdx = i
					break
				}
			}
		}
	}
	if hitIdx >= 0 {
		if row.FromMe {
			c.sentTexts[hitIdx].sent = true
		} else {
			c.sentTexts[hitIdx].received = true
		}
		if c.sentTexts[hitIdx].sent && c.sentTexts[hitIdx].received {
			c.sentTexts = slices.Delete(c.sentTexts, hitIdx, hitIdx+1)
		}
	}
	if marked || hitIdx >= 0 {
		return true
	}
	if strings.TrimSpace(strings.ReplaceAll(raw, "\ufffc", "")) == "" && len(files) > 0 {
		for name, until := range c.sendingFiles {
			if until < t {
				delete(c.sendingFiles, name)
			}
		}
		allTracked := true
		for _, f := range files {
			if f.Name == "" {
				allTracked = false
				break
			}
			if _, ok := c.sendingFiles[f.Name]; !ok {
				allTracked = false
				break
			}
		}
		return allTracked
	}
	return false
}

func (c *connection) isTwinLocked(row SelfRow, raw string) bool {
	at := AppleEpochMs + row.Date
	cutoff := c.now().UnixMilli() - 5*twinMs
	for len(c.seen) > 0 && c.seen[0].at < cutoff {
		c.seen = c.seen[1:]
	}
	if raw == "" {
		return false
	}
	for i, s := range c.seen {
		if s.text == raw && s.fromMe != row.FromMe && abs64(s.at-at) < twinMs {
			c.seen = slices.Delete(c.seen, i, i+1)
			return true
		}
	}
	c.seen = append(c.seen, seenEntry{text: raw, fromMe: row.FromMe, at: at})
	return false
}

func (c *connection) replyChatLocked(row SelfRow) string {
	here := normalizeHandle(row.ChatIdentifier)
	other := here
	if row.To != "" {
		other = normalizeHandle(row.To)
	}
	to := other
	from := here
	if row.FromMe {
		to = here
		from = other
	}
	toChat, hasTo := selfChat(c.db, to)
	if to != from {
		if back, hasBack := selfChat(c.db, from); hasBack {
			addr, hasSent := c.sendsFrom[back.guid]
			if !hasSent {
				addr = back.lastAddressed
			}
			if addr == to {
				return back.guid
			}
		}
	}
	if hasTo {
		return toChat.guid
	}
	return row.ChatGUID
}

func (c *connection) maxRowLocked() int64 {
	if c.db == nil {
		return c.lastRowID
	}
	stmt, err := c.db.prepare("SELECT IFNULL(MAX(ROWID), 0) FROM message")
	if err != nil {
		return c.lastRowID
	}
	defer stmt.finalize()
	if has, err := stmt.step(); err == nil && has {
		return stmt.colInt64(0)
	}
	return c.lastRowID
}

func (c *connection) outgoingLocked(markdown string) string {
	text := strings.TrimSpace(Plain(markdown))
	c.sentTexts = append(c.sentTexts, sentEntry{
		text: text,
		at:   c.now().UnixMilli(),
	})
	return text + AgentMark
}

type sentLookupResult struct {
	guid string
	chat string
}

func (c *connection) findSent(ctx context.Context, chatGUID string, afterRow int64) *sentLookupResult {
	deadline := c.now().Add(c.sentLookup)
	stepWait := min(250*time.Millisecond, c.pollInterval)

	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil
		}
		var found *sentLookupResult
		if c.db != nil {
			var chatArg any
			if chatGUID != "" {
				chatArg = chatGUID
			}
			stmt, err := c.db.prepare(
				`SELECT m.guid, c.guid, m.destination_caller_id FROM message m
				 JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
				 JOIN chat c ON c.ROWID = cmj.chat_id
				 WHERE (? IS NULL OR c.guid = ?) AND m.is_from_me = 1 AND m.ROWID > ?
				 ORDER BY m.ROWID DESC LIMIT 1`,
				chatArg, chatArg, afterRow,
			)
			if err == nil {
				if has, stepErr := stmt.step(); stepErr == nil && has {
					guid, _ := stmt.colText(0)
					chat, _ := stmt.colText(1)
					sender, hasSender := stmt.colText(2)
					if hasSender && sender != "" {
						c.sendsFrom[chat] = normalizeHandle(sender)
					}
					found = &sentLookupResult{guid: guid, chat: chat}
				}
				stmt.finalize()
			}
		}
		c.mu.Unlock()

		if found != nil {
			return found
		}
		if !c.now().Before(deadline) {
			return nil
		}
		if !sleepBoth(ctx, c.stopCtx, stepWait) {
			return nil
		}
	}
}

func (c *connection) Send(ctx context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	chatGUID := msg.Chat.PlatformID
	var degraded []imsdk.Degradation
	if msg.ReplyTo != "" {
		degraded = append(degraded, imsdk.DegradedQuoteDropped)
	}
	if Plain(msg.Text) != msg.Text {
		degraded = append(degraded, imsdk.DegradedMarkdownFlattened)
	}

	c.mu.Lock()
	before := c.maxRowLocked()
	var markedText string
	hasText := strings.TrimSpace(msg.Text) != ""
	if hasText {
		markedText = c.outgoingLocked(msg.Text)
	}
	c.mu.Unlock()

	textID := ""
	if hasText {
		if _, err := c.runScript(ctx, sendTextScript, []string{markedText, chatGUID}, 30*time.Second); err != nil {
			return imsdk.WithTextSent(sendFailure(err), textID), nil
		}
		textID = fmt.Sprintf("local:%d", c.now().UnixMilli())
	}

	for _, f := range msg.Files {
		name := f.Name
		if name == "" {
			name = filepath.Base(f.Path)
		}
		c.mu.Lock()
		c.sendingFiles[name] = c.now().UnixMilli() + echoMs
		c.mu.Unlock()

		staged, err := stageAttachment(c.dbPath, f.Path, name)
		if err != nil {
			return imsdk.WithTextSent(sendFailure(err), textID), nil
		}
		if _, err := c.runScript(ctx, sendFileScript, []string{staged, chatGUID}, 120*time.Second); err != nil {
			return imsdk.WithTextSent(sendFailure(err), textID), nil
		}
	}

	platformID := fmt.Sprintf("local:%d", c.now().UnixMilli())
	if sent := c.findSent(ctx, chatGUID, before); sent != nil && sent.guid != "" {
		platformID = sent.guid
	}
	return imsdk.SendResult{
		Outcome:    imsdk.OutcomeSent,
		PlatformID: platformID,
		Degraded:   degraded,
	}, nil
}

func (c *connection) MessageUser(ctx context.Context, userID string, text string) (*imsdk.DirectMessageReceipt, error) {
	c.mu.Lock()
	before := c.maxRowLocked()
	targetChat := c.lastChat
	marked := c.outgoingLocked(text)
	c.mu.Unlock()

	var err error
	if targetChat != "" {
		_, err = c.runScript(ctx, sendTextScript, []string{marked, targetChat}, 30*time.Second)
	} else {
		_, err = c.runScript(ctx, sendToHandleScript, []string{marked, userID}, 30*time.Second)
	}
	if err != nil {
		return nil, err
	}
	sent := c.findSent(ctx, targetChat, before)
	if sent == nil {
		return nil, nil
	}
	return &imsdk.DirectMessageReceipt{
		Chat:       sent.chat,
		PlatformID: sent.guid,
	}, nil
}

func (c *connection) Download(_ context.Context, att imsdk.InboundAttachment, toPath string) error {
	return copyFileBytes(expandHome(att.ID), toPath)
}

func (c *connection) ListMembers(context.Context, imsdk.ChatRef) ([]imsdk.Sender, error) {
	return []imsdk.Sender{c.owner}, nil
}

func (c *connection) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
	})
	<-c.done
	c.mu.Lock()
	c.closed = true
	if c.db != nil {
		_ = c.db.close()
		c.db = nil
	}
	c.mu.Unlock()
	return nil
}

// ------------------------------------------------------------------- setup

type setupImpl struct {
	dbPath         string
	runScript      ScriptRunner
	activeAccounts func(ctx context.Context) (*int, error)
	tccPath        string
	bundleID       string
	appPath        func(ctx context.Context, bundleID string) (string, error)
	open           func(ctx context.Context, args []string) error

	mu       sync.Mutex
	allowed  bool
	signedIn bool
	app      *resolvedApp
}

type resolvedApp struct {
	path string
	name string
}

func newIMessageSetup(dbPath string, runScript ScriptRunner, probe SetupProbe) imsdk.ChannelSetup {
	activeFn := probe.ActiveAccounts
	if activeFn == nil {
		activeFn = defaultActiveAccounts
	}
	tccPath := probe.TCCPath
	if tccPath == "" {
		home, _ := os.UserHomeDir()
		tccPath = filepath.Join(home, "Library", "Application Support", "com.apple.TCC", "TCC.db")
	}
	bundleID := probe.BundleID
	if bundleID == "" {
		bundleID = os.Getenv("__CFBundleIdentifier")
	}
	appPathFn := probe.AppPath
	if appPathFn == nil {
		appPathFn = defaultFindApp
	}
	openFn := probe.Open
	if openFn == nil {
		openFn = defaultOpenWith
	}
	return &setupImpl{
		dbPath:         dbPath,
		runScript:      runScript,
		activeAccounts: activeFn,
		tccPath:        tccPath,
		bundleID:       bundleID,
		appPath:        appPathFn,
		open:           openFn,
	}
}

func (s *setupImpl) Actions() []string {
	return []string{"open_messages", "open_full_disk_access", "open_automation", "reveal_app", "allow_automation"}
}

func (s *setupImpl) appInfo(ctx context.Context) resolvedApp {
	s.mu.Lock()
	if s.app != nil {
		info := *s.app
		s.mu.Unlock()
		return info
	}
	s.mu.Unlock()

	var resolved resolvedApp
	if s.bundleID != "" {
		if p, err := s.appPath(ctx, s.bundleID); err == nil && p != "" {
			resolved = resolvedApp{
				path: p,
				name: strings.TrimSuffix(filepath.Base(p), ".app"),
			}
		}
	}
	if resolved.name == "" {
		exe, _ := os.Executable()
		resolved = resolvedApp{
			path: exe,
			name: filepath.Base(exe),
		}
	}

	s.mu.Lock()
	s.app = &resolved
	s.mu.Unlock()
	return resolved
}

func (s *setupImpl) Check(ctx context.Context) (imsdk.SetupStatus, error) {
	accounts, err := s.activeAccounts(ctx)
	if err != nil {
		accounts = nil
	}

	s.mu.Lock()
	wasSignedIn := s.signedIn
	wasAllowed := s.allowed
	s.mu.Unlock()

	signedInState := imsdk.SetupUnknown
	if wasSignedIn || (accounts != nil && *accounts > 0) {
		signedInState = imsdk.SetupOK
	} else if accounts != nil {
		signedInState = imsdk.SetupMissing
	}

	diskState := imsdk.SetupUnknown
	if db, openErr := openMessagesDB(s.dbPath); openErr == nil {
		_ = db.close()
		diskState = imsdk.SetupOK
	} else if isPermissionError(openErr) {
		diskState = imsdk.SetupMissing
	}

	autoState := imsdk.SetupUnknown
	if diskState == imsdk.SetupOK {
		autoState = imsdk.SetupMissing
	}
	if wasAllowed {
		autoState = imsdk.SetupOK
	} else if diskState == imsdk.SetupOK {
		if st, ok := automationState(s.tccPath, s.bundleID); ok {
			autoState = st
		}
	}

	info := s.appInfo(ctx)
	return imsdk.SetupStatus{
		Checks: []imsdk.SetupCheck{
			{ID: "signed_in", State: signedInState},
			{ID: "full_disk_access", State: diskState},
			{ID: "automation", State: autoState},
		},
		App: info.name,
	}, nil
}

func (s *setupImpl) Act(ctx context.Context, action string) error {
	if action == "open_messages" {
		return s.open(ctx, []string{"-a", "Messages"})
	}
	if pane, ok := settingsPanes[action]; ok {
		return s.open(ctx, []string{pane})
	}
	if action == "reveal_app" {
		info := s.appInfo(ctx)
		if info.path != "" {
			return s.open(ctx, []string{"-R", info.path})
		}
		return nil
	}
	if action == "allow_automation" {
		out, err := s.runScript(ctx, accountScript, nil, 120*time.Second)
		if err != nil {
			if isAutomationDenied(err) {
				return nil
			}
			return err
		}
		s.mu.Lock()
		s.signedIn = len(ParseAccounts(out)) > 0
		s.allowed = true
		s.mu.Unlock()
	}
	return nil
}

func automationState(tccPath, bundleID string) (imsdk.SetupState, bool) {
	db, err := openReadOnlySQLite(tccPath)
	if err != nil {
		return "", false
	}
	defer db.close()

	stmt, err := db.prepare(
		"SELECT client, auth_value FROM access WHERE service = 'kTCCServiceAppleEvents' AND indirect_object_identifier = 'com.apple.MobileSMS'",
	)
	if err != nil {
		return "", false
	}
	defer stmt.finalize()

	type tccRow struct {
		client string
		auth   int64
	}
	var rows []tccRow
	for {
		has, stepErr := stmt.step()
		if stepErr != nil || !has {
			break
		}
		client, _ := stmt.colText(0)
		auth := stmt.colInt64(1)
		if bundleID == "" || client == bundleID {
			rows = append(rows, tccRow{client: client, auth: auth})
		}
	}
	for _, r := range rows {
		if r.auth == 2 {
			return imsdk.SetupOK, true
		}
	}
	for _, r := range rows {
		if r.auth == 0 {
			return imsdk.SetupDenied, true
		}
	}
	return "", false
}

// ---------------------------------------------------------------- database queries

func startRow(db *sqliteDB, fromMs int64) int64 {
	stmt, err := db.prepare("SELECT IFNULL(MAX(ROWID), 0) FROM message WHERE date < ?", toAppleDateNs(fromMs))
	if err != nil {
		return 0
	}
	defer stmt.finalize()
	if has, err := stmt.step(); err == nil && has {
		return stmt.colInt64(0)
	}
	return 0
}

func selfMessages(db *sqliteDB, afterRow int64, handles []string) ([]SelfRow, error) {
	if len(handles) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(handles))
	args := make([]any, 0, 1+len(handles))
	args = append(args, afterRow)
	for i, h := range handles {
		placeholders[i] = "?"
		args = append(args, h)
	}
	query := fmt.Sprintf(
		`SELECT m.ROWID, m.guid, m.text, m.attributedBody,
		        CASE WHEN m.date > 1000000000000 THEN m.date / 1000000 ELSE m.date * 1000 END AS date,
		        m.is_from_me, c.guid, c.chat_identifier, m.destination_caller_id
		 FROM message m
		 JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
		 JOIN chat c ON c.ROWID = cmj.chat_id
		 WHERE m.ROWID > ? AND c.style = 45 AND lower(c.chat_identifier) IN (%s)
		   AND IFNULL(m.associated_message_type, 0) = 0 AND IFNULL(m.item_type, 0) = 0
		 ORDER BY m.ROWID LIMIT 200`,
		strings.Join(placeholders, ","),
	)
	stmt, err := db.prepare(query, args...)
	if err != nil {
		return nil, err
	}
	defer stmt.finalize()

	var rows []SelfRow
	for {
		has, err := stmt.step()
		if err != nil {
			return nil, err
		}
		if !has {
			break
		}
		guid, _ := stmt.colText(1)
		var textPtr *string
		if txt, ok := stmt.colText(2); ok {
			textPtr = &txt
		}
		blob := stmt.colBlob(3)
		dateMs := stmt.colInt64(4)
		fromMe := stmt.colInt64(5) == 1
		chatGUID, _ := stmt.colText(6)
		chatID, _ := stmt.colText(7)
		to, _ := stmt.colText(8)
		rows = append(rows, SelfRow{
			RowID:          stmt.colInt64(0),
			GUID:           guid,
			Text:           textPtr,
			AttributedBody: blob,
			Date:           dateMs,
			FromMe:         fromMe,
			ChatGUID:       chatGUID,
			ChatIdentifier: chatID,
			To:             to,
		})
	}
	return rows, nil
}

type selfChatInfo struct {
	guid          string
	lastAddressed string
}

func selfChat(db *sqliteDB, address string) (selfChatInfo, bool) {
	if db == nil {
		return selfChatInfo{}, false
	}
	stmt, err := db.prepare(
		`SELECT guid, last_addressed_handle FROM chat
		 WHERE style = 45 AND lower(chat_identifier) = ?
		 ORDER BY guid LIKE 'SMS;%', ROWID DESC LIMIT 1`,
		address,
	)
	if err != nil {
		return selfChatInfo{}, false
	}
	defer stmt.finalize()
	has, err := stmt.step()
	if err != nil || !has {
		return selfChatInfo{}, false
	}
	guid, _ := stmt.colText(0)
	lastAddr, _ := stmt.colText(1)
	if lastAddr != "" {
		lastAddr = normalizeHandle(lastAddr)
	}
	return selfChatInfo{guid: guid, lastAddressed: lastAddr}, true
}

func attachmentsOf(db *sqliteDB, rowID int64) ([]AttachmentRow, error) {
	stmt, err := db.prepare(
		`SELECT a.filename, a.mime_type, a.transfer_name, a.total_bytes
		 FROM attachment a JOIN message_attachment_join maj ON maj.attachment_id = a.ROWID
		 WHERE maj.message_id = ?`,
		rowID,
	)
	if err != nil {
		return nil, err
	}
	defer stmt.finalize()

	var out []AttachmentRow
	for {
		has, err := stmt.step()
		if err != nil {
			return nil, err
		}
		if !has {
			break
		}
		filename, _ := stmt.colText(0)
		mime, _ := stmt.colText(1)
		name, _ := stmt.colText(2)
		var size int64
		if !stmt.isNull(3) {
			size = stmt.colInt64(3)
		}
		out = append(out, AttachmentRow{
			Filename: filename,
			Mime:     mime,
			Name:     name,
			Size:     size,
		})
	}
	return out, nil
}

func callerIDs(db *sqliteDB) []string {
	stmt, err := db.prepare(
		`SELECT DISTINCT destination_caller_id FROM message
		 WHERE is_from_me = 1 AND service = 'iMessage' AND destination_caller_id IS NOT NULL`,
	)
	if err != nil {
		return nil
	}
	defer stmt.finalize()

	var ids []string
	for {
		has, err := stmt.step()
		if err != nil || !has {
			break
		}
		if id, ok := stmt.colText(0); ok && id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

func rowText(row SelfRow) string {
	if row.Text != nil {
		return *row.Text
	}
	if len(row.AttributedBody) > 0 {
		if decoded, ok := DecodeAttributedBody(row.AttributedBody); ok {
			return decoded
		}
	}
	return ""
}

// -------------------------------------------------------------- host helpers

func defaultOSAScript(ctx context.Context, script string, args []string, timeout time.Duration) (string, error) {
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmdArgs := make([]string, 0, 2+len(args))
	cmdArgs = append(cmdArgs, "-e", script)
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.CommandContext(runCtx, "osascript", cmdArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		timedOut := errors.Is(runCtx.Err(), context.DeadlineExceeded)
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = strings.TrimSpace(err.Error())
		}
		return "", &ScriptError{Message: msg, TimedOut: timedOut}
	}
	return stdout.String(), nil
}

func defaultActiveAccounts(ctx context.Context) (*int, error) {
	runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cmd := exec.CommandContext(runCtx, "defaults", "read", "com.apple.imservice.ids.iMessage", "ActiveAccounts")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if strings.Contains(stderr.String(), "does not exist") {
			zero := 0
			return &zero, nil
		}
		return nil, nil
	}
	n := CountActiveAccounts(stdout.String())
	return &n, nil
}

func defaultFindApp(ctx context.Context, bundleID string) (string, error) {
	runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	cleanID := strings.NewReplacer(`"`, "", `\`, "").Replace(bundleID)
	cmd := exec.CommandContext(runCtx, "mdfind", fmt.Sprintf(`kMDItemCFBundleIdentifier == "%s"`, cleanID))
	out, err := cmd.Output()
	if err != nil {
		return "", nil
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasSuffix(line, ".app") {
			return line, nil
		}
	}
	return "", nil
}

func defaultOpenWith(ctx context.Context, args []string) error {
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return exec.CommandContext(runCtx, "open", args...).Run()
}

func stageAttachment(dbPath, srcPath, name string) (string, error) {
	id, err := newUUID()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(filepath.Dir(dbPath), "Attachments", "mingle", id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	safeName := strings.NewReplacer("/", "_", `\`, "_").Replace(name)
	staged := filepath.Join(dir, safeName)
	if err := copyFileBytes(srcPath, staged); err != nil {
		return "", err
	}
	return staged, nil
}

func copyFileBytes(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func sendFailure(err error) imsdk.SendResult {
	var se *ScriptError
	if errors.As(err, &se) {
		if se.TimedOut {
			return imsdk.SendResult{Outcome: imsdk.OutcomeUncertain}
		}
		if isAutomationDenied(se) {
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrForbidden, Retryable: false}
		}
		if chatGoneRe.MatchString(se.Message) {
			return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrChatGone, Retryable: false}
		}
		if appleEventTimedRe.MatchString(se.Message) {
			return imsdk.SendResult{Outcome: imsdk.OutcomeUncertain}
		}
	}
	return imsdk.SendResult{Outcome: imsdk.OutcomeFailed, Code: imsdk.SendErrUnknown, Retryable: false}
}

func isAutomationDenied(err error) bool {
	if err == nil {
		return false
	}
	return automationDeniedRe.MatchString(err.Error())
}

func fileInode(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("imessage: missing stat_t")
	}
	return st.Ino, nil
}

func expandHome(p string) string {
	if strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			return filepath.Join(home, p[2:])
		}
	}
	return p
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func sleepBoth(ctx1, ctx2 context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx1.Err() == nil && ctx2.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx1.Done():
		return false
	case <-ctx2.Done():
		return false
	case <-timer.C:
		return true
	}
}
