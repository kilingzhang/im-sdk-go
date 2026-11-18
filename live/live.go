// Package live coordinates message reactions and throttled in-place progress replies
// ("Thinking… · 5s", tool steps, stream-window expiration handover, and final answer replacement).
package live

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

const (
	RenderEvery   = 1 * time.Second
	TickEvery     = 5 * time.Second
	MaxSteps      = 4
	ShowTimeAfter = 3 * time.Second
	NoteEvery     = 60 * time.Second
)

// Lang selects Chinese ("zh") or English ("en") system/progress copy.
type Lang string

const (
	LangZH Lang = "zh"
	LangEN Lang = "en"
)

// LangFor returns LangZH for Chinese domestic chat platforms and LangEN otherwise.
func LangFor(p imsdk.Platform) Lang {
	switch p {
	case imsdk.PlatformWeChat, imsdk.PlatformWeCom, imsdk.PlatformFeishu, imsdk.PlatformDingTalk, imsdk.PlatformQQ:
		return LangZH
	default:
		return LangEN
	}
}

// StepCategory classifies a tool invocation in plain user-facing words.
type StepCategory string

const (
	StepCommand  StepCategory = "command"
	StepRead     StepCategory = "read"
	StepEdit     StepCategory = "edit"
	StepSearch   StepCategory = "search"
	StepWeb      StepCategory = "web"
	StepDelegate StepCategory = "delegate"
	StepPlan     StepCategory = "plan"
	StepOther    StepCategory = "other"
)

var (
	rePlan     = regexp.MustCompile(`todo|plan`)
	reWeb      = regexp.MustCompile(`web|fetch|browse|http|url`)
	reCommand  = regexp.MustCompile(`bash|shell|exec|command|terminal|run`)
	reSearch   = regexp.MustCompile(`grep|glob|search|find`)
	reEdit     = regexp.MustCompile(`edit|write|patch|create|replace|multiedit`)
	reRead     = regexp.MustCompile(`read|view|cat|list|ls`)
	reDelegate = regexp.MustCompile(`task|agent|delegate|spawn`)
)

// ClassifyTool maps a raw tool identifier to a StepCategory.
func ClassifyTool(tool string) StepCategory {
	name := strings.ToLower(tool)
	switch {
	case rePlan.MatchString(name):
		return StepPlan
	case reWeb.MatchString(name):
		return StepWeb
	case reCommand.MatchString(name):
		return StepCommand
	case reSearch.MatchString(name):
		return StepSearch
	case reEdit.MatchString(name):
		return StepEdit
	case reRead.MatchString(name):
		return StepRead
	case reDelegate.MatchString(name):
		return StepDelegate
	default:
		return StepOther
	}
}

func stepLabel(lang Lang, kind StepCategory) string {
	if lang == LangZH {
		switch kind {
		case StepCommand:
			return "运行命令"
		case StepRead:
			return "查看文件"
		case StepEdit:
			return "修改文件"
		case StepSearch:
			return "搜索"
		case StepWeb:
			return "查资料"
		case StepDelegate:
			return "分派子任务"
		case StepPlan:
			return "整理计划"
		default:
			return "使用工具"
		}
	}
	switch kind {
	case StepCommand:
		return "Running a command"
	case StepRead:
		return "Reading files"
	case StepEdit:
		return "Editing files"
	case StepSearch:
		return "Searching"
	case StepWeb:
		return "Looking things up"
	case StepDelegate:
		return "Handing off a subtask"
	case StepPlan:
		return "Planning"
	default:
		return "Using a tool"
	}
}

func formatElapsed(lang Lang, d time.Duration) string {
	secs := int(d / time.Second)
	if secs < int(ShowTimeAfter/time.Second) {
		return ""
	}
	if secs < 60 {
		if lang == LangZH {
			return fmt.Sprintf("%d 秒", secs)
		}
		return fmt.Sprintf("%ds", secs)
	}
	m, s := secs/60, secs%60
	if lang == LangZH {
		return fmt.Sprintf("%d 分 %d 秒", m, s)
	}
	return fmt.Sprintf("%dm %ds", m, s)
}

type stepCount struct {
	kind  StepCategory
	count int
}

func renderProgress(lang Lang, working bool, steps []stepCount, elapsed time.Duration) string {
	head := "Thinking"
	if lang == LangZH {
		head = "正在思考"
	}
	if working || len(steps) > 0 {
		if lang == LangZH {
			head = "正在处理"
		} else {
			head = "Working"
		}
	}
	if dur := formatElapsed(lang, elapsed); dur != "" {
		head = head + " · " + dur
	} else {
		head = head + "…"
	}
	if len(steps) == 0 {
		return head
	}
	shown := steps
	if len(shown) > MaxSteps {
		shown = shown[len(shown)-MaxSteps:]
	}
	var lines []string
	lines = append(lines, head)
	for _, sc := range shown {
		label := stepLabel(lang, sc.kind)
		if sc.count > 1 {
			label = fmt.Sprintf("%s ×%d", label, sc.count)
		}
		lines = append(lines, "- "+label)
	}
	return strings.Join(lines, "\n")
}

// Outcome identifies why an inbound message's live progress is settling.
type Outcome string

const (
	OutcomeAnswered   Outcome = "answered"
	OutcomeUnanswered Outcome = "unanswered"
	OutcomeStopped    Outcome = "stopped"
	OutcomePaused     Outcome = "paused"
	OutcomeFailed     Outcome = "failed"
	OutcomeWaiting    Outcome = "waiting"
)

// OpenInput describes an inbound message that is starting processing.
type OpenInput struct {
	MessageID         string
	OwnerKey          string
	Platform          imsdk.Platform
	Chat              imsdk.ChatRef
	PlatformMessageID string
	SenderID          string
	Connection        imsdk.ChannelConnection
}

type entry struct {
	mu                sync.Mutex
	ownerKey          string
	chat              imsdk.ChatRef
	platformMessageID string
	conn              imsdk.ChannelConnection
	lang              Lang
	startedAt         time.Time

	openDone  chan struct{}
	liveReply imsdk.LiveReply

	working    bool
	steps      []stepCount
	lastRender time.Time
	pending    *time.Timer
	tick       *time.Ticker
	stopTick   chan struct{}
	handover   *time.Timer

	inFlight chan struct{} // buffered(1) mutex token serializing Update/Finish after openDone
	closed   bool
}

// Manager tracks active live progress replies and reactions across messages.
type Manager struct {
	mu      sync.Mutex
	entries map[string]*entry
	notedAt map[string]time.Time
	nowFn   func() time.Time
	logFn   func(string)
}

// Options configures a Manager.
type Options struct {
	Now func() time.Time
	Log func(string)
}

// NewManager creates a live reply Manager.
func NewManager(opts Options) *Manager {
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	logFn := opts.Log
	if logFn == nil {
		logFn = func(string) {}
	}
	return &Manager{
		entries: make(map[string]*entry),
		notedAt: make(map[string]time.Time),
		nowFn:   nowFn,
		logFn:   logFn,
	}
}

// Open marks an inbound message with a processing reaction and starts an editable live reply when supported.
func (m *Manager) Open(in OpenInput) {
	lang := LangFor(in.Platform)
	now := m.nowFn()
	e := &entry{
		ownerKey:          in.OwnerKey,
		chat:              in.Chat,
		platformMessageID: in.PlatformMessageID,
		conn:              in.Connection,
		lang:              lang,
		startedAt:         now,
		lastRender:        now,
		openDone:          make(chan struct{}),
		stopTick:          make(chan struct{}),
		inFlight:          make(chan struct{}, 1),
	}
	e.inFlight <- struct{}{}

	m.mu.Lock()
	if old := m.entries[in.MessageID]; old != nil {
		m.closeEntry(old)
	}
	m.entries[in.MessageID] = e
	m.mu.Unlock()

	go func() {
		_ = in.Connection.React(context.Background(), in.Chat, in.PlatformMessageID, imsdk.ReactionProcessing)
	}()

	initial := renderProgress(lang, false, nil, 0)
	mentionID := ""
	if in.Chat.Kind != imsdk.ChatDM {
		mentionID = in.SenderID
	}

	go func() {
		defer close(e.openDone)
		lr, err := in.Connection.StartLive(context.Background(), in.Chat, imsdk.LiveOptions{
			ReplyTo: in.PlatformMessageID,
			Mention: mentionID,
			Initial: initial,
		})
		if err != nil {
			if !errors.Is(err, imsdk.ErrUnsupported) {
				m.logFn("live start failed: " + err.Error())
			}
			return
		}
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return
		}
		e.liveReply = lr
		until := lr.Until()
		if !until.IsZero() {
			delay := max(time.Duration(0), until.Sub(m.nowFn()))
			e.handover = time.AfterFunc(delay, func() {
				m.expireStream(in.MessageID, e)
			})
		}
		e.tick = time.NewTicker(TickEvery)
		tickC := e.tick.C
		stopC := e.stopTick
		e.mu.Unlock()

		go func() {
			for {
				select {
				case <-stopC:
					return
				case <-tickC:
					m.scheduleRender(e)
				}
			}
		}()
	}()
}

// Activity records thinking or a tool execution for all open entries belonging to ownerKey.
func (m *Manager) Activity(ownerKey string, kind string, toolDetail string) {
	m.mu.Lock()
	var list []*entry
	for _, e := range m.entries {
		if e.ownerKey == ownerKey {
			list = append(list, e)
		}
	}
	m.mu.Unlock()

	for _, e := range list {
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			continue
		}
		if kind == "tool" {
			e.working = true
			cat := ClassifyTool(toolDetail)
			if n := len(e.steps); n > 0 && e.steps[n-1].kind == cat {
				e.steps[n-1].count++
			} else {
				e.steps = append(e.steps, stepCount{kind: cat, count: 1})
			}
		}
		e.mu.Unlock()
		m.scheduleRender(e)
	}
}

func (m *Manager) scheduleRender(e *entry) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	elapsed := m.nowFn().Sub(e.lastRender)
	if elapsed < RenderEvery {
		if e.pending == nil {
			wait := RenderEvery - elapsed
			e.pending = time.AfterFunc(wait, func() {
				e.mu.Lock()
				e.pending = nil
				e.mu.Unlock()
				m.flushRender(e)
			})
		}
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()
	go m.flushRender(e)
}

func (m *Manager) flushRender(e *entry) {
	<-e.openDone
	select {
	case <-e.inFlight:
	default:
		// Another render or finish is in flight; drop this intermediate frame.
		return
	}
	defer func() { e.inFlight <- struct{}{} }()

	e.mu.Lock()
	if e.closed || e.liveReply == nil {
		e.mu.Unlock()
		return
	}
	now := m.nowFn()
	e.lastRender = now
	steps := append([]stepCount(nil), e.steps...)
	text := renderProgress(e.lang, e.working, steps, now.Sub(e.startedAt))
	lr := e.liveReply
	e.mu.Unlock()

	_ = lr.Update(context.Background(), text)
}

func (m *Manager) expireStream(messageID string, e *entry) {
	<-e.openDone
	<-e.inFlight
	defer func() { e.inFlight <- struct{}{} }()

	e.mu.Lock()
	if e.closed || e.liveReply == nil {
		e.mu.Unlock()
		return
	}
	lr := e.liveReply
	e.liveReply = nil
	if e.tick != nil {
		e.tick.Stop()
		e.tick = nil
	}
	note := "Still working on it. The answer will come as a new message."
	if e.lang == LangZH {
		note = "还在处理，完成后另发一条消息回复。"
	}
	e.mu.Unlock()

	_, _ = lr.Finish(context.Background(), note, nil)
	_ = messageID
}

// Answer writes the final reply into the message's open LiveReply if available.
// Returns (nil, false) when no LiveReply is active or if Finish failed so the caller sends a regular message.
func (m *Manager) Answer(ctx context.Context, messageID string, markdown string, mentions []imsdk.Mention) (*imsdk.SendResult, bool) {
	m.mu.Lock()
	e := m.entries[messageID]
	delete(m.entries, messageID)
	m.mu.Unlock()
	if e == nil {
		return nil, false
	}

	<-e.openDone
	<-e.inFlight
	m.stopEntryTimers(e)

	e.mu.Lock()
	lr := e.liveReply
	e.liveReply = nil
	e.closed = true
	conn := e.conn
	chat := e.chat
	msgID := e.platformMessageID
	e.mu.Unlock()
	e.inFlight <- struct{}{}

	if lr == nil {
		_ = conn.React(ctx, chat, msgID, imsdk.ReactionDone)
		return nil, false
	}
	res, err := lr.Finish(ctx, markdown, mentions)
	if err != nil || res.Outcome != imsdk.OutcomeSent {
		return nil, false
	}
	_ = conn.React(ctx, chat, msgID, imsdk.ReactionDone)
	return &res, true
}

// Settle closes an open live reply when the message was answered elsewhere, stopped, paused, waiting, or failed.
func (m *Manager) Settle(ctx context.Context, messageID string, outcome Outcome, customNote string) {
	m.mu.Lock()
	e := m.entries[messageID]
	delete(m.entries, messageID)
	m.mu.Unlock()
	if e == nil {
		return
	}

	<-e.openDone
	<-e.inFlight
	m.stopEntryTimers(e)

	e.mu.Lock()
	lr := e.liveReply
	e.liveReply = nil
	e.closed = true
	lang := e.lang
	conn := e.conn
	chat := e.chat
	msgID := e.platformMessageID
	e.mu.Unlock()
	e.inFlight <- struct{}{}

	reaction := imsdk.ReactionNone
	switch outcome {
	case OutcomeAnswered:
		reaction = imsdk.ReactionDone
	case OutcomeFailed:
		reaction = imsdk.ReactionError
	}
	_ = conn.React(ctx, chat, msgID, reaction)

	note := customNote
	if note == "" {
		note = defaultSettleNote(lang, outcome)
	}
	if lr != nil && note != "" {
		res, err := lr.Finish(ctx, note, nil)
		if err == nil && res.Outcome == imsdk.OutcomeSent {
			return
		}
	}
	if (outcome == OutcomeFailed || outcome == OutcomeWaiting) && customNote != "" {
		m.sendFallbackNote(ctx, conn, chat, customNote)
	}
}

func (m *Manager) sendFallbackNote(ctx context.Context, conn imsdk.ChannelConnection, chat imsdk.ChatRef, text string) {
	key := chat.PlatformID
	now := m.nowFn()
	m.mu.Lock()
	if last, ok := m.notedAt[key]; ok && now.Sub(last) < NoteEvery {
		m.mu.Unlock()
		return
	}
	m.notedAt[key] = now
	m.mu.Unlock()
	_, _ = conn.Send(ctx, imsdk.OutboundMessage{Chat: chat, Text: text})
}

func defaultSettleNote(lang Lang, outcome Outcome) string {
	if lang == LangZH {
		switch outcome {
		case OutcomeAnswered:
			return "已回复，见下方消息。"
		case OutcomeUnanswered:
			return "暂未回复。"
		case OutcomeStopped:
			return "已叫停。"
		case OutcomePaused:
			return "已暂停，这条消息不会再回复。"
		case OutcomeFailed:
			return "处理时出错了，请稍后再发一次。"
		default:
			return ""
		}
	}
	switch outcome {
	case OutcomeAnswered:
		return "Answered below."
	case OutcomeUnanswered:
		return "No reply yet."
	case OutcomeStopped:
		return "Stopped."
	case OutcomePaused:
		return "Paused. This message won't get a reply."
	case OutcomeFailed:
		return "Something went wrong. Send it again in a moment."
	default:
		return ""
	}
}

// OpenFor returns all currently open message IDs for ownerKey.
func (m *Manager) OpenFor(ownerKey string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for id, e := range m.entries {
		if e.ownerKey == ownerKey {
			ids = append(ids, id)
		}
	}
	return ids
}

// Close cancels all timers for open live replies without sending messages.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, e := range m.entries {
		m.closeEntry(e)
		delete(m.entries, id)
	}
}

func (m *Manager) closeEntry(e *entry) {
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	m.stopEntryTimers(e)
}

func (m *Manager) stopEntryTimers(e *entry) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.pending != nil {
		e.pending.Stop()
		e.pending = nil
	}
	if e.handover != nil {
		e.handover.Stop()
		e.handover = nil
	}
	if e.tick != nil {
		e.tick.Stop()
		e.tick = nil
		select {
		case <-e.stopTick:
		default:
			close(e.stopTick)
		}
	}
}
