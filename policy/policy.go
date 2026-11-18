// Package policy provides optional access-control gating, 6-digit owner claim codes,
// bot-to-bot loop circuit breakers, and group quiet-stretch aggregation.
package policy

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"sync"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

// Access represents an owner's decision for a contact or group.
type Access string

const (
	AccessUndecided Access = ""
	AccessAllowed   Access = "allowed"
	AccessBlocked   Access = "blocked"
)

// Verdict is the access-control outcome for an inbound trigger message.
type Verdict string

const (
	VerdictDeliver Verdict = "deliver"
	VerdictAsk     Verdict = "ask"
	VerdictDrop    Verdict = "drop"
)

// GateInput describes the trust context of an inbound trigger message.
type GateInput struct {
	Trust    imsdk.Trust
	AskAll   bool
	IsOwner  bool
	ChatKind imsdk.ChatKind
	Contact  Access
	Group    Access
}

// Gate decides whether an inbound trigger message is delivered, held for owner approval, or dropped.
func Gate(in GateInput) Verdict {
	if in.IsOwner {
		return VerdictDeliver
	}
	if in.Contact == AccessBlocked {
		return VerdictDrop
	}
	decided := in.Group
	if in.ChatKind == imsdk.ChatDM {
		decided = in.Contact
	}
	if decided == AccessAllowed {
		return VerdictDeliver
	}
	if decided == AccessBlocked {
		return VerdictDrop
	}
	trust := in.Trust
	if trust == "" {
		trust = imsdk.TrustPublic
	}
	if trust != imsdk.TrustOrg || in.AskAll {
		return VerdictAsk
	}
	return VerdictDeliver
}

const (
	ClaimTTL      = 10 * time.Minute
	ClaimAttempts = 5
	MaxBotHops    = 5

	HearQuiet   = 20 * time.Second
	HearMaxWait = 60 * time.Second
	HearGap     = 2 * time.Minute
)

var reClaim = regexp.MustCompile(`^(?i:/start\s+)?(\d{6})$`)

// GenerateClaimCode returns a cryptographically random 6-digit claim code.
func GenerateClaimCode() string {
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return "000000"
	}
	return fmt.Sprintf("%06d", n.Int64())
}

// ParseClaimAttempt extracts a 6-digit claim code from "/start 123456" or "123456".
func ParseClaimAttempt(text string) (string, bool) {
	m := reClaim.FindStringSubmatch(strings.TrimSpace(text))
	if len(m) != 2 {
		return "", false
	}
	return m[1], true
}

type claimEntry struct {
	code      string
	expiresAt time.Time
	misses    int
}

// ClaimBook tracks active 6-digit owner claim codes and brute-force miss limits per connection.
type ClaimBook struct {
	mu      sync.Mutex
	entries map[string]*claimEntry
}

// NewClaimBook creates a ClaimBook.
func NewClaimBook() *ClaimBook {
	return &ClaimBook{entries: make(map[string]*claimEntry)}
}

// Issue generates and stores a fresh claim code valid for ClaimTTL from now.
func (c *ClaimBook) Issue(connectionID string, now time.Time) (string, time.Time) {
	code := GenerateClaimCode()
	exp := now.Add(ClaimTTL)
	c.mu.Lock()
	c.entries[connectionID] = &claimEntry{code: code, expiresAt: exp}
	c.mu.Unlock()
	return code, exp
}

// Attempt checks text against connectionID's active claim code.
// Returns (claimed=true) on match, or (retired=true) if 5 wrong codes invalidated it.
func (c *ClaimBook) Attempt(connectionID, text string, now time.Time) (claimed bool, retired bool) {
	code, ok := ParseClaimAttempt(text)
	if !ok {
		return false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[connectionID]
	if e == nil {
		return false, false
	}
	if !e.expiresAt.After(now) {
		delete(c.entries, connectionID)
		return false, false
	}
	if code == e.code {
		delete(c.entries, connectionID)
		return true, false
	}
	e.misses++
	if e.misses >= ClaimAttempts {
		delete(c.entries, connectionID)
		return false, true
	}
	return false, false
}

// TriggerDecision classifies whether an inbound message wakes the agent, stays background, or hits the bot brake.
type TriggerDecision string

const (
	TriggerWake       TriggerDecision = "wake"
	TriggerBackground TriggerDecision = "background"
	TriggerBrake      TriggerDecision = "brake"
)

// TriggerInput describes an inbound message for wake/brake evaluation.
type TriggerInput struct {
	DM        bool
	Bot       bool
	Mentioned bool
	Addressed bool
	Hops      int
}

// Trigger evaluates whether an inbound message wakes the bot.
func Trigger(in TriggerInput) TriggerDecision {
	if !in.Bot {
		if in.DM || in.Addressed {
			return TriggerWake
		}
		return TriggerBackground
	}
	if in.DM || !in.Mentioned {
		return TriggerBackground
	}
	if in.Hops >= MaxBotHops {
		return TriggerBrake
	}
	return TriggerWake
}

type hopEntry struct {
	hops int
	told bool
}

// BotHops counts consecutive bot-triggered turns per chat since a human last spoke.
type BotHops struct {
	mu    sync.Mutex
	chats map[string]*hopEntry
}

// NewBotHops creates a BotHops tracker.
func NewBotHops() *BotHops {
	return &BotHops{chats: make(map[string]*hopEntry)}
}

// Hops returns the current consecutive bot turn count in chatID.
func (b *BotHops) Hops(chatID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := b.chats[chatID]; e != nil {
		return e.hops
	}
	return 0
}

// PersonSpoke resets the bot hop counter in chatID when a human speaks.
func (b *BotHops) PersonSpoke(chatID string) {
	b.mu.Lock()
	delete(b.chats, chatID)
	b.mu.Unlock()
}

// BotWoke increments the consecutive bot turn count in chatID.
func (b *BotHops) BotWoke(chatID string) {
	b.mu.Lock()
	e := b.chats[chatID]
	if e == nil {
		e = &hopEntry{}
		b.chats[chatID] = e
	}
	e.hops++
	b.mu.Unlock()
}

// Braked marks chatID as braked and returns true only on the first brake since a human last spoke.
func (b *BotHops) Braked(chatID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.chats[chatID]
	if e == nil {
		e = &hopEntry{hops: MaxBotHops}
		b.chats[chatID] = e
	}
	if e.told {
		return false
	}
	e.told = true
	return true
}

// BotBrakeNote formats a plain-text (never @mentioning) notice when MaxBotHops is reached.
func BotBrakeNote(p imsdk.Platform, botName string) string {
	clean := strings.TrimSpace(strings.TrimLeft(botName, "@"))
	zh := p == imsdk.PlatformWeChat || p == imsdk.PlatformWeCom || p == imsdk.PlatformFeishu || p == imsdk.PlatformDingTalk || p == imsdk.PlatformQQ
	if zh {
		if clean == "" {
			clean = "另一个机器人"
		}
		return fmt.Sprintf("我和 %s 已经来回 %d 轮了，先停下。需要我继续的话，请 @ 我。", clean, MaxBotHops)
	}
	if clean == "" {
		clean = "Another bot"
	}
	return fmt.Sprintf("%s and I have gone back and forth %d times, so I'm stopping here. @ me if you want me to go on.", clean, MaxBotHops)
}

// Pace configures group overhearing intervals.
type Pace struct {
	Quiet   time.Duration
	MaxWait time.Duration
	Gap     time.Duration
}

type hearStretch struct {
	from  time.Time
	since time.Time
	timer *time.Timer
}

// Overhear aggregates unaddressed human messages in groups that have "hear everything" enabled.
type Overhear struct {
	mu      sync.Mutex
	pace    Pace
	waiting map[string]*hearStretch
	heardAt map[string]time.Time
	nowFn   func() time.Time
}

// NewOverhear creates an Overhear scheduler with the given Pace (zeros use defaults).
func NewOverhear(pace Pace, nowFn func() time.Time) *Overhear {
	if pace.Quiet <= 0 {
		pace.Quiet = HearQuiet
	}
	if pace.MaxWait <= 0 {
		pace.MaxWait = HearMaxWait
	}
	if pace.Gap <= 0 {
		pace.Gap = HearGap
	}
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Overhear{
		pace:    pace,
		waiting: make(map[string]*hearStretch),
		heardAt: make(map[string]time.Time),
		nowFn:   nowFn,
	}
}

// Record notes an unaddressed human message at messageAt and schedules onFire when the stretch settles.
func (o *Overhear) Record(chatID string, messageAt time.Time, onFire func(chatID string, from time.Time)) {
	o.mu.Lock()
	now := o.nowFn()
	w := o.waiting[chatID]
	from := messageAt
	since := now
	if w != nil {
		w.timer.Stop()
		from = w.from
		since = w.since
	}
	quietDue := now.Add(o.pace.Quiet)
	maxDue := since.Add(o.pace.MaxWait)
	due := quietDue
	if maxDue.Before(due) {
		due = maxDue
	}
	if last, ok := o.heardAt[chatID]; ok {
		if gapDue := last.Add(o.pace.Gap); gapDue.After(due) {
			due = gapDue
		}
	}
	delay := max(time.Duration(0), due.Sub(now))
	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		o.mu.Lock()
		cur := o.waiting[chatID]
		if cur == nil || cur.timer != timer {
			o.mu.Unlock()
			return
		}
		delete(o.waiting, chatID)
		o.heardAt[chatID] = o.nowFn()
		startFrom := cur.from
		o.mu.Unlock()
		if onFire != nil {
			onFire(chatID, startFrom)
		}
	})
	o.waiting[chatID] = &hearStretch{from: from, since: since, timer: timer}
	o.mu.Unlock()
}

// Cancel drops any pending overheard stretch for chatID (e.g. when someone @-mentions the bot).
func (o *Overhear) Cancel(chatID string) {
	o.mu.Lock()
	if w := o.waiting[chatID]; w != nil {
		w.timer.Stop()
		delete(o.waiting, chatID)
	}
	o.mu.Unlock()
}

// Close stops all pending timers.
func (o *Overhear) Close() {
	o.mu.Lock()
	for id, w := range o.waiting {
		w.timer.Stop()
		delete(o.waiting, id)
	}
	o.mu.Unlock()
}
