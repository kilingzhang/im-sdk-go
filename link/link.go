// Package link implements the pure-function connection state machine, generation-guarded
// reconnect driver, and system sleep/wake clock-jump detector.
package link

import (
	"context"
	"sync"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

// Kind identifies a state in the bot connection state machine.
type Kind string

const (
	KindConnecting       Kind = "connecting"
	KindConnected        Kind = "connected"
	KindAwaitingApproval Kind = "awaiting_approval"
	KindWaiting          Kind = "waiting"
	KindStopped          Kind = "stopped"
	KindRemoved          Kind = "removed"
)

// State represents the current lifecycle state of one bot connection.
type State struct {
	Kind    Kind
	Attempt int
	Code    imsdk.ConnectionErrorCode
	RetryAt time.Time
}

// EventKind classifies an input event to Step.
type EventKind string

const (
	EventConnect  EventKind = "connect"
	EventOpened   EventKind = "opened"
	EventFailed   EventKind = "failed"
	EventReported EventKind = "reported"
	EventTimer    EventKind = "timer"
	EventRemove   EventKind = "remove"
)

// Event drives one transition of the connection state machine.
type Event struct {
	Kind     EventKind
	Code     imsdk.ConnectionErrorCode
	Reported imsdk.ConnectionState
}

// Effect instructs the caller what side effect to perform after Step.
type Effect string

const (
	EffectConnect    Effect = "connect"
	EffectRetryLater Effect = "retry_later"
	EffectClose      Effect = "close"
	EffectDiscard    Effect = "discard"
	EffectNone       Effect = "none"
)

// DefaultBackoff defines automatic retry delays; the final entry repeats indefinitely.
var DefaultBackoff = []time.Duration{
	5 * time.Second,
	15 * time.Second,
	30 * time.Second,
	60 * time.Second,
	120 * time.Second,
	300 * time.Second,
}

// Backoff returns the delay for the given 0-based attempt index.
func Backoff(attempt int, delays []time.Duration) time.Duration {
	if len(delays) == 0 {
		delays = DefaultBackoff
	}
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(delays) {
		return delays[len(delays)-1]
	}
	return delays[attempt]
}

func isStopCode(code imsdk.ConnectionErrorCode) bool {
	switch code {
	case imsdk.ConnErrCredentialsRevoked, imsdk.ConnErrPermissionMissing, imsdk.ConnErrCredentialsMissing:
		return true
	default:
		return false
	}
}

func attemptOf(s State) int {
	if s.Kind == KindConnecting || s.Kind == KindWaiting {
		return s.Attempt
	}
	return 0
}

func offline(attempt int, code imsdk.ConnectionErrorCode, now time.Time, delays []time.Duration) State {
	if isStopCode(code) {
		return State{Kind: KindStopped, Code: code}
	}
	if code == "" {
		code = imsdk.ConnErrNetwork
	}
	return State{
		Kind:    KindWaiting,
		Attempt: attempt + 1,
		Code:    code,
		RetryAt: now.Add(Backoff(attempt, delays)),
	}
}

func settle(next State) (State, Effect) {
	if next.Kind == KindWaiting {
		return next, EffectRetryLater
	}
	return next, EffectClose
}

// Step computes the next connection state and required side effect purely without I/O.
func Step(state State, event Event, now time.Time, delays []time.Duration) (State, Effect) {
	if state.Kind == KindRemoved {
		if event.Kind == EventOpened {
			return state, EffectDiscard
		}
		return state, EffectNone
	}
	switch event.Kind {
	case EventRemove:
		return State{Kind: KindRemoved}, EffectClose
	case EventConnect:
		return State{Kind: KindConnecting, Attempt: 0}, EffectConnect
	case EventTimer:
		if state.Kind == KindWaiting {
			return State{Kind: KindConnecting, Attempt: state.Attempt}, EffectConnect
		}
		return state, EffectNone
	case EventOpened:
		if state.Kind == KindConnecting {
			return State{Kind: KindConnected}, EffectNone
		}
		if state.Kind == KindConnected || state.Kind == KindAwaitingApproval {
			return state, EffectNone
		}
		return state, EffectDiscard
	case EventFailed:
		if state.Kind == KindWaiting || state.Kind == KindStopped {
			return state, EffectNone
		}
		return settle(offline(attemptOf(state), event.Code, now, delays))
	case EventReported:
		rep := event.Reported
		switch rep.Kind {
		case imsdk.StateConnected:
			return State{Kind: KindConnected}, EffectNone
		case imsdk.StateAwaitingApproval:
			return State{Kind: KindAwaitingApproval}, EffectNone
		case imsdk.StateConnecting, imsdk.StateReconnecting:
			return State{Kind: KindConnecting, Attempt: attemptOf(state)}, EffectNone
		default:
			if state.Kind == KindWaiting || state.Kind == KindStopped {
				return state, EffectNone
			}
			return settle(offline(attemptOf(state), rep.Code, now, delays))
		}
	default:
		return state, EffectNone
	}
}

// StoredState is the persistence/display projection of State.
type StoredState struct {
	State       string
	StateDetail string
}

// Stored maps a runtime State to its persisted representation.
func Stored(state State) StoredState {
	switch state.Kind {
	case KindConnected:
		return StoredState{State: "connected"}
	case KindAwaitingApproval:
		return StoredState{State: "awaiting_approval"}
	case KindWaiting, KindStopped:
		return StoredState{State: "error", StateDetail: string(state.Code)}
	default:
		return StoredState{State: "reconnecting"}
	}
}

// WakeMonitor detects system sleep by checking for wall-clock jumps exceeding threshold
// between periodic ticks, invoking onWake with the estimated sleep duration.
func WakeMonitor(ctx context.Context, every, threshold time.Duration, onWake func(slept time.Duration)) {
	if every <= 0 {
		every = 15 * time.Second
	}
	if threshold <= 0 {
		threshold = 60 * time.Second
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	last := time.Now().UnixMilli()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			slept := time.Duration(now-last)*time.Millisecond - every
			last = now
			if slept > threshold && onWake != nil {
				onWake(slept)
			}
		}
	}
}

// ManagedOptions configures a single generation-guarded managed connection.
type ManagedOptions struct {
	Channel     imsdk.Channel
	Credentials func(ctx context.Context) (imsdk.Credentials, error)
	Events      imsdk.ChannelEvents
	OnLinkState func(State)
	Backoff     []time.Duration
	Now         func() time.Time
}

// Managed supervises one ChannelConnection through Step with generation-guarded callbacks.
type Managed struct {
	opts      ManagedOptions
	mu        sync.Mutex
	state     State
	gen       uint64
	live      imsdk.ChannelConnection
	timer     *time.Timer
	attemptFn context.CancelFunc
}

// NewManaged creates a Managed supervisor in the initial Connecting(0) state without starting I/O.
func NewManaged(opts ManagedOptions) *Managed {
	return &Managed{
		opts:  opts,
		state: State{Kind: KindConnecting, Attempt: 0},
	}
}

func (m *Managed) now() time.Time {
	if m.opts.Now != nil {
		return m.opts.Now()
	}
	return time.Now()
}

// State returns the current link state.
func (m *Managed) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Live returns the currently active ChannelConnection, if any.
func (m *Managed) Live() imsdk.ChannelConnection {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.live
}

// Connect starts or restarts a fresh connection attempt immediately.
func (m *Managed) Connect(given imsdk.Credentials) {
	m.drive(Event{Kind: EventConnect}, 0, false, given)
}

// Remove permanently closes the connection and cancels any scheduled retry.
func (m *Managed) Remove() {
	m.drive(Event{Kind: EventRemove}, 0, false, nil)
}

func (m *Managed) drive(ev Event, gen uint64, checkGen bool, given imsdk.Credentials) {
	m.mu.Lock()
	if checkGen && gen != m.gen {
		m.mu.Unlock()
		return
	}
	next, eff := Step(m.state, ev, m.now(), m.opts.Backoff)
	m.state = next
	onState := m.opts.OnLinkState
	if eff == EffectNone {
		m.mu.Unlock()
		if onState != nil {
			onState(next)
		}
		return
	}
	if m.timer != nil {
		m.timer.Stop()
		m.timer = nil
	}
	if m.attemptFn != nil {
		m.attemptFn()
		m.attemptFn = nil
	}
	oldLive := m.live
	m.live = nil

	var startGen uint64
	var attemptCtx context.Context
	if eff == EffectRetryLater && next.Kind == KindWaiting {
		delay := max(time.Duration(0), next.RetryAt.Sub(m.now()))
		m.timer = time.AfterFunc(delay, func() {
			m.drive(Event{Kind: EventTimer}, 0, false, nil)
		})
	} else if eff == EffectConnect {
		m.gen++
		startGen = m.gen
		attemptCtx, m.attemptFn = context.WithCancel(context.Background())
	}
	m.mu.Unlock()

	if oldLive != nil {
		_ = oldLive.Close()
	}
	if onState != nil {
		onState(next)
	}
	if eff == EffectConnect {
		go m.runAttempt(attemptCtx, startGen, given)
	}
}

func (m *Managed) runAttempt(ctx context.Context, gen uint64, given imsdk.Credentials) {
	creds := given
	if len(creds) == 0 && m.opts.Credentials != nil {
		fetched, err := m.opts.Credentials(ctx)
		if err != nil || len(fetched) == 0 {
			m.drive(Event{Kind: EventFailed, Code: imsdk.ConnErrCredentialsMissing}, gen, true, nil)
			return
		}
		creds = fetched
	}

	var reportedCode imsdk.ConnectionErrorCode
	var repMu sync.Mutex

	conn, err := m.opts.Channel.Connect(ctx, creds, imsdk.ChannelEvents{
		OnMessage: func(msg imsdk.InboundMessage) {
			m.mu.Lock()
			ok := m.gen == gen
			m.mu.Unlock()
			if ok && m.opts.Events.OnMessage != nil {
				m.opts.Events.OnMessage(msg)
			}
		},
		OnState: func(st imsdk.ConnectionState) {
			if st.Kind == imsdk.StateError {
				repMu.Lock()
				reportedCode = st.Code
				repMu.Unlock()
			}
			if m.opts.Events.OnState != nil {
				m.opts.Events.OnState(st)
			}
			m.drive(Event{Kind: EventReported, Reported: st}, gen, true, nil)
		},
		OnMembership: func(ch imsdk.MembershipChange) {
			m.mu.Lock()
			ok := m.gen == gen
			m.mu.Unlock()
			if ok && m.opts.Events.OnMembership != nil {
				m.opts.Events.OnMembership(ch)
			}
		},
	})
	if err != nil {
		repMu.Lock()
		code := reportedCode
		repMu.Unlock()
		m.drive(Event{Kind: EventFailed, Code: code}, gen, true, nil)
		return
	}

	m.mu.Lock()
	if m.gen != gen || m.state.Kind == KindRemoved {
		m.mu.Unlock()
		_ = conn.Close()
		return
	}
	m.live = conn
	m.mu.Unlock()
	m.drive(Event{Kind: EventOpened}, gen, true, nil)
}
