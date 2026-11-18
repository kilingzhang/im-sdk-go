package live_test

import (
	"context"
	"sync"
	"testing"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/live"
)

type expiringLive struct {
	mu       sync.Mutex
	until    time.Time
	finished []string
	doneCh   chan struct{}
}

func (e *expiringLive) Until() time.Time { return e.until }
func (e *expiringLive) Update(context.Context, string) error {
	return nil
}
func (e *expiringLive) Finish(_ context.Context, md string, _ []imsdk.Mention) (imsdk.SendResult, error) {
	e.mu.Lock()
	e.finished = append(e.finished, md)
	e.mu.Unlock()
	select {
	case <-e.doneCh:
	default:
		close(e.doneCh)
	}
	return imsdk.SendResult{Outcome: imsdk.OutcomeSent, PlatformID: "stream-1"}, nil
}

type stubConn struct {
	imsdk.BaseConnection
	lr imsdk.LiveReply
}

func (s *stubConn) Send(context.Context, imsdk.OutboundMessage) (imsdk.SendResult, error) {
	return imsdk.SendResult{Outcome: imsdk.OutcomeSent, PlatformID: "m1"}, nil
}
func (s *stubConn) StartLive(context.Context, imsdk.ChatRef, imsdk.LiveOptions) (imsdk.LiveReply, error) {
	return s.lr, nil
}
func (s *stubConn) Download(context.Context, imsdk.InboundAttachment, string) error { return nil }
func (s *stubConn) Close() error                                                    { return nil }

func TestStreamWindowHandoverAndToolClassification(t *testing.T) {
	if live.ClassifyTool("bash_exec") != live.StepCommand {
		t.Fatalf("unexpected ClassifyTool: %s", live.ClassifyTool("bash_exec"))
	}
	now := time.Now()
	el := &expiringLive{until: now.Add(20 * time.Millisecond), doneCh: make(chan struct{})}
	mgr := live.NewManager(live.Options{})
	defer mgr.Close()

	mgr.Open(live.OpenInput{
		MessageID:         "msg-wecom",
		OwnerKey:          "agent1",
		Platform:          imsdk.PlatformWeCom,
		Chat:              imsdk.ChatRef{PlatformID: "u1", Kind: imsdk.ChatDM},
		PlatformMessageID: "req-1",
		SenderID:          "u1",
		Connection:        &stubConn{lr: el},
	})

	select {
	case <-el.doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream expiry handover")
	}

	// After stream expiry closed the live reply with a handover note, Answer returns (_, false)
	// so the caller sends the real answer as a fresh active message.
	if _, ok := mgr.Answer(context.Background(), "msg-wecom", "Final answer", nil); ok {
		t.Fatal("expected Answer to report false after stream window handover")
	}
}
