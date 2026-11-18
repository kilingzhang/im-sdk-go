package outbox_test

import (
	"context"
	"sync"
	"testing"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/live"
	"github.com/kilingzhang/im-sdk-go/mention"
	"github.com/kilingzhang/im-sdk-go/outbox"
)

type fakeLive struct {
	mu       sync.Mutex
	until    time.Time
	updates  []string
	finished string
}

func (l *fakeLive) Until() time.Time { return l.until }
func (l *fakeLive) Update(_ context.Context, md string) error {
	l.mu.Lock()
	l.updates = append(l.updates, md)
	l.mu.Unlock()
	return nil
}
func (l *fakeLive) Finish(_ context.Context, md string, _ []imsdk.Mention) (imsdk.SendResult, error) {
	l.mu.Lock()
	l.finished = md
	l.mu.Unlock()
	return imsdk.SendResult{Outcome: imsdk.OutcomeSent, PlatformID: "live-msg-1"}, nil
}

type fakeConn struct {
	imsdk.BaseConnection
	mu    sync.Mutex
	sends []imsdk.OutboundMessage
	live  *fakeLive
	failN int
}

func (c *fakeConn) Send(_ context.Context, msg imsdk.OutboundMessage) (imsdk.SendResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sends = append(c.sends, msg)
	if c.failN > 0 {
		c.failN--
		return imsdk.SendResult{
			Outcome:            imsdk.OutcomeFailed,
			Code:               imsdk.SendErrNetwork,
			Retryable:          true,
			TextSentPlatformID: "txt-1",
		}, nil
	}
	return imsdk.SendResult{Outcome: imsdk.OutcomeSent, PlatformID: "sent-1"}, nil
}

func (c *fakeConn) StartLive(_ context.Context, _ imsdk.ChatRef, _ imsdk.LiveOptions) (imsdk.LiveReply, error) {
	if c.live == nil {
		return nil, imsdk.ErrUnsupported
	}
	return c.live, nil
}

func (c *fakeConn) Download(context.Context, imsdk.InboundAttachment, string) error { return nil }
func (c *fakeConn) Close() error                                                    { return nil }

func TestOutboxTextSentRetriesOnlyFilesAndLiveAnswer(t *testing.T) {
	ctx := context.Background()
	conn := &fakeConn{
		BaseConnection: imsdk.BaseConnection{
			CapabilitiesValue: imsdk.Capabilities{MaxTextLength: 20, Mentions: true},
		},
		failN: 1,
	}
	ob := outbox.New([]time.Duration{time.Millisecond}, nil)
	rep, err := ob.Send(ctx, conn, outbox.Request{
		Chat:  imsdk.ChatRef{PlatformID: "g1", Kind: imsdk.ChatGroup},
		Text:  "Hello @Alice",
		Files: []imsdk.OutboundFile{{Path: "/tmp/a.txt"}},
		Candidates: []mention.Candidate{
			{PlatformID: "u_alice", ContactID: "c1", Name: "Alice", Spoke: true},
		},
	})
	if err != nil {
		t.Fatalf("Outbox.Send failed: %v", err)
	}
	if rep.State != imsdk.OutcomeSent || rep.PlatformIDs[0] != "txt-1" {
		t.Fatalf("unexpected report: %+v", rep)
	}
	if len(conn.sends) != 2 || conn.sends[0].Text != "Hello @Alice" || conn.sends[1].Text != "" || len(conn.sends[1].Files) != 1 {
		t.Fatalf("expected retry to omit already-sent text: %+v", conn.sends)
	}

	// Live reply handover: first chunk replaces live reply in place.
	lm := live.NewManager(live.Options{})
	defer lm.Close()
	fl := &fakeLive{}
	conn2 := &fakeConn{
		BaseConnection: imsdk.BaseConnection{
			CapabilitiesValue: imsdk.Capabilities{MaxTextLength: 100, Mentions: true},
		},
		live: fl,
	}
	lm.Open(live.OpenInput{
		MessageID:         "m1",
		OwnerKey:          "a1",
		Platform:          imsdk.PlatformFeishu,
		Chat:              imsdk.ChatRef{PlatformID: "g1", Kind: imsdk.ChatGroup},
		PlatformMessageID: "om_1",
		SenderID:          "ou_sender",
		Connection:        conn2,
	})
	ob2 := outbox.New([]time.Duration{time.Millisecond}, lm)
	rep2, err := ob2.Send(ctx, conn2, outbox.Request{
		Chat:              imsdk.ChatRef{PlatformID: "g1", Kind: imsdk.ChatGroup},
		Text:              "Done!",
		ReplyToMessageID:  "m1",
		ReplyToPlatformID: "om_1",
	})
	if err != nil {
		t.Fatalf("live Outbox.Send: %v", err)
	}
	if rep2.PlatformIDs[0] != "live-msg-1" || fl.finished != "Done!" {
		t.Fatalf("expected answer routed into live reply, got rep=%+v finished=%q", rep2, fl.finished)
	}
}
