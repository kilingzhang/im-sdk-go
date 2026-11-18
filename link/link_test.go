package link_test

import (
	"testing"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/link"
)

func TestStepTransitions(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := link.State{Kind: link.KindConnecting, Attempt: 0}

	// Transient network failure enters waiting with 5s backoff.
	s, eff := link.Step(s, link.Event{Kind: link.EventFailed}, now, nil)
	if eff != link.EffectRetryLater || s.Kind != link.KindWaiting || s.Attempt != 1 || !s.RetryAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("unexpected waiting state: %+v eff=%s", s, eff)
	}

	// Timer triggers reconnect at attempt=1.
	s, eff = link.Step(s, link.Event{Kind: link.EventTimer}, now.Add(5*time.Second), nil)
	if eff != link.EffectConnect || s.Kind != link.KindConnecting || s.Attempt != 1 {
		t.Fatalf("unexpected reconnect state: %+v eff=%s", s, eff)
	}

	// Credentials revoked stops retrying until human intervention.
	s, eff = link.Step(s, link.Event{
		Kind:     link.EventReported,
		Reported: imsdk.ConnectionState{Kind: imsdk.StateError, Code: imsdk.ConnErrCredentialsRevoked},
	}, now, nil)
	if eff != link.EffectClose || s.Kind != link.KindStopped || s.Code != imsdk.ConnErrCredentialsRevoked {
		t.Fatalf("expected stopped state: %+v eff=%s", s, eff)
	}
	if st := link.Stored(s); st.State != "error" || st.StateDetail != "credentials_revoked" {
		t.Fatalf("unexpected stored projection: %+v", st)
	}

	// Late opened after removal is discarded.
	s, eff = link.Step(s, link.Event{Kind: link.EventRemove}, now, nil)
	if s.Kind != link.KindRemoved || eff != link.EffectClose {
		t.Fatalf("expected removed: %+v eff=%s", s, eff)
	}
	_, eff = link.Step(s, link.Event{Kind: link.EventOpened}, now, nil)
	if eff != link.EffectDiscard {
		t.Fatalf("expected discard for late opened, got %s", eff)
	}
}
