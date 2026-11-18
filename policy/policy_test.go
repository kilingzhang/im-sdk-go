package policy_test

import (
	"testing"
	"time"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/policy"
)

func TestGateAndClaimsAndBotHops(t *testing.T) {
	if v := policy.Gate(policy.GateInput{Trust: imsdk.TrustPublic, ChatKind: imsdk.ChatDM}); v != policy.VerdictAsk {
		t.Fatalf("public DM should ask, got %s", v)
	}
	if v := policy.Gate(policy.GateInput{Trust: imsdk.TrustOrg, ChatKind: imsdk.ChatGroup}); v != policy.VerdictDeliver {
		t.Fatalf("org group should deliver by default, got %s", v)
	}
	if v := policy.Gate(policy.GateInput{Trust: imsdk.TrustOrg, AskAll: true, ChatKind: imsdk.ChatGroup}); v != policy.VerdictAsk {
		t.Fatalf("org group with AskAll should ask, got %s", v)
	}
	if v := policy.Gate(policy.GateInput{Trust: imsdk.TrustOrg, Contact: policy.AccessBlocked, ChatKind: imsdk.ChatGroup}); v != policy.VerdictDrop {
		t.Fatalf("blocked contact should drop even in org group, got %s", v)
	}

	now := time.Now()
	cb := policy.NewClaimBook()
	code, _ := cb.Issue("conn1", now)
	for i := 0; i < 4; i++ {
		claimed, retired := cb.Attempt("conn1", "999999", now)
		if claimed || retired {
			t.Fatalf("attempt %d unexpectedly ended: claimed=%v retired=%v", i, claimed, retired)
		}
	}
	// If code happens to be 999999 pick another wrong code.
	wrong := "000001"
	if code == wrong {
		wrong = "000002"
	}
	if _, retired := cb.Attempt("conn1", wrong, now); !retired {
		t.Fatal("5th wrong attempt must retire claim code")
	}
	if claimed, _ := cb.Attempt("conn1", "/start "+code, now); claimed {
		t.Fatal("retired code must not succeed")
	}

	hops := policy.NewBotHops()
	for i := 0; i < policy.MaxBotHops; i++ {
		dec := policy.Trigger(policy.TriggerInput{Bot: true, Mentioned: true, Hops: hops.Hops("c1")})
		if dec != policy.TriggerWake {
			t.Fatalf("hop %d expected wake, got %s", i, dec)
		}
		hops.BotWoke("c1")
	}
	if dec := policy.Trigger(policy.TriggerInput{Bot: true, Mentioned: true, Hops: hops.Hops("c1")}); dec != policy.TriggerBrake {
		t.Fatalf("expected brake at MaxBotHops, got %s", dec)
	}
	if !hops.Braked("c1") || hops.Braked("c1") {
		t.Fatal("Braked should return true exactly once until a person speaks")
	}
	hops.PersonSpoke("c1")
	if hops.Hops("c1") != 0 {
		t.Fatal("PersonSpoke must reset hops")
	}
}
