package mention_test

import (
	"testing"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/mention"
)

func TestSpotsAndRender(t *testing.T) {
	mentions := []imsdk.Mention{
		{ID: "u1", Name: "Bob"},
		{ID: "u2", Name: "Bob Smith"},
		{ID: "u3", Name: "张三"},
	}
	text := "Hi @Bob Smith, not @Bobby, email a@Bob.com, `@Bob` in code, and @张三请看下"
	got := mention.Render(text, mentions, func(m imsdk.Mention) string {
		return "<@" + m.ID + ">"
	})
	want := "Hi <@u2>, not @Bobby, email a@Bob.com, `@Bob` in code, and <@u3>请看下"
	if got != want {
		t.Fatalf("Render:\n got: %q\nwant: %q", got, want)
	}
}

func TestResolveDisambiguationAndUnmatched(t *testing.T) {
	candidates := []mention.Candidate{
		{PlatformID: "ou_1", ContactID: "c1", Name: "Alex", Spoke: false},
		{PlatformID: "ou_2", ContactID: "c2", Name: "Alex", Spoke: true},
		{PlatformID: "ou_3", ContactID: "c3", Name: "Mei", Spoke: false},
		{PlatformID: "ou_4", ContactID: "c4", Name: "Mei", Spoke: false},
	}
	res := mention.Resolve("@Alex @Mei @Ghost @all `@GhostCode`", candidates)
	if len(res.Mentions) != 1 || res.Mentions[0].ID != "ou_2" {
		t.Fatalf("expected Alex disambiguated to ou_2, got %+v", res.Mentions)
	}
	if len(res.Mentioned) != 1 || res.Mentioned[0].Contact != "c2" {
		t.Fatalf("unexpected Mentioned: %+v", res.Mentioned)
	}
	// @Mei is ambiguous (neither spoke) so unmatched; @Ghost is unmatched; @all and code are skipped.
	if len(res.Unmatched) != 2 || res.Unmatched[0] != "@Mei" || res.Unmatched[1] != "@Ghost" {
		t.Fatalf("unexpected Unmatched: %+v", res.Unmatched)
	}
}
