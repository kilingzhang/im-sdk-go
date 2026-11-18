package imessage

import (
	"encoding/binary"
	"strings"
	"testing"
)

func buildAttributedBody(text string) []byte {
	raw := []byte(text)
	var lenBytes []byte
	switch {
	case len(raw) < 0x80:
		lenBytes = []byte{byte(len(raw))}
	case len(raw) <= 0xFFFF:
		var buf [2]byte
		binary.LittleEndian.PutUint16(buf[:], uint16(len(raw)))
		lenBytes = append([]byte{0x81}, buf[:]...)
	default:
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(len(raw)))
		lenBytes = append([]byte{0x82}, buf[:]...)
	}

	var out []byte
	out = append(out, []byte("\x04\x0bstreamtyped\x81\xe8\x03\x84\x01@\x84\x84\x84\x12NSAttributedString\x00\x84\x84\x08NSObject\x00\x85\x92\x84\x84\x84\x08")...)
	out = append(out, []byte("NSString\x01\x94\x84\x01+")...)
	out = append(out, lenBytes...)
	out = append(out, raw...)
	out = append(out, []byte("\x86\x84\x02iI\x01")...)
	return out
}

func TestDecodeAttributedBodyAndPureHelpers(t *testing.T) {
	if got, ok := DecodeAttributedBody(buildAttributedBody("你好 Alex 🎉")); !ok || got != "你好 Alex 🎉" {
		t.Fatalf("short DecodeAttributedBody = (%q, %v)", got, ok)
	}
	med := strings.Repeat("x", 300)
	if got, ok := DecodeAttributedBody(buildAttributedBody(med)); !ok || got != med {
		t.Fatalf("0x81 DecodeAttributedBody failed (ok=%v, len=%d)", ok, len(got))
	}
	huge := strings.Repeat("y", 70_000)
	if got, ok := DecodeAttributedBody(buildAttributedBody(huge)); !ok || got != huge {
		t.Fatalf("0x82 DecodeAttributedBody failed (ok=%v, len=%d)", ok, len(got))
	}
	if _, ok := DecodeAttributedBody([]byte("not a typedstream")); ok {
		t.Fatal("expected non-typedstream blob to return ok=false")
	}
	if _, ok := DecodeAttributedBody([]byte("NSString\x01\x94\x84\x01+\x81\x01")); ok {
		t.Fatal("expected truncated 0x81 blob to return ok=false")
	}

	accts := ParseAccounts("E:Me@iCloud.com\nE:me@icloud.com\nmissing value\nP:+8613800000000\n")
	if len(accts) != 2 || accts[0] != "me@icloud.com" || accts[1] != "+8613800000000" {
		t.Fatalf("ParseAccounts = %v", accts)
	}
	if CountActiveAccounts("(\n    \"A\",\n    \"B\"\n)\n") != 2 {
		t.Fatal("CountActiveAccounts expected 2")
	}
	if CountActiveAccounts("(\n)\n") != 0 {
		t.Fatal("CountActiveAccounts expected 0")
	}

	md := "## Heading\n**Done.** See *notes* and `code.go` at [Docs](https://example.com):\n```go\nfmt.Println(1)\n```"
	wantPlain := "Heading\nDone. See notes and code.go at Docs (https://example.com):\nfmt.Println(1)\n"
	if got := Plain(md); got != wantPlain {
		t.Fatalf("Plain = %q, want %q", got, wantPlain)
	}
}
