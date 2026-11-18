package registry_test

import (
	"runtime"
	"testing"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/registry"
)

func TestDefaultRegistry(t *testing.T) {
	ch := registry.Default()
	want := 9
	if runtime.GOOS == "darwin" {
		want = 10
	}
	if len(ch) != want {
		t.Fatalf("expected %d channels on %s, got %d", want, runtime.GOOS, len(ch))
	}
	if _, ok := ch[imsdk.PlatformWeChat]; !ok {
		t.Fatal("missing wechat channel in Default registry")
	}
	for platform, adapter := range ch {
		if adapter == nil {
			t.Fatalf("nil adapter for %s", platform)
		}
		if adapter.Platform() != platform {
			t.Fatalf("platform mismatch: key=%s adapter=%s", platform, adapter.Platform())
		}
		if adapter.Trust() == "" {
			t.Fatalf("empty trust for %s", platform)
		}
	}
}
