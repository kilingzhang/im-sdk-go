package secret_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kilingzhang/im-sdk-go/secret"
)

func TestKeychainChunkingAndVerification(t *testing.T) {
	ctx := context.Background()
	vault := map[string]string{}

	mockRunner := func(_ context.Context, _ string, args []string, stdin string) (int, string, error) {
		if len(args) == 1 && args[0] == "-i" {
			for _, line := range strings.Split(strings.TrimSpace(stdin), "\n") {
				if line == "" {
					continue
				}
				if strings.HasPrefix(line, "add-generic-password ") {
					// Parse -a "<acc>" -U -w "<val>"
					aIdx := strings.Index(line, `-a "`) + 4
					aEnd := strings.Index(line[aIdx:], `"`) + aIdx
					wIdx := strings.Index(line, `-w "`) + 4
					wEnd := strings.LastIndex(line, `"`)
					vault[line[aIdx:aEnd]] = line[wIdx:wEnd]
				} else if strings.HasPrefix(line, "delete-generic-password ") {
					aIdx := strings.Index(line, `-a "`) + 4
					aEnd := strings.Index(line[aIdx:], `"`) + aIdx
					delete(vault, line[aIdx:aEnd])
				}
			}
			return 0, "", nil
		}
		if len(args) >= 5 && args[0] == "find-generic-password" {
			acc := args[4]
			v, ok := vault[acc]
			if !ok {
				return 44, "", nil
			}
			return 0, v + "\n", nil
		}
		return 1, "", nil
	}

	ks := secret.NewKeychain("test.imsdk", mockRunner)
	longSecret := strings.Repeat("中文密钥-secret-token-", 400) // > 6000 bytes -> multi-part
	if err := ks.Set(ctx, "wechat:bot1", longSecret); err != nil {
		t.Fatalf("Set long secret: %v", err)
	}
	if !strings.HasPrefix(vault["wechat:bot1"], "parts:") {
		t.Fatalf("expected multi-part index, got %q", vault["wechat:bot1"])
	}
	got, ok, err := ks.Get(ctx, "wechat:bot1")
	if err != nil || !ok || got != longSecret {
		t.Fatalf("Get mismatch: ok=%v err=%v len=%d", ok, err, len(got))
	}

	// Overwriting with a short secret cleans up stale #1..n parts.
	if err := ks.Set(ctx, "wechat:bot1", "short"); err != nil {
		t.Fatalf("Set short secret: %v", err)
	}
	if _, exists := vault["wechat:bot1#2"]; exists {
		t.Fatal("stale part #2 was not deleted")
	}
	if err := ks.Delete(ctx, "wechat:bot1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(vault) != 0 {
		t.Fatalf("expected empty vault after delete, got %+v", vault)
	}

	// FileStore round-trip
	fs := secret.NewFile(filepath.Join(t.TempDir(), "secrets.json"))
	if err := fs.Set(ctx, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := fs.Get(ctx, "k"); err != nil || !ok || v != "v" {
		t.Fatalf("FileStore Get: %q %v %v", v, ok, err)
	}
}
