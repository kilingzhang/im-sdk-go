# im-sdk-go

A standalone, pure-Go (`CGO_ENABLED=0`), outbound-only multi-platform IM bot & messaging channel SDK.

## Highlights

- **10 Platforms Behind One Semantic Contract**:
  - **Domestic China**: Personal WeChat (`wechat` via Tencent iLink `2.4.6` + AES-128-ECB CDN), Enterprise WeChat (`wecom` intelligent bot WebSocket + streaming), Feishu / Lark (`feishu` QR app creation + WebSocket + CardKit 2.0 streaming), DingTalk (`dingtalk` device QR + Stream WebSocket + AI streaming card), QQ (`qq` QR connect + official Gateway WebSocket).
  - **Global & Developer**: Slack (`slack` App Manifest + Socket Mode + Web API), Discord (`discord` Gateway WebSocket + REST), Telegram (`telegram` Bot API long polling + UTF-16 `text_mention`), GitHub (`github` OAuth Device Flow + ETag conditional notification polling + inline PR review replies), macOS iMessage (`imessage` self-chat via `purego` `libsqlite3.dylib` read-only `chat.db` polling + AppleScript).
- **Outbound-Only (No Public Webhook Server)**: Every adapter connects outward from the local machine via WebSocket, HTTP long polling, ETag polling, or local read-only SQLite.
- **Explicit State Machines & Delivery Guarantees**:
  - `link`: Pure-function connection state machine with exponential backoff, generation-guarded callbacks, and system sleep/wake clock-jump detection.
  - `live`: Throttled (`1s` render / `5s` tick) in-place progress replies, single in-flight serialization, and stream-window expiration handover.
  - `outbox` & `mention`: Paragraph/line-aware message chunking, `TextSentPlatformID` file-only retries (never resending already-delivered text), and code-block-safe `@name` resolution.
  - `policy`: Optional 3-tier access gate (`org` / `public` / `owner`), 6-digit brute-force-guarded claim codes, 5-round bot-to-bot circuit breaker, and group quiet-stretch aggregation.
  - `secret`: Native OS credential stores (macOS Keychain via `security -i` stdin with Base64 chunking and read-back verification, Windows CurrentUser DPAPI, Linux `secret-tool`, and `0600` file / in-memory stores).
- **Minimal Dependencies & `CGO_ENABLED=0`**: Depends only on `github.com/coder/websocket` and `github.com/ebitengine/purego` (used on macOS to call `/usr/lib/libsqlite3.dylib` without CGO).

## Quick Start

```go
package main

import (
	"context"
	"fmt"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/registry"
)

func main() {
	channels := registry.Default()
	tg := channels[imsdk.PlatformTelegram]

	ctx := context.Background()
	conn, err := tg.Connect(ctx, imsdk.Credentials{"token": "123456:ABCDEF..."}, imsdk.ChannelEvents{
		OnMessage: func(msg imsdk.InboundMessage) {
			fmt.Printf("[%s] %s: %s\n", msg.Chat.PlatformID, msg.Sender.Name, msg.Text)
		},
		OnState: func(st imsdk.ConnectionState) {
			fmt.Printf("state: %s\n", st.Kind)
		},
	})
	if err != nil {
		panic(err)
	}
	defer conn.Close()
}
```

## Testing

All core packages and platform adapters run offline with injected HTTP transports, loopback WebSocket servers, and temporary SQLite databases:

```sh
CGO_ENABLED=0 go vet ./...
CGO_ENABLED=0 go test -race -count=1 ./...
```

## License

[MIT](LICENSE) — includes protocol and state-machine attributions to [Mingle](https://github.com/earayu/Mingle) (MIT) and [dsh-im](https://github.com/xmanrui/dsh-im) (MIT).
