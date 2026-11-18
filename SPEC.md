# `im-sdk-go` 架构与接口规格

## 1. 目标与边界

`im-sdk-go`（`module github.com/kilingzhang/im-sdk-go`）是一个**零业务耦合、纯出站连接、支持 `CGO_ENABLED=0` 静态编译**的跨平台 IM 机器人与本地消息通道 Go SDK。

核心设计原则：
1. **统一语义，原生呈现**：顶层只暴露 `Channel` 与 `ChannelConnection` 两个核心接口，不引入“最低公分母”聊天框架；各平台适配器按自身习惯呈现（流式卡片、消息原地编辑、被动回复或输入状态），不支持的能力通过 `Degradation` 显式回报降级。
2. **纯出站连接，零公网依赖**：10 个平台全部由本机主动发起 WebSocket 长连接、HTTP 长轮询、ETag 条件轮询或本机数据库只读轮询，不需要任何公网 Webhook 地址或中转服务器。
3. **显式状态机与幂等保护**：连接重连（`link`）、流式进度节流（`live`）、分段与部分发送重试（`outbox`）和访问安全策略（`policy`）均为独立纯逻辑包，保证迟到的异步回调不覆盖新状态、文本已发出而附件失败重试时不重复发文字。
4. **零空闲资源开销**：未建立连接时不启动任何后台 goroutine、定时器或外部子进程。

---

## 2. 目录结构

```text
im-sdk-go/
├── channel.go              # 核心类型与接口契约（Channel, ChannelConnection, Capabilities, LiveReply 等）
├── mention/                # @名字 双向解析（最长匹配、跳过代码块与邮箱、群内发言消歧）
├── link/                   # 纯函数连接重连状态机（指数退避、gen 防过期事件、睡眠唤醒时钟跳变检测）
├── live/                   # 实时进度管理（1s 节流、5s 刷新、单 inFlight 串行化、流式窗口到期自动切换）
├── outbox/                 # 出站管线（按 MaxTextLength 段落切分、可重试错误退避、TextSent 仅重传附件）
├── policy/                 # 可选安全与协作策略（Gate 三级信任、6 位认领码、BotHops 5 轮熔断、群旁听聚合器）
├── secret/                 # 系统凭据存储（macOS Keychain Base64 分片读回校验、Windows DPAPI、Linux secret-tool、文件/内存）
├── imtest/                 # 离线协议测试工具（内存 HTTP RoundTripper、内存 WebSocket 对）
├── registry/               # 全平台按需注册表（macOS 含 imessage，其他系统含其余 9 个平台）
└── channels/               # 10 个独立平台适配器子包
    ├── wechat/             # 个人微信（腾讯 iLink 2.4.6：多号 local_token_list、扫码+验证码、35s 长轮询、context_token、AES-128-ECB CDN）
    ├── wecom/              # 企业微信智能机器人（扫码建机器人、WebSocket 帧长连接、replyStream 9m 切换、失效自动转主动发送）
    ├── feishu/             # 飞书 / Lark（扫码建应用与重扫补权限、WS 长连接独立去重、CardKit 8m 续期流式卡片、进群历史与机器人名册）
    ├── dingtalk/           # 钉钉（DING_DWS_CLAW 设备码扫码、Stream WebSocket、OpenAPI 发送、🤔思考中回执、共享 AI 流式卡片）
    ├── qq/                 # QQ 官方机器人（扫码连接器、Gateway WebSocket、C2C/群聊被动与主动发送、私聊 50s 续正在输入）
    ├── slack/              # Slack（预置 Manifest 含 /stop 斜杠命令、Socket Mode、Web API 外部上传、频道与线程父子映射）
    ├── discord/            # Discord（Gateway 心跳/Resume、动态感知 Message Content Intent 重连、Thread followUp、附件上传）
    ├── telegram/           # Telegram（getUpdates 30s 长轮询、t.me 一键认领深链、编辑消息流式回复、UTF-16 text_mention）
    ├── github/             # GitHub（OAuth Device Flow、/notifications ETag 条件轮询 304 零扣额、PR 行内 review 归楼、协作者白名单）
    └── imessage/           # macOS iMessage 自聊（darwin 构建标签：purego libsqlite3 只读轮询 chat.db、attributedBody 解码、防回读、双线程路由）
```

---

## 3. 平台能力矩阵

| 平台 | `Trust` | 接入方式 | 传输协议 | 实时进度 | 群全量背景 |
|---|---|---|---|---|---|
| `wechat` | `owner` | 微信扫码（支持验证码回填） | iLink HTTPS 35s 长轮询 | `sendtyping` 正在输入 | 否 |
| `wecom` | `org` | 企微扫码 / `botId+secret` | 智能机器人 WebSocket | `replyStream`（9m 切换） | 否 |
| `feishu` | `org` | 扫码 `registerApp`（可补权限）/ 手填 | 官方 WebSocket + REST | `OnIt` + CardKit 流式卡片 | 是（含进群 50 条历史） |
| `dingtalk` | `org` | 设备注册扫码 / `clientId+secret` | Stream WebSocket + REST | `🤔思考中` + AI 流式卡片 | 否 |
| `qq` | `public` | 手机 QQ 扫码 / `appId+appSecret` | Gateway WebSocket + REST | 私聊续期 `sendTyping` | 否 |
| `slack` | `org` | 预置 Manifest + `botToken+appToken` | Socket Mode WebSocket + REST | `👀` + `chat.update` | 是（线程归属父频道） |
| `discord` | `public` | `token` + 权限邀请链接 | Gateway WebSocket + REST | `👀` + `PATCH` 编辑消息 | 是（需 Message Content Intent） |
| `telegram` | `public` | `@BotFather` `token` | `getUpdates` 30s 长轮询 | `👀` + `editMessageText` | 是（需关隐私模式） |
| `github` | `org` | OAuth Device Flow / Classic Token | `/notifications` ETag 60s+ | `👀` -> `🚀`/`😕` | 同楼协作者评论存背景 |
| `imessage` | `owner` | 3 项本机环境预检，零输入直连 | 本机 `chat.db` + `osascript` | 无（零宽标记防回读） | 否（仅本人自聊） |

---

## 4. 验证要求

- 全库通过 `CGO_ENABLED=0 go vet ./...` 与 `CGO_ENABLED=0 go test -race -count=1 ./...`。
- 所有适配器测试均通过注入内存 HTTP / WebSocket / 临时 SQLite 离线运行，不访问真实公网、不触发系统权限弹窗、不读写用户真实凭据库。
