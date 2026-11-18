// Package registry wires all built-in IM channel adapters into a platform-indexed map.
package registry

import (
	"runtime"

	imsdk "github.com/kilingzhang/im-sdk-go"
	"github.com/kilingzhang/im-sdk-go/channels/dingtalk"
	"github.com/kilingzhang/im-sdk-go/channels/discord"
	"github.com/kilingzhang/im-sdk-go/channels/feishu"
	"github.com/kilingzhang/im-sdk-go/channels/github"
	"github.com/kilingzhang/im-sdk-go/channels/imessage"
	"github.com/kilingzhang/im-sdk-go/channels/qq"
	"github.com/kilingzhang/im-sdk-go/channels/slack"
	"github.com/kilingzhang/im-sdk-go/channels/telegram"
	"github.com/kilingzhang/im-sdk-go/channels/wechat"
	"github.com/kilingzhang/im-sdk-go/channels/wecom"
)

// Options configures optional overrides per built-in channel adapter.
type Options struct {
	Telegram telegram.Options
	Feishu   feishu.Options
	WeChat   wechat.Options
	WeCom    wecom.Options
	DingTalk dingtalk.Options
	QQ       qq.Options
	Slack    slack.Options
	Discord  discord.Options
	GitHub   github.Options
	IMessage imessage.Options
}

// Default returns all built-in adapters supported on the current operating system
// (9 cross-platform adapters everywhere, plus iMessage on macOS).
func Default() map[imsdk.Platform]imsdk.Channel {
	return WithOptions(Options{})
}

// WithOptions constructs the built-in adapter map using custom per-platform Options.
func WithOptions(opts Options) map[imsdk.Platform]imsdk.Channel {
	out := map[imsdk.Platform]imsdk.Channel{
		imsdk.PlatformTelegram: telegram.New(opts.Telegram),
		imsdk.PlatformFeishu:   feishu.New(opts.Feishu),
		imsdk.PlatformWeChat:   wechat.New(opts.WeChat),
		imsdk.PlatformWeCom:    wecom.New(opts.WeCom),
		imsdk.PlatformDingTalk: dingtalk.New(opts.DingTalk),
		imsdk.PlatformQQ:       qq.New(opts.QQ),
		imsdk.PlatformSlack:    slack.New(opts.Slack),
		imsdk.PlatformDiscord:  discord.New(opts.Discord),
		imsdk.PlatformGitHub:   github.New(opts.GitHub),
	}
	if runtime.GOOS == "darwin" {
		out[imsdk.PlatformIMessage] = imessage.New(opts.IMessage)
	}
	return out
}
