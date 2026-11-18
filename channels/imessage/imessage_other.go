//go:build !darwin

package imessage

import (
	"context"

	imsdk "github.com/kilingzhang/im-sdk-go"
)

type stubChannel struct {
	imsdk.BaseChannel
}

type stubSetup struct{}

func (stubSetup) Actions() []string { return nil }
func (stubSetup) Check(context.Context) (imsdk.SetupStatus, error) {
	return imsdk.SetupStatus{}, imsdk.ErrUnsupported
}
func (stubSetup) Act(context.Context, string) error { return imsdk.ErrUnsupported }

// New returns an unsupported stub adapter on non-darwin platforms so cross-platform
// callers importing channels/imessage compile cleanly with CGO_ENABLED=0.
func New(_ Options) imsdk.Channel {
	return &stubChannel{
		BaseChannel: imsdk.BaseChannel{
			PlatformValue: imsdk.PlatformIMessage,
			TrustValue:    imsdk.TrustOwner,
			OnboardingValue: imsdk.OnboardingMethod{
				Kind:   imsdk.OnboardingCredentials,
				Fields: []imsdk.CredentialField{},
			},
			SetupValue: stubSetup{},
		},
	}
}

func (c *stubChannel) Onboard(context.Context, imsdk.Credentials, imsdk.OnboardingOptions) <-chan imsdk.OnboardingEvent {
	ch := make(chan imsdk.OnboardingEvent, 1)
	ch <- imsdk.OnboardingEvent{
		Kind:      imsdk.EventFailed,
		Code:      imsdk.FailureUnknown,
		DetailKey: "imessage.unsupported",
	}
	close(ch)
	return ch
}

func (c *stubChannel) Connect(context.Context, imsdk.Credentials, imsdk.ChannelEvents) (imsdk.ChannelConnection, error) {
	return nil, imsdk.ErrUnsupported
}
