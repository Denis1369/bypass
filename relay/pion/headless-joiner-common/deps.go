package joiner

import (
	"time"

	"github.com/pion/webrtc/v4"
	"whitelist-bypass/relay/tunnel"
)

type ResolveFunc func(hostname string) (string, error)

type StatusEmitter interface {
	EmitStatus(status string)
	EmitStatusError(msg string)
}

type PeerConnectionConfigurer interface {
	ConfigureSettingEngine(settingEngine *webrtc.SettingEngine)
}

type CacheStore interface {
	Save(key string, value string)
	Load(key string) string
}

type AddTunnelTracksFunc func(pc *webrtc.PeerConnection, logFn func(string, ...any), prefix string) []*webrtc.TrackLocalStaticSample
type ReadTrackFunc func(track *webrtc.TrackRemote, resolveLane func(*webrtc.TrackRemote) *tunnel.VP8DataTunnel, releaseLane func(*webrtc.TrackRemote), logFn func(string, ...any), prefix string)

func applyFastICETimeouts(se *webrtc.SettingEngine) {
	se.SetICETimeouts(2*time.Second, 4*time.Second, 500*time.Millisecond)
}
