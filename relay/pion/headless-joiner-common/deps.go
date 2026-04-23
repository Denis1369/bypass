package joiner

import (
	"time"

	"github.com/pion/rtcp"
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

func startRTCPFeedbackReaders(pc *webrtc.PeerConnection, tracks []*webrtc.TrackLocalStaticSample, lanes []*tunnel.VP8DataTunnel, congestion tunnel.DataTunnel, logFn func(string, ...any), prefix string) {
	if pc == nil || len(tracks) == 0 || len(tracks) != len(lanes) {
		return
	}
	resetter, _ := congestion.(tunnel.CongestionResetter)
	laneByTrackID := make(map[string]*tunnel.VP8DataTunnel, len(tracks))
	for i, track := range tracks {
		if track == nil || lanes[i] == nil {
			continue
		}
		laneByTrackID[track.ID()] = lanes[i]
	}
	for _, sender := range pc.GetSenders() {
		if sender == nil || sender.Track() == nil || sender.Track().Kind() != webrtc.RTPCodecTypeVideo {
			continue
		}
		trackID := sender.Track().ID()
		lane := laneByTrackID[trackID]
		if lane == nil {
			continue
		}
		rtpSender := sender
		go func(trackID string, lane *tunnel.VP8DataTunnel) {
			for {
				packets, _, err := rtpSender.ReadRTCP()
				if err != nil {
					if logFn != nil {
						logFn("%s: RTCP reader closed for track=%s err=%v", prefix, trackID, err)
					}
					return
				}
				for _, packet := range packets {
					switch packet.(type) {
					case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
						if resetter != nil {
							resetter.ResetCongestion()
						}
						lane.HandlePLI()
						if logFn != nil {
							logFn("%s: RTCP keyframe request track=%s packet=%T", prefix, trackID, packet)
						}
					}
				}
			}
		}(trackID, lane)
	}
}
