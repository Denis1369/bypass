package pion

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"whitelist-bypass/relay/common"
	"whitelist-bypass/relay/tunnel"
)

type SignalingMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
	ID   int             `json:"id,omitempty"`
	Role string          `json:"role,omitempty"`
}

type ICEServerConfig struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type SDPMessage struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

type ICECandidateMessage struct {
	Candidate     string `json:"candidate"`
	SDPMid        string `json:"sdpMid"`
	SDPMLineIndex uint16 `json:"sdpMLineIndex"`
}

var WsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var iceLogFn func(string, ...any)
var tunnelLaneCount atomic.Int32

func init() {
	tunnelLaneCount.Store(20)
}

func SetTunnelLaneCount(n int) {
	if n < 1 {
		n = 1
	}
	if n > 20 {
		n = 20
	}
	tunnelLaneCount.Store(int32(n))
}

func GetTunnelLaneCount() int {
	n := int(tunnelLaneCount.Load())
	if n < 1 {
		return 1
	}
	return n
}

func ParseICEServers(data json.RawMessage) ([]webrtc.ICEServer, error) {
	var servers []ICEServerConfig
	if err := json.Unmarshal(data, &servers); err != nil {
		return nil, err
	}
	iceServers := make([]webrtc.ICEServer, len(servers))
	for i, s := range servers {
		urls := make([]string, len(s.URLs))
		for j, u := range s.URLs {
			fixed := common.FixICEURL(u)
			if iceLogFn != nil && fixed != u {
				iceLogFn("ice: fix URL %q -> %q", u, fixed)
			}
			urls[j] = fixed
		}
		if iceLogFn != nil {
			iceLogFn("ice: server %d: urls=%v", i, urls)
		}
		iceServers[i] = webrtc.ICEServer{
			URLs: urls, Username: s.Username, Credential: s.Credential,
		}
	}
	return iceServers, nil
}

func NewPionAPI(localIP string) *webrtc.API {
	se := webrtc.SettingEngine{}
	se.SetNet(&common.AndroidNet{LocalIP: localIP})
	applyFastICETimeouts(&se)
	return webrtc.NewAPI(webrtc.WithSettingEngine(se))
}

func applyFastICETimeouts(se *webrtc.SettingEngine) {
	se.SetICETimeouts(2*time.Second, 4*time.Second, 500*time.Millisecond)
}

type WSHelper struct {
	wsConn *websocket.Conn
	mu     sync.Mutex
}

func (h *WSHelper) SetConn(ws *websocket.Conn) {
	h.mu.Lock()
	h.wsConn = ws
	h.mu.Unlock()
}

func (h *WSHelper) SendToHook(msgType string, data any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.wsConn == nil {
		return
	}
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	msg := SignalingMessage{Type: msgType, Data: dataBytes}
	msgBytes, _ := json.Marshal(msg)
	h.wsConn.WriteMessage(websocket.TextMessage, msgBytes)
}

func (h *WSHelper) SendToHookWithRole(msgType string, data any, role string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.wsConn == nil {
		return
	}
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	msg := SignalingMessage{Type: msgType, Data: dataBytes, Role: role}
	msgBytes, _ := json.Marshal(msg)
	h.wsConn.WriteMessage(websocket.TextMessage, msgBytes)
}

func (h *WSHelper) SendResponse(id int, data any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.wsConn == nil {
		return
	}
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	msg := SignalingMessage{Type: "response", Data: dataBytes, ID: id}
	msgBytes, _ := json.Marshal(msg)
	h.wsConn.WriteMessage(websocket.TextMessage, msgBytes)
}

func (h *WSHelper) ReadMessages(handler func([]byte), onDisconnect func()) {
	for {
		_, msg, err := h.wsConn.ReadMessage()
		if err != nil {
			onDisconnect()
			return
		}
		handler(msg)
	}
}

func AddTunnelTracks(pc *webrtc.PeerConnection, logFn func(string, ...any), prefix string) []*webrtc.TrackLocalStaticSample {
	audioTrack, _ := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "tunnel-audio",
	)
	audioSender, audioErr := pc.AddTrack(audioTrack)
	logFn("%s: AddTrack audio: sender=%v err=%v", prefix, audioSender != nil, audioErr)
	count := GetTunnelLaneCount()
	tracks := make([]*webrtc.TrackLocalStaticSample, 0, count)
	for i := 0; i < count; i++ {
		sampleTrack, _ := webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
			fmt.Sprintf("video-%d", i), fmt.Sprintf("tunnel-video-%d", i),
		)
		videoSender, videoErr := pc.AddTrack(sampleTrack)
		logFn("%s: AddTrack video[%d]: sender=%v err=%v", prefix, i, videoSender != nil, videoErr)
		tracks = append(tracks, sampleTrack)
	}
	logFn("%s: senders count: %d", prefix, len(pc.GetSenders()))
	return tracks
}

func AddTelemostTunnelTracks(pc *webrtc.PeerConnection, logFn func(string, ...any), prefix string) []*webrtc.TrackLocalStaticSample {
	audioTrack, _ := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus},
		"audio", "tunnel-audio",
	)
	audioTr, audioErr := pc.AddTransceiverFromTrack(audioTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	logFn("%s: AddTrack audio: sender=%v err=%v", prefix, audioTr != nil && audioTr.Sender() != nil, audioErr)
	count := GetTunnelLaneCount()
	tracks := make([]*webrtc.TrackLocalStaticSample, 0, count)
	for i := 0; i < count; i++ {
		sampleTrack, _ := webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8},
			fmt.Sprintf("video-%d", i), fmt.Sprintf("tunnel-video-%d", i),
		)
		videoTr, videoErr := pc.AddTransceiverFromTrack(sampleTrack, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionSendonly,
		})
		logFn("%s: AddTrack video[%d]: sender=%v err=%v", prefix, i, videoTr != nil && videoTr.Sender() != nil, videoErr)
		tracks = append(tracks, sampleTrack)
	}
	logFn("%s: senders count: %d", prefix, len(pc.GetSenders()))
	return tracks
}

func ParseSDPType(t string) webrtc.SDPType {
	if t == "offer" {
		return webrtc.SDPTypeOffer
	}
	return webrtc.SDPTypeAnswer
}

type VP8LaneBinder struct {
	mu       sync.Mutex
	lanes    []*tunnel.VP8DataTunnel
	next     int
	claimed  map[string]*tunnel.VP8DataTunnel
	used     map[*tunnel.VP8DataTunnel]string
	state    map[*tunnel.VP8DataTunnel]*laneBindingState
	health   tunnel.LaneHealthProvider
	resetter tunnel.LaneResetter
}

type laneBindingState struct {
	lastRxBytes     uint64
	lastRxProgress  time.Time
	lastPingTimeout uint64
}

const (
	laneDeadPingTimeoutThreshold = 5
	laneDeadRxStallThreshold     = 2 * time.Second
)

func NewVP8LaneBinder(lanes []*tunnel.VP8DataTunnel, health tunnel.LaneHealthProvider, resetter tunnel.LaneResetter) *VP8LaneBinder {
	b := &VP8LaneBinder{
		lanes:    lanes,
		claimed:  make(map[string]*tunnel.VP8DataTunnel),
		used:     make(map[*tunnel.VP8DataTunnel]string),
		state:    make(map[*tunnel.VP8DataTunnel]*laneBindingState),
		health:   health,
		resetter: resetter,
	}
	now := time.Now()
	for _, lane := range lanes {
		b.state[lane] = &laneBindingState{lastRxProgress: now}
	}
	return b
}

func (b *VP8LaneBinder) Claim(track *webrtc.TrackRemote) *tunnel.VP8DataTunnel {
	if len(b.lanes) == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := laneTrackKey(track)
	if lane, ok := b.claimed[key]; ok {
		// Self-eviction lock: an already claimed track always keeps its lane.
		// Health degradation only affects whether a different new track may evict
		// this lane later when the pool is full.
		b.used[lane] = key
		return lane
	}
	now := time.Now()
	b.refreshHealthLocked(now)
	if preferred := b.preferredLaneLocked(track); preferred != nil {
		if b.used[preferred] == "" {
			b.bindLocked(key, preferred)
			return preferred
		}
	}
	for i := 0; i < len(b.lanes); i++ {
		lane := b.lanes[(b.next+i)%len(b.lanes)]
		if b.used[lane] != "" {
			continue
		}
		b.next = (b.next + i + 1) % len(b.lanes)
		b.bindLocked(key, lane)
		return lane
	}
	if victim := b.deadestLaneLocked(now); victim != nil {
		b.evictLocked(victim)
		b.resetLaneLocked(victim, now)
		b.bindLocked(key, victim)
		return victim
	}
	return nil
}

func (b *VP8LaneBinder) Release(track *webrtc.TrackRemote) {
	if track == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := laneTrackKey(track)
	lane, ok := b.claimed[key]
	if !ok {
		return
	}
	delete(b.claimed, key)
	if owner := b.used[lane]; owner == key {
		delete(b.used, lane)
	}
}

func (b *VP8LaneBinder) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for key := range b.claimed {
		delete(b.claimed, key)
	}
	for lane := range b.used {
		delete(b.used, lane)
	}
	now := time.Now()
	for _, state := range b.state {
		state.lastRxBytes = 0
		state.lastPingTimeout = 0
		state.lastRxProgress = now
	}
}

func BuildVP8TunnelPool(tracks []*webrtc.TrackLocalStaticSample, logFn func(string, ...any)) (tunnel.DataTunnel, []*tunnel.VP8DataTunnel, *VP8LaneBinder) {
	if len(tracks) == 0 {
		return nil, nil, NewVP8LaneBinder(nil, nil, nil)
	}
	lanes := make([]*tunnel.VP8DataTunnel, 0, len(tracks))
	physical := make([]tunnel.DataTunnel, 0, len(tracks))
	for _, track := range tracks {
		lane := tunnel.NewVP8DataTunnel(track, logFn)
		lanes = append(lanes, lane)
		physical = append(physical, lane)
	}
	var logical tunnel.DataTunnel
	if len(physical) == 1 {
		logical = physical[0]
	} else {
		logical = tunnel.NewBondedTunnel(physical, logFn)
	}
	var health tunnel.LaneHealthProvider
	if reporter, ok := logical.(tunnel.LaneHealthProvider); ok {
		health = reporter
	}
	var resetter tunnel.LaneResetter
	if laneResetter, ok := logical.(tunnel.LaneResetter); ok {
		resetter = laneResetter
	}
	return logical, lanes, NewVP8LaneBinder(lanes, health, resetter)
}

func StartVP8TunnelPool(lanes []*tunnel.VP8DataTunnel, fps int) {
	for _, lane := range lanes {
		lane.Start(fps)
	}
}

func StartRTCPFeedbackReaders(pc *webrtc.PeerConnection, tracks []*webrtc.TrackLocalStaticSample, lanes []*tunnel.VP8DataTunnel, congestion tunnel.DataTunnel, logFn func(string, ...any), prefix string) {
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
						lane.SendEmergencyKeyframe()
						if logFn != nil {
							logFn("%s: RTCP keyframe request track=%s packet=%T", prefix, trackID, packet)
						}
					}
				}
			}
		}(trackID, lane)
	}
}

func StopVP8TunnelPool(lanes []*tunnel.VP8DataTunnel) {
	for _, lane := range lanes {
		lane.Stop()
	}
}

func parseLaneSuffix(value string, max int) (int, bool) {
	lastDash := strings.LastIndexByte(value, '-')
	if lastDash < 0 || lastDash == len(value)-1 {
		return 0, false
	}
	n, err := strconv.Atoi(value[lastDash+1:])
	if err != nil || n < 0 || n >= max {
		return 0, false
	}
	return n, true
}

func laneTrackKey(track *webrtc.TrackRemote) string {
	if track == nil {
		return ""
	}
	return track.ID() + "|" + track.StreamID()
}

func (b *VP8LaneBinder) bindLocked(key string, lane *tunnel.VP8DataTunnel) {
	if lane == nil {
		return
	}
	b.claimed[key] = lane
	b.used[lane] = key
}

func (b *VP8LaneBinder) evictLocked(lane *tunnel.VP8DataTunnel) {
	if lane == nil {
		return
	}
	if owner := b.used[lane]; owner != "" {
		delete(b.claimed, owner)
		delete(b.used, lane)
	}
}

func (b *VP8LaneBinder) resetLaneLocked(lane *tunnel.VP8DataTunnel, now time.Time) {
	if lane == nil {
		return
	}
	if state := b.state[lane]; state != nil {
		state.lastRxBytes = 0
		state.lastPingTimeout = 0
		state.lastRxProgress = now
	}
	if b.resetter == nil {
		return
	}
	for idx, candidate := range b.lanes {
		if candidate != lane {
			continue
		}
		b.resetter.ResetLane(idx)
		return
	}
}

func (b *VP8LaneBinder) preferredLaneLocked(track *webrtc.TrackRemote) *tunnel.VP8DataTunnel {
	if track == nil {
		return nil
	}
	for _, candidate := range []string{track.ID(), track.StreamID()} {
		if idx, ok := parseLaneSuffix(candidate, len(b.lanes)); ok {
			return b.lanes[idx]
		}
	}
	return nil
}

func (b *VP8LaneBinder) refreshHealthLocked(now time.Time) {
	if b.health == nil {
		return
	}
	for _, snap := range b.health.LaneHealthSnapshots() {
		if snap.Index < 0 || snap.Index >= len(b.lanes) {
			continue
		}
		lane := b.lanes[snap.Index]
		state := b.state[lane]
		if state == nil {
			continue
		}
		if snap.RxBytes > state.lastRxBytes {
			state.lastRxBytes = snap.RxBytes
			state.lastRxProgress = now
		}
		state.lastPingTimeout = snap.PingTimeout
	}
}

func (b *VP8LaneBinder) isDeadLocked(lane *tunnel.VP8DataTunnel, now time.Time) bool {
	state := b.state[lane]
	if state == nil {
		return false
	}
	return state.lastPingTimeout >= laneDeadPingTimeoutThreshold && now.Sub(state.lastRxProgress) >= laneDeadRxStallThreshold
}

func (b *VP8LaneBinder) deadestLaneLocked(now time.Time) *tunnel.VP8DataTunnel {
	var victim *tunnel.VP8DataTunnel
	var victimPing uint64
	var victimAge time.Duration
	for _, lane := range b.lanes {
		if b.used[lane] == "" {
			continue
		}
		state := b.state[lane]
		if state == nil {
			continue
		}
		if state.lastPingTimeout < laneDeadPingTimeoutThreshold {
			continue
		}
		age := now.Sub(state.lastRxProgress)
		if victim == nil || state.lastPingTimeout > victimPing || (state.lastPingTimeout == victimPing && age > victimAge) {
			victim = lane
			victimPing = state.lastPingTimeout
			victimAge = age
		}
	}
	return victim
}

func ReadTrack(track *webrtc.TrackRemote, resolveLane func(*webrtc.TrackRemote) *tunnel.VP8DataTunnel, releaseLane func(*webrtc.TrackRemote), logFn func(string, ...any), prefix string) {
	if track.Codec().MimeType != webrtc.MimeTypeVP8 {
		buf := make([]byte, common.UDPBufSize)
		for {
			if _, _, err := track.Read(buf); err != nil {
				return
			}
		}
	}

	var vp8Pkt codecs.VP8Packet
	var frameBuf []byte
	dataCount := 0
	recvCount := 0
	ignoredCount := 0
	buf := make([]byte, common.RTPBufSize)
	for {
		n, _, err := track.Read(buf)
		if err != nil {
			if releaseLane != nil {
				releaseLane(track)
			}
			return
		}
		pkt := &rtp.Packet{}
		if pkt.Unmarshal(buf[:n]) != nil {
			continue
		}
		vp8Payload, err := vp8Pkt.Unmarshal(pkt.Payload)
		if err != nil {
			continue
		}
		if vp8Pkt.S == 1 {
			frameBuf = frameBuf[:0]
		}
		frameBuf = append(frameBuf, vp8Payload...)
		if !pkt.Marker {
			continue
		}
		recvCount++
		if recvCount <= 3 || recvCount%25 == 0 {
			if len(frameBuf) > 0 {
				logFn("%s: recv frame #%d %d bytes, first=0x%02x", prefix, recvCount, len(frameBuf), frameBuf[0])
			}
		}
		if !tunnel.LooksLikeVP8TunnelFrame(frameBuf) {
			ignoredCount++
			if ignoredCount <= 3 || ignoredCount%25 == 0 {
				logFn("%s: ignoring non-tunnel VP8 frame #%d id=%s stream=%s", prefix, ignoredCount, track.ID(), track.StreamID())
			}
			continue
		}
		var tun *tunnel.VP8DataTunnel
		if resolveLane != nil {
			tun = resolveLane(track)
		}
		if tun == nil {
			logFn("%s: dropping valid tunnel frame, no lane available for id=%s stream=%s", prefix, track.ID(), track.StreamID())
			continue
		}
		if dataCount == 0 {
			logFn("%s: lane activated id=%s stream=%s", prefix, track.ID(), track.StreamID())
		}
		data := tunnel.ExtractDataFromPayload(frameBuf)
		if data == nil {
			continue
		}
		dataCount++
		if dataCount <= 5 || dataCount%100 == 0 {
			logFn("%s: TUNNEL DATA #%d: %d bytes", prefix, dataCount, len(data))
		}
		if tun.OnData != nil {
			tun.OnData(data)
		}
	}
}
