package joiner

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"whitelist-bypass/relay/tunnel"
)

type vp8LaneBinder struct {
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

func newVP8LaneBinder(lanes []*tunnel.VP8DataTunnel, health tunnel.LaneHealthProvider, resetter tunnel.LaneResetter) *vp8LaneBinder {
	b := &vp8LaneBinder{
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

func (b *vp8LaneBinder) Claim(track *webrtc.TrackRemote) *tunnel.VP8DataTunnel {
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

func (b *vp8LaneBinder) Release(track *webrtc.TrackRemote) {
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

func (b *vp8LaneBinder) Reset() {
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

func buildVP8TunnelPool(tracks []*webrtc.TrackLocalStaticSample, logFn func(string, ...any)) (tunnel.DataTunnel, []*tunnel.VP8DataTunnel, *vp8LaneBinder) {
	if len(tracks) == 0 {
		return nil, nil, newVP8LaneBinder(nil, nil, nil)
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
	return logical, lanes, newVP8LaneBinder(lanes, health, resetter)
}

func startVP8TunnelPool(lanes []*tunnel.VP8DataTunnel, fps int) {
	for _, lane := range lanes {
		lane.Start(fps)
	}
}

func stopVP8TunnelPool(lanes []*tunnel.VP8DataTunnel) {
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

func (b *vp8LaneBinder) bindLocked(key string, lane *tunnel.VP8DataTunnel) {
	if lane == nil {
		return
	}
	b.claimed[key] = lane
	b.used[lane] = key
}

func (b *vp8LaneBinder) evictLocked(lane *tunnel.VP8DataTunnel) {
	if lane == nil {
		return
	}
	if owner := b.used[lane]; owner != "" {
		delete(b.claimed, owner)
		delete(b.used, lane)
	}
}

func (b *vp8LaneBinder) resetLaneLocked(lane *tunnel.VP8DataTunnel, now time.Time) {
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

func (b *vp8LaneBinder) preferredLaneLocked(track *webrtc.TrackRemote) *tunnel.VP8DataTunnel {
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

func (b *vp8LaneBinder) refreshHealthLocked(now time.Time) {
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

func (b *vp8LaneBinder) isDeadLocked(lane *tunnel.VP8DataTunnel, now time.Time) bool {
	state := b.state[lane]
	if state == nil {
		return false
	}
	return state.lastPingTimeout >= laneDeadPingTimeoutThreshold && now.Sub(state.lastRxProgress) >= laneDeadRxStallThreshold
}

func (b *vp8LaneBinder) deadestLaneLocked(now time.Time) *tunnel.VP8DataTunnel {
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
