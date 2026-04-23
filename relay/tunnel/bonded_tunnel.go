package tunnel

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bondedChunkPayloadSize   = VP8TunnelMaxPayload - vp8PayloadOverhead
	bondedDataHeaderSize     = 21
	bondedWireOverhead       = 9 + bondedDataHeaderSize
	bondedMaxDataPayloadSize = bondedChunkPayloadSize - bondedWireOverhead
	bondedMinChunkSize       = 512
	bondedInitialChunkSize   = bondedMaxDataPayloadSize
	bondedMaxChunkSize       = bondedMaxDataPayloadSize
	bondedMinCwndBytes       = 128 * 1024
	bondedInitialCwndBytes   = 256 * 1024
	bondedMaxCwndBytes       = 8 * 1024 * 1024
	bondedPriorityFrameSize  = 1200
	bondedInitialBwBytes     = 512 * 1024
	bondedMinBwBytes         = 64 * 1024
	bondedMaxBwBytes         = 32 * 1024 * 1024
	bondedMaxLaneRateBytes   = 300 * 1024
	bondedProbeGain          = 1.20
	bondedProbePeriod        = 4 * time.Second
	bondedProbeDuration      = 800 * time.Millisecond
	bondedPacerQueueSize     = 2048
	bondedControlQueueSize   = 256
	bondedMinSpacing         = 200 * time.Microsecond
	bondedControlMinSpacing  = time.Millisecond
	bondedFlushInterval      = 3 * time.Millisecond
	bondedMaxBurstBytes      = 3000

	bondedNackRetryInterval = 120 * time.Millisecond
	bondedHardTimeout       = 1500 * time.Millisecond
	bondedMaxRetransmits    = 3
	bondedHistoryTTL        = 5 * time.Second
	bondedHistoryMaxFrames  = 4096
	bondedRetiredTTL        = 5 * time.Second
	bondedFlowIdleTTL       = 10 * time.Second
	bondedStatsInterval     = 2 * time.Second
	bondedQualityInterval   = 200 * time.Millisecond
	bondedPingInterval      = time.Second
	bondedProbeInterval     = 300 * time.Millisecond
	bondedPingTimeout       = 900 * time.Millisecond
	bondedPingRetention     = 5 * time.Second
	bondedCriticalRTT       = 800 * time.Millisecond
	bondedRecoverRTT        = 350 * time.Millisecond
	bondedRTOInterval       = 150 * time.Millisecond
	bondedLossRoundThresh   = 0.03
	bondedLossSevereThresh  = 0.05
	bondedDeadPingThreshold = 10
	bondedDeadRxStall       = 2 * time.Second
)

type bondedLaneMetrics struct {
	txBytes       atomic.Uint64
	rxBytes       atomic.Uint64
	skipped       atomic.Uint64
	outOfOrder    atomic.Uint64
	lateOrDup     atomic.Uint64
	retransmitted atomic.Uint64
	nackSent      atomic.Uint64
	nackRecv      atomic.Uint64
	pingSent      atomic.Uint64
	pongRecv      atomic.Uint64
	pingTimeout   atomic.Uint64
}

type bondedBandwidthSample struct {
	rate float64
	at   time.Time
}

type bondedRTTSample struct {
	rtt time.Duration
	at  time.Time
}

type bondedBBRState uint8

const (
	bondedBBRStartup bondedBBRState = iota
	bondedBBRDrain
	bondedBBRProbeBW
	bondedBBRProbeRTT
)

func (s bondedBBRState) String() string {
	switch s {
	case bondedBBRStartup:
		return "Startup"
	case bondedBBRDrain:
		return "Drain"
	case bondedBBRProbeBW:
		return "ProbeBW"
	case bondedBBRProbeRTT:
		return "ProbeRTT"
	default:
		return "Unknown"
	}
}

var bondedProbeBWGainCycle = [...]float64{1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0}

type bondedLaneState struct {
	metrics            bondedLaneMetrics
	weight             float64
	currentWeight      float64
	score              float64
	lossEWMA           float64
	disorderEWMA       float64
	smoothedRTT        time.Duration
	lastRTT            time.Duration
	pingOutstanding    bool
	lastPingID         uint64
	pingSentAt         time.Time
	lastPong           time.Time
	lastProbe          time.Time
	failureStreak      int
	probing            bool
	lastObsOOO         uint64
	lastObsLate        uint64
	lastObsSkip        uint64
	cwndBytes          int
	inflightBytes      int
	chunkSize          int
	pacingRate         float64
	lastAckAt          time.Time
	nextSendAt         time.Time
	pacingBudget       float64
	lastPacingUpdate   time.Time
	maxBandwidth       float64
	maxBWSamples       []bondedBandwidthSample
	ltBandwidth        float64
	ltBWSamples        []bondedBandwidthSample
	minRTTWindow       time.Duration
	minRTTSamples      []bondedRTTSample
	bbrState           bondedBBRState
	pacingGain         float64
	cwndGain           float64
	fullBandwidth      float64
	fullBwCount        int
	probeBWIndex       int
	probeBWStamp       time.Time
	probeRTTUntil      time.Time
	nextProbeRTTAt     time.Time
	inflightHi         int
	bwHi               float64
	policerDetected    bool
	safeRounds         int
	roundCount         uint64
	roundPending       bool
	nextRoundDelivered uint64
	roundAppLimited    bool
	roundSentFrames    uint64
	roundLossEvents    uint64
	lastRoundLossRate  float64
	lastSentFrameID    uint64
	appLimited         bool
	lastDataSentAt     time.Time
	lastRxAt           time.Time
}

type bondedHistoryEntry struct {
	frameID         uint64
	connID          uint32
	msgType         byte
	flowSeq         uint32
	frame           []byte
	sentAt          time.Time
	retransmits     uint32
	retries         int
	chunkLanes      []int
	chunkFrameSize  []int
	chunkAppLimited []bool
	highPriority    bool
}

type bondedPendingFrame struct {
	frameID   uint64
	connID    uint32
	msgType   byte
	flowSeq   uint32
	total     int
	chunks    [][]byte
	received  int
	size      int
	firstSeen time.Time
	lastSeen  time.Time
}

type bondedGapState struct {
	frameID  uint64
	since    time.Time
	lastNack time.Time
	attempts int
}

type bondedFlowFrame struct {
	frameID  uint64
	flowSeq  uint32
	msgType  byte
	frame    []byte
	queuedAt time.Time
}

type bondedFlowState struct {
	connID       uint32
	nextDeliver  uint32
	pending      map[uint32]*bondedFlowFrame
	lastActivity time.Time
}

type bondedPingState struct {
	lane   int
	sentAt time.Time
}

type bondedSendFlowState struct {
	ewmaSize     float64
	frames       uint32
	lastActivity time.Time
}

type bondedSend struct {
	lane       int
	frame      []byte
	frameID    uint64
	appLimited bool
	retransmit bool
}

type bondedBatchState struct {
	buf             []byte
	lastDataFrameID uint64
	dataFrames      uint64
	allAppLimited   bool
	hasData         bool
}

type bondedLaneCoalescer struct {
	mu    sync.Mutex
	batch bondedBatchState
}

type bondedLaneSnapshot struct {
	rttMs       int64
	weight      float64
	score       float64
	probing     bool
	bbrState    string
	pacingGain  float64
	roundCount  uint64
	appLimited  bool
	txBps       uint64
	rxBps       uint64
	skipped     uint64
	outOfOrder  uint64
	lateOrDup   uint64
	retrans     uint64
	nackSent    uint64
	nackRecv    uint64
	pingSent    uint64
	pongRecv    uint64
	pingTimeout uint64
	cwndBytes   int
	inflight    int
	chunkSize   int
	maxBwKBps   float64
	ltBwKBps    float64
	bwHiKBps    float64
	minRTTMs    int64
	pacingKBps  float64
	lossRate    float64
	inflightHi  int
	policer     bool
}

type BondedTunnel struct {
	lanes []DataTunnel
	logFn func(string, ...any)

	onData  func([]byte)
	onClose func()

	sendMu        sync.Mutex
	sendCond      *sync.Cond
	nextFrameID   uint64
	sendFlowSeq   map[uint32]uint32
	sendFlows     map[uint32]*bondedSendFlowState
	history       map[uint64]*bondedHistoryEntry
	historyOrder  []uint64
	laneStates    []bondedLaneState
	laneQueues    []chan bondedSend
	controlQueues []chan bondedSend
	coalescers    []bondedLaneCoalescer
	nextPingID    atomic.Uint64
	pings         map[uint64]bondedPingState

	recvMu             sync.Mutex
	transportPending   map[uint64]*bondedPendingFrame
	transportGaps      map[uint64]*bondedGapState
	highestSeenFrameID uint64
	retiredFrames      map[uint64]time.Time
	retiredOrder       []uint64
	flows              map[uint32]*bondedFlowState

	closeCh     chan struct{}
	closeOnce   sync.Once
	closed      atomic.Bool
	queuedBytes atomic.Int64
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func NewBondedTunnel(lanes []DataTunnel, logFn func(string, ...any)) *BondedTunnel {
	if logFn == nil {
		logFn = func(string, ...any) {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &BondedTunnel{
		lanes:            lanes,
		logFn:            logFn,
		nextFrameID:      1,
		sendFlowSeq:      make(map[uint32]uint32),
		sendFlows:        make(map[uint32]*bondedSendFlowState),
		history:          make(map[uint64]*bondedHistoryEntry),
		historyOrder:     make([]uint64, 0, 256),
		laneStates:       make([]bondedLaneState, len(lanes)),
		laneQueues:       make([]chan bondedSend, len(lanes)),
		controlQueues:    make([]chan bondedSend, len(lanes)),
		coalescers:       make([]bondedLaneCoalescer, len(lanes)),
		pings:            make(map[uint64]bondedPingState),
		transportPending: make(map[uint64]*bondedPendingFrame),
		transportGaps:    make(map[uint64]*bondedGapState),
		retiredFrames:    make(map[uint64]time.Time),
		retiredOrder:     make([]uint64, 0, 256),
		flows:            make(map[uint32]*bondedFlowState),
		closeCh:          make(chan struct{}),
		ctx:              ctx,
		cancel:           cancel,
	}
	t.sendCond = sync.NewCond(&t.sendMu)
	now := time.Now()
	for i := range t.laneStates {
		t.laneStates[i].weight = 1
		t.laneStates[i].score = 1
		t.laneStates[i].cwndBytes = bondedInitialCwndBytes
		t.laneStates[i].chunkSize = bondedInitialChunkSize
		t.laneStates[i].maxBandwidth = bondedInitialBwBytes
		t.laneStates[i].ltBandwidth = bondedInitialBwBytes
		t.laneStates[i].pacingRate = bondedInitialBwBytes * 2.885
		t.laneStates[i].bbrState = bondedBBRStartup
		t.laneStates[i].pacingGain = 2.885
		t.laneStates[i].cwndGain = 2.0
		t.laneStates[i].probeBWStamp = now
		t.laneStates[i].nextProbeRTTAt = now.Add(10 * time.Second)
		t.laneStates[i].appLimited = true
		t.laneStates[i].lastDataSentAt = now
		t.laneStates[i].lastRxAt = now
		t.laneStates[i].lastPacingUpdate = now
		t.laneStates[i].pacingBudget = float64(bondedChunkPayloadSize)
		t.laneQueues[i] = make(chan bondedSend, bondedPacerQueueSize)
		t.controlQueues[i] = make(chan bondedSend, bondedControlQueueSize)
	}
	for i, lane := range lanes {
		laneIndex := i
		lane.SetOnData(func(data []byte) {
			t.handleLaneData(laneIndex, data)
		})
		lane.SetOnClose(func() {
			t.closef("bonded: lane=%d closed", laneIndex)
		})
		t.wg.Add(1)
		go t.pacerLoop(laneIndex)
		t.wg.Add(1)
		go t.controlLoop(laneIndex)
		t.wg.Add(1)
		go t.coalescerFlushLoop(laneIndex)
	}
	t.wg.Add(1)
	go t.gapLoop()
	t.wg.Add(1)
	go t.rtoLoop()
	t.wg.Add(1)
	go t.qualityLoop()
	t.wg.Add(1)
	go t.statsLoop()
	return t
}

func (t *BondedTunnel) SendData(data []byte) {
	if len(t.lanes) == 0 || t.closed.Load() {
		return
	}

	var ops []bondedSend
	now := time.Now()

	t.sendMu.Lock()
	defer t.sendMu.Unlock()

	for len(data) > 0 {
		frame, rest, ok := NextFrame(data)
		if !ok {
			break
		}
		connID, msgType, ok := bondedFrameMetadata(frame)
		if !ok {
			data = rest
			continue
		}

		frameID := t.nextFrameID
		t.nextFrameID++
		flowSeq := t.nextFlowSeqLocked(connID)
		highPriority := t.classifyPriorityLocked(connID, msgType, len(frame), now)
		entry := &bondedHistoryEntry{
			frameID:      frameID,
			connID:       connID,
			msgType:      msgType,
			flowSeq:      flowSeq,
			frame:        frame,
			sentAt:       now,
			highPriority: highPriority,
		}
		ops = append(ops, t.planFrameSendLocked(entry, false)...)
		if msgType == MsgClose {
			delete(t.sendFlowSeq, connID)
			delete(t.sendFlows, connID)
		}
		data = rest
	}

	go t.sendOps(ops)
}

func (t *BondedTunnel) SetOnData(fn func([]byte)) { t.onData = fn }
func (t *BondedTunnel) SetOnClose(fn func())      { t.onClose = fn }

func (t *BondedTunnel) Close() {
	t.closef("bonded: closed")
	t.wg.Wait()
}

func (t *BondedTunnel) LaneHealthSnapshots() []LaneHealthSnapshot {
	t.sendMu.Lock()
	defer t.sendMu.Unlock()
	snapshots := make([]LaneHealthSnapshot, len(t.laneStates))
	for i := range t.laneStates {
		snapshots[i] = LaneHealthSnapshot{
			Index:       i,
			RxBytes:     t.laneStates[i].metrics.rxBytes.Load(),
			PingTimeout: t.laneStates[i].metrics.pingTimeout.Load(),
		}
	}
	return snapshots
}

func (t *BondedTunnel) GetTotalBufferUsage() int {
	inflight := 0
	t.sendMu.Lock()
	for i := range t.laneStates {
		inflight += t.laneStates[i].inflightBytes
	}
	t.sendMu.Unlock()

	coalesced := 0
	for i := range t.coalescers {
		t.coalescers[i].mu.Lock()
		coalesced += len(t.coalescers[i].batch.buf)
		t.coalescers[i].mu.Unlock()
	}

	queued := t.queuedBytes.Load()
	if queued < 0 {
		queued = 0
	}
	return inflight + coalesced + int(queued)
}

func (t *BondedTunnel) ResetLane(index int) {
	now := time.Now()
	t.sendMu.Lock()
	rescue := t.collectLaneRescueEntriesLocked(index)
	for _, entry := range rescue {
		t.releaseHistoryInflightLocked(entry)
		entry.sentAt = now
		entry.retransmits++
	}
	t.drainLaneQueuesLocked(index)
	t.resetLaneLocked(index, now)
	ops := make([]bondedSend, 0, len(rescue))
	for _, entry := range rescue {
		ops = append(ops, t.planFrameSendLocked(entry, true)...)
	}
	t.sendCond.Broadcast()
	t.sendMu.Unlock()
	if len(ops) > 0 {
		go t.sendOps(ops)
	}
}

func (t *BondedTunnel) handleLaneData(lane int, data []byte) {
	if t.closed.Load() {
		return
	}
	if lane >= 0 && lane < len(t.laneStates) {
		t.laneStates[lane].metrics.rxBytes.Add(uint64(len(data)))
		t.laneStates[lane].lastRxAt = time.Now()
	}
	DecodeFrames(data, func(connID uint32, msgType byte, payload []byte) {
		if connID != BondedConnID {
			return
		}
		switch msgType {
		case MsgBondedData:
			t.handleBondedData(lane, payload)
		case MsgBondedNack:
			t.handleBondedNack(lane, payload)
		case MsgBondedDrop:
			t.handleBondedDrop(lane, payload)
		case MsgBondedPing:
			t.handleBondedPing(lane, payload)
		case MsgBondedPong:
			t.handleBondedPong(lane, payload)
		case MsgBondedAck:
			t.handleBondedAck(lane, payload)
		}
	})
}

func (t *BondedTunnel) handleBondedData(lane int, payload []byte) {
	frameID, flowSeq, chunkIdx, chunkTotal, connID, msgType, chunk, ok := decodeBondedData(payload)
	if !ok {
		return
	}

	now := time.Now()
	var deliveries [][]byte
	var nacks []uint64
	var ackFrameID uint64

	t.recvMu.Lock()
	if t.isRetiredFrameLocked(frameID) {
		t.noteLateOrDupLocked(lane)
		t.recvMu.Unlock()
		return
	}

	if frameID > t.highestSeenFrameID {
		nacks = append(nacks, t.registerMissingTransportFramesLocked(frameID, now)...)
		t.highestSeenFrameID = frameID
	} else if frameID < t.highestSeenFrameID {
		t.noteOutOfOrderLocked(lane)
	}
	delete(t.transportGaps, frameID)

	entry, exists := t.transportPending[frameID]
	if !exists {
		entry = &bondedPendingFrame{
			frameID:   frameID,
			connID:    connID,
			msgType:   msgType,
			flowSeq:   flowSeq,
			total:     chunkTotal,
			chunks:    make([][]byte, chunkTotal),
			firstSeen: now,
			lastSeen:  now,
		}
		t.transportPending[frameID] = entry
	}

	if entry.total != chunkTotal || entry.connID != connID || entry.msgType != msgType || entry.flowSeq != flowSeq {
		t.noteLateOrDupLocked(lane)
		t.recvMu.Unlock()
		return
	}
	if chunkIdx < 0 || chunkIdx >= entry.total {
		t.noteLateOrDupLocked(lane)
		t.recvMu.Unlock()
		return
	}
	if entry.chunks[chunkIdx] != nil {
		t.noteLateOrDupLocked(lane)
		t.recvMu.Unlock()
		return
	}

	chunkCopy := make([]byte, len(chunk))
	copy(chunkCopy, chunk)
	entry.chunks[chunkIdx] = chunkCopy
	entry.received++
	entry.size += len(chunkCopy)
	entry.lastSeen = now

	if entry.received == entry.total {
		frame := reassemblePendingFrame(entry)
		delete(t.transportPending, frameID)
		t.markRetiredFrameLocked(frameID, now)
		deliveries = t.enqueueFlowFrameLocked(connID, flowSeq, msgType, frame, frameID, now)
		ackFrameID = frameID
	}
	t.pruneRetiredLocked(now)
	t.pruneIdleFlowsLocked(now)
	t.recvMu.Unlock()

	for _, frame := range deliveries {
		t.deliverFrame(frame)
	}
	if ackFrameID != 0 {
		t.sendBondedAck(ackFrameID)
	}
	for _, missingFrameID := range nacks {
		t.sendBondedNack(missingFrameID)
	}
}

func (t *BondedTunnel) handleBondedAck(_ int, payload []byte) {
	frameID, ok := decodeBondedSeq(payload)
	if !ok {
		return
	}

	t.sendMu.Lock()
	defer t.sendMu.Unlock()

	entry, found := t.history[frameID]
	if !found {
		return
	}
	t.ackHistoryLocked(entry)
	delete(t.history, frameID)
	t.sendCond.Broadcast()
}

func (t *BondedTunnel) handleBondedNack(lane int, payload []byte) {
	frameID, ok := decodeBondedSeq(payload)
	if !ok {
		return
	}

	var ops []bondedSend
	var resetConnID uint32
	var resetMsgType byte

	t.sendMu.Lock()
	if lane >= 0 && lane < len(t.laneStates) {
		t.laneStates[lane].metrics.nackRecv.Add(1)
	}

	entry, found := t.history[frameID]
	if !found {
		t.sendMu.Unlock()
		return
	}
	if entry.retransmits == 0 {
		for _, trackedLane := range uniqueLanes(entry.chunkLanes) {
			if trackedLane < 0 || trackedLane >= len(t.laneStates) {
				continue
			}
			t.laneStates[trackedLane].roundLossEvents++
		}
	}
	t.penalizeHistoryLocked(entry, 0.12)
	t.releaseHistoryInflightLocked(entry)
	if entry.retransmits >= bondedMaxRetransmits || time.Since(entry.sentAt) > bondedHistoryTTL {
		resetConnID = entry.connID
		resetMsgType = entry.msgType
		for _, trackedLane := range uniqueLanes(entry.chunkLanes) {
			if trackedLane < 0 || trackedLane >= len(t.laneStates) {
				continue
			}
			t.laneStates[trackedLane].roundLossEvents++
		}
		delete(t.history, frameID)
		t.sendCond.Broadcast()
		t.sendMu.Unlock()
		t.sendBondedDrop(frameID, resetConnID, resetMsgType)
		t.resetDamagedFlow(resetConnID, resetMsgType, "retransmit-limit")
		return
	}

	entry.retransmits++
	entry.sentAt = time.Now()
	ops = t.planFrameSendLocked(entry, true)
	t.sendMu.Unlock()

	go t.sendOps(ops)
}

func (t *BondedTunnel) handleBondedDrop(lane int, payload []byte) {
	frameID, connID, msgType, ok := decodeBondedDrop(payload)
	if !ok {
		return
	}

	t.sendMu.Lock()
	if lane >= 0 && lane < len(t.laneStates) {
		t.laneStates[lane].metrics.skipped.Add(1)
	}
	if entry, found := t.history[frameID]; found {
		if connID == 0 {
			connID = entry.connID
			msgType = entry.msgType
		}
		t.penalizeHistoryLocked(entry, 0.35)
		t.markHistorySkippedLocked(entry)
		t.releaseHistoryInflightLocked(entry)
		delete(t.history, frameID)
	}
	t.sendCond.Broadcast()
	t.sendMu.Unlock()

	if connID == 0 {
		t.closef("bonded: unresolved drop frame=%d", frameID)
		return
	}
	t.resetDamagedFlow(connID, msgType, "drop")
}

func (t *BondedTunnel) handleBondedPing(lane int, payload []byte) {
	if lane < 0 || lane >= len(t.lanes) {
		return
	}
	t.sendControlOnLane(lane, MsgBondedPong, payload, nil)
}

func (t *BondedTunnel) handleBondedPong(lane int, payload []byte) {
	pingID, ok := decodeBondedPing(payload)
	if !ok {
		return
	}

	t.sendMu.Lock()
	defer t.sendMu.Unlock()

	pingState, found := t.pings[pingID]
	if !found {
		return
	}
	delete(t.pings, pingID)
	if pingState.lane < 0 || pingState.lane >= len(t.laneStates) {
		return
	}
	state := &t.laneStates[pingState.lane]
	if state.lastPingID == pingID {
		state.pingOutstanding = false
	}

	rtt := time.Since(pingState.sentAt)
	state.lastRTT = rtt
	state.lastPong = time.Now()
	state.failureStreak = 0
	if state.smoothedRTT == 0 {
		state.smoothedRTT = rtt
	} else {
		state.smoothedRTT = time.Duration(float64(state.smoothedRTT)*0.75 + float64(rtt)*0.25)
	}
	state.metrics.pongRecv.Add(1)
	if state.lossEWMA > 0.02 {
		state.lossEWMA *= 0.85
	}
	state.updateMinRTTLocked(rtt, time.Now())
	state.advanceBBRStateLocked(time.Now())
	state.recomputeLaneBudgetLocked(time.Now(), false)
}

func (t *BondedTunnel) planFrameSendLocked(entry *bondedHistoryEntry, retransmit bool) []bondedSend {
	for {
		entry.sentAt = time.Now()
		virtualInflight := make([]int, len(t.laneStates))
		virtualWeight := make([]float64, len(t.laneStates))
		for i := range t.laneStates {
			virtualInflight[i] = t.laneStates[i].inflightBytes
			virtualWeight[i] = t.laneStates[i].currentWeight
		}

		draftLanes := make([]int, 0, 4)
		draftLens := make([]int, 0, 4)
		remaining := len(entry.frame)

		for remaining > 0 {
			minRequired := bondedMinChunkSize
			if remaining < minRequired {
				minRequired = remaining
			}

			var lane, room int
			if entry.highPriority {
				lane, room = t.pickPriorityLaneWithVirtualLocked(minRequired, virtualInflight)
			} else {
				lane, room = t.pickWeightedLaneWithVirtualLocked(minRequired, virtualInflight, virtualWeight)
			}
			if lane < 0 || room <= 0 {
				break
			}

			chunkLen := room
			if chunkLen > bondedMaxDataPayloadSize {
				chunkLen = bondedMaxDataPayloadSize
			}
			if chunkLen > remaining {
				chunkLen = remaining
			}
			draftLanes = append(draftLanes, lane)
			draftLens = append(draftLens, chunkLen)
			virtualInflight[lane] += bondedWireOverhead + chunkLen
			remaining -= chunkLen
		}

		if remaining > 0 {
			if t.closed.Load() {
				return nil
			}
			t.sendCond.Wait()
			continue
		}

		entry.chunkLanes = entry.chunkLanes[:0]
		entry.chunkFrameSize = entry.chunkFrameSize[:0]
		entry.chunkAppLimited = entry.chunkAppLimited[:0]
		ops := make([]bondedSend, 0, len(draftLanes))
		offset := 0
		totalChunks := len(draftLanes)
		for i := range t.laneStates {
			t.laneStates[i].currentWeight = virtualWeight[i]
		}
		for i, lane := range draftLanes {
			chunkLen := draftLens[i]
			payload := encodeBondedData(entry.frameID, entry.flowSeq, i, totalChunks, entry.connID, entry.msgType, entry.frame[offset:offset+chunkLen])
			frame := EncodeFrame(BondedConnID, MsgBondedData, payload)
			state := &t.laneStates[lane]
			entry.chunkLanes = append(entry.chunkLanes, lane)
			entry.chunkFrameSize = append(entry.chunkFrameSize, len(frame))
			entry.chunkAppLimited = append(entry.chunkAppLimited, state.appLimited)
			ops = append(ops, bondedSend{lane: lane, frame: frame, frameID: entry.frameID, appLimited: state.appLimited, retransmit: retransmit})
			offset += chunkLen
		}

		t.history[entry.frameID] = entry
		t.historyOrder = append(t.historyOrder, entry.frameID)
		t.pruneHistoryLocked(time.Now())
		return ops
	}
}

func (t *BondedTunnel) selectLaneForChunkLocked(highPriority bool, remaining int) (int, int) {
	minRequired := bondedMinChunkSize
	if remaining < minRequired {
		minRequired = remaining
	}

	for {
		if highPriority {
			if lane, room := t.pickPriorityLaneLocked(minRequired); lane >= 0 {
				return lane, room
			}
		} else {
			if lane, room := t.pickWeightedLaneLocked(minRequired); lane >= 0 {
				return lane, room
			}
		}

		if t.closed.Load() {
			return -1, 0
		}
		t.sendCond.Wait()
	}
}

func (t *BondedTunnel) pickPriorityLaneLocked(minRequired int) (int, int) {
	bestLane := -1
	bestRTT := time.Duration(1<<63 - 1)
	bestScore := math.Inf(-1)
	bestRoom := 0

	for i := range t.laneStates {
		state := &t.laneStates[i]
		if state.probing {
			continue
		}
		room := availablePayload(state)
		if room < minRequired {
			continue
		}
		rtt := state.smoothedRTT
		if rtt == 0 {
			rtt = time.Millisecond
		}
		if bestLane == -1 || rtt < bestRTT || (rtt == bestRTT && state.score > bestScore) {
			bestLane = i
			bestRTT = rtt
			bestScore = state.score
			bestRoom = room
		}
	}
	if bestLane >= 0 {
		return bestLane, minInt(bestRoom, t.laneStates[bestLane].chunkSize)
	}

	for i := range t.laneStates {
		state := &t.laneStates[i]
		room := availablePayload(state)
		if room < minRequired {
			continue
		}
		rtt := state.smoothedRTT
		if rtt == 0 {
			rtt = bondedCriticalRTT
		}
		if bestLane == -1 || rtt < bestRTT || (rtt == bestRTT && state.score > bestScore) {
			bestLane = i
			bestRTT = rtt
			bestScore = state.score
			bestRoom = room
		}
	}
	if bestLane >= 0 {
		return bestLane, minInt(bestRoom, t.laneStates[bestLane].chunkSize)
	}
	return -1, 0
}

func (t *BondedTunnel) pickWeightedLaneLocked(minRequired int) (int, int) {
	totalWeight := 0.0
	bestIdx := -1
	bestWeight := math.Inf(-1)
	bestRoom := 0

	for i := range t.laneStates {
		state := &t.laneStates[i]
		if state.probing {
			continue
		}
		room := availablePayload(state)
		if room < minRequired {
			continue
		}
		weight := state.weight
		if weight <= 0 {
			weight = 0.25
		}
		state.currentWeight += weight
		totalWeight += weight
		if state.currentWeight > bestWeight {
			bestWeight = state.currentWeight
			bestIdx = i
			bestRoom = room
		}
	}
	if bestIdx >= 0 {
		t.laneStates[bestIdx].currentWeight -= totalWeight
		return bestIdx, minInt(bestRoom, t.laneStates[bestIdx].chunkSize)
	}

	bestIdx = -1
	bestScore := math.Inf(-1)
	for i := range t.laneStates {
		room := availablePayload(&t.laneStates[i])
		if room < minRequired {
			continue
		}
		if t.laneStates[i].score > bestScore {
			bestScore = t.laneStates[i].score
			bestIdx = i
			bestRoom = room
		}
	}
	if bestIdx >= 0 {
		return bestIdx, minInt(bestRoom, t.laneStates[bestIdx].chunkSize)
	}
	return -1, 0
}

func (t *BondedTunnel) pickPriorityLaneWithVirtualLocked(minRequired int, virtualInflight []int) (int, int) {
	bestLane := -1
	bestRTT := time.Duration(1<<63 - 1)
	bestScore := math.Inf(-1)
	bestRoom := 0

	for i := range t.laneStates {
		state := &t.laneStates[i]
		if state.probing {
			continue
		}
		room := availablePayloadWithInflight(state, virtualInflight[i])
		if room < minRequired {
			continue
		}
		rtt := state.smoothedRTT
		if rtt == 0 {
			rtt = time.Millisecond
		}
		if bestLane == -1 || rtt < bestRTT || (rtt == bestRTT && state.score > bestScore) {
			bestLane = i
			bestRTT = rtt
			bestScore = state.score
			bestRoom = room
		}
	}
	if bestLane >= 0 {
		return bestLane, minInt(bestRoom, t.laneStates[bestLane].chunkSize)
	}

	for i := range t.laneStates {
		state := &t.laneStates[i]
		room := availablePayloadWithInflight(state, virtualInflight[i])
		if room < minRequired {
			continue
		}
		rtt := state.smoothedRTT
		if rtt == 0 {
			rtt = bondedCriticalRTT
		}
		if bestLane == -1 || rtt < bestRTT || (rtt == bestRTT && state.score > bestScore) {
			bestLane = i
			bestRTT = rtt
			bestScore = state.score
			bestRoom = room
		}
	}
	if bestLane >= 0 {
		return bestLane, minInt(bestRoom, t.laneStates[bestLane].chunkSize)
	}
	return -1, 0
}

func (t *BondedTunnel) pickWeightedLaneWithVirtualLocked(minRequired int, virtualInflight []int, virtualWeight []float64) (int, int) {
	totalWeight := 0.0
	bestIdx := -1
	bestWeight := math.Inf(-1)
	bestRoom := 0

	for i := range t.laneStates {
		state := &t.laneStates[i]
		if state.probing {
			continue
		}
		room := availablePayloadWithInflight(state, virtualInflight[i])
		if room < minRequired {
			continue
		}
		weight := state.weight
		if weight <= 0 {
			weight = 0.25
		}
		virtualWeight[i] += weight
		totalWeight += weight
		if virtualWeight[i] > bestWeight {
			bestWeight = virtualWeight[i]
			bestIdx = i
			bestRoom = room
		}
	}
	if bestIdx >= 0 {
		virtualWeight[bestIdx] -= totalWeight
		return bestIdx, minInt(bestRoom, t.laneStates[bestIdx].chunkSize)
	}

	bestIdx = -1
	bestScore := math.Inf(-1)
	for i := range t.laneStates {
		room := availablePayloadWithInflight(&t.laneStates[i], virtualInflight[i])
		if room < minRequired {
			continue
		}
		if t.laneStates[i].score > bestScore {
			bestScore = t.laneStates[i].score
			bestIdx = i
			bestRoom = room
		}
	}
	if bestIdx >= 0 {
		return bestIdx, minInt(bestRoom, t.laneStates[bestIdx].chunkSize)
	}
	return -1, 0
}

func (t *BondedTunnel) bestLaneLocked(includeProbing bool) int {
	bestIdx := -1
	bestScore := math.Inf(-1)
	for i := range t.laneStates {
		state := &t.laneStates[i]
		if state.probing && !includeProbing {
			continue
		}
		score := state.score
		if score <= 0 {
			score = state.weight
		}
		if score > bestScore {
			bestScore = score
			bestIdx = i
		}
	}
	if bestIdx >= 0 {
		return bestIdx
	}
	if len(t.laneStates) == 0 {
		return -1
	}
	return 0
}

func (t *BondedTunnel) gapLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		case <-t.closeCh:
			return
		}

		now := time.Now()
		var nacks []uint64
		type dropRequest struct {
			frameID uint64
			connID  uint32
			msgType byte
		}
		drops := make([]dropRequest, 0, 4)

		t.recvMu.Lock()
		for frameID, gap := range t.transportGaps {
			switch {
			case now.Sub(gap.lastNack) >= bondedNackRetryInterval && gap.attempts < bondedMaxRetransmits:
				gap.lastNack = now
				gap.attempts++
				nacks = append(nacks, frameID)
			case now.Sub(gap.since) >= bondedHardTimeout:
				req := dropRequest{frameID: frameID}
				if entry, ok := t.transportPending[frameID]; ok {
					req.connID = entry.connID
					req.msgType = entry.msgType
					delete(t.transportPending, frameID)
				}
				delete(t.transportGaps, frameID)
				t.markRetiredFrameLocked(frameID, now)
				if req.connID != 0 {
					t.clearFlowLocked(req.connID)
					t.dropPendingTransportConnLocked(req.connID, frameID, now)
				}
				drops = append(drops, req)
			}
		}
		t.pruneRetiredLocked(now)
		t.pruneIdleFlowsLocked(now)
		t.recvMu.Unlock()

		for _, frameID := range nacks {
			t.sendBondedNack(frameID)
		}
		for _, req := range drops {
			t.sendBondedDrop(req.frameID, req.connID, req.msgType)
			if req.connID != 0 {
				t.resetDamagedFlow(req.connID, req.msgType, "hard-timeout")
			}
		}
	}
}

func (t *BondedTunnel) rtoLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(bondedRTOInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		case <-t.closeCh:
			return
		}

		now := time.Now()
		var ops []bondedSend
		type dropRequest struct {
			frameID uint64
			connID  uint32
			msgType byte
		}
		drops := make([]dropRequest, 0, 4)

		t.sendMu.Lock()
		for _, entry := range t.history {
			rto := t.historyRTOForEntryLocked(entry)
			if now.Sub(entry.sentAt) < rto {
				continue
			}
			t.releaseHistoryInflightLocked(entry)
			if entry.retries >= 5 || now.Sub(entry.sentAt) > bondedHistoryTTL {
				t.penalizeHistoryLocked(entry, 0.20)
				t.markHistorySkippedLocked(entry)
				delete(t.history, entry.frameID)
				drops = append(drops, dropRequest{frameID: entry.frameID, connID: entry.connID, msgType: entry.msgType})
				continue
			}
			t.penalizeHistoryLocked(entry, 0.08)
			entry.retransmits++
			entry.retries++
			entry.sentAt = now
			ops = append(ops, t.planFrameSendLocked(entry, true)...)
		}
		if len(ops) > 0 || len(drops) > 0 {
			t.sendCond.Broadcast()
		}
		t.sendMu.Unlock()

		for _, req := range drops {
			t.sendBondedDrop(req.frameID, req.connID, req.msgType)
			t.resetDamagedFlow(req.connID, req.msgType, "rto-timeout")
		}
		if len(ops) > 0 {
			go t.sendOps(ops)
		}
	}
}

func (t *BondedTunnel) qualityLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(bondedQualityInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		case <-t.closeCh:
			return
		}

		now := time.Now()
		var ops []bondedSend

		t.sendMu.Lock()
		t.refreshLaneQualityLocked(now)
		t.prunePingsLocked(now)
		for lane := range t.laneStates {
			state := &t.laneStates[lane]
			interval := bondedPingInterval
			if state.probing {
				interval = bondedProbeInterval
			}
			if state.pingOutstanding || now.Sub(state.lastProbe) < interval {
				continue
			}
			pingID := t.nextPingID.Add(1)
			payload := encodeBondedPing(pingID)
			frame := EncodeFrame(BondedConnID, MsgBondedPing, payload)
			state.lastPingID = pingID
			state.pingOutstanding = true
			state.pingSentAt = now
			state.lastProbe = now
			state.metrics.pingSent.Add(1)
			state.metrics.txBytes.Add(uint64(len(frame)))
			t.pings[pingID] = bondedPingState{lane: lane, sentAt: now}
			ops = append(ops, bondedSend{lane: lane, frame: frame})
		}
		t.sendMu.Unlock()

		go t.sendOps(ops)
	}
}

func (t *BondedTunnel) refreshLaneQualityLocked(now time.Time) {
	fastestRTT := time.Duration(0)

	for i := range t.laneStates {
		state := &t.laneStates[i]
		if state.pingOutstanding && now.Sub(state.pingSentAt) >= bondedPingTimeout {
			state.pingOutstanding = false
			state.failureStreak++
			state.metrics.pingTimeout.Add(1)
			state.lossEWMA = clampFloat(state.lossEWMA+0.25, 0, 1)
			t.decreaseLaneLocked(state, true)
		}

		ooo := state.metrics.outOfOrder.Load()
		late := state.metrics.lateOrDup.Load()
		skip := state.metrics.skipped.Load()
		delta := float64((ooo - state.lastObsOOO) + (late - state.lastObsLate) + (skip - state.lastObsSkip))
		state.lastObsOOO = ooo
		state.lastObsLate = late
		state.lastObsSkip = skip
		state.disorderEWMA = state.disorderEWMA*0.80 + delta*0.20
		state.lossEWMA *= 0.94
		if state.disorderEWMA > 0 {
			state.disorderEWMA *= 0.92
		}

		if state.smoothedRTT > 0 && (fastestRTT == 0 || state.smoothedRTT < fastestRTT) {
			fastestRTT = state.smoothedRTT
		}
	}
	if fastestRTT == 0 {
		fastestRTT = 120 * time.Millisecond
	}

	for i := range t.laneStates {
		state := &t.laneStates[i]
		baseRTT := state.minRTTWindow
		if baseRTT == 0 {
			baseRTT = state.smoothedRTT
		}
		if baseRTT == 0 {
			baseRTT = 120 * time.Millisecond
		}
		idleThreshold := 2 * baseRTT
		if idleThreshold < 200*time.Millisecond {
			idleThreshold = 200 * time.Millisecond
		}
		if len(t.laneQueues[i]) == 0 && state.inflightBytes <= maxInt(state.cwndBytes/4, bondedMinChunkSize) && now.Sub(state.lastDataSentAt) >= idleThreshold {
			state.appLimited = true
		} else if len(t.laneQueues[i]) > 0 {
			state.appLimited = false
		}

		rtt := state.smoothedRTT
		if rtt == 0 {
			rtt = fastestRTT
		}
		rttScore := clampFloat(float64(fastestRTT)/float64(rtt), 0.15, 3.0)
		lossPenalty := clampFloat(1.0-state.lossEWMA, 0.10, 1.0)
		disorderPenalty := clampFloat(1.0/(1.0+state.disorderEWMA), 0.10, 1.0)
		timeoutPenalty := 1.0 / (1.0 + 0.5*float64(state.failureStreak))
		score := rttScore * lossPenalty * disorderPenalty * timeoutPenalty
		state.score = score

		degraded := state.failureStreak >= 2 || (state.smoothedRTT > 0 && state.smoothedRTT >= bondedCriticalRTT) || state.lossEWMA >= 0.45 || state.disorderEWMA >= 4
		recovered := (state.smoothedRTT == 0 || state.smoothedRTT <= bondedRecoverRTT) &&
			state.lossEWMA < 0.15 &&
			(state.lastPong.IsZero() || now.Sub(state.lastPong) <= 2*bondedPingInterval)
		silentTimedOut := state.metrics.pingTimeout.Load() > bondedDeadPingThreshold &&
			now.Sub(state.lastRxAt) >= bondedDeadRxStall &&
			(state.lastPong.IsZero() || now.Sub(state.lastPong) >= 2*bondedPingInterval)

		if silentTimedOut {
			state.probing = true
			state.weight = 0
			state.score = 0
			state.chunkSize = bondedMinChunkSize
			if state.bbrState != bondedBBRProbeRTT {
				state.setBBRStateLocked(bondedBBRProbeRTT, now)
			}
		} else if degraded {
			state.probing = true
			state.chunkSize = bondedMinChunkSize
		} else if state.probing && recovered {
			state.probing = false
			state.failureStreak = 0
		}

		if silentTimedOut {
			state.weight = 0
		} else if state.probing {
			state.weight = 0.05
		} else {
			state.weight = clampFloat(score*1.8, 0.35, 6.0)
		}
		state.advanceBBRStateLocked(now)
		state.recomputeLaneBudgetLocked(now, false)
	}

	t.pruneHistoryLocked(now)
	t.pruneSendFlowsLocked(now)
	t.sendCond.Broadcast()
}

func (s *bondedLaneState) recomputeLaneBudgetLocked(now time.Time, holdChunk bool) {
	s.pruneBandwidthSamplesLocked(now)
	s.pruneLongTermBandwidthSamplesLocked(now)
	s.pruneRTTSamplesLocked(now)

	maxBW := s.maxBandwidth
	if maxBW < bondedMinBwBytes {
		maxBW = bondedMinBwBytes
	}
	if maxBW > bondedMaxBwBytes {
		maxBW = bondedMaxBwBytes
	}
	if s.ltBandwidth > 0 && s.lastRoundLossRate >= bondedLossRoundThresh && maxBW > s.ltBandwidth*1.10 {
		maxBW = s.ltBandwidth
	}
	limitByHi := (s.bbrState == bondedBBRProbeBW && s.pacingGain > 1.0) || s.policerDetected || s.lastRoundLossRate >= bondedLossRoundThresh
	if limitByHi && s.bwHi > 0 && maxBW > s.bwHi {
		maxBW = s.bwHi
	}

	baseRTT := s.minRTTWindow
	if baseRTT == 0 {
		if s.smoothedRTT > 0 {
			baseRTT = s.smoothedRTT
		} else {
			baseRTT = 120 * time.Millisecond
		}
	}

	effectivePacingGain := s.pacingGain
	effectiveCwndGain := s.cwndGain
	if s.lastRoundLossRate >= bondedLossSevereThresh {
		effectivePacingGain *= 0.75
		effectiveCwndGain *= 0.60
	} else if s.lastRoundLossRate >= bondedLossRoundThresh {
		effectivePacingGain *= 0.85
		effectiveCwndGain *= 0.75
	}

	s.pacingRate = clampFloat(maxBW*effectivePacingGain, bondedMinBwBytes, bondedMaxLaneRateBytes)
	targetCwnd := int(maxBW*baseRTT.Seconds()*effectiveCwndGain) + bondedMinChunkSize
	if s.bbrState == bondedBBRProbeRTT {
		targetCwnd = bondedMinCwndBytes
	}
	if limitByHi && s.inflightHi > 0 && targetCwnd > s.inflightHi {
		targetCwnd = s.inflightHi
	}
	s.cwndBytes = clampInt(targetCwnd, bondedMinCwndBytes, bondedMaxCwndBytes)

	if holdChunk {
		return
	}
	targetChunk := clampInt(int(maxBW/128.0), bondedMinChunkSize, bondedMaxChunkSize)
	if s.probing || s.lossEWMA >= 0.20 || s.failureStreak > 0 || s.bbrState == bondedBBRProbeRTT {
		targetChunk = bondedMinChunkSize
	}
	if targetChunk > s.chunkSize {
		s.chunkSize = minInt(targetChunk, s.chunkSize+1024)
	} else {
		s.chunkSize = maxInt(targetChunk, s.chunkSize-1024)
	}
	if s.chunkSize < bondedMinChunkSize {
		s.chunkSize = bondedMinChunkSize
	}
}

func (s *bondedLaneState) updateBandwidthSampleLocked(rate float64, now time.Time) {
	if rate < bondedMinBwBytes {
		rate = bondedMinBwBytes
	}
	if rate > bondedMaxBwBytes {
		rate = bondedMaxBwBytes
	}
	s.maxBWSamples = append(s.maxBWSamples, bondedBandwidthSample{rate: rate, at: now})
	s.pruneBandwidthSamplesLocked(now)
	maxBW := rate
	for _, sample := range s.maxBWSamples {
		if sample.rate > maxBW {
			maxBW = sample.rate
		}
	}
	s.maxBandwidth = maxBW
	s.updateLongTermBandwidthSampleLocked(rate, now)
}

func (s *bondedLaneState) updateMinRTTLocked(rtt time.Duration, now time.Time) {
	if rtt <= 0 {
		return
	}
	s.minRTTSamples = append(s.minRTTSamples, bondedRTTSample{rtt: rtt, at: now})
	s.pruneRTTSamplesLocked(now)
	minRTT := rtt
	for _, sample := range s.minRTTSamples {
		if sample.rtt < minRTT {
			minRTT = sample.rtt
		}
	}
	s.minRTTWindow = minRTT
}

func (s *bondedLaneState) pruneBandwidthSamplesLocked(now time.Time) {
	windowRTT := s.minRTTWindow
	if windowRTT == 0 {
		if s.smoothedRTT > 0 {
			windowRTT = s.smoothedRTT
		} else {
			windowRTT = 100 * time.Millisecond
		}
	}
	cutoff := now.Add(-10 * windowRTT)
	idx := 0
	for idx < len(s.maxBWSamples) && s.maxBWSamples[idx].at.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		s.maxBWSamples = append([]bondedBandwidthSample(nil), s.maxBWSamples[idx:]...)
	}
}

func (s *bondedLaneState) updateLongTermBandwidthSampleLocked(rate float64, now time.Time) {
	if rate < bondedMinBwBytes {
		rate = bondedMinBwBytes
	}
	if rate > bondedMaxBwBytes {
		rate = bondedMaxBwBytes
	}
	s.ltBWSamples = append(s.ltBWSamples, bondedBandwidthSample{rate: rate, at: now})
	s.pruneLongTermBandwidthSamplesLocked(now)
	if len(s.ltBWSamples) == 0 {
		return
	}
	sum := 0.0
	for _, sample := range s.ltBWSamples {
		sum += sample.rate
	}
	s.ltBandwidth = clampFloat(sum/float64(len(s.ltBWSamples)), bondedMinBwBytes, bondedMaxBwBytes)
}

func (s *bondedLaneState) pruneLongTermBandwidthSamplesLocked(now time.Time) {
	windowRTT := s.minRTTWindow
	if windowRTT == 0 {
		if s.smoothedRTT > 0 {
			windowRTT = s.smoothedRTT
		} else {
			windowRTT = 100 * time.Millisecond
		}
	}
	window := 30 * windowRTT
	if window < 6*time.Second {
		window = 6 * time.Second
	}
	if window > 30*time.Second {
		window = 30 * time.Second
	}
	cutoff := now.Add(-window)
	idx := 0
	for idx < len(s.ltBWSamples) && s.ltBWSamples[idx].at.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		s.ltBWSamples = append([]bondedBandwidthSample(nil), s.ltBWSamples[idx:]...)
	}
}

func (s *bondedLaneState) pruneRTTSamplesLocked(now time.Time) {
	cutoff := now.Add(-10 * time.Second)
	idx := 0
	for idx < len(s.minRTTSamples) && s.minRTTSamples[idx].at.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		s.minRTTSamples = append([]bondedRTTSample(nil), s.minRTTSamples[idx:]...)
	}
}

func (s *bondedLaneState) finishRoundLocked(ackFrameID uint64, now time.Time) {
	if !s.roundPending || ackFrameID < s.nextRoundDelivered {
		return
	}

	s.roundCount++
	if s.roundSentFrames > 0 {
		s.lastRoundLossRate = float64(s.roundLossEvents) / float64(s.roundSentFrames)
	} else {
		s.lastRoundLossRate = 0
	}
	baseRTT := s.minRTTWindow
	if baseRTT == 0 {
		baseRTT = s.smoothedRTT
	}
	if baseRTT == 0 {
		baseRTT = 120 * time.Millisecond
	}
	policerDrop := s.lastRoundLossRate >= bondedLossRoundThresh && (s.smoothedRTT == 0 || float64(s.smoothedRTT) <= float64(baseRTT)*1.10)
	if s.lastRoundLossRate >= bondedLossRoundThresh {
		s.inflightHi = clampInt(maxInt(s.inflightBytes, bondedMinCwndBytes), bondedMinCwndBytes, bondedMaxCwndBytes)
		bwCap := s.maxBandwidth
		if s.ltBandwidth > 0 {
			bwCap = minFloat(bwCap, s.ltBandwidth)
		}
		if bwCap < bondedMinBwBytes {
			bwCap = bondedMinBwBytes
		}
		s.bwHi = clampFloat(bwCap, bondedMinBwBytes, bondedMaxBwBytes)
		s.safeRounds = 0
		if policerDrop {
			s.policerDetected = true
			if s.ltBandwidth > 0 {
				s.bwHi = clampFloat(s.ltBandwidth, bondedMinBwBytes, bondedMaxBwBytes)
			}
			if s.inflightHi > bondedMinCwndBytes {
				s.inflightHi = maxInt(bondedMinCwndBytes, s.inflightHi*3/4)
			}
		}
	} else {
		s.safeRounds++
		if s.safeRounds >= 64 {
			if s.inflightHi > 0 {
				s.inflightHi = clampInt(int(float64(s.inflightHi)*1.10), bondedMinCwndBytes, bondedMaxCwndBytes)
				if s.inflightHi >= bondedMaxCwndBytes {
					s.inflightHi = 0
				}
			}
			if s.bwHi > 0 {
				s.bwHi = clampFloat(s.bwHi*1.05, bondedMinBwBytes, bondedMaxBwBytes)
				if s.bwHi >= bondedMaxBwBytes*0.95 {
					s.bwHi = 0
				}
			}
			if s.policerDetected {
				s.policerDetected = false
			}
			s.safeRounds = 0
		}
	}

	switch s.bbrState {
	case bondedBBRStartup:
		if s.lastRoundLossRate >= bondedLossRoundThresh {
			if s.bwHi > 0 {
				s.fullBandwidth = s.bwHi
			} else if s.ltBandwidth > 0 && s.maxBandwidth > s.ltBandwidth {
				s.fullBandwidth = s.ltBandwidth
			} else {
				s.fullBandwidth = s.maxBandwidth
			}
			s.fullBwCount = 3
			s.setBBRStateLocked(bondedBBRDrain, now)
		} else if !s.roundAppLimited {
			if s.fullBandwidth == 0 || s.maxBandwidth >= s.fullBandwidth*1.25 {
				s.fullBandwidth = s.maxBandwidth
				s.fullBwCount = 0
			} else {
				s.fullBwCount++
				if s.fullBwCount >= 3 {
					s.setBBRStateLocked(bondedBBRDrain, now)
				}
			}
		}
	case bondedBBRProbeBW:
		s.probeBWIndex = (s.probeBWIndex + 1) % len(bondedProbeBWGainCycle)
		s.pacingGain = bondedProbeBWGainCycle[s.probeBWIndex]
		s.cwndGain = 2.0
	}

	if s.lastSentFrameID > ackFrameID {
		s.roundPending = true
		s.nextRoundDelivered = s.lastSentFrameID
		s.roundAppLimited = false
	} else {
		s.roundPending = false
		s.nextRoundDelivered = 0
		s.roundAppLimited = false
	}
	s.roundSentFrames = 0
	s.roundLossEvents = 0
}

func (s *bondedLaneState) advanceBBRStateLocked(now time.Time) {
	baseRTT := s.minRTTWindow
	if baseRTT == 0 {
		if s.smoothedRTT > 0 {
			baseRTT = s.smoothedRTT
		} else {
			baseRTT = 120 * time.Millisecond
		}
	}
	if baseRTT < 100*time.Millisecond {
		baseRTT = 100 * time.Millisecond
	}

	if !s.nextProbeRTTAt.IsZero() && now.After(s.nextProbeRTTAt) && s.bbrState != bondedBBRProbeRTT {
		s.setBBRStateLocked(bondedBBRProbeRTT, now)
		return
	}

	switch s.bbrState {
	case bondedBBRDrain:
		bdp := int(s.maxBandwidth*baseRTT.Seconds()) + bondedMinChunkSize
		if s.inflightBytes <= bdp {
			s.setBBRStateLocked(bondedBBRProbeBW, now)
			return
		}
	case bondedBBRProbeRTT:
		if s.probeRTTUntil.IsZero() {
			s.probeRTTUntil = now.Add(200 * time.Millisecond)
		}
		if now.After(s.probeRTTUntil) {
			s.setBBRStateLocked(bondedBBRProbeBW, now)
			s.nextProbeRTTAt = now.Add(10 * time.Second)
			return
		}
	}
}

func (s *bondedLaneState) setBBRStateLocked(state bondedBBRState, now time.Time) {
	s.bbrState = state
	switch state {
	case bondedBBRStartup:
		s.pacingGain = 2.885
		s.cwndGain = 2.0
		s.fullBwCount = 0
		s.probeBWIndex = 0
		s.probeBWStamp = now
	case bondedBBRDrain:
		s.pacingGain = 1.0 / 2.885
		s.cwndGain = 2.0
	case bondedBBRProbeBW:
		s.probeBWIndex = 0
		s.probeBWStamp = now
		s.pacingGain = bondedProbeBWGainCycle[0]
		s.cwndGain = 2.0
		s.probeRTTUntil = time.Time{}
	case bondedBBRProbeRTT:
		s.pacingGain = 0.5
		s.cwndGain = 0.5
		s.probeRTTUntil = now.Add(200 * time.Millisecond)
	}
}

func (t *BondedTunnel) statsLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(bondedStatsInterval)
	defer ticker.Stop()

	prevTx := make([]uint64, len(t.laneStates))
	prevRx := make([]uint64, len(t.laneStates))

	for {
		select {
		case <-ticker.C:
		case <-t.ctx.Done():
			return
		case <-t.closeCh:
			return
		}

		snapshots := make([]bondedLaneSnapshot, len(t.laneStates))
		t.sendMu.Lock()
		for i := range t.laneStates {
			state := &t.laneStates[i]
			tx := state.metrics.txBytes.Load()
			rx := state.metrics.rxBytes.Load()
			snapshots[i] = bondedLaneSnapshot{
				rttMs:       state.smoothedRTT.Milliseconds(),
				weight:      state.weight,
				score:       state.score,
				probing:     state.probing,
				bbrState:    state.bbrState.String(),
				pacingGain:  state.pacingGain,
				roundCount:  state.roundCount,
				appLimited:  state.appLimited,
				txBps:       (tx - prevTx[i]) / uint64(bondedStatsInterval.Seconds()),
				rxBps:       (rx - prevRx[i]) / uint64(bondedStatsInterval.Seconds()),
				skipped:     state.metrics.skipped.Load(),
				outOfOrder:  state.metrics.outOfOrder.Load(),
				lateOrDup:   state.metrics.lateOrDup.Load(),
				retrans:     state.metrics.retransmitted.Load(),
				nackSent:    state.metrics.nackSent.Load(),
				nackRecv:    state.metrics.nackRecv.Load(),
				pingSent:    state.metrics.pingSent.Load(),
				pongRecv:    state.metrics.pongRecv.Load(),
				pingTimeout: state.metrics.pingTimeout.Load(),
				cwndBytes:   state.cwndBytes,
				inflight:    state.inflightBytes,
				chunkSize:   state.chunkSize,
				maxBwKBps:   state.maxBandwidth / 1024.0,
				ltBwKBps:    state.ltBandwidth / 1024.0,
				bwHiKBps:    state.bwHi / 1024.0,
				minRTTMs:    state.minRTTWindow.Milliseconds(),
				pacingKBps:  state.pacingRate / 1024.0,
				lossRate:    state.lastRoundLossRate * 100.0,
				inflightHi:  state.inflightHi,
				policer:     state.policerDetected,
			}
			prevTx[i] = tx
			prevRx[i] = rx
		}
		t.sendMu.Unlock()

		t.recvMu.Lock()
		activeFlows := len(t.flows)
		transportGaps := len(t.transportGaps)
		transportPending := len(t.transportPending)
		t.recvMu.Unlock()

		t.logFn("bonded: flows=%d transportGaps=%d transportPending=%d", activeFlows, transportGaps, transportPending)
		for i, snap := range snapshots {
			t.logFn(
				"bonded: lane=%d tx=%dB/s rx=%dB/s rtt=%dms min_rtt=%dms weight=%.2f score=%.2f probing=%t bbr_state=%s pacing_gain=%.2f round_count=%d app_limited=%t policer=%t max_bw=%.1fKB/s lt_bw=%.1fKB/s bw_hi=%.1fKB/s loss_rate=%.2f%% pace=%.1fKB/s cwnd=%d inflight=%d inflight_hi=%d chunk=%d skipped=%d ooo=%d late=%d retrans=%d nackTx=%d nackRx=%d pingTx=%d pongRx=%d pingTimeout=%d",
				i,
				snap.txBps,
				snap.rxBps,
				snap.rttMs,
				snap.minRTTMs,
				snap.weight,
				snap.score,
				snap.probing,
				snap.bbrState,
				snap.pacingGain,
				snap.roundCount,
				snap.appLimited,
				snap.policer,
				snap.maxBwKBps,
				snap.ltBwKBps,
				snap.bwHiKBps,
				snap.lossRate,
				snap.pacingKBps,
				snap.cwndBytes,
				snap.inflight,
				snap.inflightHi,
				snap.chunkSize,
				snap.skipped,
				snap.outOfOrder,
				snap.lateOrDup,
				snap.retrans,
				snap.nackSent,
				snap.nackRecv,
				snap.pingSent,
				snap.pongRecv,
				snap.pingTimeout,
			)
		}
	}
}

func (t *BondedTunnel) nextFlowSeqLocked(connID uint32) uint32 {
	next := t.sendFlowSeq[connID] + 1
	if next == 0 {
		next = 1
	}
	t.sendFlowSeq[connID] = next
	return next
}

func (t *BondedTunnel) classifyPriorityLocked(connID uint32, msgType byte, frameLen int, now time.Time) bool {
	if msgType != MsgData {
		return true
	}
	flow := t.sendFlows[connID]
	if flow == nil {
		flow = &bondedSendFlowState{}
		t.sendFlows[connID] = flow
	}
	flow.frames++
	flow.lastActivity = now
	if flow.ewmaSize == 0 {
		flow.ewmaSize = float64(frameLen)
	} else {
		flow.ewmaSize = flow.ewmaSize*0.75 + float64(frameLen)*0.25
	}
	if frameLen <= bondedPriorityFrameSize {
		return true
	}
	if flow.frames <= 6 && flow.ewmaSize <= float64(bondedPriorityFrameSize*2) {
		return true
	}
	return flow.ewmaSize <= float64(bondedPriorityFrameSize)
}

func (t *BondedTunnel) registerMissingTransportFramesLocked(frameID uint64, now time.Time) []uint64 {
	if frameID <= t.highestSeenFrameID+1 {
		return nil
	}
	nacks := make([]uint64, 0, int(frameID-t.highestSeenFrameID))
	for missing := t.highestSeenFrameID + 1; missing < frameID; missing++ {
		if _, ok := t.transportPending[missing]; ok {
			continue
		}
		if _, ok := t.transportGaps[missing]; ok {
			continue
		}
		if t.isRetiredFrameLocked(missing) {
			continue
		}
		t.transportGaps[missing] = &bondedGapState{
			frameID:  missing,
			since:    now,
			lastNack: now,
			attempts: 1,
		}
		nacks = append(nacks, missing)
	}
	return nacks
}

func (t *BondedTunnel) enqueueFlowFrameLocked(connID uint32, flowSeq uint32, msgType byte, frame []byte, frameID uint64, now time.Time) [][]byte {
	flow := t.flows[connID]
	if flow == nil {
		flow = &bondedFlowState{
			connID:       connID,
			nextDeliver:  1,
			pending:      make(map[uint32]*bondedFlowFrame),
			lastActivity: now,
		}
		t.flows[connID] = flow
	}
	flow.lastActivity = now

	if flowSeq < flow.nextDeliver {
		return nil
	}
	if _, exists := flow.pending[flowSeq]; exists {
		return nil
	}

	frameCopy := make([]byte, len(frame))
	copy(frameCopy, frame)
	flow.pending[flowSeq] = &bondedFlowFrame{
		frameID:  frameID,
		flowSeq:  flowSeq,
		msgType:  msgType,
		frame:    frameCopy,
		queuedAt: now,
	}

	deliveries := make([][]byte, 0, 2)
	for {
		next := flow.pending[flow.nextDeliver]
		if next == nil {
			break
		}
		delete(flow.pending, flow.nextDeliver)
		deliveries = append(deliveries, next.frame)
		flow.nextDeliver++
		flow.lastActivity = now
		if next.msgType == MsgClose {
			delete(t.flows, connID)
			break
		}
	}

	return deliveries
}

func (t *BondedTunnel) isRetiredFrameLocked(frameID uint64) bool {
	_, ok := t.retiredFrames[frameID]
	return ok
}

func (t *BondedTunnel) markRetiredFrameLocked(frameID uint64, now time.Time) {
	if _, ok := t.retiredFrames[frameID]; ok {
		t.retiredFrames[frameID] = now
		return
	}
	t.retiredFrames[frameID] = now
	t.retiredOrder = append(t.retiredOrder, frameID)
}

func (t *BondedTunnel) pruneRetiredLocked(now time.Time) {
	for len(t.retiredOrder) > 0 {
		frameID := t.retiredOrder[0]
		ts, ok := t.retiredFrames[frameID]
		if !ok {
			t.retiredOrder = t.retiredOrder[1:]
			continue
		}
		if now.Sub(ts) <= bondedRetiredTTL {
			break
		}
		delete(t.retiredFrames, frameID)
		t.retiredOrder = t.retiredOrder[1:]
	}
}

func (t *BondedTunnel) pruneIdleFlowsLocked(now time.Time) {
	for connID, flow := range t.flows {
		if len(flow.pending) == 0 && now.Sub(flow.lastActivity) > bondedFlowIdleTTL {
			delete(t.flows, connID)
		}
	}
}

func (t *BondedTunnel) dropPendingTransportConnLocked(connID uint32, fromFrameID uint64, now time.Time) {
	for frameID, entry := range t.transportPending {
		if frameID >= fromFrameID && entry.connID == connID {
			delete(t.transportPending, frameID)
			t.markRetiredFrameLocked(frameID, now)
		}
	}
}

func (t *BondedTunnel) clearFlowLocked(connID uint32) {
	delete(t.flows, connID)
}

func (t *BondedTunnel) sendBondedAck(frameID uint64) {
	payload := encodeBondedSeq(frameID)
	t.sendControl(MsgBondedAck, payload, nil)
}

func (t *BondedTunnel) sendBondedNack(frameID uint64) {
	payload := encodeBondedSeq(frameID)
	t.sendControl(MsgBondedNack, payload, func(state *bondedLaneState) {
		state.metrics.nackSent.Add(1)
	})
}

func (t *BondedTunnel) sendBondedDrop(frameID uint64, connID uint32, msgType byte) {
	payload := encodeBondedDrop(frameID, connID, msgType)
	t.sendControl(MsgBondedDrop, payload, nil)
}

func (t *BondedTunnel) sendControl(msgType byte, payload []byte, update func(*bondedLaneState)) {
	t.sendMu.Lock()
	lane := t.bestLaneLocked(false)
	if lane < 0 {
		lane = t.bestLaneLocked(true)
	}
	var frame []byte
	if lane >= 0 {
		frame = EncodeFrame(BondedConnID, msgType, payload)
		t.laneStates[lane].metrics.txBytes.Add(uint64(len(frame)))
		if update != nil {
			update(&t.laneStates[lane])
		}
	}
	t.sendMu.Unlock()

	if lane >= 0 {
		t.enqueueControl(bondedSend{lane: lane, frame: frame})
	}
}

func (t *BondedTunnel) sendControlOnLane(lane int, msgType byte, payload []byte, update func(*bondedLaneState)) {
	if lane < 0 || lane >= len(t.lanes) {
		return
	}
	frame := EncodeFrame(BondedConnID, msgType, payload)

	t.sendMu.Lock()
	t.laneStates[lane].metrics.txBytes.Add(uint64(len(frame)))
	if update != nil {
		update(&t.laneStates[lane])
	}
	t.sendMu.Unlock()

	t.enqueueControl(bondedSend{lane: lane, frame: frame})
}

func (t *BondedTunnel) sendOps(ops []bondedSend) {
	for _, op := range ops {
		if t.closed.Load() {
			return
		}
		if op.lane < 0 || op.lane >= len(t.lanes) {
			continue
		}
		if op.frameID == 0 {
			// Control traffic bypasses cwnd/inflight accounting, but still goes
			// through paced delivery with higher queue priority.
			t.enqueueControl(op)
			continue
		}
		t.sendMu.Lock()
		for !t.closed.Load() {
			if op.lane < 0 || op.lane >= len(t.laneStates) {
				t.sendMu.Unlock()
				return
			}
			state := &t.laneStates[op.lane]
			cwndLimit := state.cwndBytes
			if op.retransmit {
				cwndLimit += 10 * bondedMaxChunkSize
			}
			if state.inflightBytes+len(op.frame) <= cwndLimit {
				state.inflightBytes += len(op.frame)
				state.metrics.txBytes.Add(uint64(len(op.frame)))
				if op.retransmit {
					state.metrics.retransmitted.Add(1)
				}
				break
			}
			t.sendCond.Wait()
		}
		t.sendMu.Unlock()
		if t.closed.Load() {
			return
		}
		t.queuedBytes.Add(int64(len(op.frame)))
		select {
		case <-t.closeCh:
			t.queuedBytes.Add(-int64(len(op.frame)))
			return
		case t.laneQueues[op.lane] <- op:
		}
	}
}

func (t *BondedTunnel) pacerLoop(lane int) {
	defer t.wg.Done()
	if lane < 0 || lane >= len(t.laneQueues) {
		return
	}
	for {
		var op bondedSend
		select {
		case <-t.ctx.Done():
			return
		case <-t.closeCh:
			return
		case op = <-t.laneQueues[lane]:
			t.queuedBytes.Add(-int64(len(op.frame)))
		default:
			select {
			case <-t.ctx.Done():
				return
			case <-t.closeCh:
				return
			case op = <-t.laneQueues[lane]:
				t.queuedBytes.Add(-int64(len(op.frame)))
			}
		}
		t.waitForPacing(lane, len(op.frame), false)
		if t.closed.Load() {
			return
		}
		if !t.appendLaneCoalescer(lane, op) {
			return
		}
	}
}

func (t *BondedTunnel) controlLoop(lane int) {
	defer t.wg.Done()
	if lane < 0 || lane >= len(t.controlQueues) {
		return
	}
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-t.closeCh:
			return
		case op := <-t.controlQueues[lane]:
			t.queuedBytes.Add(-int64(len(op.frame)))
			if len(op.frame) == 0 {
				continue
			}
			if !t.appendLaneCoalescer(lane, op) {
				return
			}
		}
	}
}

func (t *BondedTunnel) appendLaneCoalescer(lane int, op bondedSend) bool {
	if lane < 0 || lane >= len(t.coalescers) {
		return false
	}
	if len(op.frame) == 0 {
		return true
	}
	if len(op.frame) > bondedChunkPayloadSize {
		t.logFn("bonded: oversize coalescer frame lane=%d size=%d cap=%d", lane, len(op.frame), bondedChunkPayloadSize)
		return false
	}

	shouldFlush := false
	coalescer := &t.coalescers[lane]
	coalescer.mu.Lock()
	if len(coalescer.batch.buf) > 0 && len(coalescer.batch.buf)+len(op.frame) > bondedChunkPayloadSize {
		shouldFlush = true
	}
	coalescer.mu.Unlock()
	if shouldFlush && !t.flushLaneCoalescer(lane) {
		return false
	}

	shouldFlush = false
	coalescer.mu.Lock()
	if len(coalescer.batch.buf) == 0 {
		coalescer.batch.allAppLimited = true
	}
	coalescer.batch.buf = append(coalescer.batch.buf, op.frame...)
	if op.frameID != 0 {
		coalescer.batch.hasData = true
		coalescer.batch.lastDataFrameID = op.frameID
		coalescer.batch.dataFrames++
		coalescer.batch.allAppLimited = coalescer.batch.allAppLimited && op.appLimited
	}
	if len(coalescer.batch.buf) >= bondedChunkPayloadSize {
		shouldFlush = true
	}
	coalescer.mu.Unlock()

	if shouldFlush {
		return t.flushLaneCoalescer(lane)
	}
	return true
}

func (t *BondedTunnel) flushLaneCoalescer(lane int) bool {
	if lane < 0 || lane >= len(t.coalescers) {
		return false
	}
	coalescer := &t.coalescers[lane]

	coalescer.mu.Lock()
	if len(coalescer.batch.buf) == 0 {
		coalescer.mu.Unlock()
		return true
	}
	payload := make([]byte, len(coalescer.batch.buf))
	copy(payload, coalescer.batch.buf)
	meta := coalescer.batch
	coalescer.batch = bondedBatchState{}
	coalescer.mu.Unlock()

	if t.closed.Load() {
		return false
	}
	if meta.hasData {
		t.sendMu.Lock()
		if lane >= 0 && lane < len(t.laneStates) {
			state := &t.laneStates[lane]
			now := time.Now()
			state.lastSentFrameID = meta.lastDataFrameID
			state.lastDataSentAt = now
			state.appLimited = false
			if !state.roundPending {
				state.roundPending = true
				state.roundAppLimited = true
			}
			state.nextRoundDelivered = meta.lastDataFrameID
			state.roundSentFrames += meta.dataFrames
			state.roundAppLimited = state.roundAppLimited && meta.allAppLimited
		}
		t.sendMu.Unlock()
	}
	t.lanes[lane].SendData(payload)
	return !t.closed.Load()
}

func (t *BondedTunnel) coalescerFlushLoop(lane int) {
	defer t.wg.Done()
	if lane < 0 || lane >= len(t.coalescers) {
		return
	}
	ticker := time.NewTicker(bondedFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-t.closeCh:
			return
		case <-ticker.C:
			if !t.flushLaneCoalescer(lane) {
				return
			}
		}
	}
}

func (t *BondedTunnel) waitForPacing(lane int, frameSize int, _ bool) {
	for {
		if t.closed.Load() {
			return
		}

		t.sendMu.Lock()
		if lane < 0 || lane >= len(t.laneStates) {
			t.sendMu.Unlock()
			return
		}
		state := &t.laneStates[lane]
		now := time.Now()
		if state.pacingRate < bondedMinBwBytes {
			state.pacingRate = bondedMinBwBytes
		}
		if state.pacingRate > bondedMaxLaneRateBytes {
			state.pacingRate = bondedMaxLaneRateBytes
		}
		if state.lastPacingUpdate.IsZero() {
			state.lastPacingUpdate = now
			state.pacingBudget = float64(bondedChunkPayloadSize)
		}
		if elapsed := now.Sub(state.lastPacingUpdate); elapsed > 0 {
			state.pacingBudget += elapsed.Seconds() * state.pacingRate
			bucketCap := float64(bondedMaxBurstBytes)
			if state.pacingBudget > bucketCap {
				state.pacingBudget = bucketCap
			}
			state.lastPacingUpdate = now
		}
		if state.nextSendAt.Before(now) {
			state.nextSendAt = now
		}
		sendAt := state.nextSendAt
		minSpacing := bondedMinSpacing
		deficit := float64(frameSize) - state.pacingBudget
		if deficit <= 0 && !sendAt.After(now) {
			state.pacingBudget -= float64(frameSize)
			if state.pacingBudget < 0 {
				state.pacingBudget = 0
			}
			state.nextSendAt = now.Add(minSpacing)
			t.sendMu.Unlock()
			return
		}
		waitBudget := time.Duration(0)
		if deficit > 0 {
			waitBudget = time.Duration(deficit / state.pacingRate * float64(time.Second))
		}
		waitClock := time.Until(sendAt)
		if waitClock < 0 {
			waitClock = 0
		}
		delay := waitBudget
		if waitClock > delay {
			delay = waitClock
		}
		t.sendMu.Unlock()

		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-t.ctx.Done():
				timer.Stop()
				return
			case <-t.closeCh:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		return
	}
}

func (t *BondedTunnel) deliverFrame(frame []byte) {
	if len(frame) == 0 || t.onData == nil || t.closed.Load() {
		return
	}
	t.onData(frame)
}

func (t *BondedTunnel) resetDamagedFlow(connID uint32, msgType byte, reason string) {
	if connID == 0 || t.closed.Load() {
		return
	}

	now := time.Now()

	t.recvMu.Lock()
	t.clearFlowLocked(connID)
	t.dropPendingTransportConnLocked(connID, 0, now)
	t.pruneRetiredLocked(now)
	t.recvMu.Unlock()

	t.sendMu.Lock()
	delete(t.sendFlowSeq, connID)
	delete(t.sendFlows, connID)
	t.sendMu.Unlock()

	resetFrames := buildResetFrames(connID, msgType, reason)
	if len(resetFrames) == 0 {
		return
	}

	if t.onData != nil {
		t.onData(resetFrames)
	}
	t.SendData(resetFrames)
}

func buildResetFrames(connID uint32, _ byte, reason string) []byte {
	if connID == 0 {
		return nil
	}
	connectErr := EncodeFrame(connID, MsgConnectErr, []byte(reason))
	closeMsg := EncodeFrame(connID, MsgClose, nil)
	out := make([]byte, 0, len(connectErr)+len(closeMsg))
	out = append(out, connectErr...)
	out = append(out, closeMsg...)
	return out
}

func (t *BondedTunnel) pruneHistoryLocked(now time.Time) {
	for len(t.historyOrder) > 0 {
		frameID := t.historyOrder[0]
		entry, ok := t.history[frameID]
		if !ok {
			t.historyOrder = t.historyOrder[1:]
			continue
		}
		if len(t.history) <= bondedHistoryMaxFrames && now.Sub(entry.sentAt) <= bondedHistoryTTL {
			break
		}
		t.releaseHistoryInflightLocked(entry)
		delete(t.history, frameID)
		t.historyOrder = t.historyOrder[1:]
	}
}

func (t *BondedTunnel) historyRTOForEntryLocked(entry *bondedHistoryEntry) time.Duration {
	base := time.Second
	for _, lane := range uniqueLanes(entry.chunkLanes) {
		if lane < 0 || lane >= len(t.laneStates) {
			continue
		}
		state := &t.laneStates[lane]
		rtt := state.smoothedRTT
		if rtt <= 0 {
			rtt = state.lastRTT
		}
		if rtt <= 0 {
			continue
		}
		candidate := 4 * rtt
		if candidate > base {
			base = candidate
		}
	}
	if base < time.Second {
		base = time.Second
	}
	rto := base
	for i := 0; i < entry.retries; i++ {
		rto *= 2
		if rto > bondedHistoryTTL {
			return bondedHistoryTTL
		}
	}
	return rto
}

func (t *BondedTunnel) collectLaneRescueEntriesLocked(index int) []*bondedHistoryEntry {
	if index < 0 || index >= len(t.laneStates) {
		return nil
	}
	seen := make(map[uint64]struct{})
	entries := make([]*bondedHistoryEntry, 0, 16)
	for _, entry := range t.history {
		for _, lane := range entry.chunkLanes {
			if lane != index {
				continue
			}
			if _, ok := seen[entry.frameID]; ok {
				break
			}
			seen[entry.frameID] = struct{}{}
			entries = append(entries, entry)
			break
		}
	}
	return entries
}

func (t *BondedTunnel) drainLaneQueuesLocked(index int) {
	if index < 0 || index >= len(t.laneQueues) || index >= len(t.controlQueues) {
		return
	}
	for {
		select {
		case <-t.laneQueues[index]:
		default:
			goto drainControl
		}
	}
drainControl:
	for {
		select {
		case <-t.controlQueues[index]:
		default:
			return
		}
	}
}

func (t *BondedTunnel) resetLaneLocked(index int, now time.Time) {
	if index < 0 || index >= len(t.laneStates) {
		return
	}
	if index >= 0 && index < len(t.coalescers) {
		t.coalescers[index].mu.Lock()
		t.coalescers[index].batch = bondedBatchState{}
		t.coalescers[index].mu.Unlock()
	}
	state := &t.laneStates[index]
	state.metrics.txBytes.Store(0)
	state.metrics.rxBytes.Store(0)
	state.metrics.skipped.Store(0)
	state.metrics.outOfOrder.Store(0)
	state.metrics.lateOrDup.Store(0)
	state.metrics.retransmitted.Store(0)
	state.metrics.nackSent.Store(0)
	state.metrics.nackRecv.Store(0)
	state.metrics.pingSent.Store(0)
	state.metrics.pongRecv.Store(0)
	state.metrics.pingTimeout.Store(0)
	state.weight = 1
	state.currentWeight = 0
	state.score = 1
	state.lossEWMA = 0
	state.disorderEWMA = 0
	state.smoothedRTT = 0
	state.lastRTT = 0
	state.pingOutstanding = false
	state.lastPingID = 0
	state.pingSentAt = time.Time{}
	state.lastPong = time.Time{}
	state.lastProbe = time.Time{}
	state.failureStreak = 0
	state.probing = false
	state.lastObsOOO = 0
	state.lastObsLate = 0
	state.lastObsSkip = 0
	state.cwndBytes = bondedMinCwndBytes
	state.inflightBytes = 0
	state.chunkSize = bondedMinChunkSize
	state.pacingRate = bondedInitialBwBytes
	state.lastAckAt = time.Time{}
	state.nextSendAt = now
	state.pacingBudget = float64(bondedChunkPayloadSize)
	state.lastPacingUpdate = now
	state.maxBandwidth = bondedInitialBwBytes
	state.maxBWSamples = nil
	state.ltBandwidth = bondedInitialBwBytes
	state.ltBWSamples = nil
	state.minRTTWindow = 0
	state.minRTTSamples = nil
	state.bbrState = bondedBBRStartup
	state.pacingGain = 2.885
	state.cwndGain = 2.0
	state.fullBandwidth = 0
	state.fullBwCount = 0
	state.probeBWIndex = 0
	state.probeBWStamp = now
	state.probeRTTUntil = time.Time{}
	state.nextProbeRTTAt = now.Add(10 * time.Second)
	state.inflightHi = 0
	state.bwHi = 0
	state.policerDetected = false
	state.safeRounds = 0
	state.roundCount = 0
	state.roundPending = false
	state.nextRoundDelivered = 0
	state.roundAppLimited = false
	state.roundSentFrames = 0
	state.roundLossEvents = 0
	state.lastRoundLossRate = 0
	state.lastSentFrameID = 0
	state.appLimited = true
	state.lastDataSentAt = now
	state.lastRxAt = now
	for pingID, pingState := range t.pings {
		if pingState.lane == index {
			delete(t.pings, pingID)
		}
	}
}

func (t *BondedTunnel) enqueueControl(op bondedSend) {
	if op.lane < 0 || op.lane >= len(t.controlQueues) {
		return
	}
	t.queuedBytes.Add(int64(len(op.frame)))
	select {
	case <-t.closeCh:
		t.queuedBytes.Add(-int64(len(op.frame)))
		return
	case t.controlQueues[op.lane] <- op:
	}
}

func (t *BondedTunnel) prunePingsLocked(now time.Time) {
	for pingID, pingState := range t.pings {
		if now.Sub(pingState.sentAt) <= bondedPingRetention {
			continue
		}
		delete(t.pings, pingID)
	}
}

func (t *BondedTunnel) pruneSendFlowsLocked(now time.Time) {
	for connID, flow := range t.sendFlows {
		if now.Sub(flow.lastActivity) > bondedFlowIdleTTL {
			delete(t.sendFlows, connID)
		}
	}
}

func (t *BondedTunnel) penalizeHistoryLocked(entry *bondedHistoryEntry, amount float64) {
	seen := make(map[int]struct{}, len(entry.chunkLanes))
	for _, lane := range entry.chunkLanes {
		if lane < 0 || lane >= len(t.laneStates) {
			continue
		}
		if _, ok := seen[lane]; ok {
			continue
		}
		seen[lane] = struct{}{}
		state := &t.laneStates[lane]
		state.lossEWMA = clampFloat(state.lossEWMA+amount, 0, 1)
		if amount >= 0.20 {
			t.decreaseLaneLocked(state, amount >= 0.30)
		}
	}
}

func (t *BondedTunnel) markHistorySkippedLocked(entry *bondedHistoryEntry) {
	for _, lane := range uniqueLanes(entry.chunkLanes) {
		if lane < 0 || lane >= len(t.laneStates) {
			continue
		}
		t.laneStates[lane].metrics.skipped.Add(1)
	}
}

func (t *BondedTunnel) ackHistoryLocked(entry *bondedHistoryEntry) {
	perLane := make(map[int]int, len(entry.chunkLanes))
	perLaneAppLimited := make(map[int]bool, len(entry.chunkLanes))
	for i, lane := range entry.chunkLanes {
		if lane < 0 || lane >= len(t.laneStates) {
			continue
		}
		if i < len(entry.chunkFrameSize) {
			perLane[lane] += entry.chunkFrameSize[i]
		}
		appLimited := false
		if i < len(entry.chunkAppLimited) {
			appLimited = entry.chunkAppLimited[i]
		}
		current, seen := perLaneAppLimited[lane]
		if !seen {
			perLaneAppLimited[lane] = appLimited
		} else {
			perLaneAppLimited[lane] = current && appLimited
		}
	}
	now := time.Now()
	for lane, ackedBytes := range perLane {
		state := &t.laneStates[lane]
		if ackedBytes > state.inflightBytes {
			state.inflightBytes = 0
		} else {
			state.inflightBytes -= ackedBytes
		}
		t.increaseLaneLocked(state, ackedBytes, entry.retransmits > 0, perLaneAppLimited[lane], entry.frameID, entry.sentAt, now)
	}
}

func (t *BondedTunnel) releaseHistoryInflightLocked(entry *bondedHistoryEntry) {
	for i, lane := range entry.chunkLanes {
		if lane < 0 || lane >= len(t.laneStates) {
			continue
		}
		if i >= len(entry.chunkFrameSize) {
			continue
		}
		state := &t.laneStates[lane]
		release := entry.chunkFrameSize[i]
		if release > state.inflightBytes {
			state.inflightBytes = 0
		} else {
			state.inflightBytes -= release
		}
	}
}

func (t *BondedTunnel) increaseLaneLocked(state *bondedLaneState, ackedBytes int, retransmitted bool, appLimited bool, ackFrameID uint64, sentAt, now time.Time) {
	ref := sentAt
	if !state.lastAckAt.IsZero() && state.lastAckAt.After(ref) {
		ref = state.lastAckAt
	}
	delta := now.Sub(ref)
	if delta < time.Millisecond {
		delta = time.Millisecond
	}
	sample := float64(ackedBytes) / delta.Seconds()
	if sample < bondedMinBwBytes {
		sample = bondedMinBwBytes
	}
	if sample > bondedMaxBwBytes {
		sample = bondedMaxBwBytes
	}
	if retransmitted {
		sample *= 0.9
	}
	state.lastAckAt = now
	if !appLimited {
		state.updateBandwidthSampleLocked(sample, now)
	}
	state.finishRoundLocked(ackFrameID, now)
	state.advanceBBRStateLocked(now)
	state.recomputeLaneBudgetLocked(now, retransmitted)
}

func (t *BondedTunnel) decreaseLaneLocked(state *bondedLaneState, severe bool) {
	state.maxBandwidth *= 0.5
	if severe {
		state.maxBandwidth *= 0.75
	}
	if state.maxBandwidth < bondedMinBwBytes {
		state.maxBandwidth = bondedMinBwBytes
	}
	if severe {
		state.chunkSize /= 2
	} else {
		state.chunkSize = state.chunkSize * 3 / 4
	}
	if state.chunkSize < bondedMinChunkSize {
		state.chunkSize = bondedMinChunkSize
	}
	if state.bbrState == bondedBBRStartup {
		state.setBBRStateLocked(bondedBBRDrain, time.Now())
	}
	state.recomputeLaneBudgetLocked(time.Now(), true)
}

func (t *BondedTunnel) noteOutOfOrderLocked(lane int) {
	if lane >= 0 && lane < len(t.laneStates) {
		t.laneStates[lane].metrics.outOfOrder.Add(1)
	}
}

func (t *BondedTunnel) noteLateOrDupLocked(lane int) {
	if lane >= 0 && lane < len(t.laneStates) {
		t.laneStates[lane].metrics.lateOrDup.Add(1)
	}
}

func (t *BondedTunnel) closef(format string, args ...any) {
	t.closeOnce.Do(func() {
		t.closed.Store(true)
		t.cancel()
		close(t.closeCh)
		if t.logFn != nil {
			t.logFn(format, args...)
		}
		if t.sendCond != nil {
			t.sendMu.Lock()
			t.sendCond.Broadcast()
			t.sendMu.Unlock()
		}
		if t.onClose != nil {
			t.onClose()
		}
	})
}

func encodeBondedData(frameID uint64, flowSeq uint32, chunkIdx, chunkTotal int, connID uint32, msgType byte, chunk []byte) []byte {
	buf := make([]byte, bondedDataHeaderSize+len(chunk))
	binary.BigEndian.PutUint64(buf[0:8], frameID)
	binary.BigEndian.PutUint32(buf[8:12], flowSeq)
	binary.BigEndian.PutUint16(buf[12:14], uint16(chunkIdx))
	binary.BigEndian.PutUint16(buf[14:16], uint16(chunkTotal))
	binary.BigEndian.PutUint32(buf[16:20], connID)
	buf[20] = msgType
	copy(buf[21:], chunk)
	return buf
}

func decodeBondedData(payload []byte) (frameID uint64, flowSeq uint32, chunkIdx int, chunkTotal int, connID uint32, msgType byte, chunk []byte, ok bool) {
	if len(payload) < bondedDataHeaderSize {
		return 0, 0, 0, 0, 0, 0, nil, false
	}
	frameID = binary.BigEndian.Uint64(payload[0:8])
	flowSeq = binary.BigEndian.Uint32(payload[8:12])
	chunkIdx = int(binary.BigEndian.Uint16(payload[12:14]))
	chunkTotal = int(binary.BigEndian.Uint16(payload[14:16]))
	connID = binary.BigEndian.Uint32(payload[16:20])
	msgType = payload[20]
	chunk = payload[21:]
	if chunkTotal < 1 {
		return 0, 0, 0, 0, 0, 0, nil, false
	}
	return frameID, flowSeq, chunkIdx, chunkTotal, connID, msgType, chunk, true
}

func encodeBondedSeq(frameID uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, frameID)
	return buf
}

func decodeBondedSeq(payload []byte) (uint64, bool) {
	if len(payload) < 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(payload[:8]), true
}

func encodeBondedPing(id uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)
	return buf
}

func decodeBondedPing(payload []byte) (uint64, bool) {
	if len(payload) < 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(payload[:8]), true
}

func encodeBondedDrop(frameID uint64, connID uint32, msgType byte) []byte {
	buf := make([]byte, 13)
	binary.BigEndian.PutUint64(buf[0:8], frameID)
	binary.BigEndian.PutUint32(buf[8:12], connID)
	buf[12] = msgType
	return buf
}

func decodeBondedDrop(payload []byte) (frameID uint64, connID uint32, msgType byte, ok bool) {
	if len(payload) < 13 {
		return 0, 0, 0, false
	}
	frameID = binary.BigEndian.Uint64(payload[0:8])
	connID = binary.BigEndian.Uint32(payload[8:12])
	msgType = payload[12]
	return frameID, connID, msgType, true
}

func bondedFrameMetadata(frame []byte) (uint32, byte, bool) {
	if len(frame) < 9 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint32(frame[4:8]), frame[8], true
}

func reassemblePendingFrame(entry *bondedPendingFrame) []byte {
	out := make([]byte, 0, entry.size)
	for _, chunk := range entry.chunks {
		out = append(out, chunk...)
	}
	return out
}

func availablePayload(state *bondedLaneState) int {
	room := state.cwndBytes - state.inflightBytes - bondedWireOverhead
	if room < 0 {
		return 0
	}
	return room
}

func availablePayloadWithInflight(state *bondedLaneState, inflight int) int {
	room := state.cwndBytes - inflight - bondedWireOverhead
	if room < 0 {
		return 0
	}
	return room
}

func uniqueLanes(lanes []int) []int {
	if len(lanes) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(lanes))
	out := make([]int, 0, len(lanes))
	for _, lane := range lanes {
		if _, ok := seen[lane]; ok {
			continue
		}
		seen[lane] = struct{}{}
		out = append(out, lane)
	}
	return out
}

func clampFloat(v, minV, maxV float64) float64 {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func clampInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
