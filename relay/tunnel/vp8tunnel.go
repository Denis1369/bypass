package tunnel

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
)

var VP8KeyframeMagic = []byte{
	0x10,
	0x00, 0x00,
	0x9d, 0x01, 0x2a,
	0x80, 0x02,
	0xe0, 0x01,
}

var vp8InterframeMagic = []byte{
	0xb1, 0x01, 0x00, 0x08, 0x11, 0x18, 0x00, 0x18, 0x00,
	0x18, 0x58, 0x2f, 0xf4, 0x00, 0x08, 0x00, 0x00,
}

var vp8TunnelSignature = []byte{0xDE, 0xAD, 0xBE, 0xEF}

const (
	DataFrameMarker     = 0xFF
	VP8TunnelMaxPayload = 1050
	vp8DataNonceSize    = 12
	vp8DataTagSize      = 16
	vp8PrefixMaxLen     = 17
	vp8PayloadOverhead  = vp8PrefixMaxLen + 4 + 1 + vp8DataNonceSize + vp8DataTagSize
	vp8PayloadSecret    = "whitelist-bypass-vp8-payload-v1"
)

var (
	vp8AEADOnce sync.Once
	vp8AEADInst cipher.AEAD
	vp8AEADErr  error
)

func vp8PayloadAEAD() (cipher.AEAD, error) {
	vp8AEADOnce.Do(func() {
		key := sha256.Sum256([]byte(vp8PayloadSecret))
		block, err := aes.NewCipher(key[:])
		if err != nil {
			vp8AEADErr = err
			return
		}
		vp8AEADInst, vp8AEADErr = cipher.NewGCM(block)
	})
	return vp8AEADInst, vp8AEADErr
}

type VP8DataTunnel struct {
	track         *webrtc.TrackLocalStaticSample
	mu            sync.Mutex
	writeMu       sync.Mutex
	logFn         func(string, ...any)
	frameCount    uint64
	running       bool
	sendQueue     chan []byte
	OnData        func([]byte)
	OnClose       func()
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	stopOnce      sync.Once
	nonceCtr      atomic.Uint64
	nonceSalt     [4]byte
	forceKeyframe atomic.Bool
}

func (t *VP8DataTunnel) SetOnData(fn func([]byte)) { t.OnData = fn }
func (t *VP8DataTunnel) SetOnClose(fn func())      { t.OnClose = fn }
func (t *VP8DataTunnel) ForceNextKeyframe()        { t.forceKeyframe.Store(true) }

func buildVP8TunnelNoopFrame(payloadSize int) []byte {
	if payloadSize < 0 {
		payloadSize = 0
	}
	return EncodeFrame(BondedConnID, MsgNoop, make([]byte, payloadSize))
}

func (t *VP8DataTunnel) SendEmergencyKeyframe() {
	if t.ctx.Err() != nil {
		return
	}
	// Send a "fat" but still valid tunnel frame so SFU-side heuristics see a
	// realistic keyframe-sized payload, while the receiver can still parse it as
	// a legitimate tunnel NOOP frame.
	const emergencyNoopPayloadSize = 900
	t.forceKeyframe.Store(true)
	frameID, frame, err := t.writeSampleDirect(buildVP8TunnelNoopFrame(emergencyNoopPayloadSize), 20*time.Millisecond)
	if err != nil {
		if t.logFn != nil {
			t.logFn("vp8tunnel: emergency keyframe error: %v (frame %d, %d bytes)", err, frameID, len(frame))
		}
		return
	}
	if t.logFn != nil {
		t.logFn("vp8tunnel: emergency keyframe sent frame=%d size=%d", frameID, len(frame))
	}
}

func NewVP8DataTunnel(track *webrtc.TrackLocalStaticSample, logFn func(string, ...any)) *VP8DataTunnel {
	ctx, cancel := context.WithCancel(context.Background())
	t := &VP8DataTunnel{
		track:     track,
		logFn:     logFn,
		sendQueue: make(chan []byte, 256),
		ctx:       ctx,
		cancel:    cancel,
	}
	_, _ = rand.Read(t.nonceSalt[:])
	return t
}

func (t *VP8DataTunnel) nextNonce() []byte {
	nonce := make([]byte, vp8DataNonceSize)
	copy(nonce[:4], t.nonceSalt[:])
	binary.BigEndian.PutUint64(nonce[4:], t.nonceCtr.Add(1))
	return nonce
}

func (t *VP8DataTunnel) encryptPayload(data []byte) []byte {
	aead, err := vp8PayloadAEAD()
	if err != nil {
		if t.logFn != nil {
			t.logFn("vp8tunnel: encrypt init error: %v", err)
		}
		return nil
	}
	nonce := t.nextNonce()
	ciphertext := aead.Seal(nil, nonce, data, nil)
	prefix := vp8InterframeMagic
	if t.forceKeyframe.Swap(false) || t.frameCount%60 == 0 {
		prefix = VP8KeyframeMagic
	}
	frame := make([]byte, len(prefix)+len(vp8TunnelSignature)+1+len(nonce)+len(ciphertext))
	copy(frame, prefix)
	offset := len(prefix)
	copy(frame[offset:], vp8TunnelSignature)
	offset += len(vp8TunnelSignature)
	frame[offset] = DataFrameMarker
	copy(frame[offset+1:], nonce)
	copy(frame[offset+1+len(nonce):], ciphertext)
	return frame
}

func (t *VP8DataTunnel) buildFrame(data []byte) []byte {
	t.frameCount++
	return t.encryptPayload(data)
}

func (t *VP8DataTunnel) writeSampleDirect(data []byte, duration time.Duration) (uint64, []byte, error) {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	frame := t.buildFrame(data)
	if len(frame) == 0 {
		return 0, nil, nil
	}
	frameID := t.frameCount - 1
	err := t.track.WriteSample(media.Sample{Data: frame, Duration: duration})
	return frameID, frame, err
}

var sendCount atomic.Uint64

func (t *VP8DataTunnel) SendData(data []byte) {
	n := sendCount.Add(1)
	if n <= 5 || n%100 == 0 {
		t.logFn("vp8tunnel: SendData #%d len=%d queueLen=%d", n, len(data), len(t.sendQueue))
	}
	select {
	case <-t.ctx.Done():
		return
	case t.sendQueue <- data:
	}
}

func (t *VP8DataTunnel) Start(fps int) {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return
	}
	t.running = true
	t.mu.Unlock()
	keepaliveInterval := time.Second / time.Duration(fps)
	lastSend := time.Now()

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(keepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-t.ctx.Done():
				return
			case data := <-t.sendQueue:
				now := time.Now()
				elapsed := now.Sub(lastSend)
				// Most stable interval for burst mode, prevents congestion control
				minInterval := 750 * time.Microsecond
				if elapsed < minInterval {
					time.Sleep(minInterval - elapsed)
				}
				frameID, frame, err := t.writeSampleDirect(data, keepaliveInterval)
				if len(frame) == 0 {
					continue
				}
				lastSend = time.Now()
				if err != nil {
					t.logFn("vp8tunnel: WriteSample DATA error: %v (frame %d, %d bytes)", err, frameID, len(frame))
				} else if frameID <= 10 || frameID%100 == 0 {
					t.logFn("vp8tunnel: WriteSample DATA ok frame=%d size=%d dataLen=%d first=0x%02x", frameID, len(frame), len(data), frame[0])
				}
				ticker.Reset(keepaliveInterval)
			case <-ticker.C:
				lastSend = time.Now()
				frameID, frame, err := t.writeSampleDirect(buildVP8TunnelNoopFrame(0), keepaliveInterval)
				if len(frame) == 0 {
					continue
				}
				if frameID <= 3 || frameID%500 == 0 {
					t.logFn("vp8tunnel: KEEPALIVE frame=%d first=0x%02x err=%v", frameID, frame[0], err)
				}
			}
		}
	}()
}

func (t *VP8DataTunnel) Stop() {
	t.stopOnce.Do(func() {
		t.cancel()
		t.wg.Wait()
		t.mu.Lock()
		t.running = false
		t.mu.Unlock()
		if t.OnClose != nil {
			t.OnClose()
		}
	})
}

func ExtractDataFromPayload(payload []byte) []byte {
	payload = extractVP8EncryptedPayload(payload)
	if len(payload) < 1+vp8DataNonceSize+vp8DataTagSize {
		return nil
	}
	if payload[0] != DataFrameMarker {
		return nil
	}
	aead, err := vp8PayloadAEAD()
	if err != nil {
		return nil
	}
	nonce := payload[1 : 1+vp8DataNonceSize]
	ciphertext := payload[1+vp8DataNonceSize:]
	if len(ciphertext) < vp8DataTagSize {
		return nil
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil
	}
	return plaintext
}

func extractVP8EncryptedPayload(payload []byte) []byte {
	idx := bytes.Index(payload, vp8TunnelSignature)
	if idx == -1 {
		return nil
	}
	return payload[idx+len(vp8TunnelSignature):]
}

func LooksLikeVP8TunnelFrame(frame []byte) bool {
	if len(frame) == 0 {
		return false
	}
	data := ExtractDataFromPayload(frame)
	if data == nil {
		return false
	}
	if len(data) == 0 {
		return true
	}
	for len(data) > 0 {
		next, rest, ok := NextFrame(data)
		if !ok || len(next) < 9 || !IsKnownMsgType(next[8]) {
			return false
		}
		data = rest
	}
	return true
}
