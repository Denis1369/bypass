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

var vp8Keyframe = []byte{
	16, 2, 0, 157, 1, 42, 2, 0, 2, 0, 2, 7, 8, 133, 133, 136,
	153, 132, 136, 11, 2, 0, 12, 13, 96, 0, 254, 252, 173, 16,
}

var vp8Interframe = []byte{
	177, 1, 0, 8, 17, 24, 0, 24, 0, 24, 88, 47, 244, 0, 8, 0, 0,
}

const (
	DataFrameMarker     = 0xFF
	VP8TunnelMaxPayload = 1050
	vp8DataNonceSize    = 12
	vp8DataTagSize      = 16
	vp8PayloadOverhead  = 1 + vp8DataNonceSize + vp8DataTagSize
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
	track      *webrtc.TrackLocalStaticSample
	mu         sync.Mutex
	logFn      func(string, ...any)
	frameCount uint64
	running    bool
	sendQueue  chan []byte
	OnData     func([]byte)
	OnClose    func()
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	stopOnce   sync.Once
	nonceCtr   atomic.Uint64
	nonceSalt  [4]byte
}

func (t *VP8DataTunnel) SetOnData(fn func([]byte)) { t.OnData = fn }
func (t *VP8DataTunnel) SetOnClose(fn func())      { t.OnClose = fn }

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
	frame := make([]byte, 1+len(nonce)+len(ciphertext))
	frame[0] = DataFrameMarker
	copy(frame[1:], nonce)
	copy(frame[1+len(nonce):], ciphertext)
	return frame
}

func (t *VP8DataTunnel) buildFrame(data []byte) []byte {
	t.frameCount++
	if len(data) == 0 {
		if t.frameCount%60 == 0 {
			return vp8Keyframe
		}
		return vp8Interframe
	}
	return t.encryptPayload(data)
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
				frame := t.buildFrame(data)
				if len(frame) == 0 {
					continue
				}
				err := t.track.WriteSample(media.Sample{Data: frame, Duration: keepaliveInterval})
				lastSend = time.Now()
				if err != nil {
					t.logFn("vp8tunnel: WriteSample DATA error: %v (frame %d, %d bytes)", err, t.frameCount-1, len(frame))
				} else if t.frameCount <= 10 || t.frameCount%100 == 0 {
					t.logFn("vp8tunnel: WriteSample DATA ok frame=%d size=%d dataLen=%d first=0x%02x", t.frameCount-1, len(frame), len(data), frame[0])
				}
				if t.frameCount%60 == 0 {
					kf := t.buildFrame(nil)
					t.track.WriteSample(media.Sample{Data: kf, Duration: keepaliveInterval})
				}
				ticker.Reset(keepaliveInterval)
			case <-ticker.C:
				lastSend = time.Now()
				frame := t.buildFrame(nil)
				err := t.track.WriteSample(media.Sample{Data: frame, Duration: keepaliveInterval})
				if t.frameCount <= 3 || t.frameCount%500 == 0 {
					t.logFn("vp8tunnel: KEEPALIVE frame=%d first=0x%02x err=%v", t.frameCount-1, frame[0], err)
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
	if err != nil || len(plaintext) == 0 {
		return nil
	}
	return plaintext
}

func LooksLikeVP8TunnelFrame(frame []byte) bool {
	if len(frame) == 0 {
		return false
	}
	if bytes.Equal(frame, vp8Interframe) || bytes.Equal(frame, vp8Keyframe) {
		return true
	}
	data := ExtractDataFromPayload(frame)
	if len(data) == 0 {
		return false
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
