package tunnel

import "encoding/binary"

const (
	MsgConnect    byte = 0x01
	MsgConnectOK  byte = 0x02
	MsgConnectErr byte = 0x03
	MsgData       byte = 0x04
	MsgClose      byte = 0x05
	MsgUDP        byte = 0x06
	MsgUDPReply   byte = 0x07
	MsgNoop       byte = 0x08
	MsgBondedData byte = 0x40
	MsgBondedNack byte = 0x41
	MsgBondedDrop byte = 0x42
	MsgBondedPing byte = 0x43
	MsgBondedPong byte = 0x44
	MsgBondedAck  byte = 0x45
)

func IsKnownMsgType(msgType byte) bool {
	switch msgType {
	case MsgConnect, MsgConnectOK, MsgConnectErr, MsgData, MsgClose, MsgUDP, MsgUDPReply, MsgNoop,
		MsgBondedData, MsgBondedNack, MsgBondedDrop, MsgBondedPing, MsgBondedPong, MsgBondedAck:
		return true
	default:
		return false
	}
}

const BondedConnID uint32 = 0

type DataTunnel interface {
	SendData(data []byte)
	SetOnData(fn func([]byte))
	SetOnClose(fn func())
}

type LaneHealthSnapshot struct {
	Index       int
	RxBytes     uint64
	PingTimeout uint64
}

type LaneHealthProvider interface {
	LaneHealthSnapshots() []LaneHealthSnapshot
}

type LaneResetter interface {
	ResetLane(index int)
}

type BufferUsageReporter interface {
	GetTotalBufferUsage() int
}

type CongestionResetter interface {
	ResetCongestion()
}

func EncodeFrame(connID uint32, msgType byte, payload []byte) []byte {
	buf := make([]byte, 4+5+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(5+len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], connID)
	buf[8] = msgType
	copy(buf[9:], payload)
	return buf
}

func DecodeFrames(data []byte, cb func(connID uint32, msgType byte, payload []byte)) {
	for len(data) >= 4 {
		frameLen := int(binary.BigEndian.Uint32(data[0:4]))
		if frameLen < 5 || 4+frameLen > len(data) {
			return
		}
		connID := binary.BigEndian.Uint32(data[4:8])
		msgType := data[8]
		payload := data[9 : 4+frameLen]
		cb(connID, msgType, payload)
		data = data[4+frameLen:]
	}
}

func NextFrame(data []byte) (frame []byte, rest []byte, ok bool) {
	if len(data) < 4 {
		return nil, data, false
	}
	frameLen := int(binary.BigEndian.Uint32(data[0:4]))
	if frameLen < 5 || 4+frameLen > len(data) {
		return nil, data, false
	}
	frame = make([]byte, 4+frameLen)
	copy(frame, data[:4+frameLen])
	return frame, data[4+frameLen:], true
}
