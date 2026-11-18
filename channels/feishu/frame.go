package feishu

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	FrameMethodControl int32 = 0
	FrameMethodData    int32 = 1

	headerType      = "type"
	headerMessageID = "message_id"
	headerSum       = "sum"
	headerSeq       = "seq"
	headerTraceID   = "trace_id"
	headerBizRT     = "biz_rt"

	msgTypeEvent = "event"
	msgTypePing  = "ping"
	msgTypePong  = "pong"
)

// FrameHeader is one key-value pair inside a pbbp2.Frame.
type FrameHeader struct {
	Key   string
	Value string
}

// Frame represents a Feishu long-connection pbbp2.Frame protobuf message.
type Frame struct {
	SeqID           uint64
	LogID           uint64
	Service         int32
	Method          int32
	Headers         []FrameHeader
	PayloadEncoding string
	PayloadType     string
	Payload         []byte
	LogIDNew        string
}

// HeaderValue returns the value for key or "" if absent.
func (f *Frame) HeaderValue(key string) string {
	for _, h := range f.Headers {
		if h.Key == key {
			return h.Value
		}
	}
	return ""
}

// EncodeFrame serializes f into pbbp2.Frame protobuf wire format.
func EncodeFrame(f Frame) []byte {
	buf := make([]byte, 0, 64+len(f.Payload))
	// Required proto2 fields 1..4 are always emitted even when zero.
	buf = appendTagVarint(buf, 1, 0, f.SeqID)
	buf = appendTagVarint(buf, 2, 0, f.LogID)
	buf = appendTagVarint(buf, 3, 0, uint64(int64(f.Service)))
	buf = appendTagVarint(buf, 4, 0, uint64(int64(f.Method)))
	for _, h := range f.Headers {
		var hb []byte
		hb = appendTagBytes(hb, 1, []byte(h.Key))
		hb = appendTagBytes(hb, 2, []byte(h.Value))
		buf = appendTagBytes(buf, 5, hb)
	}
	if f.PayloadEncoding != "" {
		buf = appendTagBytes(buf, 6, []byte(f.PayloadEncoding))
	}
	if f.PayloadType != "" {
		buf = appendTagBytes(buf, 7, []byte(f.PayloadType))
	}
	if len(f.Payload) > 0 {
		buf = appendTagBytes(buf, 8, f.Payload)
	}
	if f.LogIDNew != "" {
		buf = appendTagBytes(buf, 9, []byte(f.LogIDNew))
	}
	return buf
}

// DecodeFrame parses a pbbp2.Frame protobuf wire payload.
func DecodeFrame(b []byte) (Frame, error) {
	var f Frame
	var hasSeq, hasLog, hasService, hasMethod bool
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return Frame{}, errors.New("pbbp2: invalid field tag")
		}
		b = b[n:]
		fieldNum, wireType := tag>>3, tag&7
		switch wireType {
		case 0:
			v, m := binary.Uvarint(b)
			if m <= 0 {
				return Frame{}, errors.New("pbbp2: invalid varint")
			}
			b = b[m:]
			switch fieldNum {
			case 1:
				f.SeqID, hasSeq = v, true
			case 2:
				f.LogID, hasLog = v, true
			case 3:
				f.Service, hasService = int32(v), true
			case 4:
				f.Method, hasMethod = int32(v), true
			}
		case 1:
			if len(b) < 8 {
				return Frame{}, errors.New("pbbp2: truncated 64-bit field")
			}
			b = b[8:]
		case 2:
			ln, m := binary.Uvarint(b)
			if m <= 0 || ln > uint64(len(b)-m) {
				return Frame{}, errors.New("pbbp2: invalid length-delimited field")
			}
			val := b[m : m+int(ln)]
			b = b[m+int(ln):]
			switch fieldNum {
			case 5:
				hdr, err := decodeHeader(val)
				if err != nil {
					return Frame{}, err
				}
				f.Headers = append(f.Headers, hdr)
			case 6:
				f.PayloadEncoding = string(val)
			case 7:
				f.PayloadType = string(val)
			case 8:
				f.Payload = append([]byte(nil), val...)
			case 9:
				f.LogIDNew = string(val)
			}
		case 5:
			if len(b) < 4 {
				return Frame{}, errors.New("pbbp2: truncated 32-bit field")
			}
			b = b[4:]
		default:
			return Frame{}, fmt.Errorf("pbbp2: unsupported wire type %d", wireType)
		}
	}
	if !hasSeq || !hasLog || !hasService || !hasMethod {
		return Frame{}, errors.New("pbbp2: missing required frame field")
	}
	return f, nil
}

func decodeHeader(b []byte) (FrameHeader, error) {
	var h FrameHeader
	var hasKey, hasVal bool
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			return FrameHeader{}, errors.New("pbbp2: invalid header tag")
		}
		b = b[n:]
		fieldNum, wireType := tag>>3, tag&7
		switch wireType {
		case 0:
			_, m := binary.Uvarint(b)
			if m <= 0 {
				return FrameHeader{}, errors.New("pbbp2: invalid header varint")
			}
			b = b[m:]
		case 1:
			if len(b) < 8 {
				return FrameHeader{}, errors.New("pbbp2: truncated header 64-bit")
			}
			b = b[8:]
		case 2:
			ln, m := binary.Uvarint(b)
			if m <= 0 || ln > uint64(len(b)-m) {
				return FrameHeader{}, errors.New("pbbp2: invalid header string")
			}
			val := string(b[m : m+int(ln)])
			b = b[m+int(ln):]
			switch fieldNum {
			case 1:
				h.Key, hasKey = val, true
			case 2:
				h.Value, hasVal = val, true
			}
		case 5:
			if len(b) < 4 {
				return FrameHeader{}, errors.New("pbbp2: truncated header 32-bit")
			}
			b = b[4:]
		default:
			return FrameHeader{}, fmt.Errorf("pbbp2: unsupported header wire type %d", wireType)
		}
	}
	if !hasKey || !hasVal {
		return FrameHeader{}, errors.New("pbbp2: missing required header field")
	}
	return h, nil
}

func appendTagVarint(dst []byte, fieldNum uint64, wireType uint64, v uint64) []byte {
	dst = binary.AppendUvarint(dst, (fieldNum<<3)|wireType)
	return binary.AppendUvarint(dst, v)
}

func appendTagBytes(dst []byte, fieldNum uint64, val []byte) []byte {
	dst = binary.AppendUvarint(dst, (fieldNum<<3)|2)
	dst = binary.AppendUvarint(dst, uint64(len(val)))
	return append(dst, val...)
}

type fragmentEntry struct {
	parts     [][]byte
	got       int
	createdAt time.Time
}

type fragmentAssembler struct {
	mu    sync.Mutex
	items map[string]*fragmentEntry
}

func newFragmentAssembler() *fragmentAssembler {
	return &fragmentAssembler{items: make(map[string]*fragmentEntry)}
}

func (a *fragmentAssembler) add(msgID string, sum, seq int, payload []byte, now time.Time) ([]byte, error) {
	if sum <= 0 || seq < 0 || seq >= sum {
		return nil, fmt.Errorf("feishu: invalid fragment metadata sum=%d seq=%d", sum, seq)
	}
	if sum == 1 {
		if !json.Valid(payload) {
			return nil, errors.New("feishu: invalid event json")
		}
		return payload, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for k, v := range a.items {
		if now.Sub(v.createdAt) > 10*time.Second {
			delete(a.items, k)
		}
	}

	entry := a.items[msgID]
	if entry == nil {
		entry = &fragmentEntry{
			parts:     make([][]byte, sum),
			createdAt: now,
		}
		a.items[msgID] = entry
	} else if len(entry.parts) != sum {
		delete(a.items, msgID)
		return nil, fmt.Errorf("feishu: fragment sum mismatch %d vs %d", sum, len(entry.parts))
	}

	if entry.parts[seq] == nil {
		entry.got++
	}
	entry.parts[seq] = append([]byte(nil), payload...)
	if entry.got < sum {
		return nil, nil
	}

	delete(a.items, msgID)
	total := 0
	for _, p := range entry.parts {
		total += len(p)
	}
	merged := make([]byte, 0, total)
	for _, p := range entry.parts {
		merged = append(merged, p...)
	}
	if !json.Valid(merged) {
		return nil, errors.New("feishu: invalid reassembled event json")
	}
	return merged, nil
}
