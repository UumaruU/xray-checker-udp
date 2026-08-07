package udpmon

import (
	"encoding/binary"
	"errors"
	"time"
)

// Compact raw history uses one uint16 per expected probe slot. RTT is stored in
// 250us ticks (0..16.38s). The top three values are sentinels. Timestamps are
// reconstructed as blockStart + slot*probeInterval, so 10Hz consumes only 200
// bytes/path/10s before DB row/index overhead.
const (
	compactTick           = 250 * time.Microsecond
	CompactMissing uint16 = 0xfffd
	CompactInvalid uint16 = 0xfffe
	CompactLoss    uint16 = 0xffff
	compactMaxRTT  uint16 = 0xfffc
)

type CompactBlock struct {
	StartUnixMS int64
	IntervalUS  uint32
	Slots       []uint16
}

func NewCompactBlock(start time.Time, interval, duration time.Duration) CompactBlock {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	count := int(duration / interval)
	if count < 1 {
		count = 1
	}
	slots := make([]uint16, count)
	for i := range slots {
		slots[i] = CompactMissing
	}
	return CompactBlock{StartUnixMS: start.UnixMilli(), IntervalUS: uint32(interval / time.Microsecond), Slots: slots}
}

func (b *CompactBlock) Put(sample Sample) {
	interval := time.Duration(b.IntervalUS) * time.Microsecond
	if interval <= 0 || len(b.Slots) == 0 {
		return
	}
	start := time.UnixMilli(b.StartUnixMS)
	delta := sample.At.Sub(start)
	if delta < 0 {
		return
	}
	idx := int((delta + interval/2) / interval)
	if idx < 0 || idx >= len(b.Slots) {
		return
	}
	switch sample.Status {
	case SampleLoss:
		b.Slots[idx] = CompactLoss
	case SampleInvalid:
		b.Slots[idx] = CompactInvalid
	case SampleOK:
		ticks := sample.RTT / compactTick
		if ticks < 0 {
			ticks = 0
		}
		if ticks > time.Duration(compactMaxRTT) {
			ticks = time.Duration(compactMaxRTT)
		}
		b.Slots[idx] = uint16(ticks)
	}
}

func (b CompactBlock) MarshalBinary() []byte {
	buf := make([]byte, 16+len(b.Slots)*2)
	binary.BigEndian.PutUint64(buf[0:8], uint64(b.StartUnixMS))
	binary.BigEndian.PutUint32(buf[8:12], b.IntervalUS)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(b.Slots)))
	for i, value := range b.Slots {
		binary.BigEndian.PutUint16(buf[16+i*2:18+i*2], value)
	}
	return buf
}

func UnmarshalCompactBlock(buf []byte) (CompactBlock, error) {
	if len(buf) < 16 {
		return CompactBlock{}, errors.New("compact UDP block is too short")
	}
	count := int(binary.BigEndian.Uint32(buf[12:16]))
	if count < 0 || len(buf) != 16+count*2 {
		return CompactBlock{}, errors.New("compact UDP block has invalid length")
	}
	b := CompactBlock{
		StartUnixMS: int64(binary.BigEndian.Uint64(buf[0:8])),
		IntervalUS:  binary.BigEndian.Uint32(buf[8:12]),
		Slots:       make([]uint16, count),
	}
	for i := range b.Slots {
		b.Slots[i] = binary.BigEndian.Uint16(buf[16+i*2 : 18+i*2])
	}
	return b, nil
}

func (b CompactBlock) Samples() []Sample {
	interval := time.Duration(b.IntervalUS) * time.Microsecond
	start := time.UnixMilli(b.StartUnixMS)
	out := make([]Sample, 0, len(b.Slots))
	for i, value := range b.Slots {
		if value == CompactMissing {
			continue
		}
		s := Sample{At: start.Add(time.Duration(i) * interval), Sequence: uint64(i)}
		switch value {
		case CompactLoss:
			s.Status = SampleLoss
		case CompactInvalid:
			s.Status = SampleInvalid
		default:
			s.Status = SampleOK
			s.RTT = time.Duration(value) * compactTick
		}
		out = append(out, s)
	}
	return out
}
