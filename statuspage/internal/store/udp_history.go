package store

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"time"
)

const compactMissing uint16 = 0xfffd

type UDPPathAggregateRow struct {
	TS         int64
	NodeSID    string
	ReceiverID string
	State      string
	MedianUS   int64
	P95US      int64
	JitterUS   int64
	Loss       float64
	Coverage   float64
	Samples    int
}

type UDPLiveBlockRow struct {
	NodeSID    string
	ReceiverID string
	StartMS    int64
	IntervalUS uint32
	Slots      []byte
}

// MergeUDPLiveBlock compacts one-second live batches into one-minute DB rows.
// Missing local measurements remain the 0xfffd sentinel; only an explicit wire
// LOSS value can become packet loss later.
func (s *Store) MergeUDPLiveBlock(nodeSID, receiverID string, startMS int64, intervalUS uint32, slots []byte) error {
	return s.MergeUDPLiveBlocks([]UDPLiveBlockRow{{NodeSID: nodeSID, ReceiverID: receiverID, StartMS: startMS, IntervalUS: intervalUS, Slots: slots}})
}

func (s *Store) MergeUDPLiveBlocks(rows []UDPLiveBlockRow) error {
	tx, err := s.begin()
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := mergeUDPLiveBlock(tx, row); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func mergeUDPLiveBlock(tx *txw, row UDPLiveBlockRow) error {
	nodeSID, receiverID, startMS, intervalUS, slots := row.NodeSID, row.ReceiverID, row.StartMS, row.IntervalUS, row.Slots
	if nodeSID == "" || receiverID == "" || intervalUS == 0 || len(slots) == 0 || len(slots)%2 != 0 {
		return errors.New("invalid UDP live block")
	}
	parentStart := startMS - startMS%60000
	parentSlots := int(int64(time.Minute/time.Microsecond) / int64(intervalUS))
	if parentSlots < 1 || parentSlots > 5000 {
		return errors.New("UDP block interval outside storage bounds")
	}
	parent := make([]byte, parentSlots*2)
	for i := 0; i < parentSlots; i++ {
		binary.BigEndian.PutUint16(parent[i*2:i*2+2], compactMissing)
	}
	var existing []byte
	var existingInterval int64
	err := tx.QueryRow(`SELECT interval_us,samples FROM udp_raw_blocks WHERE node_sid=? AND receiver_id=? AND start_ms=? AND interval_us=?`, nodeSID, receiverID, parentStart, intervalUS).Scan(&existingInterval, &existing)
	if err == nil {
		if existingInterval != int64(intervalUS) || len(existing) != len(parent) {
			// A malformed legacy row gets its own one-second
			// row rather than corrupting either time grid.
			parentStart = startMS
			parentSlots = len(slots) / 2
			parent = make([]byte, len(slots))
			for i := 0; i < parentSlots; i++ {
				binary.BigEndian.PutUint16(parent[i*2:i*2+2], compactMissing)
			}
		} else {
			copy(parent, existing)
		}
	}
	offset := int(((startMS - parentStart) * 1000) / int64(intervalUS))
	for i := 0; i < len(slots)/2; i++ {
		dst := offset + i
		if dst < 0 || dst >= len(parent)/2 {
			continue
		}
		value := binary.BigEndian.Uint16(slots[i*2 : i*2+2])
		if value == compactMissing {
			continue
		}
		binary.BigEndian.PutUint16(parent[dst*2:dst*2+2], value)
	}
	_, err = tx.Exec(`INSERT INTO udp_raw_blocks(node_sid,receiver_id,start_ms,interval_us,samples) VALUES(?,?,?,?,?)
		ON CONFLICT(node_sid,receiver_id,start_ms,interval_us) DO UPDATE SET samples=excluded.samples`,
		nodeSID, receiverID, parentStart, intervalUS, parent)
	return err
}

func (s *Store) SaveUDPPathAggregates(rows []UDPPathAggregateRow) error {
	tx, err := s.begin()
	if err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := tx.Exec(`INSERT INTO udp_aggregates(ts,node_sid,receiver_id,state,median_us,p95_us,jitter_us,loss,coverage,samples)
			VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(ts,node_sid,receiver_id) DO UPDATE SET
			state=excluded.state,median_us=excluded.median_us,p95_us=excluded.p95_us,jitter_us=excluded.jitter_us,
			loss=excluded.loss,coverage=excluded.coverage,samples=excluded.samples`,
			row.TS, row.NodeSID, row.ReceiverID, row.State, row.MedianUS, row.P95US, row.JitterUS, row.Loss, row.Coverage, row.Samples); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) UDPHistory(nodeSID, receiverID string, fromMS, toMS int64) ([]UDPLiveBlockRow, []UDPPathAggregateRow, error) {
	if nodeSID == "" || fromMS <= 0 || toMS <= fromMS {
		return nil, nil, errors.New("invalid UDP history range")
	}
	rawFrom := fromMS - int64(time.Minute/time.Millisecond)
	if rawFrom < 0 {
		rawFrom = 0
	}
	var rawRows *sql.Rows
	var err error
	if receiverID == "" {
		rawRows, err = s.query(`SELECT node_sid,receiver_id,start_ms,interval_us,samples FROM udp_raw_blocks
			WHERE node_sid=? AND start_ms>=? AND start_ms<? ORDER BY start_ms,receiver_id LIMIT 10000`, nodeSID, rawFrom, toMS)
	} else {
		rawRows, err = s.query(`SELECT node_sid,receiver_id,start_ms,interval_us,samples FROM udp_raw_blocks
			WHERE node_sid=? AND receiver_id=? AND start_ms>=? AND start_ms<? ORDER BY start_ms LIMIT 10000`, nodeSID, receiverID, rawFrom, toMS)
	}
	if err != nil {
		return nil, nil, err
	}
	defer rawRows.Close()
	var raw []UDPLiveBlockRow
	for rawRows.Next() {
		var row UDPLiveBlockRow
		if err := rawRows.Scan(&row.NodeSID, &row.ReceiverID, &row.StartMS, &row.IntervalUS, &row.Slots); err != nil {
			return nil, nil, err
		}
		raw = append(raw, row)
	}
	if err := rawRows.Err(); err != nil {
		return nil, nil, err
	}

	fromTS, toTS := fromMS/1000, (toMS+999)/1000
	var aggRows *sql.Rows
	if receiverID == "" {
		aggRows, err = s.query(`SELECT ts,node_sid,receiver_id,state,median_us,p95_us,jitter_us,loss,coverage,samples FROM udp_aggregates
			WHERE node_sid=? AND ts>=? AND ts<? ORDER BY ts,receiver_id LIMIT 10000`, nodeSID, fromTS, toTS)
	} else {
		aggRows, err = s.query(`SELECT ts,node_sid,receiver_id,state,median_us,p95_us,jitter_us,loss,coverage,samples FROM udp_aggregates
			WHERE node_sid=? AND receiver_id=? AND ts>=? AND ts<? ORDER BY ts LIMIT 10000`, nodeSID, receiverID, fromTS, toTS)
	}
	if err != nil {
		return nil, nil, err
	}
	defer aggRows.Close()
	var aggregates []UDPPathAggregateRow
	for aggRows.Next() {
		var row UDPPathAggregateRow
		if err := aggRows.Scan(&row.TS, &row.NodeSID, &row.ReceiverID, &row.State, &row.MedianUS, &row.P95US, &row.JitterUS, &row.Loss, &row.Coverage, &row.Samples); err != nil {
			return nil, nil, err
		}
		aggregates = append(aggregates, row)
	}
	return raw, aggregates, aggRows.Err()
}
