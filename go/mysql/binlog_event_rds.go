// Copyright 2018 PlanetScale Inc.

package mysql

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

// rdsBinlogEvent wraps a raw packet buffer and provides methods to examine
// it by implementing BinlogEvent. Some methods are pulled in from binlogEvent.
type rdsBinlogEvent struct {
	binlogEvent
}

func (*rdsBinlogEvent) GTID(BinlogFormat) (GTID, bool, error) {
	return nil, false, nil
}

func (*rdsBinlogEvent) IsGTID() bool {
	return false
}

func (*rdsBinlogEvent) PreviousGTIDs(BinlogFormat) (Position, error) {
	return Position{}, fmt.Errorf("RDS should not provide PREVIOUS_GTIDS_EVENT events")
}

// StripChecksum implements BinlogEvent.StripChecksum().
func (ev *rdsBinlogEvent) StripChecksum(f BinlogFormat) (BinlogEvent, []byte, error) {
	switch f.ChecksumAlgorithm {
	case BinlogChecksumAlgOff, BinlogChecksumAlgUndef:
		// There is no checksum.
		return ev, nil, nil
	default:
		// Checksum is the last 4 bytes of the event buffer.
		data := ev.Bytes()
		length := len(data)
		checksum := data[length-4:]
		data = data[:length-4]
		return &rdsBinlogEvent{binlogEvent: binlogEvent(data)}, checksum, nil
	}
}

// nextPosition returns the next file position of the binlog.
// If no information is available, it returns 0.
func (ev *rdsBinlogEvent) nextPosition(f BinlogFormat) int {
	if f.HeaderLength <= 13 {
		// Dead code. This is just a failsafe.
		return 0
	}
	return int(binary.LittleEndian.Uint32(ev.Bytes()[13:17]))
}

// rotate implements BinlogEvent.Rotate().
//
// Expected format (L = total length of event data):
//   # bytes  field
//   8        position
//   8:L      file
func (ev *rdsBinlogEvent) rotate(f BinlogFormat) (int, string) {
	data := ev.Bytes()[f.HeaderLength:]
	pos := binary.LittleEndian.Uint64(data[0:8])
	file := data[8:]
	return int(pos), string(file)
}

//----------------------------------------------------------------------------

// rdsGTIDEvent is a fake GTID event for RDS.
type rdsGTIDEvent struct {
	gtid      rdsGTID
	timestamp uint32
	binlogEvent
}

func newRDSGTIDEvent(file string, pos int, timestamp uint32) rdsGTIDEvent {
	return rdsGTIDEvent{
		gtid: rdsGTID{
			file: file,
			pos:  strconv.Itoa(pos),
		},
		timestamp: timestamp,
	}
}

func (ev rdsGTIDEvent) IsPseudo() bool {
	return true
}

func (ev rdsGTIDEvent) IsGTID() bool {
	return false
}

func (ev rdsGTIDEvent) IsValid() bool {
	return true
}

func (ev rdsGTIDEvent) IsFormatDescription() bool {
	return false
}

func (ev rdsGTIDEvent) IsRotate() bool {
	return false
}

func (ev rdsGTIDEvent) Timestamp() uint32 {
	return ev.timestamp
}

func (ev rdsGTIDEvent) GTID(BinlogFormat) (GTID, bool, error) {
	return ev.gtid, false, nil
}

func (ev rdsGTIDEvent) PreviousGTIDs(BinlogFormat) (Position, error) {
	return Position{}, fmt.Errorf("RDS should not provide PREVIOUS_GTIDS_EVENT events")
}

// StripChecksum implements BinlogEvent.StripChecksum().
func (ev rdsGTIDEvent) StripChecksum(f BinlogFormat) (BinlogEvent, []byte, error) {
	return ev, nil, nil
}
