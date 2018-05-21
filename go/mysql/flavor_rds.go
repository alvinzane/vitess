// Copyright 2018 PlanetScale Inc.

package mysql

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"golang.org/x/net/context"
)

type rdsFlavor struct {
	format     BinlogFormat
	file       string
	pos        int
	savedEvent *rdsBinlogEvent
}

// masterGTIDSet is part of the Flavor interface.
func (flv *rdsFlavor) masterGTIDSet(c *Conn) (GTIDSet, error) {
	qr, err := c.ExecuteFetch("SHOW SLAVE STATUS", 100, true /* wantfields */)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		qr, err = c.ExecuteFetch("SHOW MASTER STATUS", 100, true /* wantfields */)
		if err != nil {
			return nil, err
		}
		if len(qr.Rows) == 0 {
			return nil, errors.New("No master or slave status")
		}
		resultMap, err := resultToMap(qr)
		if err != nil {
			return nil, err
		}
		return rdsGTID{
			file: resultMap["File"],
			pos:  resultMap["Position"],
		}, nil
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return nil, err
	}
	return rdsGTID{
		file: resultMap["Master_Log_File"],
		pos:  resultMap["Read_Master_Log_Pos"],
	}, nil
}

func (flv *rdsFlavor) startSlaveCommand() string {
	return "CALL mysql.rds_start_replication"
}

func (flv *rdsFlavor) stopSlaveCommand() string {
	return "CALL mysql.rds_stop_replication"
}

// sendBinlogDumpCommand is part of the Flavor interface.
func (flv *rdsFlavor) sendBinlogDumpCommand(c *Conn, slaveID uint32, startPos Position) error {
	rpos, ok := startPos.GTIDSet.(rdsGTID)
	if !ok {
		return fmt.Errorf("startPos.GTIDSet is wrong type - expected rdsGTID, got: %#v", startPos.GTIDSet)
	}

	pos, err := strconv.Atoi(rpos.pos)
	if err != nil {
		return fmt.Errorf("invalid position: %v", startPos.GTIDSet)
	}
	flv.file = rpos.file
	flv.pos = pos

	return c.WriteComBinlogDump(slaveID, rpos.file, uint32(pos), 0)
}

// readBinlogEvent is part of the Flavor interface.
func (flv *rdsFlavor) readBinlogEvent(c *Conn) (BinlogEvent, error) {
	if ret := flv.savedEvent; ret != nil {
		flv.savedEvent = nil
		return ret, nil
	}

	result, err := c.ReadPacket()
	if err != nil {
		return nil, err
	}
	switch result[0] {
	case EOFPacket:
		return nil, NewSQLError(CRServerLost, SSUnknownSQLState, "%v", io.EOF)
	case ErrPacket:
		return nil, ParseErrorPacket(result)
	}
	event := &rdsBinlogEvent{binlogEvent: binlogEvent(result[1:])}
	switch {
	case event.IsFormatDescription():
		format, err := event.Format()
		if err != nil {
			return nil, err
		}
		flv.format = format
	case event.IsRotate():
		if !flv.format.IsZero() {
			stripped, _, _ := event.StripChecksum(flv.format)
			flv.pos, flv.file = stripped.(*rdsBinlogEvent).rotate(flv.format)
			return newRDSGTIDEvent(flv.file, flv.pos, event.Timestamp()), nil
		}
	default:
		if !flv.format.IsZero() {
			if v := event.nextPosition(flv.format); v != 0 {
				flv.pos = v
				flv.savedEvent = event
				return newRDSGTIDEvent(flv.file, flv.pos, event.Timestamp()), nil
			}
		}
	}
	return event, nil
}

// resetReplicationCommands is part of the Flavor interface.
func (flv *rdsFlavor) resetReplicationCommands() []string {
	return []string{
		"not allowed",
	}
}

// setSlavePositionCommands is part of the Flavor interface.
func (flv *rdsFlavor) setSlavePositionCommands(pos Position) []string {
	return []string{
		"not allowed",
	}
}

// setSlavePositionCommands is part of the Flavor interface.
func (flv *rdsFlavor) changeMasterArg() string {
	return "not allowed"
}

// status is part of the Flavor interface.
func (flv *rdsFlavor) status(c *Conn) (SlaveStatus, error) {
	qr, err := c.ExecuteFetch("SHOW SLAVE STATUS", 100, true /* wantfields */)
	if err != nil {
		return SlaveStatus{}, err
	}
	if len(qr.Rows) == 0 {
		// The query returned no data, meaning the server
		// is not configured as a slave.
		return SlaveStatus{}, ErrNotSlave
	}

	resultMap, err := resultToMap(qr)
	if err != nil {
		return SlaveStatus{}, err
	}

	status := parseSlaveStatus(resultMap)
	status.Position.GTIDSet = rdsGTID{
		file: resultMap["Master_Log_File"],
		pos:  resultMap["Read_Master_Log_Pos"],
	}
	return status, nil
}

// waitUntilPositionCommand is part of the Flavor interface.
func (flv *rdsFlavor) waitUntilPositionCommand(ctx context.Context, pos Position) (string, error) {
	rdsPos, ok := pos.GTIDSet.(rdsGTID)
	if !ok {
		return "", fmt.Errorf("Position is not rds compatible: %#v", pos.GTIDSet)
	}

	if deadline, ok := ctx.Deadline(); ok {
		timeout := deadline.Sub(time.Now())
		if timeout <= 0 {
			return "", fmt.Errorf("timed out waiting for position %v", pos)
		}
		return fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %s, %.6f)", rdsPos.file, rdsPos.pos, timeout.Seconds()), nil
	}

	return fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %s)", rdsPos.file, rdsPos.pos), nil
}

// enableBinlogPlaybackCommand is part of the Flavor interface.
func (*rdsFlavor) enableBinlogPlaybackCommand() string {
	return ""
}

// disableBinlogPlaybackCommand is part of the Flavor interface.
func (*rdsFlavor) disableBinlogPlaybackCommand() string {
	return ""
}
