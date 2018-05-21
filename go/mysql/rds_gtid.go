// Copyright 2018 PlanetScale Inc.

package mysql

import (
	"fmt"
	"strings"
)

const rdsFlavorID = "rds"

// parseRDSGTID is registered as a GTID parser.
func parseRDSGTID(s string) (GTID, error) {
	// Split into parts.
	parts := strings.Split(s, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid rds GTID (%v): expecting file:pos", s)
	}

	return rdsGTID{
		file: parts[0],
		pos:  parts[1],
	}, nil
}

// parseRDSGTIDSet is registered as a GTIDSet parser.
func parseRDSGTIDSet(s string) (GTIDSet, error) {
	gtid, err := parseRDSGTID(s)
	if err != nil {
		return nil, err
	}
	return gtid.(rdsGTID), err
}

// rdsGTID implements GTID.
type rdsGTID struct {
	file, pos string
}

// String implements GTID.String().
func (gtid rdsGTID) String() string {
	return gtid.file + ":" + gtid.pos
}

// Flavor implements GTID.Flavor().
func (gtid rdsGTID) Flavor() string {
	return rdsFlavorID
}

// SequenceDomain implements GTID.SequenceDomain().
func (gtid rdsGTID) SequenceDomain() interface{} {
	return nil
}

// SourceServer implements GTID.SourceServer().
func (gtid rdsGTID) SourceServer() interface{} {
	return nil
}

// SequenceNumber implements GTID.SequenceNumber().
func (gtid rdsGTID) SequenceNumber() interface{} {
	return nil
}

// GTIDSet implements GTID.GTIDSet().
func (gtid rdsGTID) GTIDSet() GTIDSet {
	return gtid
}

// ContainsGTID implements GTIDSet.ContainsGTID().
func (gtid rdsGTID) ContainsGTID(other GTID) bool {
	if other == nil {
		return true
	}
	rdsOther, ok := other.(rdsGTID)
	if !ok {
		return false
	}
	if rdsOther.file < gtid.file {
		return true
	}
	if rdsOther.file > gtid.file {
		return false
	}
	return rdsOther.pos <= gtid.pos
}

// Contains implements GTIDSet.Contains().
func (gtid rdsGTID) Contains(other GTIDSet) bool {
	if other == nil {
		return true
	}
	rdsOther, _ := other.(rdsGTID)
	return gtid.ContainsGTID(rdsOther)
}

// Equal implements GTIDSet.Equal().
func (gtid rdsGTID) Equal(other GTIDSet) bool {
	rdsOther, ok := other.(rdsGTID)
	if !ok {
		return false
	}
	return gtid == rdsOther
}

// AddGTID implements GTIDSet.AddGTID().
func (gtid rdsGTID) AddGTID(other GTID) GTIDSet {
	rdsOther, ok := other.(rdsGTID)
	if !ok {
		return gtid
	}
	return rdsOther
}

func init() {
	gtidParsers[rdsFlavorID] = parseRDSGTID
	gtidSetParsers[rdsFlavorID] = parseRDSGTIDSet
}
