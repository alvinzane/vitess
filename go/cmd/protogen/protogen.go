package main

import (
	"fmt"

	"vitess.io/vitess/go/vt/proto/vschema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func main() {
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "lookup",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			Vschema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"rates": {
						Columns: []*vschema.Column{{
							Name: "currency",
						}, {
							Name: "rate",
						}},
					},
				},
			},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "-80",
		Filter: &binlogdatapb.Filter{
			Vschema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"user": {
						Columns: []*vschema.Column{{
							Name: "name",
						}, {
							Name: "id",
						}},
					},
				},
			},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "80-",
		Filter: &binlogdatapb.Filter{
			Vschema: &vschema.Keyspace{
				Tables: map[string]*vschema.Table{
					"user": {
						Columns: []*vschema.Column{{
							Name: "name",
						}, {
							Name: "id",
						}},
					},
				},
			},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "-80",
		Filter: &binlogdatapb.Filter{
			Vschema: &vschema.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"md5": {
						Type: "unicode_loose_md5",
					},
				},
				Tables: map[string]*vschema.Table{
					"user": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "name",
								Name:   "md5",
							},
						},
						Columns: []*vschema.Column{{
							Name: "id",
						}, {
							Name: "name",
						}, {
							Name: "currency",
						}, {
							Name: "amount",
						}},
					},
				},
			},
			KeyRange: &topodatapb.KeyRange{End: []byte{0x80}},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "80-",
		Filter: &binlogdatapb.Filter{
			Vschema: &vschema.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"md5": {
						Type: "unicode_loose_md5",
					},
				},
				Tables: map[string]*vschema.Table{
					"user": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "name",
								Name:   "md5",
							},
						},
						Columns: []*vschema.Column{{
							Name: "id",
						}, {
							Name: "name",
						}, {
							Name: "currency",
						}, {
							Name: "amount",
						}},
					},
				},
			},
			KeyRange: &topodatapb.KeyRange{End: []byte{0x80}},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "-80",
		Filter: &binlogdatapb.Filter{
			Vschema: &vschema.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"md5": {
						Type: "unicode_loose_md5",
					},
				},
				Tables: map[string]*vschema.Table{
					"user": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "name",
								Name:   "md5",
							},
						},
						Columns: []*vschema.Column{{
							Name: "id",
						}, {
							Name: "name",
						}, {
							Name: "currency",
						}, {
							Name: "amount",
						}},
					},
				},
			},
			KeyRange: &topodatapb.KeyRange{Start: []byte{0x80}},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "80-",
		Filter: &binlogdatapb.Filter{
			Vschema: &vschema.Keyspace{
				Vindexes: map[string]*vschemapb.Vindex{
					"md5": {
						Type: "unicode_loose_md5",
					},
				},
				Tables: map[string]*vschema.Table{
					"user": {
						ColumnVindexes: []*vschemapb.ColumnVindex{
							{
								Column: "name",
								Name:   "md5",
							},
						},
						Columns: []*vschema.Column{{
							Name: "id",
						}, {
							Name: "name",
						}, {
							Name: "currency",
						}, {
							Name: "amount",
						}},
					},
				},
			},
			KeyRange: &topodatapb.KeyRange{Start: []byte{0x80}},
		},
	})
}
