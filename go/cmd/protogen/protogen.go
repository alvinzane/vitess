package main

import (
	"fmt"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func main() {
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "lookup",
		Shard:    "0",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"urates": "select * from rates",
			},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "-80",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"name_user_idx": "select name, id from user",
			},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "80-",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"name_user_idx": "select name, id from user",
			},
		},
	})

	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "-80",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"nuser": "select * from user where in_keyrange(name, 'unicode_loose_md5', '-80')",
			},
		},
	})

	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "80-",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"nuser": "select * from user where in_keyrange(name, 'unicode_loose_md5', '-80')",
			},
		},
	})

	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "-80",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"nuser": "select * from user where in_keyrange(name, 'unicode_loose_md5', '80-')",
			},
		},
	})

	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "80-",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"nuser": "select * from user where in_keyrange(name, 'unicode_loose_md5', '80-')",
			},
		},
	})
}
