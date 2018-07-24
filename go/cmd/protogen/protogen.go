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
				"uproduct": "select * from product",
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
				"morder": "select * from uorder where in_keyrange(merchant_id, \\'hash\\', \\'-80\\')",
			},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "80-",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"morder": "select * from uorder where in_keyrange(merchant_id, \\'hash\\', \\'-80\\')",
			},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "-80",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"morder": "select * from uorder where in_keyrange(merchant_id, \\'hash\\', \\'80-\\')",
			},
		},
	})
	fmt.Println(&binlogdatapb.BinlogSource{
		Keyspace: "user",
		Shard:    "80-",
		Filter: &binlogdatapb.Filter{
			TableMap: map[string]string{
				"morder": "select * from uorder where in_keyrange(merchant_id, \\'hash\\', \\'80-\\')",
			},
		},
	})
}
