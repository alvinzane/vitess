/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package endtoend

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func TestConsistentLookup(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	// Simple insert.
	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	exec(t, conn, "commit")
	qr := exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Inserting again should fail.
	exec(t, conn, "begin")
	_, err = conn.ExecuteFetch("insert into t1(id1, id2) values(1, 4)", 1000, false)
	exec(t, conn, "rollback")
	want := "duplicate entry"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("second insert: %v, must contain %s", err, want)
	}

	// Simple delete.
	exec(t, conn, "begin")
	exec(t, conn, "delete from t1 where id1=1")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Autocommit insert.
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select id2 from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// Autocommit delete.
	exec(t, conn, "delete from t1 where id1=1")

	// Dangling row pointing to existing keyspace id.
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	// Delete the main row only.
	exec(t, conn2, "use `ks:-80`")
	exec(t, conn2, "delete from t1 where id1=1")
	// Verify the lookup row is still there.
	qr = exec(t, conn, "select id2 from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// Insert should still succeed.
	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// Lookup row should be unchanged.
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Dangling row not pointing to existing keyspace id.
	exec(t, conn2, "use `ks:-80`")
	exec(t, conn2, "delete from t1 where id1=1")
	// Update the lookup row with bogus keyspace id.
	exec(t, conn, "update t1_id2_idx set keyspace_id='aaa' where id2=4")
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"aaa\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// Insert should still succeed.
	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1, 4)")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	// lookup row must be updated.
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Update, but don't change anything. This should not deadlock.
	exec(t, conn, "begin")
	exec(t, conn, "update t1 set id2=4 where id1=1")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Update, and change the lookup value. This should change main and lookup rows.
	exec(t, conn, "begin")
	exec(t, conn, "update t1 set id2=5 where id1=1")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(5)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(5) VARBINARY(\"\\x16k@\\xb4J\\xbaK\\xd6\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	exec(t, conn, "delete from t1 where id1=1")
}

func TestConsistentLookupMultiInsert(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1,4), (2,5)")
	exec(t, conn, "commit")
	qr := exec(t, conn, "select * from t1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(4)] [INT64(2) INT64(5)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select count(*) from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Delete one row but leave its lookup dangling.
	exec(t, conn2, "use `ks:-80`")
	exec(t, conn2, "delete from t1 where id1=1")
	// Insert a bogus lookup row.
	exec(t, conn, "insert into t1_id2_idx(id2, keyspace_id) values(6, 'aaa')")
	// Insert 3 rows:
	// first row will insert without changing lookup.
	// second will insert and change lookup.
	// third will be a fresh insert for main and lookup.
	exec(t, conn, "begin")
	exec(t, conn, "insert into t1(id1, id2) values(1,2), (3,6), (4,7)")
	exec(t, conn, "commit")
	qr = exec(t, conn, "select id1, id2 from t1 order by id1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) INT64(2)] [INT64(2) INT64(5)] [INT64(3) INT64(6)] [INT64(4) INT64(7)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t1_id2_idx where id2=6")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(6) VARBINARY(\"N\\xb1\\x90ɢ\\xfa\\x16\\x9c\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select count(*) from t1_id2_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(5)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	exec(t, conn, "delete from t1 where id1=1")
	exec(t, conn, "delete from t1 where id1=2")
	exec(t, conn, "delete from t1 where id1=3")
	exec(t, conn, "delete from t1 where id1=4")
	exec(t, conn, "delete from t1_id2_idx where id2=4")
}

func TestHashLookupMultiInsertIgnore(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// conn2 is for queries that target shards.
	conn2, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	// DB should start out clean
	qr := exec(t, conn, "select count(*) from t2_id4_idx")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(0)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select count(*) from t2")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(0)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Try inserting a bunch of ids at once
	exec(t, conn, "begin")
	exec(t, conn, "insert ignore into t2(id3, id4) values(50,60), (30,40), (10,20)")
	exec(t, conn, "commit")

	// Verify
	qr = exec(t, conn, "select id3, id4 from t2 order by id3")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(10) INT64(20)] [INT64(30) INT64(40)] [INT64(50) INT64(60)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select id3, id4 from t2_id4_idx order by id3")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(10) INT64(20)] [INT64(30) INT64(40)] [INT64(50) INT64(60)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func TestSecondaryLookup(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// connShard1 is for queries that target shards.
	connShard1, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer connShard1.Close()

	// connShard2 is for queries that target shards.
	connShard2, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer connShard2.Close()

	// insert multiple values
	exec(t, conn, "begin")
	exec(t, conn, "insert into t3(user_id, lastname, address) values(1,'snow','castle_black'), (2,'stark','winterfell')")
	exec(t, conn, "commit")
	qr := exec(t, conn, "select * from t3")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) VARCHAR(\"snow\") VARCHAR(\"castle_black\")] [INT64(2) VARCHAR(\"stark\") VARCHAR(\"winterfell\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//verify that the lookup is created for lastname
	qr = exec(t, conn, "select count(*) from t3_lastname_map")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select lastname from t3_lastname_map where user_id=1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[VARCHAR(\"snow\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//verify that the lookup is created for address
	qr = exec(t, conn, "select count(*) from t3_address_map")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select address from t3_address_map where user_id=2")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[VARCHAR(\"winterfell\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//update both videxes
	exec(t, conn, "begin")
	exec(t, conn, "update t3 set lastname='targaryen', address='dragonstone' where user_id=2 ")
	exec(t, conn, "commit")
	//Verify that values are updated in the table by fecthin in all combination
	qr = exec(t, conn, "select * from t3 where user_id=2")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) VARCHAR(\"targaryen\") VARCHAR(\"dragonstone\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t3 where lastname='targaryen'")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) VARCHAR(\"targaryen\") VARCHAR(\"dragonstone\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t3 where address='dragonstone'")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) VARCHAR(\"targaryen\") VARCHAR(\"dragonstone\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t3 where address='dragonstone' and lastname='targaryen'")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) VARCHAR(\"targaryen\") VARCHAR(\"dragonstone\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//TODO: verify that values are updated in lookup table
	// qr = exec(t, conn, "select lastname from t3_lastname_map where user_id=2")
	// if got, want := fmt.Sprintf("%v", qr.Rows), "[[VARCHAR(\"targaryen\")]]"; got != want {
	// 	t.Errorf("select:\n%v want\n%v", got, want)
	// }
	// qr = exec(t, conn, "select address from t3_address_map where user_id=2")
	// if got, want := fmt.Sprintf("%v", qr.Rows), "[[VARCHAR(\"dragonstone\")]]"; got != want {
	// 	t.Errorf("select:\n%v want\n%v", got, want)
	// }

	//update single value
	exec(t, conn, "begin")
	exec(t, conn, "update t3 set lastname='stark' where user_id=1 ")
	exec(t, conn, "commit")
	//Verify that value is updated in the table
	qr = exec(t, conn, "select * from t3 where user_id=1")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) VARCHAR(\"stark\") VARCHAR(\"castle_black\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Fetch table based of lookup indexes
	qr = exec(t, conn, "select * from t3 where lastname='targaryen'")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) VARCHAR(\"targaryen\") VARCHAR(\"dragonstone\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t3 where address='dragonstone'")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) VARCHAR(\"targaryen\") VARCHAR(\"dragonstone\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Insert another row with same lastname with autocommit
	exec(t, conn, "insert into t3(user_id, lastname, address) values(3,'targaryen','kings_landing')")

	// Verify that select on main table retuns the right results
	qr = exec(t, conn, "select * from t3 where lastname='targaryen'")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(2) VARCHAR(\"targaryen\") VARCHAR(\"dragonstone\")] [INT64(3) VARCHAR(\"targaryen\") VARCHAR(\"kings_landing\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	qr = exec(t, conn, "select * from t3 where lastname='targaryen' and user_id=3")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(3) VARCHAR(\"targaryen\") VARCHAR(\"kings_landing\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Insert row with same lastname should fail
	exec(t, conn, "begin")
	_, err = conn.ExecuteFetch("insert into t3(user_id, lastname, address) values(3,'targaryen','black_tower')", 1000, false)
	exec(t, conn, "rollback")
	want := "AlreadyExists"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("second insert: %v, must contain %s", err, want)
	}

	//Insert duplicate address direct into address lookup table should fail
	// exec(t, conn, "begin")
	// _, err = conn.ExecuteFetch("insert into t3_address_map(user_id, address) values(4,'castle_black')", 1000, false)
	// exec(t, conn, "rollback")
	// want = "AlreadyExists"
	// fmt.Println(err)
	// if err == nil || !strings.Contains(err.Error(), want) {
	// 	t.Errorf("second insert: %v, must contain %s", err, want)
	// }

	//Insert duplicate address direct into main table should also fail
	// exec(t, conn, "begin")
	// _, err = conn.ExecuteFetch("insert into t3(user_id, lastname, address) values(4,'targaryen','castle_black')", 1000, false)
	// exec(t, conn, "rollback")
	// want = "AlreadyExists"
	// fmt.Println(err)
	// if err == nil || !strings.Contains(err.Error(), want) {
	// 	t.Errorf("second insert: %v, must contain %s", err, want)
	// }

	// Testing non-unique vindexes
	// insert multiple values and verifying across multiple shards
	exec(t, conn, "begin")
	exec(t, conn, "insert into t3(user_id, lastname, address) values(4,'lannister','casterly_rock'), (5,'tyrell','highgarden')")
	exec(t, conn, "commit")

	exec(t, connShard1, "use `ks:-80`")
	exec(t, connShard2, "use `ks:80-`")

	//Shard1 will have 1,2,3,5
	qr = exec(t, connShard1, "select * from t3")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) VARCHAR(\"stark\") VARCHAR(\"castle_black\")] [INT64(2) VARCHAR(\"targaryen\") VARCHAR(\"dragonstone\")] [INT64(3) VARCHAR(\"targaryen\") VARCHAR(\"kings_landing\")] [INT64(5) VARCHAR(\"tyrell\") VARCHAR(\"highgarden\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	//Shard2 will have value 4
	qr = exec(t, connShard2, "select * from t3")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARCHAR(\"lannister\") VARCHAR(\"casterly_rock\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
	//Shard2 will have value 4
	qr = exec(t, connShard2, "select * from t3_lastname_map")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[VARCHAR(\"lannister\") INT64(4)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	// Test IN clause using non-unique vindex
	qr = exec(t, conn, "select user_id, lastname, address from t3 where lastname IN ('lannister','tyrell') ORDER by user_id")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARCHAR(\"lannister\") VARCHAR(\"casterly_rock\")] [INT64(5) VARCHAR(\"tyrell\") VARCHAR(\"highgarden\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test Delete
	exec(t, conn, "delete from t3 where user_id=5")
	qr = exec(t, connShard1, "select * from t3")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(1) VARCHAR(\"stark\") VARCHAR(\"castle_black\")] [INT64(2) VARCHAR(\"targaryen\") VARCHAR(\"dragonstone\")] [INT64(3) VARCHAR(\"targaryen\") VARCHAR(\"kings_landing\")]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//TODO:Ajeet verify why there are multiple values and delete is not working
	qr = exec(t, connShard1, "select * from t3_lastname_map")
	if got, want := fmt.Sprintf("%v", qr.Rows), "[[VARCHAR(\"snow\") INT64(1)] [VARCHAR(\"stark\") INT64(1)] [VARCHAR(\"stark\") INT64(2)] [VARCHAR(\"targaryen\") INT64(2)] [VARCHAR(\"targaryen\") INT64(3)] [VARCHAR(\"tyrell\") INT64(5)]]"; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	//Test Scatter Delete, should throw unsupported error
	exec(t, conn, "begin")
	_, err = conn.ExecuteFetch("delete from t3 where user_id>2", 1000, false)
	exec(t, conn, "rollback")
	want = "unsupported: multi shard delete on a table with owned lookup vindexes"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Scatter delete: %v, must contain %s", err, want)
	}

	// Test scatter update
	// exec(t, conn, "UPDATE t3 SET lastname='martell', address='drone' WHERE user_id>2")
	// qr = exec(t, conn, "select user_id, lastname, address from t3 where user_id>2")
	// if got, want := fmt.Sprintf("%v", qr.Rows), "[[INT64(4) VARCHAR(\"lannister\") VARCHAR(\"casterly_rock\")] [INT64(5) VARCHAR(\"tyrell\") VARCHAR(\"highgarden\")]]"; got != want {
	// 	t.Errorf("select:\n%v want\n%v", got, want)
	// }

	/*
	 */
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}
