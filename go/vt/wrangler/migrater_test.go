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

package wrangler

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/sqltypes"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
)

// TestTableMigrate tests table mode migrations.
// This has to be kept in sync with TestShardMigrate.
func TestTableMigrate(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)

	//-------------------------------------------------------------------------------------------------------------------
	// Single cell RDONLY migration.
	err := tme.wr.MigrateReads(ctx, MigrateTables, tme.streams, []string{"cell1"}, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellRouting(t, tme.wr, "cell1", map[string][]string{
		"t1":            {"ks1.t1"},
		"ks2.t1":        {"ks1.t1"},
		"t2":            {"ks1.t2"},
		"ks2.t2":        {"ks1.t2"},
		"t1@rdonly":     {"ks2.t1"},
		"ks2.t1@rdonly": {"ks2.t1"},
		"t2@rdonly":     {"ks2.t2"},
		"ks2.t2@rdonly": {"ks2.t2"},
	})
	checkCellRouting(t, tme.wr, "cell2", map[string][]string{
		"t1":     {"ks1.t1"},
		"ks2.t1": {"ks1.t1"},
		"t2":     {"ks1.t2"},
		"ks2.t2": {"ks1.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Other cell REPLICA migration.
	// The global routing already contains redirections for rdonly.
	// So, adding routes for replica and deploying to cell2 will also cause
	// cell2 to migrat rdonly. This is a quirk that can be fixed later if necessary.
	err = tme.wr.MigrateReads(ctx, MigrateTables, tme.streams, []string{"cell2"}, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellRouting(t, tme.wr, "cell1", map[string][]string{
		"t1":            {"ks1.t1"},
		"ks2.t1":        {"ks1.t1"},
		"t2":            {"ks1.t2"},
		"ks2.t2":        {"ks1.t2"},
		"t1@rdonly":     {"ks2.t1"},
		"ks2.t1@rdonly": {"ks2.t1"},
		"t2@rdonly":     {"ks2.t2"},
		"ks2.t2@rdonly": {"ks2.t2"},
	})
	checkCellRouting(t, tme.wr, "cell2", map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Single cell backward REPLICA migration.
	err = tme.wr.MigrateReads(ctx, MigrateTables, tme.streams, []string{"cell2"}, topodatapb.TabletType_REPLICA, directionBackward)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, tme.wr, map[string][]string{
		"t1":            {"ks1.t1"},
		"ks2.t1":        {"ks1.t1"},
		"t2":            {"ks1.t2"},
		"ks2.t2":        {"ks1.t2"},
		"t1@rdonly":     {"ks2.t1"},
		"ks2.t1@rdonly": {"ks2.t1"},
		"t2@rdonly":     {"ks2.t2"},
		"ks2.t2@rdonly": {"ks2.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Migrate all REPLICA.
	err = tme.wr.MigrateReads(ctx, MigrateTables, tme.streams, nil, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// All cells RDONLY backward migration.
	err = tme.wr.MigrateReads(ctx, MigrateTables, tme.streams, nil, topodatapb.TabletType_RDONLY, directionBackward)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
	})
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't migrate master with MigrateReads.
	err = tme.wr.MigrateReads(ctx, MigrateTables, tme.streams, nil, topodatapb.TabletType_MASTER, directionForward)
	want := "tablet type must be REPLICA or RDONLY: MASTER"
	if err == nil || err.Error() != want {
		t.Errorf("MigrateReads(master) err: %v, want %v", err, want)
	}
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't migrate writes if REPLICA and RDONLY have not fully migrated yet.
	err = tme.wr.MigrateWrites(ctx, MigrateTables, tme.streams, 1*time.Second)
	want = "missing tablet type specific routing, read-only traffic must be migrated before migrating writes"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites err: %v, want %v", err, want)
	}
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Test MigrateWrites cancelation on failure.

	// Migrate all the reads first.
	err = tme.wr.MigrateReads(ctx, MigrateTables, tme.streams, nil, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
	})

	// Check for journals.
	tme.dbSource1Client.addQuery("select 1 from _vt.resharding_journal where id = 445516443381867838", &sqltypes.Result{}, nil)
	tme.dbSource2Client.addQuery("select 1 from _vt.resharding_journal where id = 445516443381867838", &sqltypes.Result{}, nil)

	// Wait for position: Reads current state, updates to Stopped, and re-reads.
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running|",
	)
	tme.dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbDest2Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
	tme.dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest2Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 2", &sqltypes.Result{}, nil)
	stopped := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|state",
		"int64|varchar"),
		"1|Stopped",
	)
	tme.dbDest1Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbDest2Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbDest1Client.addQuery("select * from _vt.vreplication where id = 2", stopped, nil)

	// Cancel Migration
	cancel1 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 1"
	cancel2 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 2"
	tme.dbDest1Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	tme.dbDest2Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	tme.dbDest1Client.addQuery(cancel2, &sqltypes.Result{}, nil)

	err = tme.wr.MigrateWrites(ctx, MigrateTables, tme.streams, 0*time.Second)
	want = "DeadlineExceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites(0 timeout) err: %v, must contain %v", err, want)
	}
	checkRouting(t, tme.wr, map[string][]string{
		"t1":             {"ks1.t1"},
		"ks2.t1":         {"ks1.t1"},
		"t2":             {"ks1.t2"},
		"ks2.t2":         {"ks1.t2"},
		"t1@replica":     {"ks2.t1"},
		"ks2.t1@replica": {"ks2.t1"},
		"t2@replica":     {"ks2.t2"},
		"ks2.t2@replica": {"ks2.t2"},
		"t1@rdonly":      {"ks2.t1"},
		"ks2.t1@rdonly":  {"ks2.t1"},
		"t2@rdonly":      {"ks2.t2"},
		"ks2.t2@rdonly":  {"ks2.t2"},
	})
	checkBlacklist(t, tme.ts, "ks1:-40", nil)
	checkBlacklist(t, tme.ts, "ks1:40-", nil)
	checkBlacklist(t, tme.ts, "ks2:-80", nil)
	checkBlacklist(t, tme.ts, "ks2:80-", nil)

	//-------------------------------------------------------------------------------------------------------------------
	// Test successful MigrateWrites.

	// Create journals.
	journal1 := "insert into _vt.resharding_journal.*445516443381867838.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*-80.*MariaDB/5-456-893.*participants.*40.*40"
	tme.dbSource1Client.addQueryRE(journal1, &sqltypes.Result{}, nil)
	journal2 := "insert into _vt.resharding_journal.*445516443381867838.*tables.*t1.*t2.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*80.*participants.*40.*40"
	tme.dbSource2Client.addQueryRE(journal2, &sqltypes.Result{}, nil)

	// Create backward replicaions.
	tme.dbSource1Client.addQueryRE("insert into _vt.vreplication.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*-40.*t2.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	tme.dbSource2Client.addQueryRE("insert into _vt.vreplication.*ks2.*-80.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	tme.dbSource2Client.addQueryRE("insert into _vt.vreplication.*ks2.*80-.*t1.*in_keyrange.*c1.*hash.*40-.*t2.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
	tme.dbSource1Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbSource2Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbSource2Client.addQuery("select * from _vt.vreplication where id = 2", stopped, nil)

	// Delete the target replications.
	tme.dbDest1Client.addQuery("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest2Client.addQuery("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest1Client.addQuery("delete from _vt.vreplication where id = 2", &sqltypes.Result{}, nil)

	err = tme.wr.MigrateWrites(ctx, MigrateTables, tme.streams, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	checkRouting(t, tme.wr, map[string][]string{
		"t1": {"ks2.t1"},
		"t2": {"ks2.t2"},
	})
	checkBlacklist(t, tme.ts, "ks1:-40", []string{"t1", "t2"})
	checkBlacklist(t, tme.ts, "ks1:40-", []string{"t1", "t2"})
	checkBlacklist(t, tme.ts, "ks2:-80", nil)
	checkBlacklist(t, tme.ts, "ks2:80-", nil)

	verifyQueries(t, tme.allDBClients)
}

// TestShardMigrate tests table mode migrations.
// This has to be kept in sync with TestTableMigrate.
func TestShardMigrate(t *testing.T) {
	ctx := context.Background()
	tme := newTestShardMigrater(ctx, t)
	defer tme.stopTablets(t)

	// Initial check
	checkServedTypes(t, tme.ts, "ks:-40", 3)
	checkServedTypes(t, tme.ts, "ks:40-", 3)
	checkServedTypes(t, tme.ts, "ks:-80", 0)
	checkServedTypes(t, tme.ts, "ks:80-", 0)

	//-------------------------------------------------------------------------------------------------------------------
	// Single cell RDONLY migration.
	err := tme.wr.MigrateReads(ctx, MigrateShards, tme.streams, []string{"cell1"}, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell2", 0)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell2", 0)
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Other cell REPLICA migration.
	err = tme.wr.MigrateReads(ctx, MigrateShards, tme.streams, []string{"cell2"}, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell2", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell2", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell2", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell2", 1)
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Single cell backward REPLICA migration.
	err = tme.wr.MigrateReads(ctx, MigrateShards, tme.streams, []string{"cell2"}, topodatapb.TabletType_REPLICA, directionBackward)
	if err != nil {
		t.Fatal(err)
	}
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell1", 2)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell1", 1)
	checkCellServedTypes(t, tme.ts, "ks:-40", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:40-", "cell2", 3)
	checkCellServedTypes(t, tme.ts, "ks:-80", "cell2", 0)
	checkCellServedTypes(t, tme.ts, "ks:80-", "cell2", 0)
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Migrate all RDONLY.
	// This is an extra step that does not exist in the tables test.
	// The per-cell migration mechanism is different for tables. So, this
	// extra step is needed to bring things in sync.
	err = tme.wr.MigrateReads(ctx, MigrateShards, tme.streams, nil, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 2)
	checkServedTypes(t, tme.ts, "ks:40-", 2)
	checkServedTypes(t, tme.ts, "ks:-80", 1)
	checkServedTypes(t, tme.ts, "ks:80-", 1)
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Migrate all REPLICA.
	err = tme.wr.MigrateReads(ctx, MigrateShards, tme.streams, nil, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// All cells RDONLY backward migration.
	err = tme.wr.MigrateReads(ctx, MigrateShards, tme.streams, nil, topodatapb.TabletType_RDONLY, directionBackward)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 2)
	checkServedTypes(t, tme.ts, "ks:40-", 2)
	checkServedTypes(t, tme.ts, "ks:-80", 1)
	checkServedTypes(t, tme.ts, "ks:80-", 1)
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't migrate master with MigrateReads.
	err = tme.wr.MigrateReads(ctx, MigrateShards, tme.streams, nil, topodatapb.TabletType_MASTER, directionForward)
	want := "tablet type must be REPLICA or RDONLY: MASTER"
	if err == nil || err.Error() != want {
		t.Errorf("MigrateReads(master) err: %v, want %v", err, want)
	}
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Can't migrate writes if REPLICA and RDONLY have not fully migrated yet.
	err = tme.wr.MigrateWrites(ctx, MigrateShards, tme.streams, 1*time.Second)
	want = "cannot migrate MASTER away"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites err: %v, want %v", err, want)
	}
	verifyQueries(t, tme.allDBClients)

	//-------------------------------------------------------------------------------------------------------------------
	// Test MigrateWrites cancelation on failure.

	// Migrate all the reads first.
	err = tme.wr.MigrateReads(ctx, MigrateShards, tme.streams, nil, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)
	checkIsMasterServing(t, tme.ts, "ks:-40", true)
	checkIsMasterServing(t, tme.ts, "ks:40-", true)
	checkIsMasterServing(t, tme.ts, "ks:-80", false)
	checkIsMasterServing(t, tme.ts, "ks:80-", false)

	// Check for journals.
	tme.dbSource1Client.addQuery("select 1 from _vt.resharding_journal where id = 8372031610433464572", &sqltypes.Result{}, nil)
	tme.dbSource2Client.addQuery("select 1 from _vt.resharding_journal where id = 8372031610433464572", &sqltypes.Result{}, nil)

	// Wait for position: Reads current state, updates to Stopped, and re-reads.
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running|",
	)
	tme.dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbDest2Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
	tme.dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest2Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 2", &sqltypes.Result{}, nil)
	stopped := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|state",
		"int64|varchar"),
		"1|Stopped",
	)
	tme.dbDest1Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbDest2Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbDest1Client.addQuery("select * from _vt.vreplication where id = 2", stopped, nil)

	// Cancel Migration
	cancel1 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 1"
	cancel2 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 2"
	tme.dbDest1Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	tme.dbDest2Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	tme.dbDest1Client.addQuery(cancel2, &sqltypes.Result{}, nil)

	err = tme.wr.MigrateWrites(ctx, MigrateShards, tme.streams, 0*time.Second)
	want = "DeadlineExceeded"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites(0 timeout) err: %v, must contain %v", err, want)
	}
	checkServedTypes(t, tme.ts, "ks:-40", 1)
	checkServedTypes(t, tme.ts, "ks:40-", 1)
	checkServedTypes(t, tme.ts, "ks:-80", 2)
	checkServedTypes(t, tme.ts, "ks:80-", 2)
	checkIsMasterServing(t, tme.ts, "ks:-40", true)
	checkIsMasterServing(t, tme.ts, "ks:40-", true)
	checkIsMasterServing(t, tme.ts, "ks:-80", false)
	checkIsMasterServing(t, tme.ts, "ks:80-", false)

	//-------------------------------------------------------------------------------------------------------------------
	// Test successful MigrateWrites.

	// Create journals.
	journal1 := "insert into _vt.resharding_journal.*8372031610433464572.*local_position.*MariaDB/5-456-892.*shard_gtids.*-80.*MariaDB/5-456-893.*participants.*40.*40"
	tme.dbSource1Client.addQueryRE(journal1, &sqltypes.Result{}, nil)
	journal2 := "insert into _vt.resharding_journal.*8372031610433464572.*local_position.*MariaDB/5-456-892.*shard_gtids.*80.*MariaDB/5-456-893.*shard_gtids.*80.*MariaDB/5-456-893.*participants.*40.*40"
	tme.dbSource2Client.addQueryRE(journal2, &sqltypes.Result{}, nil)

	// Create backward replicaions.
	tme.dbSource1Client.addQueryRE("insert into _vt.vreplication.*-80.*-40.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	tme.dbSource2Client.addQueryRE("insert into _vt.vreplication.*-80.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 1}, nil)
	tme.dbSource2Client.addQueryRE("insert into _vt.vreplication.*80-.*40-.*MariaDB/5-456-893.*Stopped", &sqltypes.Result{InsertID: 2}, nil)
	tme.dbSource1Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbSource2Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbSource2Client.addQuery("select * from _vt.vreplication where id = 2", stopped, nil)

	// Delete the target replications.
	tme.dbDest1Client.addQuery("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest2Client.addQuery("delete from _vt.vreplication where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest1Client.addQuery("delete from _vt.vreplication where id = 2", &sqltypes.Result{}, nil)

	err = tme.wr.MigrateWrites(ctx, MigrateShards, tme.streams, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	checkServedTypes(t, tme.ts, "ks:-40", 0)
	checkServedTypes(t, tme.ts, "ks:40-", 0)
	checkServedTypes(t, tme.ts, "ks:-80", 3)
	checkServedTypes(t, tme.ts, "ks:80-", 3)

	checkIsMasterServing(t, tme.ts, "ks:-40", false)
	checkIsMasterServing(t, tme.ts, "ks:40-", false)
	checkIsMasterServing(t, tme.ts, "ks:-80", true)
	checkIsMasterServing(t, tme.ts, "ks:80-", true)

	verifyQueries(t, tme.allDBClients)
}

// TestMigrateFailJournal tests that cancel doesn't get called after point of no return.
func TestMigrateFailJournal(t *testing.T) {
	ctx := context.Background()
	tme := newTestTableMigrater(ctx, t)
	defer tme.stopTablets(t)

	err := tme.wr.MigrateReads(ctx, MigrateTables, tme.streams, nil, topodatapb.TabletType_RDONLY, directionForward)
	if err != nil {
		t.Fatal(err)
	}
	err = tme.wr.MigrateReads(ctx, MigrateTables, tme.streams, nil, topodatapb.TabletType_REPLICA, directionForward)
	if err != nil {
		t.Fatal(err)
	}

	// Check for journals.
	tme.dbSource1Client.addQuery("select 1 from _vt.resharding_journal where id = 445516443381867838", &sqltypes.Result{}, nil)
	tme.dbSource2Client.addQuery("select 1 from _vt.resharding_journal where id = 445516443381867838", &sqltypes.Result{}, nil)

	// Wait for position: Reads current state, updates to Stopped, and re-reads.
	state := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"pos|state|message",
		"varchar|varchar|varchar"),
		"MariaDB/5-456-892|Running|",
	)
	tme.dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbDest2Client.addQuery("select pos, state, message from _vt.vreplication where id=1", state, nil)
	tme.dbDest1Client.addQuery("select pos, state, message from _vt.vreplication where id=2", state, nil)
	tme.dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest2Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 1", &sqltypes.Result{}, nil)
	tme.dbDest1Client.addQuery("update _vt.vreplication set state = 'Stopped', message = 'stopped for cutover' where id = 2", &sqltypes.Result{}, nil)
	stopped := sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|state",
		"int64|varchar"),
		"1|Stopped",
	)
	tme.dbDest1Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbDest2Client.addQuery("select * from _vt.vreplication where id = 1", stopped, nil)
	tme.dbDest1Client.addQuery("select * from _vt.vreplication where id = 2", stopped, nil)

	// Cancel Migration: these must not get called.
	cancel1 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 1"
	cancel2 := "update _vt.vreplication set state = 'Running', stop_pos = null where id = 2"
	tme.dbDest1Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	tme.dbDest2Client.addQuery(cancel1, &sqltypes.Result{}, nil)
	tme.dbDest1Client.addQuery(cancel2, &sqltypes.Result{}, nil)

	// Make the journal call fail.
	tme.dbSource1Client.addQueryRE("insert into _vt.resharding_journal", nil, errors.New("journaling intentionally failed"))
	tme.dbSource2Client.addQueryRE("insert into _vt.resharding_journal", nil, errors.New("journaling intentionally failed"))

	err = tme.wr.MigrateWrites(ctx, MigrateTables, tme.streams, 1*time.Second)
	want := "journaling intentionally failed"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("MigrateWrites(0 timeout) err: %v, must contain %v", err, want)
	}

	// Verify that cancel didn't happen.
	if tme.dbDest1Client.queries[cancel1].called {
		t.Errorf("tme.dbDest1Client.queries[cancel1].called: %v, want false", tme.dbDest1Client.queries[cancel1])
	}
	if tme.dbDest2Client.queries[cancel1].called {
		t.Errorf("tme.dbDest1Client.queries[cancel1].called: %v, want false", tme.dbDest1Client.queries[cancel1])
	}
	if tme.dbDest1Client.queries[cancel2].called {
		t.Errorf("tme.dbDest1Client.queries[cancel1].called: %v, want false", tme.dbDest1Client.queries[cancel1])
	}
}

func checkRouting(t *testing.T, wr *Wrangler, want map[string][]string) {
	t.Helper()
	ctx := context.Background()
	got, err := wr.getRoutingRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("rules:\n%v, want\n%v", got, want)
	}
	cells, err := wr.ts.GetCellInfoNames(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, cell := range cells {
		checkCellRouting(t, wr, cell, want)
	}
}

func checkCellRouting(t *testing.T, wr *Wrangler, cell string, want map[string][]string) {
	t.Helper()
	ctx := context.Background()
	svs, err := wr.ts.GetSrvVSchema(ctx, cell)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string][]string)
	for _, rr := range svs.RoutingRules.Rules {
		got[rr.FromTable] = append(got[rr.FromTable], rr.ToTables...)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("srv rules for cell %s:\n%v, want\n%v", cell, got, want)
	}
}

func checkBlacklist(t *testing.T, ts *topo.Server, keyspaceShard string, want []string) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	si, err := ts.GetShard(ctx, splits[0], splits[1])
	if err != nil {
		t.Fatal(err)
	}
	tc := si.GetTabletControl(topodatapb.TabletType_MASTER)
	var got []string
	if tc != nil {
		got = tc.BlacklistedTables
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Blacklisted tables for %v: %v, want %v", keyspaceShard, got, want)
	}
}

func checkServedTypes(t *testing.T, ts *topo.Server, keyspaceShard string, want int) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	si, err := ts.GetShard(ctx, splits[0], splits[1])
	if err != nil {
		t.Fatal(err)
	}

	servedTypes, err := ts.GetShardServingTypes(ctx, si)
	if err != nil {
		t.Fatal(err)
	}

	if len(servedTypes) != want {
		t.Errorf("shard %v has wrong served types: got: %v, want: %v", keyspaceShard, len(servedTypes), want)
	}
}

func checkCellServedTypes(t *testing.T, ts *topo.Server, keyspaceShard, cell string, want int) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	srvKeyspace, err := ts.GetSrvKeyspace(ctx, cell, splits[0])
	if err != nil {
		t.Fatal(err)
	}
	count := 0
outer:
	for _, partition := range srvKeyspace.GetPartitions() {
		for _, ref := range partition.ShardReferences {
			if ref.Name == splits[1] {
				count++
				continue outer
			}
		}
	}
	if count != want {
		t.Errorf("serving types for keyspaceShard %s, cell %s: %d, want %d", keyspaceShard, cell, count, want)
	}
}

func checkIsMasterServing(t *testing.T, ts *topo.Server, keyspaceShard string, want bool) {
	t.Helper()
	ctx := context.Background()
	splits := strings.Split(keyspaceShard, ":")
	si, err := ts.GetShard(ctx, splits[0], splits[1])
	if err != nil {
		t.Fatal(err)
	}
	if want != si.IsMasterServing {
		t.Errorf("IsMasterServing(%v): %v, want %v", keyspaceShard, si.IsMasterServing, want)
	}
}
