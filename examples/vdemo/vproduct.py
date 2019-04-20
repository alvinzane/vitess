#!/usr/bin/env python

# Copyright 2018 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# vim: tabstop=8 expandtab shiftwidth=2 softtabstop=2

import subprocess

dbname = "vt_user"

cmd = [
  './lvtctl.sh',
  'VReplicationExec',
  'test-200',
  """insert into _vt.vreplication
  (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values
  ('%s', 'keyspace:"lookup" shard:"0" filter:<rules:<match:"uproduct" filter:"select * from product" > >', '', 9999, 9999, 'master', 0, 0, 'Running')""" % (dbname),
  ]

print "executing:", cmd
subprocess.call(cmd)

cmd = [
  './lvtctl.sh',
  'VReplicationExec',
  'test-300',
  """insert into _vt.vreplication
  (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values
  ('%s','keyspace:"lookup" shard:"0" filter:<rules:<match:"uproduct" filter:"select * from product" > >', '', 9999, 9999, 'master', 0, 0, 'Running')""" % (dbname),
  ]

print "executing:", cmd
subprocess.call(cmd)

cmd = [
  './lvtctl.sh',
  'ApplyRoutingRules',
  """-rules={"rules": [{"from_table": "product","to_tables": ["lookup.product", "user.uproduct"]}]}""",
  ]

print "executing:", cmd
subprocess.call(cmd)
