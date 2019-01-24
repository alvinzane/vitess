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

import MySQLdb as db

import positions

cmd = [
  './lvtctl.sh',
  'VReplicationExec',
  'test-100',
  """insert into _vt.vreplication
  (source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values
  ('keyspace:"user" shard:"-80" filter:<table_map:<key:"sales" value:"select pid, sum(price) as amount from uorder" > >', 'MySQL56/%s', 9999, 9999, 'master', 0, 0, 'Running')""" % positions.positions[200],
  ]

print "executing:", cmd
subprocess.call(cmd)

cmd = [
  './lvtctl.sh',
  'VReplicationExec',
  'test-100',
  """insert into _vt.vreplication
  (source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values
  ('keyspace:"user" shard:"80-" filter:<table_map:<key:"sales" value:"select pid, sum(price) as amount from uorder" > >', 'MySQL56/%s', 9999, 9999, 'master', 0, 0, 'Running')""" % positions.positions[300],
  ]

print "executing:", cmd
subprocess.call(cmd)
