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

source = """keyspace:"event" shard:"-80" filter:<table_map:<key:"by_tenant" value:"select tenant_id, user_id, app, hour(event_time) as mon, sum(spent) as spent, count(*) as rcount from event where in_keyrange(tenant_id, \\'hash\\', \\'-80\\')" > >"""
cmd = [
  './lvtctl.sh',
  'VReplicationExec',
  'test-800',
  """insert into _vt.vreplication
  (source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values
  ('%s', 'MySQL56/%s', 9999, 9999, 'master', 0, 0, 'Running')""" % (source, positions.positions[200]),
  ]

print "executing:", cmd
subprocess.call(cmd)

source = """keyspace:"event" shard:"80-" filter:<table_map:<key:"by_tenant" value:"select tenant_id, user_id, app, hour(event_time) as mon, sum(spent) as spent, count(*) as rcount from event where in_keyrange(tenant_id, \\'hash\\', \\'-80\\')" > >"""
cmd = [
  './lvtctl.sh',
  'VReplicationExec',
  'test-800',
  """insert into _vt.vreplication
  (source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values
  ('%s', 'MySQL56/%s', 9999, 9999, 'master', 0, 0, 'Running')""" % (source, positions.positions[300]),
  ]

print "executing:", cmd
subprocess.call(cmd)

source = """keyspace:"event" shard:"-80" filter:<table_map:<key:"by_tenant" value:"select tenant_id, user_id, app, hour(event_time) as mon, sum(spent) as spent, count(*) as rcount from event where in_keyrange(tenant_id, \\'hash\\', \\'80-\\')" > >"""
cmd = [
  './lvtctl.sh',
  'VReplicationExec',
  'test-900',
  """insert into _vt.vreplication
  (source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values
  ('%s', 'MySQL56/%s', 9999, 9999, 'master', 0, 0, 'Running')""" % (source, positions.positions[200]),
  ]

print "executing:", cmd
subprocess.call(cmd)

source = """keyspace:"event" shard:"80-" filter:<table_map:<key:"by_tenant" value:"select tenant_id, user_id, app, hour(event_time) as mon, sum(spent) as spent, count(*) as rcount from event where in_keyrange(tenant_id, \\'hash\\', \\'80-\\')" > >"""
cmd = [
  './lvtctl.sh',
  'VReplicationExec',
  'test-900',
  """insert into _vt.vreplication
  (source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values
  ('%s', 'MySQL56/%s', 9999, 9999, 'master', 0, 0, 'Running')""" % (source, positions.positions[300]),
  ]

print "executing:", cmd
subprocess.call(cmd)
