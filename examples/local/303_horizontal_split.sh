#!/bin/bash

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

# this script copies the data from customer/0 to customer/-80 and customer/80-
# each row will be copied to exactly one shard based on the vindex value

set -e

# shellcheck disable=SC2128
script_root=$(dirname "${BASH_SOURCE}")

# shellcheck disable=SC1091
source "${script_root}/env.sh"

./lvtctl.sh VReplicationExec zone1-300 'insert into _vt.vreplication (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('"'"'vt_customer'"'"', '"'"'keyspace:\"commerce\" shard:\"0\" filter:<rules:<match:\"customer\" filter:\"select * from customer where in_keyrange(customer_id, \'"'"'hash\'"'"', \'"'"'-80\'"'"')\" > > '"'"', '"'"''"'"', 9999, 9999, '"'"'master'"'"', 0, 0, '"'"'Running'"'"')'
./lvtctl.sh VReplicationExec zone1-400 'insert into _vt.vreplication (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('"'"'vt_customer'"'"', '"'"'keyspace:\"commerce\" shard:\"0\" filter:<rules:<match:\"customer\" filter:\"select * from customer where in_keyrange(customer_id, \'"'"'hash\'"'"', \'"'"'80-\'"'"')\" > > '"'"', '"'"''"'"', 9999, 9999, '"'"'master'"'"', 0, 0, '"'"'Running'"'"')'
