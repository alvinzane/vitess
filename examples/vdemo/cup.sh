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

# This is an example script that creates a single shard vttablet deployment.

set -e

./zk-up.sh
./vtctld-up.sh
SHARD=0 UID_BASE=100 KEYSPACE=lookup ./vttablet-up.sh "$@" &
SHARD=-80 UID_BASE=200 KEYSPACE=user ./vttablet-up.sh "$@" &
SHARD=80- UID_BASE=300 KEYSPACE=user ./vttablet-up.sh "$@" &
SHARD=-80 UID_BASE=400 KEYSPACE=merchant ./vttablet-up.sh "$@" &
SHARD=80- UID_BASE=500 KEYSPACE=merchant ./vttablet-up.sh "$@" &
SHARD=-80 UID_BASE=600 KEYSPACE=event ./vttablet-up.sh "$@" &
SHARD=80- UID_BASE=700 KEYSPACE=event ./vttablet-up.sh "$@" &
SHARD=-80 UID_BASE=800 KEYSPACE=rollup ./vttablet-up.sh "$@" &
SHARD=80- UID_BASE=900 KEYSPACE=rollup ./vttablet-up.sh "$@" &
wait

sleep 10s
./lvtctl.sh InitShardMaster -force lookup/0 test-100 &
./lvtctl.sh InitShardMaster -force user/-80 test-200 &
./lvtctl.sh InitShardMaster -force user/80- test-300 &
./lvtctl.sh InitShardMaster -force merchant/-80 test-400 &
./lvtctl.sh InitShardMaster -force merchant/80- test-500 &
./lvtctl.sh InitShardMaster -force event/-80 test-600 &
./lvtctl.sh InitShardMaster -force event/80- test-700 &
./lvtctl.sh InitShardMaster -force rollup/-80 test-800 &
./lvtctl.sh InitShardMaster -force rollup/80- test-900 &
wait

./lvtctl.sh ApplySchema -sql "$(cat lookup.sql)" lookup
./lvtctl.sh ApplyVSchema -vschema "$(cat lookup.json)" lookup
./lvtctl.sh ApplySchema -sql "$(cat user.sql)" user
./lvtctl.sh ApplyVSchema -vschema "$(cat user.json)" user
./lvtctl.sh ApplySchema -sql "$(cat merchant.sql)" merchant
./lvtctl.sh ApplyVSchema -vschema "$(cat merchant.json)" merchant
./lvtctl.sh ApplySchema -sql "$(cat event.sql)" event
./lvtctl.sh ApplyVSchema -vschema "$(cat event.json)" event
./lvtctl.sh ApplySchema -sql "$(cat rollup.sql)" rollup
./lvtctl.sh ApplyVSchema -vschema "$(cat rollup.json)" rollup
./vtgate-up.sh

sleep 5s
rm positions.py
./get_positions.py >positions.py
cat positions.py
