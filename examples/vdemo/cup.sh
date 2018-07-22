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
SHARD=-80 UID_BASE=400 KEYSPACE=nuser ./vttablet-up.sh "$@" &
SHARD=80- UID_BASE=500 KEYSPACE=nuser ./vttablet-up.sh "$@" &
wait

sleep 10s
./lvtctl.sh InitShardMaster -force lookup/0 test-100 &
./lvtctl.sh InitShardMaster -force user/-80 test-200 &
./lvtctl.sh InitShardMaster -force user/80- test-300 &
./lvtctl.sh InitShardMaster -force nuser/-80 test-400 &
./lvtctl.sh InitShardMaster -force nuser/80- test-500 &
wait

./lvtctl.sh ApplySchema -sql "$(cat lookup.sql)" lookup
./lvtctl.sh ApplyVSchema -vschema "$(cat lookup.json)" lookup
./lvtctl.sh ApplySchema -sql "$(cat user.sql)" user
./lvtctl.sh ApplyVSchema -vschema "$(cat user.json)" user
./lvtctl.sh ApplySchema -sql "$(cat nuser.sql)" nuser
./lvtctl.sh ApplyVSchema -vschema "$(cat nuser.json)" nuser
./vtgate-up.sh
