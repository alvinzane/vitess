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

SHARD=0 UID_BASE=100 KEYSPACE=lookup ./tablet-up.sh "$@" &
SHARD=-80 UID_BASE=200 KEYSPACE=user ./tablet-up.sh "$@" &
SHARD=80- UID_BASE=300 KEYSPACE=user ./tablet-up.sh "$@" &
SHARD=-80 UID_BASE=400 KEYSPACE=nuser ./tablet-up.sh "$@" &
SHARD=80- UID_BASE=500 KEYSPACE=nuser ./tablet-up.sh "$@" &
