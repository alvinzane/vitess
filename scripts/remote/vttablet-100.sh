#!/bin/bash

MYSQL_HOST_PREFIX="ar0m" UNIQUE_ID=100 SHARD=0 TABLET_TYPE=replica SERVER_ID=316348461 /home/planetscale/dev/scripts/vttablet-gen.sh $1
