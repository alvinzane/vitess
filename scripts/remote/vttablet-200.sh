#!/bin/bash

MYSQL_HOST_PREFIX="ar0080m" UNIQUE_ID=200 SHARD=-80 TABLET_TYPE=replica SERVER_ID=746136662 /home/planetscale/dev/scripts/vttablet-gen.sh $1
