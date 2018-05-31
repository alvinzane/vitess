#!/bin/bash

#MYSQL_HOST_PREFIX="a0m" UNIQUE_ID=100 SHARD=0 TABLET_TYPE=replica SERVER_ID=316348461 /home/planetscale/dev/scripts/vttablet-gen.sh $1
ssh ip-172-31-21-206 /home/planetscale/dev/scripts/vttablet-100.sh $1
