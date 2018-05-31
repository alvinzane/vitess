#!/bin/bash

#MYSQL_HOST_PREFIX="a0r" UNIQUE_ID=101 SHARD=0 TABLET_TYPE=rdonly SERVER_ID=1054440066 /home/planetscale/dev/scripts/vttablet-gen.sh $1
ssh ip-172-31-21-206 /home/planetscale/dev/scripts/vttablet-101.sh $1
