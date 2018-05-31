#!/bin/bash

MYSQL_HOST_PREFIX="ar0r" UNIQUE_ID=101 SHARD=0 TABLET_TYPE=rdonly SERVER_ID=1054440066 /home/planetscale/dev/scripts/vttablet-gen.sh $1
