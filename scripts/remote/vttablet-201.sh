#!/bin/bash

MYSQL_HOST_PREFIX="ar0080r" UNIQUE_ID=201 SHARD=-80 TABLET_TYPE=rdonly SERVER_ID=845533510 /home/planetscale/dev/scripts/vttablet-gen.sh $1
