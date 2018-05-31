#!/bin/bash

MYSQL_HOST_PREFIX="ar8000r" UNIQUE_ID=301 SHARD=80- TABLET_TYPE=rdonly SERVER_ID=62633911 /home/planetscale/dev/scripts/vttablet-gen.sh $1
