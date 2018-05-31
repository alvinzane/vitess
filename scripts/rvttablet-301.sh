#!/bin/bash

#MYSQL_HOST_PREFIX="ar8000r" UNIQUE_ID=301 SHARD=80- TABLET_TYPE=rdonly SERVER_ID=62633911 /home/planetscale/dev/scripts/vttablet-gen.sh $1
ssh ec2-18-220-166-48.us-east-2.compute.amazonaws.com /home/planetscale/dev/scripts/vttablet-301.sh $1
