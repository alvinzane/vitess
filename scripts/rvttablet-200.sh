#!/bin/bash

#MYSQL_HOST_PREFIX="ar0080m" UNIQUE_ID=200 SHARD=-80 TABLET_TYPE=replica SERVER_ID=746136662 /home/planetscale/dev/scripts/vttablet-gen.sh $1
ssh ec2-18-222-93-61.us-east-2.compute.amazonaws.com /home/planetscale/dev/scripts/vttablet-200.sh $1
