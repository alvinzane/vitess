#!/bin/bash

#MYSQL_HOST_PREFIX="ar0080r" UNIQUE_ID=201 SHARD=-80 TABLET_TYPE=rdonly SERVER_ID=845533510 /home/planetscale/dev/scripts/vttablet-gen.sh $1
ssh ec2-18-222-93-61.us-east-2.compute.amazonaws.com /home/planetscale/dev/scripts/vttablet-201.sh $1
