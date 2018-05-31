#!/bin/bash

#MYSQL_HOST_PREFIX="ar8000m" UNIQUE_ID=300 SHARD=80- TABLET_TYPE=replica SERVER_ID=1409043767 /home/planetscale/dev/scripts/vttablet-gen.sh $1
ssh ec2-18-220-166-48.us-east-2.compute.amazonaws.com /home/planetscale/dev/scripts/vttablet-300.sh $1
