#!/bin/bash
# Generated file, edit at your own risk.

export VTROOT=/home/planetscale/dev
export VTDATAROOT=/home/planetscale/dev/vtdataroot
ZK_ID=2
ZK_DIR=zk_002
ZK_CONFIG=1@ec2-18-221-242-40.us-east-2.compute.amazonaws.com:28881:38881:21811,2@ec2-18-221-242-40.us-east-2.compute.amazonaws.com:28882:38882:21812,3@ec2-18-221-242-40.us-east-2.compute.amazonaws.com:28883:38883:21813


# Variables used below would be assigned values above this line

mkdir -p $VTDATAROOT/tmp

action='shutdown'

$VTROOT/bin/zkctl -zk.myid $ZK_ID -zk.cfg $ZK_CONFIG -log_dir $VTDATAROOT/tmp $action \
		  > $VTDATAROOT/tmp/zkctl_$ZK_ID.out 2>&1 &

if ! wait $!; then
    echo "ZK server number $ZK_ID failed to stop. See log:"
    echo "    $VTDATAROOT/tmp/zkctl_$ZK_ID.out"
else
    echo "Stopped zk server $ZK_ID"
fi
