#!/bin/bash

./zk-up-instance-001.sh
./zk-up-instance-002.sh
./zk-up-instance-003.sh

# Copied from vitess-deployment
ZK_CONFIG="1@ec2-18-221-242-40.us-east-2.compute.amazonaws.com:28881:38881:21811,2@ec2-18-221-242-40.us-east-2.compute.amazonaws.com:28882:38882:21812,3@ec2-18-221-242-40.us-east-2.compute.amazonaws.com:28883:38883:21813"
ZK_SERVER="ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21811,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21812,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21813"
TOPOLOGY_FLAGS="-topo_implementation zk2 -topo_global_server_address ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21811,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21812,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21813 -topo_global_root /vitess/global"
CELL="cell1"

# Create /vitess/global and /vitess/CELLNAME paths if they do not exist.
/home/planetscale/dev/bin/zk -server ${ZK_SERVER} touch -p /vitess/global
/home/planetscale/dev/bin/zk -server ${ZK_SERVER} touch -p /vitess/${CELL}

# Initialize cell.
/home/planetscale/dev/bin/vtctl ${TOPOLOGY_FLAGS} AddCellInfo -root /vitess/${CELL} -server_address ${ZK_SERVER} ${CELL}
