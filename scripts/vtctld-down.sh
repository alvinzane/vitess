
#!/bin/bash
set -e

export VTROOT=/home/planetscale/dev
export VTDATAROOT=/home/planetscale/dev/vtdataroot

HOSTNAME="ec2-18-221-242-40.us-east-2.compute.amazonaws.com"
TOPOLOGY_FLAGS="-topo_implementation zk2 -topo_global_server_address ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21811,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21812,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21813 -topo_global_root /vitess/global"
CELL="cell1"
GRPC_PORT=15999
WEB_PORT=15000
MYSQL_AUTH_PARAM=""

# This script stops vtctld.

set -e

pid=`cat $VTDATAROOT/tmp/vtctld.pid`
echo "Stopping vtctld..."
kill $pid
