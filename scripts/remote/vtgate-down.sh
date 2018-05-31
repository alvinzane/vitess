
#!/bin/bash
set -e

# This is an example script that starts a single vtgate.

export VTROOT=/home/planetscale/dev
export VTDATAROOT=/home/planetscale/dev/vtdataroot

HOSTNAME="ec2-18-221-242-40.us-east-2.compute.amazonaws.com"
TOPOLOGY_FLAGS="-topo_implementation zk2 -topo_global_server_address ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21811,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21812,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21813 -topo_global_root /vitess/global"
CELL="cell1"
GRPC_PORT=15991
WEB_PORT=15001
MYSQL_SERVER_PORT=15306
MYSQL_AUTH_PARAM=""

# This script stops vtgate.

set -e

pid=`cat $VTDATAROOT/tmp/vtgate.pid`
echo "Stopping vtgate..."
kill $pid
