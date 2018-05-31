
#!/bin/bash
set -e

# This is an example script that starts a single vtgate.

export VTROOT=/home/planetscale/dev
export VTDATAROOT=/home/planetscale/dev/vtdataroot

HOSTNAME=$(curl -s http://169.254.169.254/latest/meta-data/public-hostname)
TOPOLOGY_FLAGS="-topo_implementation zk2 -topo_global_server_address ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21811,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21812,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21813 -topo_global_root /vitess/global"
CELL="cell1"
GRPC_PORT=15991
WEB_PORT=15001
MYSQL_SERVER_PORT=15306
MYSQL_AUTH_PARAM=""

mkdir -p $VTDATAROOT/tmp
mkdir -p $VTDATAROOT/backups

# Start vtgate.
$VTROOT/bin/vtgate \
  $TOPOLOGY_FLAGS \
  -log_dir $VTDATAROOT/tmp \
  -port ${WEB_PORT} \
  -grpc_port ${GRPC_PORT} \
  -mysql_server_port ${MYSQL_SERVER_PORT} \
  -mysql_auth_server_static_string '{"mysql_user":{"Password":"mysql_password"}}' \
  -cell ${CELL} \
  -cells_to_watch ${CELL} \
  -tablet_types_to_wait MASTER,REPLICA \
  -gateway_implementation discoverygateway \
  -service_map 'grpc-vtgateservice' \
  -pid_file $VTDATAROOT/tmp/vtgate.pid \
  ${MYSQL_AUTH_PARAM} \
  > $VTDATAROOT/tmp/vtgate.out 2>&1 &

echo "Access vtgate at http://${HOSTNAME}:${WEB_PORT}/debug/status"
echo Note: vtgate writes logs under $VTDATAROOT/tmp.

disown -a
