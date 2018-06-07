
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


echo "Starting vtctld..."

mkdir -p $VTDATAROOT/backups
mkdir -p $VTDATAROOT/tmp

${VTROOT}/bin/vtctld \
  ${TOPOLOGY_FLAGS} \
  -cell ${CELL} \
  -web_dir ${VTTOP}/web/vtctld \
  -web_dir2 ${VTTOP}/web/vtctld2/app \
  -workflow_manager_init \
  -workflow_manager_use_election \
  -service_map 'grpc-vtctl' \
  -backup_storage_implementation file \
  -file_backup_storage_root ${VTDATAROOT}/backups \
  -log_dir ${VTDATAROOT}/tmp \
  -port ${WEB_PORT} \
  -grpc_port ${GRPC_PORT} \
  -pid_file ${VTDATAROOT}/tmp/vtctld.pid \
  ${MYSQL_AUTH_PARAM} \
  > ${VTDATAROOT}/tmp/vtctld.out 2>&1 &

disown -a


echo "Access vtctld web UI at http://${HOSTNAME}:${WEB_PORT}"
echo "Send commands with: vtctlclient -server ${HOSTNAME}:${GRPC_PORT} ..."
echo Note: vtctld writes logs under $VTDATAROOT/tmp.
