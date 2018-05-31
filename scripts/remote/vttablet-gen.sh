#!/bin/bash

MYSQL_HOST_SUFFIX="cdvais0yw4do.us-east-2.rds.amazonaws.com"
MYSQL_HOST=${MYSQL_HOST_PREFIX}.${MYSQL_HOST_SUFFIX}

export VTDATAROOT=/home/planetscale/dev/vtdataroot
export VTROOT=/home/planetscale/dev
export VTTOP=/home/planetscale/dev/src/vitess.io/vitess
export VT_MYSQL_ROOT=/usr
export MYSQL_FLAVOR=MySQL56

DBNAME=sb
KEYSPACE=sb
TOPOLOGY_FLAGS="-topo_implementation zk2 -topo_global_server_address ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21811,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21812,ec2-18-221-242-40.us-east-2.compute.amazonaws.com:21813 -topo_global_root /vitess/global"
DBCONFIG_DBA_FLAGS="-db-config-dba-uname vtuser -db-config-dba-pass vtpassword -db-config-dba-charset utf8"
DBCONFIG_FLAGS="-db-config-dba-uname vtuser -db-config-dba-pass vtpassword -db-config-dba-charset utf8 -db-config-allprivs-uname vtuser -db-config-allprivs-pass vtpassword -db-config-allprivs-charset utf8 -db-config-app-uname vtuser -db-config-app-pass vtpassword -db-config-app-charset utf8 -db-config-repl-uname vtuser -db-config-repl-pass vtpassword -db-config-repl-charset utf8 -db-config-filtered-uname vtuser -db-config-filtered-pass vtpassword -db-config-filtered-charset utf8"
DBCONFIG_HOST_FLAGS="-db-config-app-host ${MYSQL_HOST} -db-config-app-port 3306 -db-config-repl-host ${MYSQL_HOST} -db-config-repl-port 3306 -db-config-dba-host ${MYSQL_HOST} -db-config-dba-port 3306 -db-config-allprivs-host ${MYSQL_HOST} -db-config-allprivs-port 3306 -db-config-filtered-host ${MYSQL_HOST} -db-config-filtered-port 3306"
VTCTLD_HOST=ec2-18-221-242-40.us-east-2.compute.amazonaws.com
VTCTLD_WEB_PORT=15000
HOSTNAME=$(curl -s http://169.254.169.254/latest/meta-data/public-hostname)

TABLET_DIR=vt_0000000${UNIQUE_ID}
WEB_PORT=15${UNIQUE_ID}
GRPC_PORT=16${UNIQUE_ID}
ALIAS=cell1-0000000${UNIQUE_ID}

EXTRA_PARAMS="-disable_active_reparents -restore_from_backup=false -enforce_strict_trans_tables=false -binlog_player_tablet_type=master -queryserver-config-stream-pool-size=20"

export LD_LIBRARY_PATH=${VTROOT}/dist/grpc/usr/local/lib
export PATH=${VTROOT}/bin:${VTROOT}/.local/bin:${VTROOT}/dist/chromedriver:${VTROOT}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/usr/local/go/bin:/usr/local/mysql/bin

mkdir -p ${VTDATAROOT}/tmp
mkdir -p ${VTDATAROOT}/backups
mkdir -p ${VTDATAROOT}/${TABLET_DIR}

if [ "$1" = "start" ]; then
  echo "Starting vttablet for $ALIAS..."

  MYSQL_FLAVOR=rds $VTROOT/bin/vttablet \
    $TOPOLOGY_FLAGS \
    -log_dir $VTDATAROOT/tmp \
    -tablet-path $ALIAS \
    -tablet_hostname "$HOSTNAME" \
    -init_keyspace $KEYSPACE \
    -init_shard $SHARD \
    -init_tablet_type $TABLET_TYPE \
    -init_db_name_override $DBNAME \
    -mycnf_server_id ${SERVER_ID} \
    -enable_replication_reporter \
    -health_check_interval 5s \
    -binlog_use_v3_resharding_mode \
    -port $WEB_PORT \
    -grpc_port $GRPC_PORT \
    -service_map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
    -pid_file $VTDATAROOT/$TABLET_DIR/vttablet.pid \
    -vtctld_addr http://${VTCTLD_HOST}:${VTCTLD_WEB_PORT}/ \
    $DBCONFIG_FLAGS \
    $DBCONFIG_HOST_FLAGS \
    ${EXTRA_PARAMS}\
    > $VTDATAROOT/$TABLET_DIR/vttablet.out 2>&1 &

  echo "Access tablet $ALIAS at http://$HOSTNAME:$WEB_PORT/debug/status"
  exit 0
fi

if [ "$1" = "stop" ]; then
  echo "Stopping vttablet for $ALIAS..."
  pid=`cat $VTDATAROOT/$TABLET_DIR/vttablet.pid`
  kill $pid

  while ps -p $pid > /dev/null; do sleep 1; done
  exit 0
fi

echo "Please specify start or stop"
exit 1
