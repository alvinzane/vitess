#!/bin/bash

# ar8000m
aws rds create-db-cluster --availability-zone=us-east-2b --database-name=sb --db-cluster-identifier=ar8000mcluster --db-cluster-parameter-group-name=vta57 --engine=aurora-mysql --master-username=vtuser --master-user-password=vtpassword --no-storage-encrypted
sleep 120
aws rds create-db-instance --db-instance-identifier=ar8000m --db-instance-class=db.r4.large --engine=aurora-mysql --availability-zone=us-east-2b --no-publicly-accessible --db-cluster-identifier=ar8000mcluster
aws rds wait db-instance-available --db-instance-identifier=ar8000m
echo "set the security group for ar8000m to vtdata to make it accessible"

# ar8000r
aws rds restore-db-cluster-to-point-in-time --db-cluster-identifier=ar8000rcluster --source-db-cluster-identifier=ar8000mcluster --use-latest-restorable-time
sleep 120
aws rds create-db-instance --db-instance-identifier=ar8000r --db-instance-class=db.r4.large --engine=aurora-mysql --availability-zone=us-east-2b --no-publicly-accessible --db-cluster-identifier=ar8000rcluster
aws rds wait db-instance-available --db-instance-identifier=ar8000r
echo "set the security group for ar8000m to vtdata to make it accessible"
echo "run show binary logs on ar8000m, and point ar8000r at the master's latest log position: call mysql.rds_set_external_master('ar8000m.cdvais0yw4do.us-east-2.rds.amazonaws.com', 3306, 'vtuser', 'vtpassword', 'mysql-bin-changelog.xxxx', xxxx, 0);"
echo "then: call mysql.rds_start_replication;"
