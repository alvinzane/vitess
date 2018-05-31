#!/bin/bash

# ar0m
aws rds create-db-cluster --availability-zone=us-east-2b --database-name=sb --db-cluster-identifier=ar0mcluster --db-cluster-parameter-group-name=vta57 --engine=aurora-mysql --master-username=vtuser --master-user-password=vtpassword --no-storage-encrypted
sleep 120
aws rds create-db-instance --db-instance-identifier=ar0m --db-instance-class=db.r4.large --engine=aurora-mysql --availability-zone=us-east-2b --no-publicly-accessible --db-cluster-identifier=ar0mcluster
aws rds wait db-instance-available --db-instance-identifier=ar0m
echo "set the security group for ar0m to vtdata to make it accessible"

# ar0r
aws rds restore-db-cluster-to-point-in-time --db-cluster-identifier=ar0rcluster --source-db-cluster-identifier=ar0mcluster --use-latest-restorable-time
sleep 120
aws rds create-db-instance --db-instance-identifier=ar0r --db-instance-class=db.r4.large --engine=aurora-mysql --availability-zone=us-east-2b --no-publicly-accessible --db-cluster-identifier=ar0rcluster
aws rds wait db-instance-available --db-instance-identifier=ar0r
echo "set the security group for ar0m to vtdata to make it accessible"
echo "run show binary logs on ar0m, and point ar0r at the master's latest log position: call mysql.rds_set_external_master('ar0m.cdvais0yw4do.us-east-2.rds.amazonaws.com', 3306, 'vtuser', 'vtpassword', 'mysql-bin-changelog.xxxx', xxxx, 0);"
echo "then: call mysql.rds_start_replication;"
