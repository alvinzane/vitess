#!/bin/bash

sysbench\
  --tables=1\
  --mysql-db=sb\
  --mysql-user=mysql_user\
  --mysql-password=mysql_password\
  --mysql-host=ec2-13-58-62-116.us-east-2.compute.amazonaws.com\
  --mysql-port=15306\
  --table_size=100000\
  --auto_inc=off\
  --db-ps-mode=disable\
  --db-driver=mysql\
  oltp_read_write prepare
