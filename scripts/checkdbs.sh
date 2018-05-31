#!/bin/bash

./lmysql.sh ar0m 'select count(*) from sbtest1'
./lmysql.sh ar0080m 'select count(*) from sbtest1'
./lmysql.sh ar8000m 'select count(*) from sbtest1'
