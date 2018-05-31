#!/bin/bash

./lmysql.sh ar0r 'show slave status\G' | grep Master
./lmysql.sh ar0080m 'select * from _vt.blp_checkpoint'
./lmysql.sh ar8000m 'select * from _vt.blp_checkpoint'
