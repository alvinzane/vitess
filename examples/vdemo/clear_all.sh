#!/bin/bash

# Copyright 2018 The Vitess Authors.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

VTDATAROOT=/home/sougou/dev/demoroot

mysql -S $VTDATAROOT/vt_0000000100/mysql.sock -u vt_dba -e 'delete from vt_lookup.name_user_idx'
mysql -S $VTDATAROOT/vt_0000000100/mysql.sock -u vt_dba -e 'delete from vt_lookup.rates'
mysql -S $VTDATAROOT/vt_0000000200/mysql.sock -u vt_dba -e 'delete from vt_user.user'
mysql -S $VTDATAROOT/vt_0000000200/mysql.sock -u vt_dba -e 'delete from vt_user.rates'
mysql -S $VTDATAROOT/vt_0000000300/mysql.sock -u vt_dba -e 'delete from vt_user.user'
mysql -S $VTDATAROOT/vt_0000000300/mysql.sock -u vt_dba -e 'delete from vt_user.rates'
mysql -S $VTDATAROOT/vt_0000000400/mysql.sock -u vt_dba -e 'delete from vt_nuser.user'
mysql -S $VTDATAROOT/vt_0000000500/mysql.sock -u vt_dba -e 'delete from vt_nuser.user'
