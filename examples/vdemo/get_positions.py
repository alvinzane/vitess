#!/usr/bin/env python

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

# vim: tabstop=8 expandtab shiftwidth=2 softtabstop=2

import subprocess

import MySQLdb as db

def master_pos(id):
  conn = db.connect(unix_socket='/home/sougou/dev/demoroot/vt_0000000%d/mysql.sock' % id, user='vt_dba')
  cursor = conn.cursor()
  cursor.execute('show master status')
  position = cursor.fetchone()[4]
  conn.close()
  return position

print "positions =", { 100: master_pos(100), 200: master_pos(200), 300: master_pos(300) }
