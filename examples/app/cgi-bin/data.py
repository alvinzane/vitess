#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
"""This module allows you to bring up and tear down keyspaces."""

import cgi
import json
import sys
import time

import MySQLdb as db


def exec_query(conn, title, query, response, keyspace=None, kr=None):  # pylint: disable=missing-docstring
  cursor = conn.cursor()

  try:
    cursor.execute(query)
    response[title] = {
        "title": title,
        "description": cursor.description,
        "rowcount": cursor.rowcount,
        "lastrowid": cursor.lastrowid,
        "results": cursor.fetchall(),
        }
    cursor.close()
  except Exception as e:  # pylint: disable=broad-except
    response["error"] = str(e)
  finally:
    cursor.close()

def get_value(args, name):
  val = args.getvalue(name)
  if not val or val == "undefined":
    return None
  if not val.isalnum():
    raise Exception("%s is not alphanumeric"%name)
  return val


def main():
  print "Content-Type: application/json\n"
  try:
    conn = db.connect(host="127.0.0.1", port=15306)

    args = cgi.FieldStorage()
    response = {}
    if get_value(args, "product"):
      sku = get_value(args, "sku")
      desc = get_value(args, "desc")
      price = get_value(args, "price")
      if not sku or not desc or not price:
        response["error"] = "sku, desc or price not specified"
      else:
        query = "insert into product(sku, description, price) values('%s', '%s', %s) on duplicate key update sku=values(sku), description=values(description), price=values(price)"%(sku, desc, price)
        exec_query(conn, "result", query, response)
        response["query"] = query
    elif get_value(args, "customer"):
      name = get_value(args, "name")
      if not name:
        response["error"] = "name not specified"
      else:
        query = "insert into customer(name) values('%s')"%(name)
        exec_query(conn, "result", query, response)
        response["query"] = query
    elif get_value(args, "order"):
      cid = get_value(args, "cid")
      sku = get_value(args, "sku")
      if not cid or not sku:
        response["error"] = "cid or sku not specified"
      else:
        query = "insert into corder(customer_id, sku, price) values(%s, '%s', 10)"%(cid, sku)
        exec_query(conn, "result", query, response)
        response["query"] = query

    exec_query(conn, "product", "select * from product order by sku desc limit 20", response)
    exec_query(conn, "customer", "select customer_id, name from customer order by customer_id desc limit 20", response)
    exec_query(conn, "corder", "select order_id, customer_id, sku, price from corder order by order_id desc limit 20", response)

    if response.get("error"):
      print >> sys.stderr, response["error"]
    print json.dumps(response)
  except Exception as e:  # pylint: disable=broad-except
    print >> sys.stderr, str(e)
    print json.dumps({"error": str(e)})


if __name__ == "__main__":
  main()
