#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import re
import time
import os
import sys
import MySQLdb

def insert_func(host, port, user, password, database, select_sql, insert_sql):
    """
    insert into doris table select xxx 
    :param host:
    :param port:
    :param user:
    :param password:
    :param database:
    :param select_sql: SELECT column1, column2,..., columnN [FROM TABLE_X WHERE xxx]
    :param insert_sql: INSERT INTO TABLE_Y[(column1, column2,...,columnN)]
    :return:
    """
    db_conn = MySQLdb.connect(host=host,
                                 port=port,
                                 user=user,
                                 passwd=password,
                                 db=database)

    db_cursor = db_conn.cursor()
    insert_process(select_sql, insert_sql, db_cursor)


def insert_process(select_sql, insert_sql, cursor):
    """
    issue insert task and check insert task status.
    :param select_sql: SELECT column1, column2,..., columnN [FROM TABLE_X WHERE xxx]
    :param insert_sql: INSERT INTO TABLE_Y[(column1, column2,...,columnN)]
    :param cursor:
    :return:
    """

    print insert_sql
    print select_sql

    cursor.execute(select_sql)
    rows = cursor.fetchall()
    if len(rows) == 0:
        print "select result is empty, don't need insert"
        return

    cursor.execute(insert_sql + select_sql)

    label_info = cursor._info
    label = re.match(
        r'{\'label\':\'([a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12})\'}',
        label_info).group(1)
    print label

    # check insert task status
    sql = "show load where label = '" + label + "' order by CreateTime desc limit 1"
    print sql
    cursor.execute(sql)
    rows = cursor.fetchall()

    timeout = 60 * 60
    load_status = ""
    while timeout > 0:
        load_status = rows[0][2]
        print "insert status: " + load_status
        if load_status == 'FINISHED' or load_status == 'CANCELLED':
            break
        time.sleep(5)
        timeout = timeout - 5
        cursor.execute(sql)
        rows = cursor.fetchall()

    if load_status == "CANCELLED":
        exit("error: insert data CANCELLED")
    elif load_status != "FINISHED":
        exit("error: insert data timout")
    else:
        print "insert success."

if __name__ == '__main__':
    """
    Befor you run this demo, you should do as below.
    First, you need install MySQLdb, execute cmd by root:
        pip install MySQL-python
        # if you met error: "mysql_config not found", you can execute the following cmd to solve it.
        ln -s /usr/local/mysql/bin/mysql_config /usr/local/bin/mysql_config
    Second, you need input your db connect config & input insert/select sql
    """
    insert_func("127.0.0.1", 8080, 'user', 'password', 'test_db', 
        "SELECT column1, column2 FROM TABLE_X WHERE column1 = 'test'", 
        "INSERT INTO TABLE_Y(column1, column2)")
