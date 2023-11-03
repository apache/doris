#!/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

############################################################################
#
#   @file test_sys_hll_basic.py
#   @date 2017-04-10 15:02:22
#   @brief This file is a test file for palo small load in complex scenarios.
#
#############################################################################

"""
测试hll data type
"""

import os
import sys
import time
import pytest

from data import small_load_complex as DATA
from data import schema as SCHEMA
sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import util
from lib import common

client = None

config = palo_config.config
hdfs = palo_config.gen_remote_file_path("/qe/baseall.txt")
rollup_column_name_list = DATA.rollup_column_name_list
CLUSTER = 'default_cluster'
ERROR = 100
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, 
                                    password=config.fe_password, http_port=config.fe_http_port)
    ret = client.execute('drop database if exists test_hll_simple')
    retry_times = 10
    while retry_times > 0:
        try:
            client.create_database('test_hll_simple')
            client.use('test_hll_simple')
            break
        except Exception as e:
            print(str(e))
            if 'Try' in str(e):
                retry_times -= 1
                time.sleep(10)
            else:
                break


def wait_end(database_name):
    """
    wait to finished
    """
    ret = True
    retry_times = 200
    while ret and retry_times > 0:
        job_list = client.get_unfinish_load_job_list(database_name)
        if len(job_list) == 0:
            ret = False
        else:
            time.sleep(3)
        retry_times -= 1


def execute(line):
    """execute palo sql and return reuslt"""
    print(line)
    palo_result = client.execute(line)
    print(palo_result)
    return palo_result


def test_hll_bitmap_create_table():
    """
    {
    "title": "test_sys_hll_bitmap_basic.test_hll_bitmap_create_table",
    "describe": "hll create table sql, for issue 3768",
    "tag": "system,p1,fuzz"
    }
    """
    sql = 'CREATE TABLE t (c_date date NOT NULL COMMENT "date", user_id bitmap NULL DEFAULT " " COMMENT "userid")' \
          'ENGINE=OLAP DUPLICATE KEY(c_date) DISTRIBUTED BY HASH(c_date)'
    msg = 'is not compatible with primitive type bitmap'
    util.assert_return(False, msg, execute, sql)
    sql = 'CREATE TABLE t (c_date date NOT NULL COMMENT "date", user_id hll NULL DEFAULT " " COMMENT "userid")' \
          'ENGINE=OLAP DUPLICATE KEY(c_date) DISTRIBUTED BY HASH(c_date)'
    msg = 'is not compatible with primitive type hll'
    util.assert_return(False, msg, execute, sql)
    sql = 'CREATE TABLE bitmaptable (TSID BIGINT NOT NULL, PKEY VARCHAR(96) NOT NULL, EDITFLAG BITMAP NULL)' \
          'UNIQUE KEY(TSID,PKEY) DISTRIBUTED BY HASH(TSID) BUCKETS 10'
    msg = 'is not compatible with primitive type bitmap'
    util.assert_return(False, msg, execute, sql)


def compute_deviation(actual, computed):
    """
    Args:
        actual: 实际值
        computed: 计算值
    Returns: 误差
    """
    tmp = abs(actual - computed)
    return tmp / float(actual)


hll_db = 'test_hll_simple'


def test_tinyint():
    """
    {
    "title": "test_sys_hll_basic.test_tinyint",
    "describe": "test intsert tinyint",
    "tag": "system,p1"
    }
    """
    """test intsert tinyint"""
    client.use(hll_db)
    table_name = 'test_tinyint'
    columns = [('id', 'tinyint'), ('hll_set', 'hll', 'hll_union')]
    client.execute('drop table if exists %s' % table_name)
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k1, hll_hash(k1) from test_query_qa.test' % table_name
    r = execute(line)
    line = 'select ndv(id), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1

    line = 'select count(distinct k1) from test_query_qa.test'
    r2 = execute(line)
    # check
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
           deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_smallint():
    """
    {
    "title": "test_sys_hll_basic.test_smallint",
    "describe": "test insert hll smallint",
    "tag": "system,p1"
    }
    """
    """test insert hll smallint"""
    client.use(hll_db)
    table_name = 'test_smallint'
    columns = [('id', 'smallint'), ('hll_set', 'hll', 'hll_union')]
    client.execute('drop table if exists %s' % table_name)
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k2, hll_hash(k2) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(id), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k2) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
           deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert  deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_int():
    """
    {
    "title": "test_sys_hll_basic.test_int",
    "describe": "test hll insert int",
    "tag": "system,p1"
    }
    """
    """test hll insert int"""
    table_name = 'test_int'
    client.use(hll_db)
    client.execute('drop table  if exists %s' % table_name)
    columns = [('id', 'int'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k3, hll_hash(k3) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(id), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k3) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
          deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_bigint():
    """
    {
    "title": "test_sys_hll_basic.test_bigint",
    "describe": "test insert hlll bigint",
    "tag": "system,p1"
    }
    """
    """test insert hlll bigint"""
    table_name = 'test_bigint'
    client.use(hll_db)
    client.execute('drop table if exists %s' % table_name)
    columns = [('id', 'bigint'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k4, hll_hash(k4) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(id), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k4) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
           deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_decimal():
    """
    {
    "title": "test_sys_hll_basic.test_decimal",
    "describe": "test insert hll decimal",
    "tag": "system,p1"
    }
    """
    """test insert hll decimal"""
    table_name = 'test_decimal'
    client.use(hll_db)
    client.execute('drop table if exists %s' % table_name)
    columns = [('id', 'decimal(9, 3)'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k5, hll_hash(k5) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(id), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k5) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
           deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_char():
    """
    {
    "title": "test_sys_hll_basic.test_char",
    "describe": "test insesrt hll char",
    "tag": "system,p1"
    }
    """
    """test insesrt hll char"""
    table_name = 'test_char'
    client.use(hll_db)
    client.execute('drop table if exists %s' % table_name)
    columns = [('id', 'char(11)'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k6, hll_hash(k6) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(id), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k6) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
           deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_varchar():
    """
    {
    "title": "test_sys_hll_basic.test_varchar",
    "describe": "test insert hll varchar",
    "tag": "system,p1"
    }
    """
    """test insert hll varchar"""
    table_name = 'test_varchar'
    client.use(hll_db)
    client.execute('drop table if exists %s' % table_name)
    columns = [('id', 'varchar(51)'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k7, hll_hash(k7) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(id), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k7) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
           deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_date():
    """
    {
    "title": "test_sys_hll_basic.test_date",
    "describe": "test insert hll date",
    "tag": "system,p1"
    }
    """
    """test insert hll date"""
    table_name = 'test_date'
    client.use(hll_db)
    client.execute('drop table if exists %s' % table_name)
    columns = [('id', 'date'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k10, hll_hash(k10) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(id), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k10) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
           deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_datetime():
    """
    {
    "title": "test_sys_hll_basic.test_datetime",
    "describe": "test insert hll datetime",
    "tag": "system,p1"
    }
    """
    """test insert hll datetime"""
    table_name = 'test_datetime'
    client.use(hll_db)
    client.execute('drop table if exists %s' % table_name)
    columns = [('id', 'datetime'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k11, hll_hash(k11) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(id), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k11) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
          deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_double():
    """
    {
    "title": "test_sys_hll_basic.test_double",
    "describe": "test insert hll double",
    "tag": "system,p1"
    }
    """
    """test insert hll double"""
    table_name = 'test_double'
    client.use(hll_db)
    client.execute('drop table if exists %s' % table_name)
    columns = [('id', 'int'), ('id1', 'tinyint'), 
               ('c_double', 'double', 'sum'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k4, k1, k8, hll_hash(k8) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(c_double), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k8) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
           deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def test_float():
    """
    {
    "title": "test_sys_hll_basic.test_float",
    "describe": "test hll insert float",
    "tag": "system,p1"
    }
    """
    """test hll insert float"""
    table_name = 'test_float'
    client.use(hll_db)
    client.execute('drop table if exists %s' % table_name)
    columns = [('id', 'int'), ('id1', 'tinyint'), 
               ('c_float', 'float', 'sum'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select k4, k1, k9, hll_hash(k9) from test_query_qa.test' % table_name
    execute(line)
    wait_end(hll_db)
    line = 'select ndv(c_float), hll_union_agg(hll_set) from %s' % table_name
    r1 = execute(line)
    retry_times = 60
    while int(r1[0][0]) == 0 and retry_times > 0:
        time.sleep(1)
        r1 = execute(line)
        retry_times -= 1
    line = 'select count(distinct k9) from test_query_qa.test'
    r2 = execute(line)
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][1]))
    print('actual count is : %s, computed is: %s, deviation is : %s\n' % (r2[0][0], r1[0][1], \
          deviation))
    deviation = compute_deviation(int(r2[0][0]), int(r1[0][0]))
    print('actual count is %s, ndv is: %s, deviation is : %s \n' % (r2[0][0], r1[0][0], deviation))
    assert deviation < ERROR
    client.execute('drop table %s' % table_name)


def checkwrong(sql):
    """check sql execute error"""
    try:
        client.execute(sql)
        assert 0 == 1, 'expect failed'
    except Exception as e:
        pass


def test_union_agg():
    """
    {
    "title": "test_sys_hll_basic.test_union_agg",
    "describe": "test union agg",
    "tag": "system,p1,fuzz"
    }
    """
    """test union agg"""
    table_name = 'test_union_agg_table'
    init_union_agg(table_name)
    union_agg_1(table_name)
    union_agg_2(table_name)
    union_agg_3(table_name)
    union_agg_4(table_name)


def init_union_agg(table_name):
    """init union agg"""
    client.execute('drop table if exists %s' % table_name)
    columns = [('id', 'int'), ('hll_set', 'hll', 'hll_union')]
    assert client.create_table(table_name, columns)
    line = 'insert into %s select 0, unhex(null) union select 1, unhex(null)' % table_name
    checkwrong(line)
    line = 'insert into %s select 0, hll_hash(null) union select 1, hll_hash(null)' % table_name
    line = 'insert into %s select 0, hll_hash(null)' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 1, hll_hash(null)' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 2, hll_hash(null)' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 3, hll_hash(null)' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 4, hll_hash("")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 5, hll_hash("")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 6, hll_hash("")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 7, hll_hash("")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 8, hll_hash("118b7f")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 9, hll_hash("118b7f")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 10, hll_hash("118b7f")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 11, hll_hash("118b7f")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 12, hll_hash("128b7f1111111111111111")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 13, hll_hash("128b7f1111111111111111")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 14, hll_hash("128b7f2222222222222222")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 15, hll_hash("128b7f3333333333333333")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 16, hll_hash("138b7f0001")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 17, hll_hash("138b7f0022")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 18, hll_hash("138b7f0041")' % table_name
    ret = client.execute(line)
    line = 'insert into %s select 19, hll_hash("138b7f0061")' % table_name
    ret = client.execute(line)
    wait_end(client.database_name)
    line = 'SELECT count(*) from %s' % table_name
    ret = client.execute(line)
    assert int(ret[0][0]) == 20


def union_agg_1(table_name):
    """select union_aggg_1"""
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id > 100;' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3)' \
            % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(8, 9, 10, 11);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(8, 9, 10, 11) OR id IN(0, 1, 2, 3)' \
           % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7)' \
           % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(8, 9, 10, 11) OR \
            id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(12, 13, 14, 15);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(0, 1, 2, 3)' \
            % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(4, 5, 6, 7)' \
           % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(4, 5, 6, 7) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(8, 9,10,11)'\
           % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(8, 9,10,11) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(8, 9,10,11) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(8, 9,10,11) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)


def union_agg_2(table_name):
    """
    # Don't feel like a full sparse/explicit permuatation is adding
    # anything here...just replace explicit w / sparse.
    """
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(16, 17, 18, 19);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(0, 1, 2, 3)' \
           % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(4, 5, 6, 7)' \
           % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(4, 5, 6, 7) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(8, 9,10,11)' \
           % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(8, 9, 10, 11) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(8, 9, 10, 11) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(8, 9, 10, 11) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    #-- ----------------------------------------------------------------
    #-- Aggregate Cardinality
    #-- ----------------------------------------------------------------

    # No rows selected
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id > 100;' % table_name
    r = execute(line)

    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(4, 5, 6, 7) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)


def union_agg_3(table_name):
    """test union agg"""
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(8, 9, 10, 11);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(8, 9, 10, 11) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(8, 9, 10, 11) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(8, 9, 10, 11) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(12, 13, 14, 15);' \
           % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(8, 9, 10, 11);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(8, 9, 10, 11) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)


def union_agg_4(table_name):
    """
    # Don't feel like a full sparse/explicit permuatation is adding
    # anything here...just replace explicit w / sparse.
    """
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(16, 17, 18, 19);' \
           % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(8, 9, 10, 11);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(8, 9, 10, 11) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    line = 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    line= 'SELECT ceiling(hll_union_agg(hll_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    time.sleep(10)
    line = 'SELECT count(id) FROM %s WHERE hll_set is null' % table_name
    r = execute(line)
    line = 'SELECT count(hll_cardinality(hll_set)) FROM %s WHERE hll_cardinality(hll_set) = 1' \
           % table_name
    r = execute(line)
    line = 'SELECT count(id) FROM %s WHERE hll_cardinality(hll_set) is null' % table_name
    r = execute(line)
    line = 'SELECT hll_union_agg(hll_set) FROM %s' % table_name
    r = execute(line)


def test_hll_issue_5798():
    """
    {
    "title": "test_hll_insert_5798",
    "describe": "update数据类型tinyint, 数据溢出，Null，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = common.create_workspace(database_name)
    ret = client.create_table(table_name, SCHEMA.hll_tinyint_column_list)
    assert ret, 'create table failed'
    sql = 'insert into %s values(1, hll_hash("test_uv"), 0)' % table_name
    assert client.execute(sql) == (), 'insert error'
    sql1 = 'select * from %s' % table_name
    sql2 = 'select 1, null, 0'
    common.check2(client, sql1=sql1, sql2=sql2)
    msg = "Hll type dose not support operand: `v1` = 'a'"
    sql = "select * from %s where v1='a'" % table_name
    util.assert_return(False, msg, client.execute, sql)
    client.clean(database_name)


def test_hll_issue_5424():
    """
    {
    "title": "test_hll_issue_5424",
    "describe": "update数据类型tinyint, 数据溢出，Null，数据校验正确",
    "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    client = common.create_workspace(database_name)
    column_list = [('id', 'int'), ]
    ret = client.create_table(table_name, column_list)
    assert ret, 'create table failed'
    t = list()
    for i in range(0, 1000):
        t.append(str(i))
    values = '),('.join(t)
    sql = 'insert into %s values (%s)' % (table_name, values)
    assert client.execute(sql) == (), 'insert error'
    sql1 = 'select hll_union_agg(cast(hll_hash(id) as hll)) from %s where id < 160' % table_name
    sql2 = 'select 160'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_hll_empty():
    """
    {
    "title": "test_sys_hll_basic.test_hll_empty",
    "describe": "返回一个空的HLL类型值",
    "tag": "function,p0"
    }
    """
    sql1 = "select hll_cardinality(hll_empty())"
    sql2 = "select 0"
    common.check2(client, sql1=sql1, sql2=sql2)


def test_hll_union():
    """
    {
    "title": "test_sys_hll_basic.test_hll_union",
    "describe": "返回一组HLL值的并集",
    "tag": "function,p0"
    }
    """
    table_name = 'test_hll_union'
    init_union_agg(table_name)
    sql1 = "select hll_cardinality(hll_union(hll_set)) from %s" % table_name
    sql2 = "select hll_union_agg(hll_set) from %s" % table_name
    common.check2(client, sql1=sql1, sql2=sql2)
    sql1 = "select hll_cardinality(hll_raw_agg(hll_set)) from %s" % table_name
    common.check2(client, sql1=sql1, sql2=sql2)


if __name__ == '__main__':
    setup_module()
    print(client)
    test_union_agg()

