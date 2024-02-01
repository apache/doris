#!/bin/env pyth
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
#   @file test_sys_bitmap_basic.py
#   @date 2020-02-20
#
#############################################################################

import os
import sys
import time
import random

from data import small_load_complex as DATA
sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import util

client = None

config = palo_config.config
hdfs = palo_config.gen_remote_file_path("/qe/baseall.txt")
rollup_column_name_list = DATA.rollup_column_name_list
CLUSTER = 'default_cluster'
ERROR = '100'
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, 
                                    password=config.fe_password, http_port=config.fe_http_port)
    ret = client.execute('drop database if exists test_bitmap_simple')
    retry_times = 10
    while retry_times > 0:
        try:
            client.create_database('test_bitmap_simple')
            client.use('test_bitmap_simple')
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


def test_hash_smallint():
    """
    {
    "title": "test_sys_bitmap_basic.test_hash_smallint",
    "describe": "test_hash smallint",
    "tag": "function,p1"
    }
    """
    """test_hash smallint"""
    result = set()
    line = "SELECT bitmap_to_string(bitmap_hash(0));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(-1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(-32768));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(32767));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(32768));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(NULL));"
    r = execute(line)
    result.add(r[0][0])
    print(len(result))
    assert len(result) == 7


def test_hash_tinyint():
    """
    {
    "title": "test_sys_bitmap_basic.test_hash_tinyint",
    "describe": "test_hash tinyint",
    "tag": "function,p1"
    }
    """
    """test_hash tinyint"""
    result = set()
    line = "SELECT bitmap_to_string(bitmap_hash(0));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(-1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(-128));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(127));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(128));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(NULL));"
    r = execute(line)
    result.add(r[0][0])
    print(len(result))
    assert len(result) == 7


def test_hash_int():
    """
    {
    "title": "test_sys_bitmap_basic.test_hash_int",
    "describe": "test_hash int",
    "tag": "function,p1"
    }
    """
    """test_hash int"""
    result = set()
    line = "SELECT bitmap_to_string(bitmap_hash(0));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(-1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(-2147483648));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(2147483648-1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(2147483648));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(NULL));"
    r = execute(line)
    result.add(r[0][0])
    print(len(result))
    assert len(result) == 7


def test_hash_bigint():
    """
    {
    "title": "test_sys_bitmap_basic.test_hash_bigint",
    "describe": "test_hash bigint",
    "tag": "function,p1"
    }
    """
    """test_hash bigint"""
    result = set()
    line = "SELECT bitmap_to_string(bitmap_hash(0));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(-1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(-9223372036854775808));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(9223372036854775808-1));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(9223372036854775808));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(NULL));"
    r = execute(line)
    result.add(r[0][0])
    print(len(result))
    assert len(result) == 7


def test_hash_float():
    """
    {
    "title": "test_sys_bitmap_basic.test_hash_float",
    "describe": "test_hash float",
    "tag": "function,p1"
    }
    """
    """test_hash float"""
    result = set()
    line = "SELECT bitmap_to_string(bitmap_hash(0));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(0.0));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(0.1e-10));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(0.1e-45));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(9223372036854775808.011));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(-9223372036854775808.0));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(NULL));"
    r = execute(line)
    result.add(r[0][0])
    print(len(result))
    assert len(result) == 7


def test_hash_double():
    """
    {
    "title": "test_sys_bitmap_basic.test_hash_double",
    "describe": "test_hash double",
    "tag": "function,p1"
    }
    """
    """test_hash double"""
    result = set()
    line = "SELECT bitmap_to_string(bitmap_hash(0));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(0.0000000000));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(9223372036854775808.011));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(9223372036854775808.0));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(3.1415926536));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(3.14159265359));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(3.141592653));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(3.1415926535893238));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(3.14159265358932384));"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(NULL));"
    r = execute(line)
    result.add(r[0][0])
    print(len(result))
    assert len(result) == 10


def test_hash_date():
    """
    {
    "title": "test_sys_bitmap_basic.test_hash_date",
    "describe": "test hash date",
    "tag": "function,p1"
    }
    """
    """test hash date"""
    result = set()
    line = "SELECT bitmap_to_string(bitmap_hash('1990-12-10'))"
    r1 = execute(line)
    result.add(r1[0][0])
    #line = "SELECT bitmap_to_string(bitmap_hash_date(1990-12-11))"
    r2 = execute(line)
    result.add(r2[0][0])
    print(r2)
    line = "SELECT bitmap_to_string(bitmap_hash('1990-12-10 12:00:10'))"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash('1990/12/10 12:00:10'))"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash('9999-9-9'))"
    r = execute(line)
    result.add(r[0][0])
    # check_same(r1, r2)
    line = "SELECT bitmap_to_string(bitmap_hash(NULL))"
    r = execute(line)
    result.add(r[0][0])
    print(len(result))
    assert len(result) == 5


def test_hash_datetime():
    """
    {
    "title": "test_sys_bitmap_basic.test_hash_datetime",
    "describe": "test hash datetime",
    "tag": "function,p1"
    }
    """
    """test hash datetime"""
    result = set()
    line = "SELECT bitmap_to_string(bitmap_hash('1990-12-10'))"
    r1 = execute(line)
    result.add(r1[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash('1990-12-10 00:00:00'))"
    r2 = execute(line)
    result.add(r2[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash('1990-12-10 12:00:10'))"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash('1990/12/10 12:00:10'))"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash('9999-9-9'))"
    r = execute(line)
    result.add(r[0][0])
    # check_same(r1, r2)
    line = "SELECT bitmap_to_string(bitmap_hash(NULL))"
    r = execute(line)
    result.add(r[0][0])
    print(len(result))
    assert len(result) == 6


def test_hash_decimal():
    """
    {
    "title": "test_sys_bitmap_basic.test_hash_decimal",
    "describe": "test hash decimal",
    "tag": "function,p1"
    }
    """
    """test hash decimal"""
    result = set()
    line = "SELECT bitmap_to_string(bitmap_hash(0))"
    r1 = execute(line)
    result.add(r1[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(0.0))"
    r2 = execute(line)
    result.add(r2[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(0.123))"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(0.1234))"
    r = execute(line)
    result.add(r[0][0])
    line = "SELECT bitmap_to_string(bitmap_hash(199999.13567896251))"
    r = execute(line)
    result.add(r[0][0])
    # check_same(r1, r2)
    line = "SELECT bitmap_to_string(bitmap_hash(NULL))"
    r = execute(line)
    result.add(r[0][0])
    print(len(result))
    assert len(result) == 6


def compute_deviation(actual, computed):
    """
    Args:
        actual: 实际值
        computed: 计算值
    Returns: 误差
    """
    tmp = abs(actual - computed)
    return tmp / float(actual)


def check_bitmap_union_count(table_name, kv1, kv2, deviation_acceptable=False):
    """
    check_bitmap_union_count
    :param table_name:
    :param kv1:
    :param kv2:
    :param deviation_acceptable:
    :return:
    """
    if len(kv1) < 2:
        assert False

    if len(kv1[0]) == 0:
        v1 = kv1[1][0]
        line1 = 'select bitmap_union_count({0}) from {1}'.format(v1, table_name)

        v2 = kv2[1][0]
        line2 = 'select count(distinct({0})) from test_query_qa.test'.format(v2)
    else:
        k1 = ','.join(kv1[0])
        v1 = kv1[1][0]
        line1 = 'select {0},bitmap_union_count({1}) from {2} group by {0} order by {0}'.format(k1, v1, table_name)

        k2 = ','.join(kv2[0])
        v2 = kv2[1][0]
        line2 = 'select {0}, count(distinct({1})) from test_query_qa.test' \
                ' group by {0} order by {0}'.format(k2, v2)

    ret1 = client.execute(line1)
    ret2 = client.execute(line2)
    if deviation_acceptable:
        assert abs((ret1[0][0] - ret2[0][0]) / float(max(ret1[0][0], ret2[0][0]))) < 0.0001
    else:
        assert ret1 == ret2


bitmap_db = 'test_bitmap_simple'


def test_tinyint():
    """
    {
    "title": "test_sys_bitmap_basic.test_tinyint",
    "describe": "test intsert tinyint",
    "tag": "function,p1"
    }
    """
    """test intsert tinyint"""
    client.use(bitmap_db)
    table_name = 'test_tinyint'
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id` tinyint COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k1, bitmap_hash(k1) from test_query_qa.test' % table_name
    r = execute(line)
    print(r)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k1']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k1'], ['k1']])

    client.execute('drop table %s' % table_name)


def test_smallint():
    """
    {
    "title": "test_sys_bitmap_basic.test_smallint",
    "describe": "test insert smallint",
    "tag": "function,p1"
    }
    """
    """test insert hll smallint"""
    client.use(bitmap_db)
    table_name = 'test_smallint'
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id` smallint COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k2, bitmap_hash(k2) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k2']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k2'], ['k2']])

    client.execute('drop table %s' % table_name)


def test_int():
    """
    {
    "title": "test_sys_bitmap_basic.test_int",
    "describe": "test insert int",
    "tag": "function,p1"
    }
    """
    """test hll insert int"""
    table_name = 'test_int'
    client.use(bitmap_db)
    client.execute('drop table  if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id` int COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k3, bitmap_hash(k3) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k3']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k3'], ['k3']])

    client.execute('drop table %s' % table_name)


def test_bigint():
    """
    {
    "title": "test_sys_bitmap_basic.test_bigint",
    "describe": "test insert bigint",
    "tag": "function,p1"
    }
    """
    """test insert hlll bigint"""
    table_name = 'test_bigint'
    client.use(bitmap_db)
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id` bigint COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k4, bitmap_hash(k4) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k4']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k4'], ['k4']])

    client.execute('drop table %s' % table_name)


def test_decimal():
    """
    {
    "title": "test_sys_bitmap_basic.test_decimal",
    "describe": "test insert decimal",
    "tag": "function,p1"
    }
    """
    """test insert hll decimal"""
    table_name = 'test_decimal'
    client.use(bitmap_db)
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id` decimal(9, 3) COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k5, bitmap_hash(k5) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k5']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k5'], ['k5']])

    client.execute('drop table %s' % table_name)


def test_char():
    """
    {
    "title": "test_sys_bitmap_basic.test_char",
    "describe": "test insesrt char",
    "tag": "function,p1"
    }
    """
    """test insesrt hll char"""
    table_name = 'test_char'
    client.use(bitmap_db)
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id` char(11) COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k6, bitmap_hash(k6) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k6']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k6'], ['k6']])

    client.execute('drop table %s' % table_name)


def test_varchar():
    """
    {
    "title": "test_sys_bitmap_basic.test_varchar",
    "describe": "test insert varchar",
    "tag": "function,p1"
    }
    """
    """test insert hll varchar"""
    table_name = 'test_varchar'
    client.use(bitmap_db)
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id` varchar(51) COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k7, bitmap_hash(k7) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k7']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k7'], ['k7']])

    client.execute('drop table %s' % table_name)


def test_date():
    """
    {
    "title": "test_sys_bitmap_basic.test_date",
    "describe": "test insert date",
    "tag": "function,p1"
    }
    """
    """test insert hll date"""
    table_name = 'test_date'
    client.use(bitmap_db)
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id` date COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k10, bitmap_hash(k10) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k10']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k10'], ['k10']])

    client.execute('drop table %s' % table_name)


def test_datetime():
    """
    {
    "title": "test_sys_bitmap_basic.test_datetime",
    "describe": "test insert datetime",
    "tag": "function,p1"
    }
    """
    """test insert hll datetime"""
    table_name = 'test_datetime'
    client.use(bitmap_db)
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id` datetime COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k11, bitmap_hash(k11) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k11']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k11'], ['k11']])

    client.execute('drop table %s' % table_name)


def test_double():
    """
    {
    "title": "test_sys_bitmap_basic.test_double",
    "describe": "test insert double",
    "tag": "function,p1"
    }
    """
    """test insert hll double"""
    table_name = 'test_double'
    client.use(bitmap_db)
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id`  BIGINT COMMENT "", \
    `id1` tinyint COMMENT "", \
    `c_double` double SUM COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k4, k1, k8, bitmap_hash(k8) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k8']])
    check_bitmap_union_count(table_name, [['id1'], ['bitmap_set']], [['k1'], ['k8']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k4'], ['k8']])

    client.execute('drop table %s' % table_name)


def test_float():
    """
    {
    "title": "test_sys_bitmap_basic.test_float",
    "describe": "test insert float",
    "tag": "function,p1"
    }
    """
    """test hll insert float"""
    table_name = 'test_float'
    client.use(bitmap_db)
    client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id`  BIGINT COMMENT "", \
    `id1` tinyint COMMENT "", \
    `c_float` float SUM COMMENT "", \
    `bitmap_set` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`, `id1`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k4, k1, k9, bitmap_hash(k9) from test_query_qa.test' % table_name
    execute(line)
    wait_end(bitmap_db)

    check_bitmap_union_count(table_name, [[], ['bitmap_set']], [[], ['k9']], True)
    check_bitmap_union_count(table_name, [['id1'], ['bitmap_set']], [['k1'], ['k9']])
    check_bitmap_union_count(table_name, [['id'], ['bitmap_set']], [['k4'], ['k9']])

    client.execute('drop table %s' % table_name)


def test_to_bitmap_correct():
    """
    {
    "title": "test_sys_bitmap_basic.test_to_bitmap_correct",
    "describe": "test_to_bitmap, to_bitmap只能将 0 ~ 18446744073709551615 的 unsigned int 转为 bitmap",
    "tag": "function,p1"
    }
    """
    """
    test_to_bitmap
    to_bitmap只能将 0 ~ 18446744073709551615 的 unsigned int 转为 bitmap
    :return:
    """
    check_list = [0, random.randint(1, 18446744073709551614), 18446744073709551615, 'NULL']
    for i in range(len(check_list)):
        check_value = check_list[i]

        line1 = "SELECT bitmap_to_string(to_bitmap({0}));".format(str(check_value))
        r = execute(line1)
        if check_value == 'NULL':
            assert r[0][0] == ''
        else:
            assert int(r[0][0]) == check_value


def test_to_bitmap_wrong():
    """
    {
    "title": "test_sys_bitmap_basic.test_to_bitmap_wrong",
    "describe": "to_bitmap只能将 0 ~ 18446744073709551615 的 unsigned int 转为 bitmap",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test_to_bitmap
    to_bitmap只能将 0 ~ 18446744073709551615 的 unsigned int 转为 bitmap
    :return:
    """
    check_list = [-1, random.randint(18446744073709551616, 10 ** 20),
                  'cast("1990/12/10 12:00:10" as datetime)',
                  'cast("1990/12/10 12:00:10" as date)'
                  ]
    for i in range(len(check_list)):
        check_value = check_list[i]
        line1 = "SELECT bitmap_to_string(to_bitmap({0}));".format(str(check_value))
        ret1 = execute(line1)
        line2 = "SELECT ''"
        ret2 = execute(line2)
        assert ret1 == ret2, "expect:, result:%s" % ret1

    check_list = [-1, random.randint(18446744073709551616, 10 ** 20),
                  '124vaergvq', '', '3.1415926536', '1990/12/10 12:00:10',
                  'cast("1990/12/10 12:00:10" as datetime)',
                  'cast("1990/12/10 12:00:10" as date)'
                  ]
    for i in range(len(check_list)):
        check_value = check_list[i]
        line1 = "SELECT bitmap_to_string(to_bitmap('{0}'));".format(str(check_value))
        ret1 = execute(line1)
        line2 = "SELECT ''"
        ret2 = execute(line2)
        assert ret1 == ret2, "expect:, actual:%s" % ret1


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
    "title": "test_sys_bitmap_basic.test_union_agg",
    "describe": "test union agg",
    "tag": "function,p1"
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
    line = 'CREATE TABLE `%s` (\
            `id` int(11) COMMENT "", \
            `bitmap_set` bitmap bitmap_union COMMENT "" \
            ) ENGINE=OLAP \
            DISTRIBUTED BY HASH(`id`) BUCKETS 5 \
            PROPERTIES ( \
            "storage_type" = "COLUMN" \
            );' % table_name
    ret = client.execute(line)
    assert ret == ()
    # todo
    # line = 'insert into %s values(0, unbitmap_to_string(null) union values(1, unbitmap_to_string(null))' % table_name
    # checkwrong(line)
    # line = 'insert into %s values(0, bitmap_hash(null) union values(1, bitmap_hash(null))' % table_name
    line = 'insert into %s values(0, bitmap_hash(null))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(1, bitmap_hash(null))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(2, bitmap_hash(null))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(3, bitmap_hash(null))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(4, bitmap_hash(""))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(5, bitmap_hash(""))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(6, bitmap_hash(""))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(7, bitmap_hash(""))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(8, bitmap_hash("118b7f"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(9, bitmap_hash("118b7f"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(10, bitmap_hash("118b7f"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(11, bitmap_hash("118b7f"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(12, bitmap_hash("128b7f1111111111111111"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(13, bitmap_hash("128b7f1111111111111111"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(14, bitmap_hash("128b7f2222222222222222"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(15, bitmap_hash("128b7f3333333333333333"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(16, bitmap_hash("138b7f0001"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(17, bitmap_hash("138b7f0022"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(18, bitmap_hash("138b7f0041"))' % table_name
    ret = client.execute(line)
    line = 'insert into %s values(19, bitmap_hash("138b7f0061"))' % table_name
    ret = client.execute(line)
    wait_end(client.database_name)
    line = 'SELECT count(*) from %s' % table_name
    ret = client.execute(line)
    assert int(ret[0][0]) == 20


def union_agg_1(table_name):
    """select union_aggg_1"""
    # 0
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id > 100;' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 0

    # 0
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 1
    # 1
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 1

    # 1
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3)' \
            % table_name
    r = execute(line)
    # assert int(r[0][0]) == 2

    # 1
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(8, 9, 10, 11);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 1

    # 1
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(8, 9, 10, 11) OR id IN(0, 1, 2, 3)' \
           % table_name
    r = execute(line)
    # assert int(r[0][0]) == 2

    # 2
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7)' \
           % table_name
    r = execute(line)
    # assert int(r[0][0]) == 2

    # 2
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(8, 9, 10, 11) OR \
            id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 2

    # 3
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(12, 13, 14, 15);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 3

    # 3
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(0, 1, 2, 3)' \
            % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 4
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(4, 5, 6, 7)' \
           % table_name
    r = execute(line)
    # assert int(r[0][0]) == 4

    # 4
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(4, 5, 6, 7) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 4
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(8, 9,10,11)'\
           % table_name
    r = execute(line)
    # assert int(r[0][0]) == 4

    # 4
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(8, 9,10,11) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 5
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(8, 9,10,11) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 5
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(12, 13, 14, 15) OR id IN(8, 9,10,11) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5


def union_agg_2(table_name):
    """
    # Don't feel like a full sparse/explicit permuatation is adding
    # anything here...just replace explicit w / sparse.
    """
    # 4
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(16, 17, 18, 19);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 4

    # 4
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(0, 1, 2, 3)' \
           % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6

    # 5
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(4, 5, 6, 7)' \
           % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 5
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(4, 5, 6, 7) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6
 
    # 5   
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(8, 9,10,11)' \
           % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 5
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(8, 9, 10, 11) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6

    # 6
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(8, 9, 10, 11) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6

    # 6
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s WHERE id IN(16, 17, 18, 19) OR id IN(8, 9, 10, 11) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6

    #-- ----------------------------------------------------------------
    #-- Aggregate Cardinality
    #-- ----------------------------------------------------------------

    # No rows selected
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id > 100;' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 0

    # 0
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 1

    # 1
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 1

    # 1
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(4, 5, 6, 7) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 2


def union_agg_3(table_name):
    """test union agg"""
    # 1
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(8, 9, 10, 11);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 1

    # 1
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(8, 9, 10, 11) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 2

    # 2
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(8, 9, 10, 11) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 2

    # 2
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(8, 9, 10, 11) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 2

    # 3
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(12, 13, 14, 15);' \
           % table_name
    r = execute(line)
    # assert int(r[0][0]) == 3

    # 3
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 4
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 4

    # 4
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 4
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(8, 9, 10, 11);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 4

    # 4
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(8, 9, 10, 11) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 5
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 5
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(12, 13, 14, 15) \
            OR id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5


def union_agg_4(table_name):
    """
    # Don't feel like a full sparse/explicit permuatation is adding
    # anything here...just replace explicit w / sparse.
    """
    # 4
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(16, 17, 18, 19);' \
           % table_name
    r = execute(line)
    # assert int(r[0][0]) == 4

    # 4
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6

    # 5
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 5
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6

    # 5
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(8, 9, 10, 11);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 5

    # 5
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(8, 9, 10, 11) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6

    # 6
    line = 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6

    # 6
    line= 'SELECT ceiling(bitmap_union_count(bitmap_set)) FROM %s WHERE id IN(16, 17, 18, 19) \
            OR id IN(8, 9, 10, 11) OR id IN(4, 5, 6, 7) OR id IN(0, 1, 2, 3);' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 6

    # 0
    time.sleep(10)
    line = 'SELECT count(id) FROM %s WHERE bitmap_set is null' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 0

    # # 20
    # line = 'SELECT count(hll_cardinality(bitmap_set)) FROM %s WHERE hll_cardinality(bitmap_set) = 1' \
    #        % table_name
    # r = execute(line)
    # # assert int(r[0][0]) == 20
    #
    # # 0
    # line = 'SELECT count(id) FROM %s WHERE hll_cardinality(bitmap_set) is null' % table_name
    # r = execute(line)
    # # assert int(r[0][0]) == 0

    # 9
    line = 'SELECT bitmap_union_count(bitmap_set) FROM %s' % table_name
    r = execute(line)
    # assert int(r[0][0]) == 9


if __name__ == '__main__':
    setup_module()
    print(client)
    print('hello')
