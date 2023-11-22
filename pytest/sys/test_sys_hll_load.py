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
#   @file test_sys_load.py
#   @date 2017-04-10 15:02:22
#   @brief This file is a test file for palo small load in complex scenarios.
#
#############################################################################

"""
测试hll data type
"""
import numpy
import sys
import time
import pytest
from data import hll_load as DATA

sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import util
import palo_logger
import palo_exception

client = None
config = palo_config.config
local_data_file = '../hdfs/data/qe/xaaa'
local_data_file1 = '../hdfs/data/qe/baseall.txt'
hdfs_test = palo_config.gen_remote_file_path("/qe/xaaa")
hdfs_baseall = palo_config.gen_remote_file_path("/qe/baseall.txt")
hdfs_x = palo_config.gen_remote_file_path("/qe/x0")
broker_info = palo_config.broker_info

client = None
HLL_ERROR = 100000

#日志 异常 对象
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage
PaloClientException = palo_exception.PaloException

CLUSTER = 'default_cluster'
test_load_db = 'test_load_db'
compare = 'test_query_qa.test'
compare_distinct_count = ((255, 39388, 60008, 60010, 9484, 19815, 19767, 60013, 59956, 30579, 9),)
compare_distinct_count_1 = ((255, 39388, 60008, 60010, 9484),)
compare_distinct_count_2 = ((19815, 19767, 60013, 59956, 30579, 9),)
hll_union_sql_1 = 'SELECT hll_union_agg(k1_hll), hll_union_agg(k2_hll), hll_union_agg(k3_hll), \
                  hll_union_agg(k4_hll), hll_union_agg(k5_hll) FROM %s'
hll_union_sql_2 = 'SELECT hll_union_agg(k6_hll), hll_union_agg(k7_hll), hll_union_agg(k8_hll), \
                  hll_union_agg(k9_hll), hll_union_agg(k10_hll), hll_union_agg(k11_hll) FROM %s'
ndv_sql_1 = 'SELECT ndv(k1), ndv(k2), ndv(k3), ndv(k4), ndv(k5) FROM %s'
ndv_sql_2 = 'SELECT ndv(k6), ndv(k7), ndv(k8), ndv(k9), ndv(k10), ndv(k11) FROM %s'
hll_cardinality_1 = 'SELECT hll_cardinality(k1_hll), \
                    hll_cardinality(k2_hll), hll_cardinality(k3_hll), hll_cardinality(k4_hll), \
                    hll_cardinality(k5_hll) FROM %s ORDER BY %s'
hll_cardinality_2 = 'SELECT hll_cardinality(k6_hll), hll_cardinality(k7_hll), \
                    hll_cardinality(k8_hll), hll_cardinality(k9_hll), hll_cardinality(k10_hll), \
                    hll_cardinality(k11_hll) FROM %s ORDER BY %s'
ndv_cardinality_1 = 'SELECT ndv(k1), ndv(k2), ndv(k3), ndv(k4), ndv(k5) FROM %s GROUP BY %s \
                    ORDER BY %s '
ndv_cardinality_2 = 'SELECT ndv(k6), ndv(k7), ndv(k8), ndv(k9), ndv(k10), ndv(k11) FROM %s \
                    GROUP BY %s ORDER BY %s'
distinct_cardinality_1 = 'SELECT count(distinct k1), count(distinct k2), count(distinct k3), \
                        count(distinct k4),count(distinct k5) FROM %s GROUP BY %s ORDER BY %s'
distinct_cardinality_2 = 'SELECT count(distinct k6), \
                        count(distinct k7), count(distinct k8), count(distinct k9), \
                        count(distinct k10), count(distinct k11) FROM %s GROUP BY %s ORDER BY %s'


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)


def wait_end(database_name):
    """
    wait to finished
    """
    ret = True
    print('waitint for load...')
    state = None
    while ret:
        job_list = client.get_load_job_list(database_name=database_name)
        state = job_list[-1][2]
        if state == "FINISHED" or state == "CANCELLED":
            print(state)
            ret = False
        time.sleep(1)
    assert state == "FINISHED"


def wait_all_end(database_name):
    """
    wait to finished
    """
    ret = True
    flag = 0
    while ret:
        job_list = client.get_load_job_list(database_name=database_name, cluster_name=CLUSTER)
        for job in job_list:
            state = job[2]
            label = job[1]
            while state != "FINISHED" and state != "CANCELLED":
                time.sleep(1)
                state = client.get_load_job_state(label, database_name, cluster_name=CLUSTER)
                if state == "CANCELLED":
                    flag += 1
        ret = False
    assert flag == 0


def execute(line):
    """execute sql"""
    print(line)
    palo_result = client.execute(line)
    return palo_result


def bulk_load(client, table_family_name, load_label, data_file, max_filter_ratio=0,
              column_name_list=None, timeout=100, database_name=None, backend_id=None,
              is_wait=False, be_user="root", be_password="", hll_column=None):
    """
    小批量导入
    """
    pass


def init(db_name, table_name, create_sql, pull_load=True):
    """
    create db, table, bulk load, batch load
    Args:
        db_name:
        table_name:
        create_sql:
        key_column:

    Returns:
    """
    # create db & table1 & table2
    client.execute('drop database if exists %s' % db_name)
    client.create_database(db_name)
    client.use(db_name)
    table_list = list()
    if pull_load:
        table = pull_load_init(table_name, db_name, create_sql)
        table_list.append(table)
    wait_all_end(db_name)
    return table_list


def pull_load_init(table_name, db_name, create_sql):
    """create table and init data by pull load"""
    table3 = table_name + '_pull'
    sql = create_sql % table3
    ret = client.execute(sql)
    assert ret == ()
    broker_list = client.get_broker_list()
    broker = broker_list[0][0]

    set_list = list(DATA.columns_func)
    data_load_info = palo_client.LoadDataInfo(hdfs_baseall, table3, 
                     column_name_list=DATA.data_columns, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_load_info, database_name=db_name, broker=broker_info)
    set_list = list(DATA.columns_func)
    data_load_info = palo_client.LoadDataInfo(hdfs_test, table3, column_name_list=DATA.data_columns,
                                              set_list=set_list)
    ret = client.batch_load(util.get_label(), data_load_info, database_name=db_name, broker=broker_info)
    return table3


def test_load_k1():
    """
    {
    "title": "test_sys_hll_load.test_load_k1",
    "describe": "k1 is the only key, load and aggregate",
    "tag": "function,p1"
    }
    """
    """
    k1 is the only key, load and aggregate
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    table_list = init(db_name, table_name, DATA.k1_hll)
    p_key = 'k1'
    # test hll_cardinality
    distinct_cardinality_r_1 = client.execute(distinct_cardinality_1 % (compare, p_key, p_key))
    distinct_cardinality_r_2 = client.execute(distinct_cardinality_2 % (compare, p_key, p_key))
    for table in table_list:
        hll_cardinality_r = client.execute(hll_cardinality_1 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_1 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_1)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_1)
        hll_cardinality_r = client.execute(hll_cardinality_2 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_2 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_2)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_2)

    # test hll_union_agg
    r = tuple(compare_distinct_count)
    r_r_1 = tuple(compare_distinct_count_1)
    r_r_2 = tuple(compare_distinct_count_2)
    r_n_1 = execute(ndv_sql_1 % compare)
    r_n_2 = execute(ndv_sql_2 % compare)
    check_union_agg(r_n_1[0], r_r_1[0])
    check_union_agg(r_n_2[0], r_r_2[0])
    for table in table_list:
        print('hll union check, table: ', table)
        r1 = execute(hll_union_sql_1 % table)
        r2 = execute(hll_union_sql_2 % table)
        check_union_agg(r1[0], r_r_1[0])
        check_union_agg(r2[0], r_r_2[0])
    client.clean(db_name)


def test_load_k2():
    """
    {
    "title": "test_sys_hll_load.test_load_k2",
    "describe": "k2 is the only key, load and aggregate",
    "tag": "function,p1"
    }
    """
    """
    k2 is the only key, load and aggregate
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    table_list = init(db_name, table_name, DATA.k2_hll)
    p_key = 'k2'
    # test hll_cardinality
    """
    distinct_cardinality_r_1 = client.execute(distinct_cardinality_1 % (compare, p_key, p_key))
    distinct_cardinality_r_2 = client.execute(distinct_cardinality_2 % (compare, p_key, p_key))
    for table in table_list:
        hll_cardinality_r = client.execute(hll_cardinality_1 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_1 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_1)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_1)
        hll_cardinality_r = client.execute(hll_cardinality_2 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_2 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_2)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_2)
    """
    # test hll_union_agg
    r = tuple(compare_distinct_count)
    r_r_1 = tuple(compare_distinct_count_1)
    r_r_2 = tuple(compare_distinct_count_2)
    r_n_1 = execute(ndv_sql_1 % compare)
    r_n_2 = execute(ndv_sql_2 % compare)
    check_union_agg(r_n_1[0], r_r_1[0])
    check_union_agg(r_n_2[0], r_r_2[0])
    for table in table_list:
        print('hll union check, table: ', table)
        r1 = execute(hll_union_sql_1 % table)
        r2 = execute(hll_union_sql_2 % table)
        check_union_agg(r1[0], r_r_1[0])
        check_union_agg(r2[0], r_r_2[0])
    client.clean(db_name)


def test_load_k3():
    """
    {
    "title": "test_sys_hll_load.test_load_k3",
    "describe": "k3 is the only key, load and aggregate",
    "tag": "function,p1"
    }
    """
    """
    k3 is the only key, load and aggregate
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    table_list = init(db_name, table_name, DATA.k3_hll)
    p_key = 'k3'
    """
    # test hll_cardinality
    distinct_cardinality_r_1 = client.execute(distinct_cardinality_1 % (compare, p_key, p_key))
    distinct_cardinality_r_2 = client.execute(distinct_cardinality_2 % (compare, p_key, p_key))
    for table in table_list:
        hll_cardinality_r = client.execute(hll_cardinality_1 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_1 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_1)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_1)
        hll_cardinality_r = client.execute(hll_cardinality_2 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_2 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_2)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_2)
    """
    # test hll_union_agg
    r = tuple(compare_distinct_count)
    r_r_1 = tuple(compare_distinct_count_1)
    r_r_2 = tuple(compare_distinct_count_2)
    r_n_1 = execute(ndv_sql_1 % compare)
    r_n_2 = execute(ndv_sql_2 % compare)
    check_union_agg(r_n_1[0], r_r_1[0])
    check_union_agg(r_n_2[0], r_r_2[0])
    for table in table_list:
        print('hll union check, table: ', table)
        r1 = execute(hll_union_sql_1 % table)
        r2 = execute(hll_union_sql_2 % table)
        check_union_agg(r1[0], r_r_1[0])
        check_union_agg(r2[0], r_r_2[0])
    client.clean(db_name)


def test_load_k4():
    """
    {
    "title": "test_sys_hll_load.test_load_k4",
    "describe": "k4 is the only key, test load and aggregate",
    "tag": "function,p1"
    }
    """
    """
    k4 is the only key, test load and aggregate
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    table_list = init(db_name, table_name, DATA.k4_hll)
    p_key = 'k4'
    """
    # test hll_cardinality
    distinct_cardinality_r_1 = client.execute(distinct_cardinality_1 % (compare, p_key, p_key))
    distinct_cardinality_r_2 = client.execute(distinct_cardinality_2 % (compare, p_key, p_key))
    for table in table_list:
        hll_cardinality_r = client.execute(hll_cardinality_1 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_1 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_1)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_1)
        hll_cardinality_r = client.execute(hll_cardinality_2 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_2 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_2)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_2)
    """
    # test hll_union_agg
    r = tuple(compare_distinct_count)
    r_r_1 = tuple(compare_distinct_count_1)
    r_r_2 = tuple(compare_distinct_count_2)
    r_n_1 = execute(ndv_sql_1 % compare)
    r_n_2 = execute(ndv_sql_2 % compare)
    check_union_agg(r_n_1[0], r_r_1[0])
    check_union_agg(r_n_2[0], r_r_2[0])
    for table in table_list:
        print('hll union check, table: ', table)
        r1 = execute(hll_union_sql_1 % table)
        r2 = execute(hll_union_sql_2 % table)
        check_union_agg(r1[0], r_r_1[0])
        check_union_agg(r2[0], r_r_2[0])
    client.clean(db_name)


def test_load_k5():
    """
    {
    "title": "test_sys_hll_load.test_load_k5",
    "describe": "k5 is the only key, test load and aggregate",
    "tag": "function,p1"
    }
    """
    """
    k5 is the only key, test load and aggregate
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    table_list = init(db_name, table_name, DATA.k5_hll)
    p_key = 'k5'
    """
    # test hll_cardinality
    distinct_cardinality_r_1 = client.execute(distinct_cardinality_1 % (compare, p_key, p_key))
    distinct_cardinality_r_2 = client.execute(distinct_cardinality_2 % (compare, p_key, p_key))
    for table in table_list:
        hll_cardinality_r = client.execute(hll_cardinality_1 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_1 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_1)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_1)
        hll_cardinality_r = client.execute(hll_cardinality_2 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_2 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_2)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_2)
    """
    # test hll_union_agg
    r = tuple(compare_distinct_count)
    r_r_1 = tuple(compare_distinct_count_1)
    r_r_2 = tuple(compare_distinct_count_2)
    r_n_1 = execute(ndv_sql_1 % compare)
    r_n_2 = execute(ndv_sql_2 % compare)
    check_union_agg(r_n_1[0], r_r_1[0])
    check_union_agg(r_n_2[0], r_r_2[0])
    for table in table_list:
        print('hll union check, table: ', table)
        r1 = execute(hll_union_sql_1 % table)
        r2 = execute(hll_union_sql_2 % table)
        check_union_agg(r1[0], r_r_1[0])
        check_union_agg(r2[0], r_r_2[0])
    client.clean(db_name)


def test_load_k6():
    """
    {
    "title": "test_sys_hll_load.test_load_k6",
    "describe": "k6 is the only key, test load and aggregate",
    "tag": "function,p1"
    }
    """
    """
    k6 is the only key, test load and aggregate
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    table_list = init(db_name, table_name, DATA.k6_hll)
    p_key = 'k6'
    """
    # test hll_cardinality
    distinct_cardinality_r_1 = client.execute(distinct_cardinality_1 % (compare, p_key, p_key))
    distinct_cardinality_r_2 = client.execute(distinct_cardinality_2 % (compare, p_key, p_key))
    for table in table_list:
        hll_cardinality_r = client.execute(hll_cardinality_1 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_1 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_1)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_1)
        hll_cardinality_r = client.execute(hll_cardinality_2 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_2 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_2)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_2)
    """
    # test hll_union_agg
    r = tuple(compare_distinct_count)
    r_r_1 = tuple(compare_distinct_count_1)
    r_r_2 = tuple(compare_distinct_count_2)
    r_n_1 = execute(ndv_sql_1 % compare)
    r_n_2 = execute(ndv_sql_2 % compare)
    check_union_agg(r_n_1[0], r_r_1[0])
    check_union_agg(r_n_2[0], r_r_2[0])
    for table in table_list:
        print('hll union check, table: ', table)
        r1 = execute(hll_union_sql_1 % table)
        r2 = execute(hll_union_sql_2 % table)
        check_union_agg(r1[0], r_r_1[0])
        check_union_agg(r2[0], r_r_2[0])
    client.clean(db_name)


def test_load_k7():
    """
    {
    "title": "test_sys_hll_load.test_load_k7",
    "describe": "k7 is the only key, test load and aggregate",
    "tag": "function,p1"
    }
    """
    """
    k7 is the only key, test load and aggregate
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    table_list = init(db_name, table_name, DATA.k7_hll)
    p_key = 'k7'
    """
    # test hll_cardinality
    distinct_cardinality_r_1 = client.execute(distinct_cardinality_1 % (compare, p_key, p_key))
    distinct_cardinality_r_2 = client.execute(distinct_cardinality_2 % (compare, p_key, p_key))
    for table in table_list:
        hll_cardinality_r = client.execute(hll_cardinality_1 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_1 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_1)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_1)
        hll_cardinality_r = client.execute(hll_cardinality_2 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_2 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_2)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_2)
    """
    # test hll_union_agg
    r = tuple(compare_distinct_count)
    r_r_1 = tuple(compare_distinct_count_1)
    r_r_2 = tuple(compare_distinct_count_2)
    r_n_1 = execute(ndv_sql_1 % compare)
    r_n_2 = execute(ndv_sql_2 % compare)
    check_union_agg(r_n_1[0], r_r_1[0])
    check_union_agg(r_n_2[0], r_r_2[0])
    for table in table_list:
        print('hll union check, table: ', table)
        r1 = execute(hll_union_sql_1 % table)
        r2 = execute(hll_union_sql_2 % table)
        check_union_agg(r1[0], r_r_1[0])
        check_union_agg(r2[0], r_r_2[0])
    client.clean(db_name)


def test_load_k10():
    """
    {
    "title": "test_sys_hll_load.test_load_k10",
    "describe": "k10 is the only key, test load and aggregate",
    "tag": "function,p1"
    }
    """
    """
    k10 is the only key, test load and aggregate
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    table_list = init(db_name, table_name, DATA.k10_hll)
    p_key = 'k10'
    """
    # test hll_cardinality
    distinct_cardinality_r_1 = client.execute(distinct_cardinality_1 % (compare, p_key, p_key))
    distinct_cardinality_r_2 = client.execute(distinct_cardinality_2 % (compare, p_key, p_key))
    for table in table_list:
        hll_cardinality_r = client.execute(hll_cardinality_1 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_1 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_1)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_1)
        hll_cardinality_r = client.execute(hll_cardinality_2 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_2 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_2)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_2)
    """
    # test hll_union_agg
    r = tuple(compare_distinct_count)
    r_r_1 = tuple(compare_distinct_count_1)
    r_r_2 = tuple(compare_distinct_count_2)
    r_n_1 = execute(ndv_sql_1 % compare)
    r_n_2 = execute(ndv_sql_2 % compare)
    check_union_agg(r_n_1[0], r_r_1[0])
    check_union_agg(r_n_2[0], r_r_2[0])
    for table in table_list:
        print('hll union check, table: ', table)
        r1 = execute(hll_union_sql_1 % table)
        r2 = execute(hll_union_sql_2 % table)
        check_union_agg(r1[0], r_r_1[0])
        check_union_agg(r2[0], r_r_2[0])
    client.clean(db_name)


def test_load_k11():
    """
    {
    "title": "test_sys_hll_load.test_load_k11",
    "describe": "k11 is the only key, test load and aggregate",
    "tag": "function,p1"
    }
    """
    """
    k11 is the only key, test load and aggregate
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    table_list = init(db_name, table_name, DATA.k11_hll)
    p_key = 'k11'
    # test hll_cardinality
    distinct_cardinality_r_1 = client.execute(distinct_cardinality_1 % (compare, p_key, p_key))
    distinct_cardinality_r_2 = client.execute(distinct_cardinality_2 % (compare, p_key, p_key))
    for table in table_list:
        hll_cardinality_r = client.execute(hll_cardinality_1 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_1 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_1)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_1)
        hll_cardinality_r = client.execute(hll_cardinality_2 % (table, p_key))
        ndv_cardinality_r = client.execute(ndv_cardinality_2 % (table, p_key, p_key))
        check_mat(hll_cardinality_r, distinct_cardinality_r_2)
        check_mat(ndv_cardinality_r, distinct_cardinality_r_2)

    # test hll_union_agg
    r = tuple(compare_distinct_count)
    r_r_1 = tuple(compare_distinct_count_1)
    r_r_2 = tuple(compare_distinct_count_2)
    r_n_1 = execute(ndv_sql_1 % compare)
    r_n_2 = execute(ndv_sql_2 % compare)
    check_union_agg(r_n_1[0], r_r_1[0])
    check_union_agg(r_n_2[0], r_r_2[0])
    for table in table_list:
        print('hll union check, table: ', table)
        r1 = execute(hll_union_sql_1 % table)
        r2 = execute(hll_union_sql_2 % table)
        check_union_agg(r1[0], r_r_1[0])
        check_union_agg(r2[0], r_r_2[0])
    client.clean(db_name)


def check_mat(hll_result, correct_result):
    """check mat """
    print('the length is : ', len(hll_result))
    if len(hll_result) != len(correct_result):
        print('hll table length is %s' % len(hll_result))
        print('actual length is %s' % len(correct_result))
        assert 0 == 1, 'result len is diff'
    hll_mat = numpy.mat(hll_result, dtype='int')
    correct_mat = numpy.mat(correct_result, dtype='int')
    minus_mat = abs(hll_mat - correct_mat)
    tmp = numpy.mat(correct_result, dtype='float64')
    deviation = minus_mat / tmp
    print('每列的最大误差:\n %s' % numpy.max(deviation, axis=0))
    print('每列的最小误差:\n %s' % numpy.min(deviation, axis=0))
    print('每列的平均误差:\n %s' % numpy.mean(deviation, axis=0))


def check_union_agg(hll_result, correct_result):
    """check union agg"""
    if len(hll_result) == len(correct_result):
        for i in range(0, len(hll_result)):
            deviation = compute_deviation(int(hll_result[i]), int(correct_result[i]))
            print('%s deviation: %s' % (DATA.columns[i * 2], deviation))
            assert deviation < HLL_ERROR


def compute_deviation(actual, computed):
    """
    Args:
        actual: 实际值
        computed: 计算值
    Returns: 误差
    """
    tmp = abs(actual - computed)
    return tmp / float(actual)


if __name__ == '__main__':
    setup_module()
    test_load_k2()



