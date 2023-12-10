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
#   @file test_sys_bitmap_function.py
#   @date 2020-02-20
#
#############################################################################

"""
测试bitmap data type
"""
import os
import sys
import time
import random
sys.path.append("../../")
from lib import palo_config
from lib import palo_client
from lib import util
from lib import palo_job
from lib import common
import palo_logger
import palo_exception

client = None

#日志 异常 对象
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage
PaloClientException = palo_exception.PaloException

config = palo_config.config
compare = 'test_query_qa.test'

file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port)


def wait_end(database_name):
    """
    wait to finished
    """
    ret = True
    print('waitint for load...')
    state = None
    while ret:
        job_list = client.get_load_job_list(database_name=database_name)
        state = palo_job.LoadJob(job_list[-1]).get_state()
        # print(state)
        if state == "FINISHED" or state == "CANCELLED":
            print(state)
            ret = False
        time.sleep(1)
    assert state == "FINISHED"


def execute(line):
    """execte sql"""
    print(line)
    palo_result = client.execute(line)
    print(palo_result)
    return palo_result


def init(db_name, table_name):
    """
    create db, table, bulk load, batch load
    Args:
        db_name:
        table_name:
        create_sql:
        key_column:

    Returns:
    """
    line = 'DROP DATABASE IF EXISTS %s' % db_name
    client.execute(line)
    client.create_database(db_name)
    client.use(db_name)
    # client.execute('drop table if exists %s' % table_name)
    line = 'CREATE TABLE %s (\
    `id`  int COMMENT "", \
    `id1` tinyint COMMENT "", \
    `c_float` float SUM COMMENT "", \
    `bitmap_set1` bitmap bitmap_union COMMENT "", \
    `bitmap_set2` bitmap bitmap_union COMMENT "", \
    `bitmap_set3` bitmap bitmap_union COMMENT "" \
    ) ENGINE=OLAP \
     DISTRIBUTED BY HASH(`id`, `id1`) BUCKETS 5 \
     PROPERTIES ( \
    "storage_type" = "COLUMN" \
    );' % table_name
    execute(line)
    line = 'insert into %s select k4, k1, k9, bitmap_hash(k9), bitmap_hash(k9),' \
           ' bitmap_hash(k3) from %s' % (table_name, compare)
    execute(line)
    # wait_end(db_name)


def init_for_intersect(db_name, table_name):
    """
    create db, table, bulk load, batch load
    Args:
        db_name:
        table_name:
        create_sql:
        key_column:

    Returns:
    """
    line = 'DROP DATABASE IF EXISTS %s' % db_name
    client.execute(line)
    client.create_database(db_name)
    client.use(db_name)
    # client.execute('drop table if exists %s' % table_name)

    line = """
        CREATE TABLE `pv_bitmap` (
          `dt` int(11) NULL COMMENT "",
          `page` varchar(10) NULL COMMENT "",
          `user_id_bitmap` bitmap BITMAP_UNION NULL COMMENT ""
        ) ENGINE=OLAP
        AGGREGATE KEY(`dt`, `page`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`dt`) BUCKETS 2;
        """
    execute(line)

    line = """
        CREATE TABLE `pv_base` (
          `dt` int(11) NULL COMMENT "",
          `page` varchar(10) NULL COMMENT "",
          `user_id_str` varchar(40) NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`dt`, `page`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`dt`) BUCKETS 2;
        """
    execute(line)

    file_path = palo_config.gen_remote_file_path('sys/bitmap_load/bitmap_test.data')

    set_list = ['user_id_bitmap = to_bitmap(id)']
    column_name_list = ['dt', 'page', 'id']
    data_desc_list = palo_client.LoadDataInfo(file_path,
                                              'pv_bitmap', column_name_list=column_name_list, set_list=set_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.7)

    column_name_list = ['dt', 'page', 'user_id_str']
    data_desc_list = palo_client.LoadDataInfo(file_path,
                                              'pv_base', column_name_list=column_name_list)
    assert client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.7)


def test_bitmap_and():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_and",
    "describe": "test_bitmap_and",
    "tag": "function,p1"
    }
    """
    """
    test_bitmap_and
    计算两个输入bitmap的交集，返回新的bitmap.
    :return:
    """
    line = "select bitmap_to_string(bitmap_and(to_bitmap(NULL), to_bitmap(2))) cnt"
    r = execute(line)
    assert r[0][0] == '', "expect:NULL, actual:%s" % r[0][0]
    line = "select bitmap_to_string(bitmap_and(to_bitmap(NULL), bitmap_hash(2))) cnt"
    r = execute(line)
    assert r[0][0] == '', "expect:NULL, actual:%s" % r[0][0]
    line = "select bitmap_count(bitmap_and(to_bitmap(NULL), bitmap_hash(2))) cnt"
    r = execute(line)
    assert r[0][0] == 0, "expect:0, actual:%s" % r[0][0]

    line = "select bitmap_count(bitmap_and(to_bitmap(NULL), bitmap_hash(NULL))) cnt"
    r = execute(line)
    assert r[0][0] == 0, "expect:0, actual:%s" % r[0][0]

    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init(db_name, table_name)
    client.use(db_name)

    line1 = 'select bitmap_union_count(bitmap_and(bitmap_set1,bitmap_set2)) from {0}'.format(table_name)
    ret1 = execute(line1)
    line2 = 'select count(distinct(bitmap_to_string(bitmap_set1))) from {0}' \
            ' where bitmap_to_string(bitmap_set1)=bitmap_to_string(bitmap_set2)'.format(table_name)
    ret2 = execute(line2)
    assert ret1 == ret2

    line1 = 'select bitmap_union_count(bitmap_and(bitmap_set1,bitmap_set3)) from {0}'.format(table_name)
    ret1 = execute(line1)
    line2 = 'select count(distinct(bitmap_to_string(bitmap_set1))) from {0}' \
            ' where bitmap_to_string(bitmap_set1)=bitmap_to_string(bitmap_set3)'.format(table_name)
    ret2 = execute(line2)
    assert ret1 == ret2
    client.clean(db_name)


def test_bitmap_empty():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_empty",
    "describe": "test_bitmap_empty",
    "tag": "function,p1"
    }
    """
    """
    test_bitmap_empty
    返回一个空bitmap
    :return:
    """
    line = "select bitmap_count(bitmap_empty());"
    r = execute(line)
    assert r[0][0] == 0
    line = "select bitmap_to_string(bitmap_empty());"
    r = execute(line)
    assert r[0][0] == ''
    # todo 增加load的case


def test_bitmap_or():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_or",
    "describe": "test_bitmap_or",
    "tag": "function,p1"
    }
    """
    """
    test_bitmap_or
    计算两个输入bitmap的并集，返回新的bitmap.
    :return:
    """
    # line = "select bitmap_to_string(bitmap_or(to_bitmap(NULL), to_bitmap(2))) cnt"
    # r = execute(line)
    # assert r[0][0] == '2'
    # line = "select bitmap_to_string(bitmap_or(to_bitmap(NULL), bitmap_hash(2))) cnt"
    # r = execute(line)
    # assert r[0][0] == '2'
    # todo 目前会出core
    # line = "select bitmap_count(bitmap_and(to_bitmap(NULL), bitmap_hash(2))) cnt"
    # r = execute(line)
    # assert r[0][0] == 0
    # line = "select bitmap_count(bitmap_or(to_bitmap(NULL), bitmap_hash(NULL))) cnt"
    # r = execute(line)
    # assert r[0][0] == 0

    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init(db_name, table_name)
    client.use(db_name)

    line1 = 'select bitmap_union_count(bitmap_or(bitmap_set1,bitmap_set2)) from {0}'.format(table_name)
    ret1 = execute(line1)
    line2 = """with tmp as (
                select bitmap_to_string(bitmap_set1) as a from {0}
                union select bitmap_to_string(bitmap_set2) as a from {0})
            select count(distinct(a)) from tmp limit 1""".format(table_name)
    ret2 = execute(line2)
    assert ret1 == ret2

    line1 = 'select bitmap_union_count(bitmap_or(bitmap_set1,bitmap_set3)) from {0}'.format(table_name)
    ret1 = execute(line1)
    line2 = """with tmp as (
                select bitmap_to_string(bitmap_set1) as a from {0}
                union select bitmap_to_string(bitmap_set3) as a from {0})
            select count(distinct(a)) from tmp limit 1""".format(table_name)
    ret2 = execute(line2)
    assert ret1 == ret2
    client.clean(db_name)


def test_bitmap_count_udaf():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_count_udaf",
    "describe": "test_bitmap_count_udaf",
    "tag": "function,p1"
    }
    """
    """test_bitmap_count_udaf"""
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init_for_intersect(db_name, table_name)

    user_id_str_limit = 'user_id_str>0 and user_id_str<18446744073709551615'

    line = 'select dt,page,bitmap_union_count(user_id_bitmap) from pv_bitmap group by dt,page order by dt,page'
    ret1 = client.execute(line)
    line = 'select dt,page,bitmap_count(bitmap_union(user_id_bitmap)) from pv_bitmap group by dt,page order by dt,page'
    ret2 = client.execute(line)
    line = 'select dt,page,count(user_id_str) from pv_base where {0}' \
           ' group by dt,page order by dt,page'.format(user_id_str_limit)
    ret3 = client.execute(line)
    assert ret1 == ret2
    assert ret1 == ret3

    line = 'select dt,bitmap_union_count(user_id_bitmap) from pv_bitmap group by dt order by dt'
    ret1 = client.execute(line)
    line = 'select dt,bitmap_count(bitmap_union(user_id_bitmap)) from pv_bitmap group by dt order by dt'
    ret2 = client.execute(line)
    line = 'select dt,count(user_id_str) from pv_base where {0}' \
           ' group by dt order by dt'.format(user_id_str_limit)
    ret3 = client.execute(line)
    assert ret1 == ret2
    assert ret1 == ret3

    line = 'select dt,page,bitmap_union_count(user_id_bitmap) from pv_bitmap group by dt,page order by dt,page'
    ret1 = client.execute(line)
    line = 'select dt,page,bitmap_count(bitmap_union(user_id_bitmap)) from pv_bitmap' \
           ' group by dt,page order by dt,page'
    ret2 = client.execute(line)
    line = 'select dt,page,count(user_id_str) from pv_base where {0}' \
           ' group by dt,page order by dt,page'.format(user_id_str_limit)
    ret3 = client.execute(line)
    assert ret1 == ret2
    assert ret1 == ret3

    client.clean(db_name)


def test_bitmap_count_udaf2():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_count_udaf2",
    "describe": "test_bitmap_count_udaf2",
    "tag": "function,p1"
    }
    """
    """test_bitmap_count_udaf2"""
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    line = 'DROP DATABASE IF EXISTS %s' % db_name
    client.execute(line)
    client.create_database(db_name)
    client.use(db_name)

    line = 'CREATE TABLE test_bitmap_tb ( id int COMMENT "", id1 tinyint COMMENT "", c_float float SUM COMMENT "",' \
           ' bitmap_set bitmap BITMAP_UNION COMMENT "" ) ENGINE=OLAP' \
           ' DISTRIBUTED BY HASH(id, id1) BUCKETS 5 PROPERTIES ( "storage_type" = "COLUMN" );'
    client.execute(line)
    line = 'insert into test_bitmap_tb select k3, k1, k9, TO_BITMAP(k3) from test_query_qa.baseall;'
    client.execute(line)
    line = 'select id,id1,c_float,bitmap_count(bitmap_set) from test_bitmap_tb where id=2147483647;'
    ret2 = client.execute(line)

    for i in range(len(ret2)):
        assert ret2[i][3] == 1
    line = 'select id,id1,c_float,bitmap_count(bitmap_set) from test_bitmap_tb where id=-2147483647;'
    ret3 = client.execute(line)
    # "to_bitmap(-2147483647) should be 0"
    for i in range(len(ret2)):
        assert ret3[i][3] == 0, 'expect: 0, actural: %s' % ret3[i][3]

    client.clean(db_name)


def test_intersect_count_udaf():
    """
    {
    "title": "test_sys_bitmap_function.test_intersect_count_udaf",
    "describe": "test_intersect_count_udaf",
    "tag": "function,p1"
    }
    """
    """test_intersect_count_udaf"""
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init_for_intersect(db_name, table_name)

    user_id_str_limit = 'user_id_str>0 and user_id_str<18446744073709551615'

    date_list = ['20191001', '20191002', '20191003', '20191004', '20191005']
    d1, d2, d3 = random.sample(date_list, 3)

    line = """
        select intersect_count(user_id_bitmap, dt, '{0}') as first_day,
            intersect_count(user_id_bitmap, dt, '{1}') as second_day,
            intersect_count(user_id_bitmap, dt, '{2}') as third_day
        from pv_bitmap
        where dt in ('{0}', '{1}', '{2}');
        """.format(d1, d2, d3)
    ret1 = client.execute(line)
    line = """
        with t1 as (select count(distinct(user_id_str)) from pv_base where dt='{0}' and {3}),
            t2 as (select count(distinct(user_id_str)) from pv_base where dt='{1}' and {3}),
            t3 as (select count(distinct(user_id_str)) from pv_base where dt='{2}' and {3})
        select * from t1, t2, t3;
    """.format(d1, d2, d3, user_id_str_limit)
    ret2 = client.execute(line)
    assert ret1 == ret2

    line = """
            select intersect_count(user_id_bitmap, dt, '{0}', '{1}', '{2}') as retention
            from pv_bitmap
            where dt in ('{0}', '{1}', '{2}');
            """.format(d1, d2, d3)
    ret1 = client.execute(line)
    line = """
            with t1 as (select distinct(user_id_str) from pv_base where dt='{0}' and {3}),
                t2 as (select distinct(user_id_str) from pv_base where dt='{1}' and {3}),
                t3 as (select distinct(user_id_str) from pv_base where dt='{2}' and {3})
            select count(t1.user_id_str)
            from t1 join t2 join t3
                on t1.user_id_str = t2.user_id_str and t1.user_id_str = t3.user_id_str;
        """.format(d1, d2, d3, user_id_str_limit)
    ret2 = client.execute(line)
    assert ret1 == ret2

    client.clean(db_name)


def test_bitmap_with_rollup():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_with_rollup",
    "describe": "test_bitmap_with_rollup",
    "tag": "function,p1"
    }
    """
    """test_bitmap_with_rollup"""
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init_for_intersect(db_name, table_name)

    tb = 'pv_bitmap'
    rollup_table_name = tb + '_index'
    column_name_list = ['dt', 'user_id_bitmap']

    client.create_rollup_table(tb, rollup_table_name, column_name_list, is_wait=True)

    time.sleep(5)

    line = 'select dt, bitmap_union_count(user_id_bitmap) from %s group by dt' % (tb,)
    rolllup = common.get_explain_rollup(client, line)
    assert rollup_table_name in rolllup, 'expect %s, actural %s' % (rollup_table_name, rolllup)
    client.clean(db_name)


def test_bitmap_contains():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_contains",
    "describe": "判断一个bitmap中是否包含指定的数值",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_contains(to_bitmap(4), 1), bitmap_contains(to_bitmap(1), 1), \
                bitmap_contains(to_bitmap(0), 0), bitmap_contains(to_bitmap(0), 1)"
    ret = execute(line)
    assert ret == ((0, 1, 1, 0), )
    line = "select bitmap_contains(bitmap_from_string('4, 5, 6, 7'), 4), \
            bitmap_contains(bitmap_from_string('4, 5, 6, 7'), 8), \
            bitmap_contains(bitmap_from_string('4, 4, 4, 4'), 4), \
            bitmap_contains(bitmap_from_string('423, 5'), 4), \
            bitmap_contains(bitmap_from_string('4, 00'), 0)"
    ret = execute(line)
    assert ret == ((1, 0, 1, 0, 1), )
    line = " select bitmap_contains(to_bitmap(9), 964291337)"
    assert execute(line) == ((0, ), )


def test_bitmap_from_string():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_from_string",
    "describe": "判断一个bitmap中是否包含指定的数值",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_to_string(bitmap_from_string('4, 5, 6')), bitmap_to_string(bitmap_from_string('0'))"
    ret = execute(line)
    assert ret == (('4,5,6', '0'), )
    line = "select bitmap_to_string(bitmap_from_string('abc')), bitmap_to_string(bitmap_from_string(NULL))"
    ret = execute(line)
    assert ret == ((None, None), )


def test_bitmap_has_any():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_has_any",
    "describe": "判断两个bitmap是否有交集",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_has_any(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_has_any(bitmap_from_string('1,2,3'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_has_any(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_has_any(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3,4,5,6'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_has_any(bitmap_from_string('1'), bitmap_from_string('1,1,1'))"
    ret = execute(line)
    assert ret == ((1, ), )


def test_bitmap_not():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_not",
    "describe": "第一个bitmap减去第二个bitmap",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_to_string(bitmap_not(bitmap_from_string('1,2,3,4'), bitmap_from_string('4')))"
    ret = execute(line)
    assert ret == (('1,2,3', ), )
    line = "select bitmap_to_string(bitmap_not(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6')))"
    ret = execute(line)
    assert ret == (('1,2,3', ), )
    line = "select bitmap_to_string(bitmap_not(bitmap_from_string('1,2,3'), bitmap_from_string('4,5,6')))"
    ret = execute(line)
    assert ret == (('1,2,3', ), )
    line = "select bitmap_to_string(bitmap_not(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3')))"
    ret = execute(line)
    assert ret == (('', ), )
    line = "select bitmap_to_string(bitmap_not(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3,4,5,6')))"
    ret = execute(line)
    assert ret == (('', ), )
    line = "select bitmap_to_string(bitmap_not(bitmap_from_string('1'), bitmap_from_string('1,1,1')))"
    ret = execute(line)
    assert ret == (('', ), )

    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init(db_name, table_name)
    client.use(db_name)

    line1 = 'select bitmap_union_count(bitmap_not(bitmap_set1,bitmap_set2)) from %s' % table_name
    ret1 = execute(line1)
    line2 = 'select count(distinct(bitmap_to_string(bitmap_set1))) - bitmap_union_count(bitmap_and(bitmap_set1, \
            bitmap_set2)) from %s' % table_name
    ret2 = execute(line2)
    assert ret1 == ret2, "bitmap_not function failed" 
    client.clean(db_name)


def test_bitmap_xor():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_xor",
    "describe": "返回两个bitmap异或的结果",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_to_string(bitmap_xor(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6')))"
    ret = execute(line)
    assert ret == (('1,2,3,5,6', ), )
    line = "select bitmap_to_string(bitmap_xor(bitmap_from_string('1,2,3'), bitmap_from_string('4,5,6')))"
    ret = execute(line)
    assert ret == (('1,2,3,4,5,6', ), )
    line = "select bitmap_to_string(bitmap_xor(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3')))"
    ret = execute(line)
    assert ret == (('', ), )
    line = "select bitmap_to_string(bitmap_xor(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3,4,5,6')))"
    ret = execute(line)
    assert ret == (('4,5,6', ), )
    line = "select bitmap_to_string(bitmap_xor(bitmap_from_string('1'), bitmap_from_string('1,1,1')))"
    ret = execute(line)
    assert ret == (('', ), )


def test_bitmap_min():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_min",
    "describe": "计算并返回bitmap中的最小值",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_min(bitmap_from_string(''))"
    ret = execute(line)
    assert ret == ((None, ), )
    line = "select bitmap_min(bitmap_from_string('10,99,100,1000,1,9999'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_min(bitmap_from_string('9,9,9,9,10,9'))"
    ret = execute(line)
    assert ret == ((9, ), )


def test_bitmap_union_int():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_union_int",
    "describe": "计算整型列的去重值",
    "tag": "function,p1"
    }
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init_for_intersect(db_name, table_name)
    user_id_str_limit = 'user_id_str>0 and user_id_str<18446744073709551615'
    line = 'select bitmap_union_int(cast(user_id_str as int)) from pv_base where %s group by dt,page order by dt,page' \
            % user_id_str_limit
    ret1 = client.execute(line)
    line = 'select count(distinct cast(user_id_str as int)) from pv_base where %s group by dt,page order by dt,page' \
            % user_id_str_limit
    ret2 = client.execute(line)
    assert ret1 == ret2
    client.clean(db_name)


def test_bitmap_union():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_union",
    "describe": "返回一组bitmap的并集",
    "tag": "function,p1"
    }
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init_for_intersect(db_name, table_name)
    user_id_str_limit = 'user_id_str>0 and user_id_str<18446744073709551615'
    line = 'select bitmap_count(bitmap_union(user_id_bitmap)) from pv_bitmap group by dt,page order by dt,page'
    ret1 = client.execute(line)
    line = 'select count(distinct user_id_str) from pv_base where %s group by dt,page order by dt,page' \
            % user_id_str_limit
    ret2 = client.execute(line)
    assert ret1 == ret2
    client.clean(db_name)


def test_bitmap_intersect():
    """
    {
    "title": "test_sys_bitmap_function.test_bitmap_intersect",
    "describe": "返回一组bitmap的交集",
    "tag": "function,p1"
    }
    """
    db_name, table_name, invalied_name_1 = util.gen_name_list()
    init_for_intersect(db_name, table_name)
    user_id_str_limit = 'user_id_str>0 and user_id_str<18446744073709551615'
    line = 'select count(*) from (select user_id_str from pv_base where dt="20191003" and page="baidu" \
            union all select user_id_str from pv_base where dt="20191001" and page="baidu") a where %s' \
            % user_id_str_limit
    ret1 = client.execute(line)
    line = 'select bitmap_count(bitmap_intersect(x)) from (select bitmap_union(user_id_bitmap) x from pv_bitmap \
            where dt in ("20191003", "20191001") and page="baidu") a' 
    ret2 = client.execute(line)
    assert ret1 == ret2
    client.clean(db_name)


def test_bitmap_or_count():
    """
    {
    "title": "test_sys_bitmap_function:test_bitmap_or_count",
    "describe": "计算两个输入bitmap的并集:返回并集的个数",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_or_count(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((6, ), )
    line = "select bitmap_or_count(bitmap_from_string('1,2,3'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((6, ), )
    line = "select bitmap_or_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3'))"
    ret = execute(line)
    assert ret == ((3, ), )
    line = "select bitmap_or_count(bitmap_from_string(''), bitmap_from_string('1,2,3,4,5,6'))"
    ret = execute(line)
    assert ret == ((6, ), )
    line = "select bitmap_or_count(bitmap_from_string('1,1'), bitmap_from_string('1,1,1,1,1,1,1'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_or_count(bitmap_from_string('101,110,100,1110'), bitmap_from_string('1,0,110,101,11111'))"
    ret = execute(line)
    assert ret == ((7, ), )


def test_bitmap_and_count():
    """
    {
    "title": "test_sys_bitmap_function:test_bitmap_and_count",
    "describe": "计算两个输入bitmap的交集，返回交集的个数",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_and_count(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_and_count(bitmap_from_string('1,2,3'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_and_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3'))"
    ret = execute(line)
    assert ret == ((3, ), )
    line = "select bitmap_and_count(bitmap_from_string(''), bitmap_from_string('1,2,3,4,5,6'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_and_count(bitmap_from_string('1,1'), bitmap_from_string('1,1,1,1,1,1,1'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_and_count(bitmap_from_string('101,110,100,1110'), bitmap_from_string('1,0,110,101,11111'))"
    ret = execute(line)
    assert ret == ((2, ), )


def test_bitmap_xor_count():
    """
    {
    "title": "test_sys_bitmap_function:test_bitmap_xor_count",
    "describe": "将两个bitmap进行异或操作，返回交集的个数",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_xor_count(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((5, ), )
    line = "select bitmap_xor_count(bitmap_from_string('1,2,3'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((6, ), )
    line = "select bitmap_xor_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_xor_count(bitmap_from_string(''), bitmap_from_string('1,2,3,4,5,6'))"
    ret = execute(line)
    assert ret == ((6, ), )
    line = "select bitmap_xor_count(bitmap_from_string('1,1'), bitmap_from_string('1,1,1,1,1,1,1'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_xor_count(bitmap_from_string('101,110,100,1110'), bitmap_from_string('1,0,110,101,11111'))"
    ret = execute(line)
    assert ret == ((5, ), )


def test_bitmap_and_not():
    """
    {
    "title": "test_sys_bitmap_function:test_bitmap_and_not",
    "describe": "将两个bitmap进行与非操作并返回计算结果",
    "tag": "function,p1"
    }
    """
    line_1 = "select bitmap_to_string(bitmap_and_not(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6')))"
    ret_1 = execute(line_1)
    line_2 = "select bitmap_to_string(bitmap_not(bitmap_from_string('1,2,3,4'), \
                bitmap_and(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6'))))"
    ret_2 = execute(line_2)
    assert ret_1 == ret_2
    line_1 = "select bitmap_to_string(bitmap_and_not(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3')))"
    ret_1 = execute(line_1)
    line_2 = "select bitmap_to_string(bitmap_not(bitmap_from_string('1,2,3'), bitmap_and(bitmap_from_string('1,2,3'),\
                bitmap_from_string('1,2,3'))))"
    ret_2 = execute(line_2)
    assert ret_1 == ret_2
    line_1 = "select bitmap_to_string(bitmap_and_not(bitmap_from_string(''), bitmap_from_string('1,2,3,4,5,6')))"
    ret_1 = execute(line_1)
    line_2 = "select bitmap_to_string(bitmap_not(bitmap_from_string(''), bitmap_and(bitmap_from_string(''), \
                bitmap_from_string('1,2,3,4,5,6'))))"
    ret_2 = execute(line_2)
    assert ret_1 == ret_2
    line_1 = "select bitmap_to_string(bitmap_and_not(bitmap_from_string('1,1'), bitmap_from_string('1,1,1,1,1,1,1')))"
    ret_1 = execute(line_1)
    line_2 = "select bitmap_to_string(bitmap_not(bitmap_from_string('1,1'), bitmap_and(bitmap_from_string('1,1'), \
                bitmap_from_string('1,1,1,1,1,1,1'))))"
    ret_2 = execute(line_2)
    assert ret_1 == ret_2
    line_1 = "select bitmap_to_string(bitmap_and_not(bitmap_from_string('101,110,100,1110'), \
                bitmap_from_string('1,0,110,101,11111')))"
    ret_1 = execute(line_1)
    line_2 = "select bitmap_to_string(bitmap_not(bitmap_from_string('101,110,100,1110'), \
                bitmap_and(bitmap_from_string('101,110,100,1110'), bitmap_from_string('1,0,110,101,11111'))))"
    ret_2 = execute(line_2)
    assert ret_1 == ret_2


def test_bitmap_and_not_count():
    """
    {
    "title": "test_sys_bitmap_function:test_bitmap_and_not_count",
    "describe": "将两个bitmap进行与非操作并返回计算返回的大小",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_and_not_count(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((3, ), )
    line = "select bitmap_and_not_count(bitmap_from_string('1,2,3'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((3, ), )
    line = "select bitmap_and_not_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_and_not_count(bitmap_from_string(''), bitmap_from_string('1,2,3,4,5,6'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_and_not_count(bitmap_from_string('1,1'), bitmap_from_string('1,1,1,1,1,1,1'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_and_not_count(bitmap_from_string('101,110,100,1110'), \
            bitmap_from_string('1,0,110,101,11111'))"
    ret = execute(line)
    assert ret == ((2, ), )


def test_bitmap_has_all():
    """
    {
    "title": "test_sys_bitmap_function:test_bitmap_has_all",
    "describe": "如果第一个bitmap包含第二个bitmap的全部元素，则返回true。如果第二个bitmap包含的元素为空，返回true",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_has_all(bitmap_from_string('1,2,3,4'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_has_all(bitmap_from_string('1,2,3'), bitmap_from_string('4,5,6'))"
    ret = execute(line)
    assert ret == ((0, ), )
    line = "select bitmap_has_all(bitmap_from_string('1,2,3'), bitmap_from_string('1,2,3'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_has_all(bitmap_from_string('1,2,3,4,5,6'), bitmap_from_string(''))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_has_all(bitmap_from_string('1,2,3,4,5,6'), bitmap_from_string('6,5,4,3,2'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_has_all(bitmap_from_string('1,1'), bitmap_from_string('1,1,1,1,1,1,1'))"
    ret = execute(line)
    assert ret == ((1, ), )
    line = "select bitmap_has_all(bitmap_from_string(''), bitmap_from_string(''))"
    ret = execute(line)
    assert ret == ((1, ), )


def test_bitmap_max():
    """
    {
    "title": "test_sys_bitmap_function:test_bitmap_max",
    "describe": "计算并返回bitmap中的最大值",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_max(bitmap_from_string(''))"
    ret = execute(line)
    assert ret == ((None, ), )
    line = "select bitmap_max(bitmap_from_string('10,99,100,1000,1,9999'))"
    ret = execute(line)
    assert ret == ((9999, ), )
    line = "select bitmap_max(bitmap_from_string('9,9,9,9,8,9'))"
    ret = execute(line)
    assert ret == ((9, ), )
    line = "select bitmap_max(bitmap_from_string('3,3,3,3,3,3,3,3,3'))"
    ret = execute(line)
    assert ret == ((3, ), )


def test_bitmap_subset_in_range():
    """
    {
    "title": "test_sys_bitmap_function:test_bitmap_subset_in_range",
    "describe": "返回bitmap指定范围内的子集",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string(''), 1, 10))"
    ret = execute(line)
    assert ret == (('', ), )
    line = "select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5'), 1, 5))"
    ret = execute(line)
    assert ret == (('1,2,3,4', ), )
    line = "select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5'), 2, 6))"
    ret = execute(line)
    assert ret == (('2,3,4,5', ), )
    line = "select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5'), 6, 10))"
    ret = execute(line)
    assert ret == (('', ), )
    line = "select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5'), 6, 1))"
    ret = execute(line)
    assert ret == ((None, ), )
    line = "select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5'), 5, 5))"
    ret = execute(line)
    assert ret == ((None, ), )
    line = "select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5,5,3,2,7,8,2,5'), 2, 6))"
    ret = execute(line)
    assert ret == (('2,3,4,5', ), )


def test_bitmap_subset_limit():
    """
    {
    "title": "test_sys_bitmap_function:test_bitmap_subset_limit",
    "describe": "生成子bitmap",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_to_string(bitmap_subset_limit(bitmap_from_string(''), 1, 10))"
    ret = execute(line)
    assert ret == (('', ), )
    line = "select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,2,3,4,5'), 1, 4))"
    ret = execute(line)
    assert ret == (('1,2,3,4', ), )
    line = "select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,2,3,4,5'), 2, 6))"
    ret = execute(line)
    assert ret == (('2,3,4,5', ), )
    line = "select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,2,3,4,5'), 6, 10))"
    ret = execute(line)
    assert ret == (('', ), )
    line = "select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('8,8,9,10,6,7'), 3, 3))"
    ret = execute(line)
    assert ret == (('6,7,8', ), )
    line = "select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,2,3,4,5'), 5, 5))"
    ret = execute(line)
    assert ret == (('5', ), )
    line = "select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,2,3,4,5,5,3,2,7,8,2,5'), 2, 6))"
    ret = execute(line)
    assert ret == (('2,3,4,5,7,8', ), )


def test_sub_bitmap():
    """
    {
    "title": "test_sys_bitmap_function:test_sub_bitmap",
    "describe": "截取bitmap，返回子集",
    "tag": "function,p1"
    }
    """
    line = "select bitmap_to_string(sub_bitmap(bitmap_from_string(''), 1, 10))"
    ret = execute(line)
    assert ret == ((None, ), )
    line = "select bitmap_to_string(sub_bitmap(bitmap_from_string('1,2,3,4,5'), 1, 4))"
    ret = execute(line)
    assert ret == (('2,3,4,5', ), )
    line = "select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 0, 3))"
    ret = execute(line)
    assert ret == (('0,1,2', ), )
    line = "select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), -3, 2))"
    ret = execute(line)
    assert ret == (('2,3', ), )
    line = "select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 2, 100))"
    ret = execute(line)
    assert ret == (('2,3,5', ), )
    line = "select bitmap_to_string(sub_bitmap(bitmap_from_string('2,1,5,4,3'), 2, 2))"
    ret = execute(line)
    assert ret == (('3,4', ), )
    line = "select bitmap_to_string(sub_bitmap(bitmap_from_string('6,4,5,2,6,7,8,1,2,3'), 2, 6))"
    ret = execute(line)
    assert ret == (('3,4,5,6,7,8', ), )


if __name__ == '__main__':
    setup_module()
    tb = 'test_sys_bitmap_function_test_bitmap_with_rollup_db.pv_bitmap'
    line = 'select dt, bitmap_union_count(user_id_bitmap) from %s group by dt' % (tb,)
    rollup = common.get_explain_rollup(client, line)
    print(rollup)
    # test_sc_add_bitmap_column()
    # test_sc_drop_bitmap_column()
    # test_sc_modified_bitmap_column()

