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

"""
百分位函数测试,和已有的baseall、test数据相关，结果校验依赖表里的数据！！！！
Date:    2019/7/17 19:23:06
"""
import random
import sys
import time
from operator import eq

sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


def setup_module():
    """
    setUp
    """
    global runner
    runner = QueryBase()
    global database_name
    database_name = runner.query_db


def create_table(table):
    """create table
    """
    line = 'drop table if exists %s' % (table)
    LOG.info(L('palo sql', palo_sql=line))
    palo_result = runner.query_palo.do_sql(line)
    line = 'create table %s( \
                   k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), \
                   k6 char(11), k7 varchar(51), k10 date, k11 datetime, \
                   k8 double max, k9 float sum) \
                   engine=olap distributed by hash(k1) buckets 3 \
                   properties("storage_type"="COLUMN")' % (table)
    print(line)
    times = 0
    flag = 0
    while(times < 5 and flag == 0):
        try:
            LOG.info(L('palo sql', palo_sql=line))
            palo_result = runner.query_palo.do_sql(line)
            print("create table succ")
            LOG.info('palo create table succ')
            flag = 1
        except Exception as e:
            print(Exception, ":", e)
            LOG.error(L('err', error=e))
            time.sleep(1)
            times += 1
            if times == 3:
                assert 0 == 1


def test_percentile_type():
    """
    {
    "title": "test_query_percentile.test_percentile_type",
    "describe": "对各个类型的列求百分位 baseall表",
    "tag": "function,p1"
    }
    """
    """对各个类型的列求百分位 baseall表
    char varchar:not support
    """
    table_name = "baseall"

    for column in range(11):
        line1 = "select k1, percentile_approx(k%s, 1) from %s group by k1 order by k1"\
                % (column + 1, table_name)
        line2 = "select k1, k%s from %s order by k1" % (column + 1, table_name)
        print("line1: %s, line2: %s" % (line1, line2))
        LOG.info(L('palo sql 1', palo_sql_1=line1))
        LOG.info(L('palo sql 2', palo_sql_2=line2))
        res1 = runner.query_palo.do_sql(line1)
        res2 = runner.query_palo.do_sql(line2)
        print("res1: %s, res2: %s" % (res1, res2))
        if column in [3]:
            print("line1 %s, line2 %s, percentile_approx res %s, excepted res %s" \
                % (line1, line2, res1, res2))
            for item in range(len(res1)):
               print("index %s, percentile_approx %s, excepted %s" % (item, res1[item], res2[item]))
               if 'e+' not in str(res1[item]):
                   assert eq(res1[item], res2[item]), "excepetd %s == %s" % (res1[item], res2[item])
               else:
                   assert int(res1[item][1]) - int(res2[item][1]) <= 1, "excepetd %s - %s < 1"\
                       % (res1[item], res2[item])
        elif column in [5, 6]:
            #var not support
            assert '(1, None)' == str(res1[0]), "res %s, excepetd (1, None)" % res1[0]
 

def test_same_date_diff_percent():
    """
    {
    "title": "test_query_percentile.test_same_date_diff_percent",
    "describe": "表里插入相同的数据，求不同的百分位值，结果应该都一致",
    "tag": "function,p1"
    }
    """
    """表里插入相同的数据，求不同的百分位值，结果应该都一致
    """
    table_name = 'percent_same_date_table'
    create_table(table_name)
    value = "222, 33333, 44444, 8.99, 'k6', 'k7', '2019-07-19', '2019-07-19 15:13:00',\
     '0.888', '9.999'"
    # 插入相同的value 10条
    insert_len = 10
    for i in range(insert_len):
        sql = "insert into %s values" % (table_name)
        sql += "(%s, %s)" % (i, value)
        print(sql)
        LOG.info(L('palo sql', palo_sql=sql))
        runner.query_palo.do_sql(sql)
        time.sleep(2)

    sql = "select count(*) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print('sql %s, res %s' % (sql, res))
    assert insert_len == res[0][0],"res %s,excepetd %s" % (res[0][0], insert_len)
    # 1次执行多个执行百分比
    sql = "select percentile_approx(k1,0.1), percentile_approx(k1,1), percentile_approx(k2,1),\
        percentile_approx(k3,1), percentile_approx(k4,1), percentile_approx(k10,1), \
        percentile_approx(k8,1) from %s group by k1 order by k1 limit 2" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print('sql %s, res %s' % (sql, res))
    assert ((0.0, 0.0, 222.0, 33333.0, 44444.0, 20190720.0, 0.8880000114440918), 
         (1.0, 1.0, 222.0, 33333.0, 44444.0, 20190720.0, 0.8880000114440918)) == res, "res: %s, excepetd \
        ((0.0, 0.0, 222.0, 33333.0, 44444.0, 20190720.0, 0.8880000114440918),\
        (1.0, 1.0, 222.0, 33333.0, 44444.0, 20190720.0, 0.8880000114440918))" % (res,)
    for i in range(2):
        percent = round(random.uniform(0, 1), 1)
        sql = "select percentile_approx(k1,1), percentile_approx(k1,%s), percentile_approx(k2,%s),\
           percentile_approx(k3,%s), percentile_approx(k4,%s), percentile_approx(k10,1),\
           percentile_approx(k8,1) from %s group by k1 order by k1 limit 2"\
              % (percent, percent, percent, percent, table_name)
        LOG.info(L('palo sql', palo_sql=sql))
        res1 = runner.query_palo.do_sql(sql)
        print('sql %s, res %s' % (sql, res1))
        assert res == res1, "excepted %s == %s" % (res, res1)


def test_err_operate():
    """
    {
    "title": "test_query_percentile.test_err_operate",
    "describe": "percent语法测试",
    "tag": "function,p1,fuzz"
    }
    """
    """percent语法测试
    select percentile_approx(k10,k2,0.5) from baseall;
    ERROR 1064 (HY000): percentile_approx(expr, DOUBLE) requires two parameters
    """
    table_name = "baseall"
    sql_1 = "select percentile_approx(k10,k2,0.5) from %s" % table_name
    sql_2 = "select percentile_approx(k1, k2) from %s" % table_name
    sql_3= "select k2, percentile_approx(k1,0.7) from %s" % table_name
    sql_4= "select k2, percentile_approx(k1,'0.7') from %s" % table_name

    sql_list = [sql_1, sql_2, sql_3, sql_4]

    res_1 = "requires second parameter must be a constant"
    res_2 = "requires second parameter must be a constant"
    res_3 = "select list expression not produced by aggregation output"
    res_4 = "select list expression not produced by aggregation output"
    res_err_list = [res_1, res_2, res_3, res_4]
    res = ''
    for sql, err in zip(sql_list, res_err_list):
        try:
            print("db %s, table %s, sql %s" % (database_name, table_name, sql))
            LOG.info(L('palo sql', palo_sql=sql))
            res = runner.query_palo.do_sql(sql)
            print(res)
        except Exception as e:
            # print("exception %s, res %s" % (e, res))
            find_index = str(e).find('err')
            find_str = str(e)[find_index : -1]
            if err not in find_str:
                print("actual err: %s, excepted err:%s" % (find_str, err))
                LOG.error(L('error', error=find_str, excepted=err))
                assert 0 == 1


def test_CUID_percent():
    """
    {
    "title": "test_query_percentile.test_CUID_percent",
    "describe": "建表，插入数据，增删改查，求百分位值，看结果是否正确",
    "tag": "function,p1"
    }
    """
    """建表，插入数据，增删改查，求百分位值，看结果是否正确
    """
    table_name = 'percent_CUID_table'
    create_table(table_name)
    # 空表
    sql_1 = "select percentile_approx(k1, 0.5) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql_1))
    res = runner.query_palo.do_sql(sql_1)
    print("table: %s, sql: %s, percenty res: %s" % (table_name, sql_1, res))
    assert '(None,)' == str(res[0]), "res %s, excepted (None,)" % res[0]
    # insert data
    value = "222, 33333, 44444, 8.99, 'k6', 'k7', '2019-07-19', '2019-07-19 15:13:00',\
     '0.888', '9.999'"
    len_data = 3
    for i in range(len_data):
        sql = "insert into %s values" % (table_name)
        sql += "(%s, %s)" % (i, value)
        print(sql)
        LOG.info(L('palo sql', palo_sql=sql))
        runner.query_palo.do_sql(sql)
    sql = "select count(*) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print('sql %s, res %s' % (sql, res))
    assert len_data == res[0][0], "res %s, excepted %s" % (res[0][0], len_data)
    sql_1 = "select percentile_approx(k1, 0.5) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql_1))
    res = runner.query_palo.do_sql(sql_1)
    print("null table: %s, sql: %s, percenty res: %s" % (table_name, sql_1, res))
    excepted = ((1,),)
    # util.check_same(res, excepted)
    # assert ((1L,),) == res, "res %s, excepted (1L,)" % res
    # 删除数据
    sql = "delete from %s where k1 = 0" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print("sql: %s, percenty res: %s" % (sql, res))
    sql = "select * from %s order by k1" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print('sql %s, res %s' % (sql, res))
    time.sleep(3)
    sql_1 = "select percentile_approx(k1, 0.5) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql_1))
    res = runner.query_palo.do_sql(sql_1)
    print("null table: %s, sql: %s, percenty res: %s" % (table_name, sql_1, res))
    excepted = ((1.5,),)
    # util.check_same(res, excepted)


def test_bigdata_percent():
    """
    {
    "title": "test_query_percentile.test_bigdata_percent",
    "describe": "建表，插入大量数据，求百分位值，看结果是否正确",
    "tag": "function,p1"
    }
    """
    """建表，插入大量数据，求百分位值，看结果是否正确
    """
    table_name = 'test'
    # select 1列
    sql_1 = "select percentile_approx(k1, 0.55) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql_1))
    res = runner.query_palo.do_sql(sql_1)
    print("table: %s, sql: %s, percenty res: %s" % (table_name, sql_1, res))
    excepted = ((13,),)
    util.check_same(res, excepted)
    # assert ((13L,),) == res, "diff: res %s, excepted (13L,)" % res
    # select多个列
    sql_multy = "select percentile_approx(k1, 0.5), percentile_approx(k1, 0.99999), \
          percentile_approx(k2, 0.5), percentile_approx(k2, 0.9999999999999999999) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql_multy))
    res = runner.query_palo.do_sql(sql_multy)
    print("table: %s, sql: %s, percenty res: %s" % (table_name, sql_multy, res))
    excepted = ((0.0, 127.0, 17.629631042480469, 32767.0),)
    util.check_same(res, excepted)


def test_percent_other_function():
    """
    {
    "title": "test_query_percentile.test_percent_other_function",
    "describe": "百分位结合其他的函数，最大最小值，看结果是否正确",
    "tag": "function,p1"
    }
    """
    """百分位结合其他的函数，最大最小值，看结果是否正确
    """
    table_name = 'test'
    sql = "select percentile_approx(k1, 0.5), min(k2), max(k2) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print("table: %s, sql: %s, percenty res: %s" % (table_name, sql, res))
    excepted = ((0.0, -32767, 32767),)
    util.check_same(res, excepted)
    sql = "select percentile_approx(k1, 0.0000000000001), percentile_approx(k1, 0.999999999991)\
           from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print("table: %s, sql: %s, percenty res: %s" % (table_name, sql, res))
    excepted = ((-127.0, 127.0),)
    util.check_same(res, excepted)
    sql = "select percentile_approx(k1+k2, 0.55) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print("table: %s, sql: %s, percenty res: %s" % (table_name, sql, res))
    excepted = ((3347.682098765432,),) 
    util.check_same(res, excepted)
    sql = "select percentile_approx(k1+k2, 0.55+0.12345) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print("table: %s, sql: %s, percenty res: %s" % (table_name, sql, res))
    excepted = ((11460.73415234375,),)
    util.check_same(res, excepted)
    sql = "select percentile_approx(k1+k2, floor(0.65)) from %s" % table_name
    LOG.info(L('palo sql', palo_sql=sql))
    res = runner.query_palo.do_sql(sql)
    print("table: %s, sql: %s, percenty res: %s" % (table_name, sql, res))
    excepted = ((-32886.0,),)
    util.check_same(res, excepted)


def test_percentile():
    """
    {
    "title": "test_query_percentile.test_percentile",
    "describe": "对各个类型的列求百分位,percentile功能",
    "tag": "function,p1"
    }
    """
    table_name = "baseall"

    for column in range(11):
        line1 = "select k1, percentile(k%s, 1) from %s group by k1 order by k1"\
                % (column + 1, table_name)
        line2 = "select k1, k%s from %s order by k1" % (column + 1, table_name)
        print("line1: %s, line2: %s" % (line1, line2))
        LOG.info(L('palo sql 1', palo_sql_1=line1))
        LOG.info(L('palo sql 2', palo_sql_2=line2))
        res1 = runner.query_palo.do_sql(line1)
        res2 = runner.query_palo.do_sql(line2)
        print("res1: %s, res2: %s" % (res1, res2))
        if column in [3]:
            print("line1 %s, line2 %s, percentile res %s, excepted res %s" \
                % (line1, line2, res1, res2))
            for item in range(len(res1)):
               print("index %s, percentile %s, excepted %s" % (item, res1[item], res2[item]))
               if 'e+' not in str(res1[item]):
                   assert eq(res1[item], res2[item]), "excepetd %s == %s" % (res1[item], res2[item])
               else:
                   assert int(res1[item][1]) - int(res2[item][1]) <= 1, "excepetd %s - %s < 1"\
                       % (res1[item], res2[item])
        elif column in [5, 6]:
            # var not support
            assert (1, None) == res1[0], "res %s, excepetd (1, None)" % str(res1[0])
    # null值求百分位
    line1 = "select percentile(NULL, 0.3) from %s" % table_name
    line2 = "select NULL"
    runner.check2(line1, line2)
    line1 = "select percentile(NULL, 0) from %s" % table_name
    runner.check2(line1, line2)
    line1 = "select percentile(NULL, 1) from %s" % table_name
    runner.check2(line1, line2)


def test_bigdata_percentile():
    """
    {
    "title": "test_query_percentile.test_bigdata_percentile",
    "describe": "大量数据，求百分位值，看结果是否正确",
    "tag": "function,p1"
    }
    """
    table_name = 'test'
    sql_1 = "select percentile(k1, 0.55) from %s" % table_name
    sql_2 = "select 13"
    runner.check2(sql_1, sql_2)
    sql_1 = "select percentile(k1, 0.5), percentile(k1, 0.99999), percentile(k2, 0.5), \
            percentile(k2, 0.9999999999999999999) from %s" % table_name
    sql_2 = "select 0, 127, 18, 32767"
    runner.check2(sql_1, sql_2)


def teardown_module():
    """
    tearDown
    """
    pass


if __name__ == '__main__':
    import pdb
    setup_module()
    test_CUID_percent()

