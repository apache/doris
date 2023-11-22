#/bin/env python
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
############################################################################
#
#   @file test_sys_materialized_view_2.py
#   @date 2020-08-03 14:36:10
#   比物化视图一期增加count，hll_union, bitmap_union三种function
#############################################################################
"""

import sys
import time

from data import schema as DATA
from data import load_file as FILE
sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import util
from lib import common
from lib.palo_job import DescInfo
from lib.palo_job import RollupJob
from lib.palo_job import LoadJob

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L
broker_info = palo_config.broker_info
check_db = 'mv_check_db'
check_agg_tb = 'mv_check_agg_tb'


def setup_module():
    """setup"""
    global check_db, check_dup_tb, check_agg_tb
    try:
        client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user, \
                password=config.fe_password)
        ret = client.execute('select * from %s.%s' % (check_db, check_agg_tb))
        assert len(ret) == 15, 'need to init check db'
    except Exception as e:
        client = common.create_workspace(check_db)
        distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
        ret = client.create_table(check_agg_tb, DATA.datatype_column_list, distribution_info=distribution_info)
        assert ret, 'create table failed'
        column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
        set_list = ['k1=v1', 'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                    'k12=v12', 'k0=v7', 'k5=v4*101', 'k13=hll_hash(v2)', 'k14=to_bitmap(v1)']
        data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, check_agg_tb,
                                                  column_name_list=column_name_list, set_list=set_list)
        ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
        assert ret, 'load data failed'


def teardown_module():
    """teardown"""
    pass


def test_create_agg_mv():
    """
    {
       "title": "test_create_agg_mv",
       "describe": "测试创建agg表的物化视图，验证创建成功，查询命中，结果正确",
       "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    agg_tb = table_name + '_agg'
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    ret = client.create_table(agg_tb, DATA.datatype_column_list, distribution_info=distribution_info)
    assert ret, 'create table failed'
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10',
                'k11=v11', 'k12=v12', 'k0=v7', 'k5=v4*101', 'k13=hll_hash(v2)', 'k14=to_bitmap(abs(v2))']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, agg_tb, 
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'broker load failed'
    # 创建物化视图
    sql = 'select k6, k7 from %s group by k6,k7' % agg_tb
    assert client.create_materialized_view(agg_tb, 'mv1', sql, is_wait=True), 'create mv failed'
    assert client.get_index(agg_tb, 'mv1'), 'get mv1 failed'
    ret = client.get_index_schema(agg_tb, 'mv1')
    assert 'DECIMALV3(9,3)' == util.get_attr_condition_value(ret, DescInfo.Field, 'k6', DescInfo.Type), \
           'k6 type expect DECIMAL(9,3)'
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        rollup = common.get_explain_rollup(client, sql)
        if rollup == ['mv1']:
            break
    assert rollup == ['mv1'], 'use wrong rollup %s, expect mv1' % rollup

    sql = 'select k4, k2, max(k11), sum(k12), hll_union(k13), bitmap_union(k14) from %s group by k4, k2' % agg_tb
    assert client.create_materialized_view(agg_tb, 'mv2', sql, is_wait=True), 'create mv2 failed'
    assert client.get_index(agg_tb, 'mv2'), 'get mv2 failed'
    ret = client.get_index_schema(agg_tb, 'mv2')
    assert 'DOUBLE' == util.get_attr_condition_value(ret, DescInfo.Field, 'k12', DescInfo.Type)
    assert 'HLL' == util.get_attr_condition_value(ret, DescInfo.Field, 'k13', DescInfo.Type)
    assert 'BITMAP' == util.get_attr_condition_value(ret, DescInfo.Field, 'k14', DescInfo.Type)
    sql = 'select k4, max(k11), sum(k12), hll_union_agg(k13), bitmap_union(k14) from %s ' \
          'group by k4, k2 order by k4, k2' % agg_tb
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        rollup = common.get_explain_rollup(client, sql)
        if rollup == ['mv2']:
            break
    assert rollup == ['mv2'], 'use wrong rollup %s, expect mv2' % rollup
    sql2 = 'select k4, max(k11), sum(k12), hll_union_agg(k13), bitmap_union(k14) from %s.%s ' \
            'group by k4, k2 order by k4, k2' % (check_db, check_agg_tb)
    common.check2(client, sql1=sql, sql2=sql2)
    client.clean(database_name)


def test_create_dup_max_mv():
    """
       {
       "title": "test_create_dup_max_mv",
       "describe": "测试创建dup表的物化视图，各种类型的max聚合类型，验证创建成功，查询命中物化视图，结果正确",
       "tag": "function,p0"
       }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table 
    dup_tb = table_name + '_dup'
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 2)
    ret = client.create_table(dup_tb, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert client.show_tables(dup_tb), 'get table failed'
    assert ret, 'create table failed'
    column_fields = util.get_attr(DATA.datatype_column_no_agg_list, 0)
    column_type = util.get_attr(DATA.datatype_column_no_agg_list, 1)
    # load data
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10',
                'k11=v11', 'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_tb, 
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'batch load failed'

    # 创建物化视图 & 验证schema
    sql = 'select k0, max(k1), max(k2), max(k3), max(k4), max(k5), max(k6), max(k7), max(k8), max(k9), ' \
          'max(k10), max(k11), max(k12) from %s group by k0' % dup_tb
    max_mv = 'max_mv'
    assert client.create_materialized_view(dup_tb, max_mv, sql, is_wait=True), 'create mv failed'
    assert client.get_index(dup_tb, max_mv), 'get mv failed'
    ret = client.get_index_schema(dup_tb, max_mv)
    assert column_fields == util.get_attr(ret, DescInfo.Field), 'check field failed, expect: %s' % column_fields
    assert column_type == util.get_attr(ret, DescInfo.Type), 'check feild type failed, expect: %s' % column_type
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql)
        if max_mv in mv_list:
            break
    assert max_mv in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (max_mv, mv_list)
    sql2 = 'select k0, max(k1), max(k2), max(k3), max(k4), max(k5), max(k6), max(k7), max(k8), max(k9), ' \
           'max(k10), max(k11), max(k12) from %s.%s group by k0' % (check_db, check_agg_tb)
    common.check2(client, sql, sql2=sql2, forced=True)
    client.clean(database_name)


def test_create_dup_min_mv():
    """
    {
       "title": "test_create_agg_mv",
       "describe": "测试创建dup表的物化视图，各种类型的min聚合类型，验证创建成功，查询命中物化视图，结果正确",
       "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table
    dup_tb = table_name + '_dup'
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 2)
    ret = client.create_table(dup_tb, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert client.show_tables(dup_tb), 'get table failed'
    assert ret, 'create table failed'
    column_fields = util.get_attr(DATA.datatype_column_no_agg_list, 0)
    column_type = util.get_attr(DATA.datatype_column_no_agg_list, 1)
    # load data
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10',
                'k11=v11', 'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_tb, 
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    sql = 'select k0, min(k1), min(k2), min(k3), min(k4), min(k5), min(k6), min(k7), min(k8), min(k9), ' \
          'min(k10), min(k11), min(k12) from %s group by k0' % dup_tb
    min_mv = 'min_mv'
    assert client.create_materialized_view(dup_tb, min_mv, sql, is_wait=True), 'create mv failed'
    assert client.get_index(dup_tb, min_mv), 'get mv failed'
    ret = client.get_index_schema(dup_tb, min_mv)
    assert column_fields == util.get_attr(ret, DescInfo.Field), 'check field failed, expect %s' % column_fields
    assert column_type == util.get_attr(ret, DescInfo.Type), 'check field type failed, expect %s' % column_type
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql)
        if min_mv in mv_list:
            break
    assert min_mv in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (min_mv, mv_list)
    sql2 = 'select k0, min(k1), min(k2), min(k3), min(k4), min(k5), min(k6), min(k7), min(k8), min(k9), ' \
           'min(k10), min(k11), min(k12) from %s.%s group by k0' % (check_db, check_agg_tb)
    common.check2(client, sql, sql2=sql2, forced=True)
    client.clean(database_name)


def test_create_dup_sum_mv():
    """
    {
       "title": "test_create_agg_mv",
       "describe": "测试创建dup表的物化视图，各种类型的sum聚合类型，验证创建成功，查询命中物化视图，结果正确",
       "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    dup_tb = table_name + '_dup'
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 10)
    ret = client.create_table(dup_tb, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert client.show_tables(dup_tb), 'get table failed'
    assert ret, 'create table failed'
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10',
                'k11=v11', 'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_tb, 
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'

    sql = 'select k0, sum(k1), sum(k2), sum(k3), sum(k4), sum(k5), sum(k6), sum(k11), sum(k12) from %s ' \
          'group by k0' % dup_tb
    sum_mv = 'sum_mv'
    assert client.create_materialized_view(dup_tb, sum_mv, sql, is_wait=True), 'create mv failed'
    assert client.get_index(dup_tb, sum_mv), 'get index failed'
    ret = client.get_index_schema(dup_tb, sum_mv)
    column_fields = [u'k0', u'k1', u'k2', u'k3', u'k4', u'k5', u'k6', u'k11', u'k12']
    column_type = [u'BOOLEAN', u'BIGINT', u'BIGINT', u'BIGINT', u'BIGINT', u'LARGEINT',
                   u'DECIMAL(9,3)', u'DOUBLE', u'DOUBLE']
    assert column_fields == util.get_attr(ret, DescInfo.Field), 'check field failed, expect: %s' % column_fields
    assert column_type == util.get_attr(ret, DescInfo.Type), 'check field type failed, expect: %s' % column_type
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql)
        if sum_mv in mv_list:
            break
    assert sum_mv in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (sum_mv, mv_list)
    # TODO: 结果校验不正确，存在溢出问题，暂不修复
    sql2 = 'select k0, sum(k1), sum(k2), sum(k3), sum(k4), sum(k5), sum(k6), sum(k11), sum(k12) from %s.%s ' \
           'group by k0' % (check_db, check_agg_tb)
    # common.check2(client, sql, sql2=sql2, forced=True)
    client.clean(database_name)


def test_create_dup_count_mv():
    """
    {
       "title": "test_create_agg_mv",
       "describe": "测试创建dup表的物化视图，各种类型的count聚合类型，验证创建成功，查询命中物化视图，结果正确",
       "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    dup_tb = table_name + '_dup'
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 10)
    ret = client.create_table(dup_tb, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert client.show_tables(dup_tb), 'get table failed'
    assert ret, 'create table failed'
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10',
                'k11=v11', 'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_tb, 
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    sql = 'select k0, count(k1), count(k2), count(k3), count(k4), count(k5), count(k6), count(k7), ' \
          'count(k8), count(k9), count(k10), count(k11), count(k12) from %s group by k0' % dup_tb
    count_mv = 'count_mv'
    assert client.create_materialized_view(dup_tb, count_mv, sql, is_wait=True), 'create mv failed'
    assert client.get_index(dup_tb, count_mv), 'get mv failed'
    ret = client.get_index_schema(dup_tb, count_mv)
    column_fields = [u'k0', u'CASE WHEN k1 IS NULL THEN 0 ELSE 1 END', u'CASE WHEN k2 IS NULL THEN 0 ELSE 1 END',
                     u'CASE WHEN k3 IS NULL THEN 0 ELSE 1 END', u'CASE WHEN k4 IS NULL THEN 0 ELSE 1 END',
                     u'CASE WHEN k5 IS NULL THEN 0 ELSE 1 END', u'CASE WHEN k6 IS NULL THEN 0 ELSE 1 END',
                     u'CASE WHEN k7 IS NULL THEN 0 ELSE 1 END', u'CASE WHEN k8 IS NULL THEN 0 ELSE 1 END', 
                     u'CASE WHEN k9 IS NULL THEN 0 ELSE 1 END', u'CASE WHEN k10 IS NULL THEN 0 ELSE 1 END',
                     u'CASE WHEN k11 IS NULL THEN 0 ELSE 1 END', u'CASE WHEN k12 IS NULL THEN 0 ELSE 1 END']
    column_types = [u'BOOLEAN', u'BIGINT', u'BIGINT', u'BIGINT', u'BIGINT', u'BIGINT', u'BIGINT',
                    u'BIGINT', u'BIGINT', u'BIGINT', u'BIGINT', u'BIGINT', u'BIGINT']
    real_fields = [x.replace('`', '') for x in util.get_attr(ret, DescInfo.Field)]
    assert column_fields == real_fields, 'check mv field failed, expect:%s, actural: %s' \
           % (column_fields, real_fields)
    assert column_types == util.get_attr(ret, DescInfo.Type), 'check mv field type failed, expect: %s' % column_types
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql)
        if count_mv in mv_list:
            break
    assert count_mv in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (count_mv, mv_list)
    sql2 = 'select k0, count(k1), count(k2), count(k3), count(k4), count(k5), count(k6), count(k7), ' \
           'count(k8), count(k9), count(k10), count(k11), count(k12) from %s.%s group by k0' % (check_db, check_agg_tb)
    common.check2(client, sql, sql2=sql2, forced=True)
    client.clean(database_name)


def test_create_dup_hll_mv():
    """
    {
       "title": "test_create_agg_mv",
       "describe": "测试创建dup表的物化视图，各种类型的hll_union聚合类型，验证创建成功，查询命中物化视图，结果正确",
       "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create db
    dup_tb = table_name + '_dup'
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 10)
    ret = client.create_table(dup_tb, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert client.show_tables(dup_tb), 'get table failed'
    assert ret, 'create table failed'
    # load data
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10',
                'k11=v11', 'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_tb, 
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    # create mv & check
    sql = 'select k0, hll_union(hll_hash(k1)), hll_union(hll_hash(k2)), hll_union(hll_hash(k3)),' \
          ' hll_union(hll_hash(k4)), hll_union(hll_hash(k5)), ' \
          'hll_union(hll_hash(k7)), hll_union(hll_hash(k8)), hll_union(hll_hash(k9)), ' \
          'hll_union(hll_hash(k10)), hll_union(hll_hash(k11)), hll_union(hll_hash(k12)) ' \
          'from %s group by k0' % dup_tb
    hll_mv = 'hll_mv'
    assert client.create_materialized_view(dup_tb, hll_mv, sql, is_wait=True), 'create mv failed'
    assert client.get_index(dup_tb, hll_mv), 'get mv failed'
    ret = client.get_index_schema(dup_tb, hll_mv)
    column_fields = ['k0', 'hll_hash(k1)', 'hll_hash(k2)', 'hll_hash(k3)', 'hll_hash(k4)', 'hll_hash(k5)', 
                     'hll_hash(k7)', 'hll_hash(k8)', 'hll_hash(k9)', 'hll_hash(k10)', 'hll_hash(k11)', 
                     'hll_hash(k12)']
    column_type = ['BOOLEAN', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL']
    real_fields = [x.replace('`', '') for x in util.get_attr(ret, DescInfo.Field)]
    assert column_fields == real_fields, 'check mv field failed, expect %s' % column_fields
    assert column_type == util.get_attr(ret, DescInfo.Type), 'check mv field type failed, expect %s' % column_type
    sql = 'select k0, hll_union_agg(hll_hash(k1)), hll_union_agg(hll_hash(k2)), hll_union_agg(hll_hash(k3)),' \
          ' hll_union_agg(hll_hash(k4)), hll_union_agg(hll_hash(k5)), ' \
          'hll_union_agg(hll_hash(k7)), hll_union_agg(hll_hash(k8)), hll_union_agg(hll_hash(k9)), ' \
          'hll_union_agg(hll_hash(k10)), hll_union_agg(hll_hash(k11)), hll_union_agg(hll_hash(k12)) ' \
          'from %s.%s group by k0'
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql % (database_name, dup_tb))
        if hll_mv in mv_list:
            break
    assert hll_mv in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (hll_mv, mv_list)
    common.check2(client, sql % (database_name, dup_tb), sql2=sql % (check_db, check_agg_tb), forced=True)
    client.clean(database_name)


def test_create_dup_hll_mv_1():
    """
    {
       "title": "test_create_dup_hll_mv_1",
       "describe": "测试创建dup表的物化视图，各种类型的hll_union聚合类型，验证创建成功，查询命中物化视图，结果正确",
       "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table
    dup_tb = table_name + '_dup'
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 10)
    ret = client.create_table(dup_tb, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert client.show_tables(dup_tb), 'get table failed'
    assert ret, 'create table failed'
    # create mv
    sql = 'select k0, hll_union(hll_hash(k1)), hll_union(hll_hash(k2)), hll_union(hll_hash(k3)),' \
          ' hll_union(hll_hash(k4)), hll_union(hll_hash(k5)), ' \
          'hll_union(hll_hash(k7)), hll_union(hll_hash(k8)), hll_union(hll_hash(k9)), ' \
          'hll_union(hll_hash(k10)), hll_union(hll_hash(k11)), hll_union(hll_hash(k12)) ' \
          'from %s group by k0' % dup_tb
    hll_mv = 'hll_mv'
    assert client.create_materialized_view(dup_tb, hll_mv, sql, is_wait=True), 'create mv failed'
    assert client.get_index(dup_tb, hll_mv), 'get mv failed'
    ret = client.get_index_schema(dup_tb, hll_mv)
    print(util.get_attr(ret, DescInfo.Field))
    print(util.get_attr(ret, DescInfo.Type))
    column_fields = ['k0', 'hll_hash(k1)', 'hll_hash(k2)', 'hll_hash(k3)', 'hll_hash(k4)', 'hll_hash(k5)',
                     'hll_hash(k7)', 'hll_hash(k8)', 'hll_hash(k9)', 'hll_hash(k10)', 'hll_hash(k11)',
                     'hll_hash(k12)']
    column_type = ['BOOLEAN', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL', 'HLL']
    real_fields = [x.replace('`', '') for x in util.get_attr(ret, DescInfo.Field)]
    assert column_fields == real_fields, 'check mv field failed, expect %s' % column_fields
    assert column_type == util.get_attr(ret, DescInfo.Type), 'check mv field type failed, expect %s' % column_type
    # load & check
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10',
                'k11=v11', 'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_tb, 
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    sql = 'select k0, hll_union_agg(hll_hash(k1)), hll_union_agg(hll_hash(k2)), hll_union_agg(hll_hash(k3)),' \
          ' hll_union_agg(hll_hash(k4)), hll_union_agg(hll_hash(k5)), ' \
          'hll_union_agg(hll_hash(k7)), hll_union_agg(hll_hash(k8)), hll_union_agg(hll_hash(k9)), ' \
          'hll_union_agg(hll_hash(k10)), hll_union_agg(hll_hash(k11)), hll_union_agg(hll_hash(k12)) ' \
          'from %s.%s group by k0'
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql % (database_name, dup_tb))
        if hll_mv in mv_list:
            break
    assert hll_mv in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (hll_mv, mv_list)
    common.check2(client, sql % (database_name, dup_tb), sql2=sql % (check_db, check_agg_tb), forced=True)
    client.clean(database_name)


def test_create_dup_bitmap_mv():
    """
    {
       "title": "test_create_dup_bitmap_mv",
       "describe": "测试创建dup表的物化视图，各种类型的bitmap_union聚合类型，验证创建成功，查询命中物化视图，结果正确。to_bitmap只支持正整数",
       "tag": "function,p0,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    dup_tb = table_name + '_dup'
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 10)
    ret = client.create_table(dup_tb, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert client.show_tables(dup_tb)
    assert ret
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=abs(v1)', 'k2=abs(v2)', 'k3=abs(v3)', 'k4=abs(cast(v4 as bigint))', 'k6=v6', 'k7=v7',
                'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11', 'k12=v12', 'k0=v7', 'k5=abs(v4)*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_tb, 
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret
    # 预期创建物化视图时抛出异常，目前不抛异常，只是alter 任务cancel
    sql = 'select k0, bitmap_union(to_bitmap(k1)), bitmap_union(to_bitmap(k2)), bitmap_union(to_bitmap(k3)),' \
          ' bitmap_union(to_bitmap(k4)), bitmap_union(to_bitmap(k5)), bitmap_union(to_bitmap(k6)), ' \
          'bitmap_union(to_bitmap(k7)), bitmap_union(to_bitmap(k8)), bitmap_union(to_bitmap(k9)), ' \
          'bitmap_union(to_bitmap(k10)), bitmap_union(to_bitmap(k11)), bitmap_union(to_bitmap(k12)) ' \
          'from %s group by k0' % dup_tb
    bitmap_mv = 'bitmap_mv'
    flag = True
    try:
        ret = client.create_materialized_view(dup_tb, bitmap_mv, sql, is_wait=True)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag, 'expect error'
    sql = 'select k0, bitmap_union(to_bitmap(k1)), bitmap_union(to_bitmap(k2)), bitmap_union(to_bitmap(k3)), ' \
          'bitmap_union(to_bitmap(k4)) from %s group by k0' % dup_tb
    assert client.create_materialized_view(dup_tb, bitmap_mv, sql, is_wait=True)
    assert client.get_index(dup_tb, bitmap_mv)
    ret = client.get_index_schema(dup_tb, bitmap_mv)
    
    column_fields = ['k0', 'to_bitmap_with_check(k1)', 'to_bitmap_with_check(k2)',
                     'to_bitmap_with_check(k3)', 'to_bitmap_with_check(k4)']
    column_type = ['BOOLEAN', 'BITMAP', 'BITMAP', 'BITMAP', 'BITMAP']
    real_fields = [x.replace('`', '') for x in util.get_attr(ret, DescInfo.Field)]
    assert column_fields == real_fields, 'check mv field failed, expect %s' % column_fields
    assert column_type == util.get_attr(ret, DescInfo.Type), 'check mv field type failed, expect %s' % column_type
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql)
        if bitmap_mv in mv_list:
            break
    assert bitmap_mv in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (bitmap_mv, mv_list)
    sql2 = 'select k0, bitmap_union(to_bitmap(abs(k1))), bitmap_union(to_bitmap(abs(k2))), ' \
           'bitmap_union(to_bitmap(abs(k3))), bitmap_union(to_bitmap(abs(k4))) from %s.%s group by k0' \
           % (check_db, check_agg_tb)
    common.check2(client, sql, sql2=sql2, forced=True)
    sql1 = 'select k0, count(distinct k1), count(distinct k2), count(distinct k3), count(distinct k4) ' \
           'from %s.%s group by k0' % (database_name, dup_tb)
    mv_list = common.get_explain_rollup(client, sql1)
    assert bitmap_mv in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (bitmap_mv, mv_list)
    sql2 = 'select k0, count(distinct abs(k1)), count(distinct abs(k2)), count(distinct abs(k3)), ' \
           'count(distinct abs(k4)) from %s.%s group by k0' % (check_db, check_agg_tb)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_create_dup_bitmap_mv_1():
    """
    {
       "title": "test_create_agg_mv_1",
       "describe": "测试dup表物化视图bitmap聚合，建表，创建物化视图，导入，校验",
       "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table
    dup_tb = table_name + '_dup'
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 10)
    ret = client.create_table(dup_tb, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert ret, 'create table failed'
    assert client.show_tables(dup_tb), 'get table failed'
    # create mv
    sql = 'select k0, bitmap_union(to_bitmap(k1)), bitmap_union(to_bitmap(k2)), bitmap_union(to_bitmap(k3)), ' \
          'bitmap_union(to_bitmap(k4)) from %s group by k0' % dup_tb
    assert client.create_materialized_view(dup_tb, index_name, sql, is_wait=True)
    assert client.get_index(dup_tb, index_name)
    # load & check
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=abs(v1)', 'k2=abs(v2)', 'k3=abs(v3)', 'k4=abs(cast(v4 as bigint))', 'k6=v6', 'k7=v7',
                'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11', 'k12=v12', 'k0=v7', 'k5=abs(v4)*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, dup_tb,
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret 

    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql)
        if index_name in mv_list:
            break
    assert index_name in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (index_name, mv_list)
    sql2 = 'select k0, bitmap_union(to_bitmap(abs(k1))), bitmap_union(to_bitmap(abs(k2))), ' \
           'bitmap_union(to_bitmap(abs(k3))), bitmap_union(to_bitmap(abs(k4))) from %s.%s group by k0' \
            % (check_db, check_agg_tb)
    common.check2(client, sql, sql2=sql2, forced=True)
    sql1 = 'select k0, count(distinct k1), count(distinct k2), count(distinct k3), count(distinct k4) ' \
           'from %s.%s group by k0' % (database_name, dup_tb)
    mv_list = common.get_explain_rollup(client, sql1)
    assert index_name in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (index_name, mv_list)
    sql2 = 'select k0, count(distinct abs(k1)), count(distinct abs(k2)), count(distinct abs(k3)), ' \
           'count(distinct abs(k4)) from %s.%s group by k0' % (check_db, check_agg_tb)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)

    client.clean(database_name)


def test_create_dup_bitmap_mv_negative():
    """
    {
       "title": "test_create_agg_mv",
       "describe": "测试创建dup表的物化视图，bitmap_union聚合类型，只支持to_bitmap，创建mv成功后，导入负数失败",
       "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 10)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'get table failed'
    # create mv
    sql = 'select k0, bitmap_union(to_bitmap(k1)), bitmap_union(to_bitmap(k2)), bitmap_union(to_bitmap(k3)), ' \
          'bitmap_union(to_bitmap(k4)) from %s group by k0' % table_name
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'expect create mv failed'
    assert client.get_index(table_name, index_name)
    # load负数，导入失败
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=abs(v1)', 'k2=v2', 'k3=v3', 'k4=cast(v4 as bigint)', 'k6=v6', 'k7=v7',
                'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11', 'k12=v12', 'k0=v7', 'k5=abs(v4)*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name,
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert not ret, 'expect load failed'
    """
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql)
        if index_name in mv_list:
            break
    assert index_name in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (index_name, mv_list)
    sql1 = 'select k0, bitmap_union_count(to_bitmap(k1)), bitmap_union_count(to_bitmap(k2)), ' \
           'bitmap_union_count(to_bitmap(k3)), bitmap_union_count(to_bitmap(k4)) from %s group by k0' \
            %  table_name
    sql2 = 'select k0, bitmap_union_count(to_bitmap(k1)), bitmap_union_count(to_bitmap(k2)), ' \
           'bitmap_union_count(to_bitmap(k3)), bitmap_union_count(to_bitmap(k4)) from %s.%s group by k0' \
            % (check_db, check_agg_tb)
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select k0, count(distinct k1), count(distinct k2), count(distinct k3), ' \
           'count(distinct k4) from %s.%s group by k0' % (database_name, table_name)
    sql2 = 'select k0, count(distinct k1), count(distinct k2), count(distinct k3), ' \
           'count(distinct k4) from %s.%s group by k0' % (check_db, check_agg_tb)
    common.check2(client, sql1, sql2=sql2, forced=True)
    """
    client.clean(database_name)


def test_create_dup_bitmap_mv_negative_1():
    """
    {
       "title": "test_create_agg_mv",
       "describe": "测试创建dup表的物化视图，bitmap_union聚合类型，建表，导入负数，创建bitmap_union物化视图失败",
       "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 10)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'get table failed'
    # load
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=abs(v1)', 'k2=v2', 'k3=v3', 'k4=cast(v4 as bigint)', 'k6=v6', 'k7=v7',
                'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11', 'k12=v12', 'k0=v7', 'k5=abs(v4)*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, 
                                              column_name_list=column_name_list, set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    # create mv，有负数，创建失败
    sql = 'select k0, bitmap_union(to_bitmap(k1)), bitmap_union(to_bitmap(k2)), bitmap_union(to_bitmap(k3)), ' \
          'bitmap_union(to_bitmap(k4)) from %s group by k0' % table_name
    assert not client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'expect create mv failed'
    """
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'create mv failed'
    timeout = 600
    while timeout > 0:
        time.sleep(1)
        timeout -= 1
        mv_list = common.get_explain_rollup(client, sql)
        if index_name in mv_list:
            break
    assert index_name in mv_list, 'check sql shoot mv failed.expect %s, actural: %s' % (index_name, mv_list)
    sql1 = 'select k0, bitmap_union_count(to_bitmap(k1)), bitmap_union_count(to_bitmap(k2)), ' \
           'bitmap_union_count(to_bitmap(k3)), bitmap_union_count(to_bitmap(k4)) from %s group by k0' \
            %  table_name
    sql2 = 'select k0, bitmap_union_count(to_bitmap(k1)), bitmap_union_count(to_bitmap(k2)), ' \
           'bitmap_union_count(to_bitmap(k3)), bitmap_union_count(to_bitmap(k4)) from %s.%s group by k0' \
            % (check_db, check_agg_tb)
    common.check2(client, sql1, sql2=sql2, forced=True)
    sql1 = 'select k0, count(distinct k1), count(distinct k2), count(distinct k3), ' \
           'count(distinct k4) from %s.%s group by k0' % (database_name, table_name)
    sql2 = 'select k0, count(distinct k1), count(distinct k2), count(distinct k3), ' \
           'count(distinct k4) from %s.%s group by k0' % (check_db, check_agg_tb)
    common.check2(client, sql1, sql2=sql2, forced=True)
    """
    client.clean(database_name)


def test_create_agg_mv_failed():
    """
    {
       "title": "test_create_agg_mv_failed",
       "describe": "测试创建agg表的物化视图的限制, 支持一个列出现多次, 不支持其他函数，只支持select group其他不支持，物化视图的聚合方式必须和建表时value的保持一致",
       "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    agg_tb = table_name + '_agg'
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    ret = client.create_table(agg_tb, DATA.datatype_column_list, distribution_info=distribution_info)
    assert ret
    sql = 'select k1 ,k2, sum(k3) from %s group by k1, k2' % agg_tb
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, agg_tb, 'mv1_sum', sql)

    sql = 'select k1 ,k2, sum(k11) from %s group by k1, k2' % agg_tb
    msg = 'Aggregate function require same with slot aggregate type'
    util.assert_return(False, msg, client.create_materialized_view, agg_tb, 'mv1', sql)

    sql = 'select k1 ,k2, k1, sum(k12) from %s group by k1, k2' % agg_tb
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, agg_tb, 'mv1', sql)

    sql = 'select k1 ,k2, sum(k12) from %s' % agg_tb
    msg = 'select list expression not produced by aggregation output (missing from GROUP BY clause?): `k1`'
    util.assert_return(False, msg, client.create_materialized_view, agg_tb, 'mv1', sql)

    sql = 'select sum(k12) from %s' % agg_tb
    msg = 'The materialized view must contain at least one key column'
    util.assert_return(False, msg, client.create_materialized_view, agg_tb, 'mv1', sql)

    sql = 'select k2 ,k1, lower(k6) from %s' % agg_tb
    msg = 'The materialized view of aggregation or unique table must has grouping columns'
    util.assert_return(False, msg, client.create_materialized_view, agg_tb, 'mv1_lower', sql)

    sql = 'select k2 ,k1, k12 from %s group by k2, k1, k12' % agg_tb
    msg = ''
    util.assert_return(False, msg, client.create_materialized_view, agg_tb, 'mv1_agg_type', sql)

    sql = 'select k2 ,k1, k13 from %s group by k2, k1, k13' % agg_tb
    msg = "must use with specific function, and don't support filter"
    util.assert_return(False, msg, client.create_materialized_view, agg_tb, 'mv1', sql)

    sql = 'select k2 ,k1, k3 from %s where k3 > 0 group by k2, k1, k3' % agg_tb
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, agg_tb, 'mv1_where', sql)
    
    sql = 'select k2 ,k1, k3, sum(k4 + k5) from %s group by k2, k1, k3' % agg_tb
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, agg_tb, 'mv1_sum_expr', sql)

    sql = 'select k1, k2 from %s' % agg_tb
    msg = 'The materialized view of aggregation or unique table must has grouping columns'
    util.assert_return(False, msg, client.create_materialized_view, agg_tb, 'mv1_no_agg', sql)

    sql = 'select k1, k2, sum(k4) from %s group by k1, k2' % agg_tb
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, agg_tb, 'mv1_not_key', sql)
    
    sql = 'select k1, k2, count(k4) from %s group by k1, k2' % agg_tb
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, agg_tb, 'mv1_count', sql, is_wait=True)

    sql = 'select k1, k2, hll_union(k4) from %s group by k1, k2' % agg_tb
    msg = 'HLL_UNION, HLL_UNION_AGG, HLL_RAW_AGG and HLL_CARDINALITY\'s params must be hll column'
    util.assert_return(False, msg, client.create_materialized_view, agg_tb, 'mv1', sql)

    client.clean(database_name)


def test_create_dup_mv_failed():
    """
    {
       "title": "test_create_agg_mv_failed",
       "describe": "测试创建dup表的物化视图的限制, 支持一个列出现多次, 不支持其他函数，支持select和select group其他不支持",
       "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert ret
    sql = 'select k1 ,k2, sum(k3), min(k3) from %s group by k1, k2' % table_name
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, table_name, 'mv100', sql, is_wait=True)

    sql = 'select k4 ,k5, sum(k1), max(k2), min(k3) from %s group by k4, k5' % table_name
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, table_name, 'mv11', sql, is_wait=True)

    sql = 'select k1 ,k2, k1, sum(k12) from %s group by k1, k2' % table_name
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, table_name, 'mv101', sql, is_wait=True)

    sql = 'select k1 ,k2, sum(k12) from %s' % table_name
    msg = 'select list expression not produced by aggregation output (missing from GROUP BY clause?): `k1`'
    util.assert_return(False, msg, client.create_materialized_view, table_name, 'mv1', sql)

    sql = 'select sum(k12) from %s' % table_name
    msg = 'The materialized view must contain at least one key column'
    util.assert_return(False, msg, client.create_materialized_view, table_name, 'mv1', sql)

    sql = 'select k2 ,k1, lower(k6) from %s' % table_name
    msg = ''
    util.assert_return(True, '', client.create_materialized_view, table_name, 'mv102', sql)

    sql = 'select k2 ,k1, k12 from %s' % table_name
    msg = ''
    util.assert_return(True, msg, client.create_materialized_view, table_name, 'mv12', sql, is_wait=True)

    sql = 'select k2 ,k1, k3 from %s where k3 > 0' % table_name
    msg = 'The where clause is not supported in add materialized view clause, expr:`k3` > 0'
    util.assert_return(False, msg, client.create_materialized_view, table_name, 'mv1', sql)

    sql = 'select k2 ,k1, k3, sum(k4 + k5) from %s group by k2, k1, k3' % table_name
    msg = 'The function sum must match pattern:sum(column)'
    util.assert_return(False, msg, client.create_materialized_view, table_name, 'mv1', sql)

    sql = 'select k2 ,k1, k3, sum(k4) from %s group by k2, k1, k3' % table_name
    msg = 'The partition and distributed columns k4 must be key column in mv'
    util.assert_return(False, msg, client.create_materialized_view, table_name, 'mv1', sql)

    sql = 'select k2 ,k1, k3, hll_union(hll_hash(k6)) from %s group by k2, k1, k3' % table_name
    # msg = 'The function hll_union must match pattern:hll_union(hll_hash(column)) column could not be decimal.' \
    #       ' Or hll_union(hll_column) in agg table'
    util.assert_return(True, 'ok', client.create_materialized_view, table_name, 'mv1', sql, is_wait=True)

    client.clean(database_name)


def test_broker_load_with_mv():
    """
    {
       "title": "test_create_agg_mv_failed",
       "describe": "测试创建dup表的物化视图后，进行broker导入，导入成功，base表和物化视图的查询结果正确",
       "tag": "function,p1"
    }
    """
    """todo"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert client.show_tables(table_name), 'get table failed'
    assert ret, 'create table failed'
    # create mv
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
          'bitmap_union(to_bitmap(k1)) from %s.%s group by k0'
    assert client.create_materialized_view(table_name, index_name, sql % (database_name, table_name), 
                                           is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'get mv failed'
    # broker load
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    assert ret, 'load data failed'
    # check
    mv_list = common.get_explain_rollup(client, sql % (database_name, table_name))
    assert mv_list == [index_name], 'expect rollup: %s' % mv_list
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, check_agg_tb), forced=True)
    sql = 'select k0, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12 from %s.%s order by k1'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, check_agg_tb))
    sql = 'select k0, max(k9), min(k11), sum(k12), count(k10), hll_union_agg(hll_hash(k7)), ' \
          'count(distinct k1) from %s.%s group by k0' 
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, check_agg_tb), forced=True)
    client.clean(database_name)


def test_stream_load_with_mv():
    """
    {
       "title": "test_stream_load_with_mv",
       "describe": "测试创建dup表的物化视图后，进行stream导入，导入成功，base表和物化视图的查询结果正确",
       "tag": "function,p1"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'get table failed'
    # create mv
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
          'bitmap_union(to_bitmap(k1)) from %s.%s group by k0'
    assert client.create_materialized_view(table_name, index_name, sql % (database_name, table_name),
                                           is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'get mv failed'
    # stream load
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12', 'k1=v1',
                        'k2=v2', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                        'k12=v12', 'k0=v7', 'k5=v4*101']
    ret = client.stream_load(table_name, FILE.baseall_local_file, column_name_list=column_name_list)
    assert ret, 'stream load failed'
    # check data
    mv_list = common.get_explain_rollup(client, sql % (database_name, table_name))
    assert mv_list == [index_name], 'expect mv: %s, actural mv: %s' % (mv_list, mv_list)
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, check_agg_tb), forced=True)
    sql = 'select k0, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12 from %s.%s order by k1'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, check_agg_tb))
    sql = 'select k0, max(k9), min(k11), sum(k12), count(k10), hll_union_agg(hll_hash(k7)), ' \
          'count(distinct k1) from %s.%s group by k0'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql % (check_db, check_agg_tb), forced=True)
    client.clean(database_name)


def test_insert_with_mv():
    """
    {
       "title": "test_stream_load_with_mv",
       "describe": "测试创建dup表的物化视图后，进行stream导入，导入成功，base表和物化视图的查询结果正确",
       "tag": "function,p1"
    }
    """
    """
    验证创建物化视图后，进行数据导入，删除，查询结果正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'get table failed'
    # create mv
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
          'bitmap_union(to_bitmap(k1)) from %s.%s group by k0'
    assert client.create_materialized_view(table_name, index_name, sql % (database_name, table_name), is_wait=True)
    assert client.get_index(table_name, index_name)
    # insert data
    isql = 'insert into %s.%s values(1, 0, 0, 0, 0, 0, 0, "true", "2020-01-01", "2020-01-01 09:00:00", ' \
           '"hello", 0.12, 0.13)' % (database_name, table_name)
    ret = client.execute(isql)
    assert ret == ()
    # check data
    mv_list = common.get_explain_rollup(client, sql % (database_name, table_name))
    assert mv_list == [index_name], 'expect rollup: %s' % mv_list
    sql2 = 'select 1, cast("2020-01-01 09:00:00" as datetime), 0.12, 0.13, 1, null, null'
    common.check2(client, sql1=sql % (database_name, table_name), sql2=sql2, forced=True)
    sql1 = 'select * from %s.%s' % (database_name, table_name)
    sql2 = 'select 1, 0, 0, 0, 0, "0", 0.0, "true", cast("2020-01-01" as date), ' \
           'cast("2020-01-01 09:00:00" as datetime), "hello", 0.12, 0.13'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_null_with_mv():
    """
       {
       "title": "test_null_with_mv",
       "describe": "测试物化视图的Null值，包括创建和查询命中，与分区表无关",
       "tag": "function,p0"
       }
    """
    """测试物化视图的Null值，包括创建和查询命中，与分区表无关"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table 
    distribution_info = palo_client.DistributionInfo('HASH(k0)', 10)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list,
                              distribution_info=distribution_info, set_null=True)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'get table failed'
    # create mv
    sql1 = 'select k0, max(k1), max(k2), max(k3), max(k4), max(k5), max(k6), max(k7), max(k8), max(k9), ' \
           'max(k10), max(k11), max(k12) from %s group by k0' % table_name
    sql2 = 'select k0, min(k1), min(k2), min(k3), min(k4), min(k5), min(k6), min(k7), min(k8), min(k9), ' \
           'min(k10), min(k11), min(k12) from %s group by k0' % table_name
    sql3 = 'select k0, sum(k1), sum(k2), sum(k3), sum(k4), sum(k5), sum(k6), sum(k11), sum(k12) from %s ' \
           'group by k0' % table_name
    sql4 = 'select k0, count(k1), count(k2), count(k3), count(k4), count(k5), count(k6), count(k7), ' \
           'count(k8), count(k9), count(k10), count(k11), count(k12) from %s group by k0' % table_name
    sql5 = 'select k0, hll_union(hll_hash(k1)), hll_union(hll_hash(k2)), hll_union(hll_hash(k3)),' \
           ' hll_union(hll_hash(k4)), hll_union(hll_hash(k5)), ' \
           'hll_union(hll_hash(k7)), hll_union(hll_hash(k8)), hll_union(hll_hash(k9)), ' \
           'hll_union(hll_hash(k10)), hll_union(hll_hash(k11)), hll_union(hll_hash(k12)) ' \
           'from %s group by k0' % table_name
    sql6 = 'select k0, bitmap_union(to_bitmap(k1)), bitmap_union(to_bitmap(k2)), bitmap_union(to_bitmap(k3)), ' \
           'bitmap_union(to_bitmap(k4)) from %s group by k0' % table_name
    mv1, mv2, mv3, mv4, mv5, mv6 = 'mv_max', 'mv_min', 'mv_sum', 'mv_count', 'mv_hll', 'mv_bitmap'
    assert client.create_materialized_view(table_name, mv1, sql1, is_wait=True), 'create mv failed'
    assert client.create_materialized_view(table_name, mv2, sql2, is_wait=True), 'create mv failed'
    assert client.create_materialized_view(table_name, mv3, sql3, is_wait=True), 'create mv failed'
    assert client.create_materialized_view(table_name, mv4, sql4, is_wait=True), 'create mv failed'
    assert client.create_materialized_view(table_name, mv5, sql5, is_wait=True), 'create mv failed'
    assert client.create_materialized_view(table_name, mv6, sql6, is_wait=True), 'create mv failed'
    # insert data
    insert_sql = 'insert into %s.%s values(1, 0, 0, 0, 0, 0, 0, "true", "2020-01-01", ' \
                 '"2020-01-01 09:00:00", "hello", 0.12, 0.13),' \
                 '(0, null, null, null, null, null, null, null, null, null, null, null, null), ' \
                 '(1, null, null, null, null, null, null, null, null, null, null, null, null)' \
                 % (database_name, table_name)
    rows, ret = client.execute(insert_sql, True)
    assert ret == (), 'insert data failed'
    assert rows == 3, 'expect 3 rows affected'
    # check
    sql1_check = 'select 1, 0, 0, 0, 0, "0", 0, "true", cast("2020-01-01" as date), ' \
                 'cast("2020-01-01 09:00:00" as datetime), "hello", 0.12, 0.13  union ' \
                 'select 0, null, null, null, null, null, null, null, null, null, null, null, null'
    common.check2(client, sql1=sql1, sql2=sql1_check, forced=True)
    common.check2(client, sql1=sql2, sql2=sql1_check, forced=True)
    sql3_check = 'select 1, 0, 0, 0, 0, "0", 0, 0.12, 0.13 union ' \
                 'select 0, null, null, null, null, null, null, null, null'
    common.check2(client, sql1=sql3, sql2=sql3_check, forced=True)
    sql4_check = 'select 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 union select 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0'
    common.check2(client, sql1=sql4, sql2=sql4_check, forced=True)
    sql5 = 'select k0, hll_union_agg(hll_hash(k1)), hll_union_agg(hll_hash(k2)), hll_union_agg(hll_hash(k3)),' \
           ' hll_union_agg(hll_hash(k4)), hll_union_agg(hll_hash(k5)), ' \
           'hll_union_agg(hll_hash(k7)), hll_union_agg(hll_hash(k8)), hll_union_agg(hll_hash(k9)), ' \
           'hll_union_agg(hll_hash(k10)), hll_union_agg(hll_hash(k11)), hll_union_agg(hll_hash(k12)) ' \
           'from %s group by k0' % table_name
    sql5_check = 'select 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 union select 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0'
    common.check2(client, sql1=sql5, sql2=sql5_check, forced=True)
    sql6 = 'select k0, count(distinct k1), count(distinct k2), count(distinct k3), count(distinct k4) ' \
           'from %s group by k0' % table_name
    sql6_check = 'select 1, 1, 1, 1, 1 union select 0, 0, 0, 0, 0'
    common.check2(client, sql1=sql6, sql2=sql6_check, forced=True)
    client.clean(database_name)


def test_add_partiton_with_mv():
    """
       {
       "title": "test_add_partiton_with_mv",
       "describe": "验证创建物化视图后，增加分区，向分区导入数据，数据正确，查询结果正确",
       "tag": "system,p1"
       }
    """
    """
    验证创建物化视图后，增加分区，向分区导入数据，数据正确，查询结果正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # 建表
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3"], ["-10", "0", "10"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, 
                              distribution_info=distribution_info, partition_info=partition_info)
    assert ret, 'create table filed'
    assert client.show_tables(table_name), 'get table failed'
    # 创建物化视图
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
          'bitmap_union(to_bitmap(k2)) from %s.%s group by k0' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'get mv failed'
    # 导入
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)

    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret, 'load data failed'
    # 增加分区后导入
    assert client.add_partition(table_name, 'p4', '20'), 'add partition failed'
    assert client.get_partition(table_name, 'p4'), 'get new partition failed'
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info)
    # check data
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % mv_list
    sql2 = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
           'bitmap_union(to_bitmap(abs(k1))) from (select k0, k1, k9, k11, k12, k7, k10 from %s.%s ' \
           'union all select k0, k1, k9, k11, k12, k7, k10 from %s.%s where k1 < 10) sub ' \
           'group by k0' % (check_db, check_agg_tb, check_db, check_agg_tb)
    common.check2(client, sql1=sql, sql2=sql2, forced=True)
    client.clean(database_name)


def test_drop_partition_with_mv():
    """
       {
       "title": "test_drop_partiton_with_mv",
       "describe": "验证创建物化视图后，删除分区，向分区导入数据，数据正确，查询结果正确",
       "tag": "system,p1"
       }
    """
    """
    验证创建物化视图后，增加分区，向分区导入数据，数据正确，查询结果正确
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, 
                              distribution_info=distribution_info, partition_info=partition_info)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'get table failed'
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'get mv failed'

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)

    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret, 'load data failed'
    # 删除分区
    assert client.drop_partition(table_name, 'p4'), 'drop partition failed'
    assert not client.get_partition(table_name, 'p4'), 'get dropped partition'
    # check data
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % mv_list
    sql2 = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
           'bitmap_union(to_bitmap(abs(k2))) from (select * from %s.%s where k1 < 10) sub group by k0' \
           % (check_db, check_agg_tb)
    common.check2(client, sql1=sql, sql2=sql2, forced=True)
    client.clean(database_name)


def test_drop_value_column_in_mv():
    """
       {
       "title": "test_drop_value_column_in_mv",
       "describe": "创建mv后，删除mv中的列，验证删除value列成功，验证mv中的schema，查询命中mv，数据正确",
       "tag": "system,p1"
       }
    """
    """
    创建mv后，删除mv中的列
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'get table failed'
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'get mv failed'

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)

    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret, 'load data failed'
    ret = client.schema_change_drop_column(table_name, ['k12'], index_name, is_wait_job=True)
    assert ret, 'drop column failed'
    sql = 'SELECT k0, max(k9), min(k11), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0' % (database_name, table_name)
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'shoot mv error. expect %s, actural %s' % (mv_list, index_name)
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k0, k1, abs(k2), k3, k4, k5, k6, k7, k8, k9, k10, k11, k12 from %s.%s ' \
           'order by k1' % (check_db, check_agg_tb)
    common.check2(client, sql1=sql1, sql2=sql2)

    ret = client.get_index_schema(table_name, index_name)
    column_fields = [x.replace('`', '') for x in util.get_attr(ret, DescInfo.Field)]
    expect_fields = ['k0', 'k9', 'k11', 'CASE WHEN k10 IS NULL THEN 0 ELSE 1 END',
                     'hll_hash(k7)', 'to_bitmap_with_check(k2)']
    assert column_fields == expect_fields, 'mv filed check error, expect %s' % expect_fields
    client.clean(database_name)


def test_drop_key_column_in_mv():
    """
       {
       "title": "test_drop_key_column_in_mv",
       "describe": "duplicate表创建mv后，删除mv中的key列, 验证删除key列成功，验证mv中的schema，查询命中mv，数据正确",
       "tag": "system,p1"
       }
    """
    """duplicate表，删除mv中的key列"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert client.show_tables(table_name), 'can not get table'
    assert ret, 'create table failed'

    sql = 'SELECT k0, k8, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
          'bitmap_union(to_bitmap(k2)) from %s.%s group by k0, k8' 
    assert client.create_materialized_view(table_name, index_name, sql % (database_name, table_name), 
                                           is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'can not get mv: %s' % index_name

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret, 'load data failed'
    mv_list = common.get_explain_rollup(client, sql % (database_name, table_name))
    assert mv_list == [index_name], 'expect rollup: %s' % mv_list

    ret = client.schema_change_drop_column(table_name, ['k0'], index_name, is_wait_job=True)
    assert ret, 'drop mv k0 failed'
    sql = 'SELECT k8, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k8' % (database_name, table_name)
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % index_name
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k0, k1, abs(k2), k3, k4, k5, k6, k7, k8, k9, k10, k11, k12 from %s.%s ' \
           'order by k1' % (check_db, check_agg_tb)
    common.check2(client, sql1=sql1, sql2=sql2)

    ret = client.get_index_schema(table_name, index_name)
    column_fields = [x.replace('`', '') for x in util.get_attr(ret, DescInfo.Field)]
    expect_fields = ['k8', 'k9', 'k11', 'k12', 'CASE WHEN k10 IS NULL THEN 0 ELSE 1 END',
                     'hll_hash(k7)', 'to_bitmap_with_check(k2)']
    assert column_fields == expect_fields, 'mv filed check error'
    client.clean(database_name)


def test_add_key_column_in_mv():
    """
       {
       "title": "test_add_key_column_in_mv",
       "describe": "duplicate表创建mv后，向mv和base表中增加key列，验证增加成功，验证mv中的schema，查询命中mv，数据正确",
       "tag": "system,p1"
       }
    """
    """duplicate表，向mv中增加key列"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert client.show_tables(table_name), 'can not get table'
    assert ret, 'create table failed'

    sql = 'SELECT k0, max(k8), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0' 
    assert client.create_materialized_view(table_name, index_name, sql % (database_name, table_name),
                                           is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'can not get mv: %s' % index_name

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret, 'load data failed'
    mv_list = common.get_explain_rollup(client, sql % (database_name, table_name))
    assert mv_list == [index_name], 'expect rollup: %s' % mv_list
    add_column_list = [('k_add', 'int', 'key', '0')]
    ret = client.schema_change_add_column(table_name, add_column_list, to_table_name=index_name, is_wait_job=True)
    assert ret, 'drop mv k0 failed'
    sql = 'SELECT k0, k_add, max(k8), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
          'bitmap_union(to_bitmap(k2)) from %s.%s group by k0, k_add' % (database_name, table_name)
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % index_name
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k0, k1, abs(k2), 0, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12 from %s.%s order by k1' % (
            check_db, check_agg_tb)
    common.check2(client, sql1=sql1, sql2=sql2)

    ret = client.get_index_schema(table_name, index_name)
    column_fields = [x.replace('`', '') for x in util.get_attr(ret, DescInfo.Field)]
    expect_fields = ['k0', 'k_add', 'k8', 'k11', 'k12', 'CASE WHEN k10 IS NULL THEN 0 ELSE 1 END',
                     'hll_hash(k7)', 'to_bitmap_with_check(k2)']
    assert column_fields == expect_fields, 'mv filed check error: expect %s' % expect_fields
    client.clean(database_name)


def test_modify_column_type_in_mv():
    """
       {
       "title": "test_modify_column_type_in_mv",
       "describe": "duplicate表，修改mv中的列类型",
       "tag": "system,p1"
       }
    """
    """duplicate表，修改mv中的列类型"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info)
    assert client.show_tables(table_name), 'can not get table'
    assert ret, 'create table failed'

    sql = 'SELECT k1, max(k8), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k1' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'can not get mv: %s' % index_name

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret, 'load data failed'
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % mv_list
    modify_column_list = [('k1', 'int', 'key')]
    ret = client.schema_change_modify_column(table_name, 'k1', 'INT KEY', is_wait_job=True)
    assert ret, 'drop mv k0 failed'
    sql = 'SELECT k1, max(k8), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k1' % (database_name, table_name)
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % index_name
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select k0, k1, abs(k2), k3, k4, k5, k6, k7, k8, k9, k10, k11, k12 from %s.%s order by k1' % (
        check_db, check_agg_tb)
    common.check2(client, sql1=sql1, sql2=sql2)

    ret = client.get_index_schema(table_name, index_name)
    column_fields = [x.replace('`', '') for x in util.get_attr(ret, DescInfo.Field)]
    expect_fields = ['k1', 'k8', 'k11', 'k12', 'CASE WHEN k10 IS NULL THEN 0 ELSE 1 END', 'hll_hash(k7)',
                     'to_bitmap_with_check(k2)']
    assert column_fields == expect_fields, 'mv filed check error, expect: %s' % expect_fields
    client.clean(database_name)


def test_modify_column_order_in_mv():
    """
       {
       "title": "test_modify_column_order_in_mv",
       "describe": "duplicate表，修改mv中的列顺序",
       "tag": "function,p1,fuzz"
       }
    """
    """duplicate表，修改mv中的列顺序"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert client.show_tables(table_name), 'can not get table'
    assert ret, 'create table failed'

    sql = 'SELECT k0, k1, max(k8), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
          'bitmap_union(to_bitmap(k2)) from %s.%s group by k1, k0' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'can not get mv: %s' % index_name

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret, 'load data failed'
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % mv_list
    modify_column_list = ['k1', 'k0', 'mv_hll_union_k7', 'mv_bitmap_union_k2', 'k8', 'k11', 'k12']
    flag = True
    try:
        ret = client.schema_change_order_column(table_name, column_name_list=modify_column_list,
                                                from_table_name=index_name, is_wait_job=True)
        flag = False
    except Exception as e:
        print(str(e))
    # 暂时不支持修改含有hll_union, bitmap_union, count聚合的物化视图，语法解析报错
    # assert ret, 'drop mv k0 failed'
    # sql = 'SELECT k1, max(k8), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
    #       'from %s.%s group by k1' % (database_name, table_name)
    # mv_list = common.get_explain_rollup(client, sql)
    # assert mv_list == [index_name], 'expect rollup: %s' % index_name
    # sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    # sql2 = 'select k0, k1, abs(k2), k3, k4, k5, k6, k7, k8, k9, k10, k11, k12 from %s.%s order by k1' % (
    #     check_db, check_agg_tb)
    # common.check2(client, sql1=sql1, sql2=sql2)

    # ret = client.get_index_schema(table_name, index_name)
    # column_fields = util.get_attr(ret, DescInfo.Field)
    # assert column_fields == modify_column_list, 'mv filed check error'
    client.clean(database_name)


def test_alter_not_support():
    """
       {
       "title": "test_alter_not_support",
       "describe": "duplicate表，验证不支持的alter操作",
       "tag": "function,p0,fuzz"
       }
    """
    """验证不支持的alter操作"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    # init db & table
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert ret, 'create table failed'
    assert client.show_tables(table_name), 'can not get table'
    # create mv
    sql = 'SELECT k0, k1, k3, max(k8), min(k11), sum(k12), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0, k1, k3' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'create mv failed'
    assert client.get_index(table_name, index_name), 'can not get mv: %s' % index_name
    # load data & check
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret, 'load data failed'
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % mv_list
    sql = 'ALTER TABLE %s.%s ADD COLUMN v_add HLL TO %s' % (database_name, table_name, index_name)
    msg = 'Bitmap and hll type have to use aggregate function'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'ALTER TABLE %s.%s ADD COLUMN k1 INT KEY TO %s' % (database_name, table_name, index_name)
    msg = 'Can not add column which already exists in base table: k1'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'ALTER TABLE %s.%s ADD COLUMN v_add INT TO %s' % (database_name, table_name, index_name)
    msg = 'Please add non-key column on base table directly'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'ALTER TABLE %s.%s ADD COLUMN v_add INT SUM DEFAULT "0" TO %s' % (database_name, table_name, index_name)
    msg = 'Can not assign aggregation method on column in Duplicate data model table: v_add'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'ALTER TABLE %s.%s MODIFY COLUMN k1 BIGINT KEY' % (database_name, table_name)
    msg = 'Can not modify partition column[k1]'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'ALTER TABLE %s.%s MODIFY COLUMN k1 BIGINT' % (database_name, table_name)
    # msg = 'Invalid column order'
    msg = ' '
    util.assert_return(False, msg, client.execute, sql)
    sql = 'ALTER TABLE %s.%s MODIFY COLUMN k3 BIGINT DEFAULT "1"' % (database_name, table_name)
    msg = 'Can not change default value'
    util.assert_return(False, msg, client.execute, sql)
    sql = 'ALTER TABLE %s.%s MODIFY COLUMN k2 BIGINT DEFAULT NULL' % (database_name, table_name)
    msg = 'Can not change aggregation'
    util.assert_return(False, msg, client.execute, sql) 
    sql = 'ALTER TABLE %s.%s MODIFY COLUMN k3 BIGINT KEY DEFAULT NULL FROM %s' % (database_name, 
                                                                                  table_name, index_name)
    msg = 'Do not need to specify index name when just modifying column type'
    util.assert_return(False, msg, client.execute, sql)
    client.clean(database_name)


def test_delete_with_mv():
    """
    {
    "title": "test_delete_with_mv",
    "describe": "duplicate表，创建物化视图后，删除某几行记录，验证删除成功，数据正确",
    "tag": "function,p1"
    }
    """
    """
    创建物化视图后，删除某几行记录
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert client.show_tables(table_name)
    assert ret
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True)
    assert client.get_index(table_name, index_name)

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)

    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret
    ret = client.delete(table_name, [('k0', '=', '0')], 'p3')
    assert ret
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % index_name
    sql2 = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), ' \
           'bitmap_union(to_bitmap(abs(k2))) from %s.%s where (k0 != 0 and k1 < 10) or ' \
           'k1 >= 10 group by k0' % (check_db, check_agg_tb)
    common.check2(client, sql1=sql, sql2=sql2, forced=True)
    sql1 = 'SELECT * FROM %s.%s order by k1' % (database_name, table_name)
    sql2 = 'SELECT k0, k1, abs(k2), k3, k4, k5, k6, k7, k8, k9, k10, k11, k12 FROM %s.%s ' \
           'WHERE (k0 != 0 and k1 < 10) or k1 >= 10 order by k1' % (check_db, check_agg_tb)
    common.check2(client, sql1=sql1, sql2=sql2)  
    client.clean(database_name)


def test_delete_with_mv_failed():
    """
    {
    "title": "test_delete_with_mv_failed",
    "describe": "duplicate表，物化视图，删除操作的限制",
    "tag": "function,p1,fuzz"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert client.show_tables(table_name)
    assert ret
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True)
    assert client.get_index(table_name, index_name)

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)

    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret
   
    ret = client.delete(table_name, [('k0', '=', '0')], 'p3')
    msg =  "Unknown column 'k1' in 'index"
    util.assert_return(False, msg, client.delete, table_name, [('k1', '<', '0')], 'p1')
    msg =  " "
    util.assert_return(False, msg, client.delete, table_name, [('k2', '<', '0')], 'p1')
    client.clean(database_name)


def test_create_mv_when_load():
    """
    {
    "title": "test_create_mv_when_load",
    "describe": "duplicate表，验证创建物化视图的同时向表进行数据导入，验证创建成功",
    "tag": "system,p0,stability"
    }
    """
    """
    验证创建物化视图的同时向表进行数据导入，验证创建成功
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert client.show_tables(table_name)
    assert ret

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)

    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql)
    state = RollupJob(client.show_rollup_job()[0]).get_state()
    succ_cnt = 0
    while state != 'FINISHED' and state != 'CANCELLED':
        ret = client.batch_load(util.get_label(), data_desc_list, broker=broker_info, is_wait=True)
        if ret:
            succ_cnt += 1
        time.sleep(2)
        state = RollupJob(client.show_rollup_job()[0]).get_state()
        print(state)
    assert 'FINISHED' == RollupJob(client.show_rollup_job()[0]).get_state()
    client.wait_table_load_job()
    ret = client.show_load()
    finished_job = util.get_attr_condition_list(ret, LoadJob.State, 'FINISHED', LoadJob.Label)
    count = len(finished_job)
    # check
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % index_name
    sql2 = 'SELECT k0, max(k9), min(k11), sum(k12) * %s, count(k10) * %s, hll_union(hll_hash(k7)), ' \
           'bitmap_union(to_bitmap(abs(k2))) from %s.%s group by k0' % (count, count, check_db, check_agg_tb)
    common.check2(client, sql1=sql, sql2=sql2, forced=True)
    client.clean(database_name)


def test_create_mv_based_on_mv():
    """
    {
    "title": "test_create_mv_based_on_mv",
    "describe": "duplicate表，基于物化视图创建物化视图",
    "tag": "function,p1"
    }
    """
    """基于物化视图创建物化视图"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    # create table
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert client.show_tables(table_name)
    assert ret
    # load data
    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)
    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret
    # create mv
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True)
    assert client.get_index(table_name, index_name)
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % index_name
    # create mv based on mv
    sql = 'SELECT k0, max(k9), min(k11), count(k10) from %s.%s group by k0' % (database_name, table_name)
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name], 'expect rollup: %s' % index_name
    index_name1 = 'new_' + index_name
    assert client.create_materialized_view(table_name, index_name1, sql, is_wait=True)
    assert client.get_index(table_name, index_name1)
    mv_list = common.get_explain_rollup(client, sql)
    assert mv_list == [index_name1], 'expect rollup: %s' % index_name1    
    sql2 = 'SELECT k0, max(k9), min(k11), count(k10) from %s.%s group by k0' % (check_db, check_agg_tb)
    common.check2(client, sql1=sql, sql2=sql2, forced=True)

    client.clean(database_name)


def test_drop_mv():
    """
    {
    "title": "test_create_mv_based_on_mv",
    "describe": "duplicate表，验证删除物化视图",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    distribution_info = palo_client.DistributionInfo('HASH(k4, k5)', 10)
    partition_info = palo_client.PartitionInfo("k1", ["p1", "p2", "p3", "p4"], ["-10", "0", "10", "20"])
    ret = client.create_table(table_name, DATA.datatype_column_no_agg_list, distribution_info=distribution_info,
                              partition_info=partition_info)
    assert client.show_tables(table_name)
    assert ret
    sql = 'SELECT k0, max(k9), min(k11), sum(k12), count(k10), hll_union(hll_hash(k7)), bitmap_union(to_bitmap(k2)) ' \
          'from %s.%s group by k0' % (database_name, table_name)
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True)
    assert client.get_index(table_name, index_name)

    column_name_list = ['v1', 'v2', 'v3', 'v4', 'v6', 'v7', 'v8', 'v9', 'v10', 'v11', 'v12']
    set_list = ['k1=v1', 'k2=abs(v2)', 'k3=v3', 'k4=v4', 'k6=v6', 'k7=v7', 'k8=v8', 'k9=v9', 'k10=v10', 'k11=v11',
                'k12=v12', 'k0=v7', 'k5=v4*101']
    data_desc_list = palo_client.LoadDataInfo(FILE.baseall_hdfs_file, table_name, column_name_list=column_name_list,
                                              set_list=set_list)

    ret = client.batch_load(util.get_label(), data_desc_list, is_wait=True, broker=broker_info, max_filter_ratio=0.5)
    assert ret
    ret = client.drop_materialized_view(database_name, table_name, index_name)
    assert ret == ()
    assert not client.get_index(table_name, index_name)
    sql1 = 'select * from %s' % table_name
    sql2 = 'select k0, k1, abs(k2), k3, k4, k5, k6, k7, k8, k9, k10, k11, k12 from %s.%s' % (database_name, table_name)
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_issue_5164():
    """
    {
    "title": "test_issue_5164",
    "describe": "5164",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    column_list = [('record_id', 'int'), ('seller_id', 'int'), ('store_id', 'int'),
                   ('sale_date', 'date'), ('sale_amt', 'bigint')]
    ret = client.create_table(table_name, column_list)
    assert ret
    sql = 'select store_id from %s' % table_name
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True)
    sql = 'insert into %s(record_id, seller_id, store_id, sale_date, sale_amt) ' \
          'values(1, 1, 1, "2020-12-30",1)' % table_name
    assert client.execute(sql) == ()
    sql1 = "with t as (SELECT store_id as id , 'kks' as aaa, sum(seller_id) as seller, sum(sale_amt) as a " \
           "FROM %s GROUP BY store_id) " \
           "select id, t1 from(select id, a as t1 from t union all select id, aaa as t1 from t) k2" % table_name
    sql2 = "select 1, '1' union select 1, 'kks'"
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


def test_issue_7361():
    """
    {
    "title": "test_issue_7361",
    "describe": "Mv rewrite bug may cause SQL failure",
    "tag": "function,p0"
    }
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client = common.create_workspace(database_name)
    column_list = [('k1', 'int'), ('k2', 'int')]
    ret = client.create_table(table_name, column_list, set_null=True)
    assert ret, 'create table failed'
    sql = 'select k1, count(k2) from %s group by k1' % table_name
    assert client.create_materialized_view(table_name, index_name, sql, is_wait=True), 'create mv failed'
    sql = 'insert into %s values(1, 1), (2, 2), (3, 3)' % table_name
    assert client.execute(sql) == ()
    sql = 'insert into %s values(1, 1), (2, null), (3, null)' % table_name
    assert client.execute(sql) == ()
    sql1 = 'select k1, count(k2) / count(1) from %s group by k1' % table_name
    sql2 = 'select 1, 1 union select 2, 0.5 union select 3, 0.5'
    common.check2(client, sql1=sql1, sql2=sql2, forced=True)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
