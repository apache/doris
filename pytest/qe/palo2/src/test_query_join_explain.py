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
join case, need to check explain
"""
import os
import pymysql
import sys
import time

sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util
import palo_query_plan
from warnings import filterwarnings
filterwarnings('ignore', category = pymysql.Warning)


sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "baseall"
join_name = "test"

if 'FE_DB' in os.environ.keys():
    db_name = os.environ["FE_DB"]
else:
    db_name = "test_query_qa"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryExecuter()
    timeout = 600
    print('check show data...')
    while timeout > 0:
        timeout -= 1
        flag = True
        ret = runner.query_palo.do_sql('show data')
        print(ret)
        for data in ret:
            if data[0] == table_name and data[1] == '.000':
                time.sleep(1)
                flag = False
            elif data[0] == join_name and data[1] == '.000':
                time.sleep(1)
                flag = False
            else:
                pass
        if flag:
            print('check show data ok')
            break


class QueryExecuter(QueryBase):
    """qeury case executer"""
    def __init__(self):
        self.get_clients()

    def check(self, psql, property_list, force_order=False, msql=None):
        """check palo sql result with mysql sql result"""
        print(psql)
        times = 0
        flag = 0
        if msql is None:
            msql = psql
        else:
            print(msql)
        while times <= 3:
            time.sleep(1)
            times += 1
            try:
                LOG.info(L('palo sql', palo_sql=psql))
                palo_result = self.query_palo.do_set_properties_sql(psql, property_list)
                LOG.info(L('mysql sql', mysql_sql=msql))
                self.mysql_cursor.execute(msql)
                mysql_result = self.mysql_cursor.fetchall()
                util.check_same(palo_result, mysql_result, force_order)
            except Exception as e:
                print(Exception, ":", e)
                LOG.error(L('error', error=e))
                assert 0 == 1, 'check error'

    def get_explain(self, sql, property_list):
        """get explain"""
        sql = 'EXPLAIN %s' % sql
        palo_result = self.query_palo.do_set_properties_sql(sql, property_list)
        return palo_result

    def check_join_op(self, sql, frag_no, expect, property_list=None):
        """get explain and checck join op"""
        if property_list is None:
            property_list = ['set enable_bucket_shuffle_join = true', 'set enable_infer_predicate = true']
        LOG.info(L('explain sql', palo_sql=sql))
        plan = self.get_explain(sql, property_list)
        plan_info = palo_query_plan.PlanInfo(plan)
        frag = plan_info.get_one_fragment(frag_no)
        join_op = plan_info.get_frag_hash_join(frag, 'join op')
        if isinstance(expect, str):
            assert join_op.find(expect) != -1, 'actural:%s, expect: %s' % (join_op, expect)
        elif isinstance(expect, list):
            check_flag = False
            for exp in expect:
                if join_op.find(exp) != -1:
                    check_flag = True
            assert check_flag, 'actural:%s, expect: %s' % (join_op, expect)


def test_shuffle_join():
    """
    {
    "title": "test_shuffle_join",
    "describe": "开启shuffle join，执行join，验证查询计划及结果",
    "tag": "function,p0"
    }
    """
    sql = 'select * from test, baseall where baseall.k1 = test.k1 and test.k1 < 64 and test.k1 > 0'
    if db_name in ['test_query_qa_multi', 'test_query_qa_list']:
        runner.check_join_op(sql, 2, 'INNER JOIN(BROADCAST)')
    else:
        runner.check_join_op(sql, 2, 'INNER JOIN(BUCKET_SHUFFLE)')
    runner.check(sql, ['set enable_bucket_shuffle_join = true'], True)
    sql = 'select * from test, baseall where baseall.k1 = test.k1 and test.k1 < 69 and test.k1 > 0'
    runner.check_join_op(sql, 2, ['INNER JOIN(BROADCAST)', 'INNER JOIN(PARTITIONED)'])
    runner.check(sql, ['set enable_bucket_shuffle_join = true'], True)
    sql = 'select * from bigtable, baseall where baseall.k1 = bigtable.k1 and baseall.k2 = bigtable.k3'
    if db_name in ['test_query_qa_multi', 'test_query_qa_list']:
        runner.check_join_op(sql, 2, 'INNER JOIN(PARTITIONED)')
    else:
        runner.check_join_op(sql, 2, 'INNER JOIN(BUCKET_SHUFFLE)')
    runner.check(sql, ['set enable_bucket_shuffle_join = true'], True)
    sql = 'select * from bigtable, baseall where baseall.k2 = bigtable.k3'
    runner.check_join_op(sql, 2, ['INNER JOIN(PARTITIONED)', 'INNER JOIN(BROADCAST)'])
    runner.check(sql, ['set enable_bucket_shuffle_join = true'], True)
    sql = 'select * from test, baseall where baseall.k1 = test.k1 and test.k1 = 32'
    if db_name == 'test_query_qa_list':
        runner.check_join_op(sql, 2, 'INNER JOIN(BUCKET_SHUFFLE)')
    elif db_name == 'test_query_qa_multi':
        runner.check_join_op(sql, 2, 'INNER JOIN(BROADCAST)')
    else:
        runner.check_join_op(sql, 1, 'INNER JOIN(BUCKET_SHUFFLE)')
    runner.check(sql, ['set enable_bucket_shuffle_join = true'], True)


def test_shuffle_join_1():
    """
    {
    "title": "test_shuffle_join_1",
    "describe": "开启shuffle join，执行join，验证查询计划及结果",
    "tag": "function,p0"
    }
    """
    sql = 'drop table if exists test1'
    runner.query_palo.do_sql(sql)
    sql = 'create table test1(k1 tinyint, k2 smallint, k3 int, k4 bigint, k5 decimal(9,3), k6 char(5), ' \
          'k10 date, k11 datetime, k7 varchar(20), k8 double max, k9 float sum) engine=olap ' \
          'partition by range(k1) (partition p1 values less than ("-64"), partition p2 values less than ("0"), ' \
          'partition p3 values less than ("64"), partition p4 values less than maxvalue ) ' \
          'distributed by hash(k1) buckets 5 properties("storage_type"="column")'
    runner.query_palo.do_sql(sql)
    sql = 'insert into test1 select * from baseall'
    runner.query_palo.do_sql(sql)
    
    sql1 = 'select * from test t, test1 t1 where t1.k1 = t.k1 and t1.k1 < 64 and t1.k1 > 0'
    if db_name in ['test_query_qa_multi', 'test_query_qa_list']:
        runner.check_join_op(sql1, 2, 'INNER JOIN(BROADCAST)')
    else:
        runner.check_join_op(sql1, 2, 'INNER JOIN(BUCKET_SHUFFLE)')
    sql2 = 'select * from test t, baseall t1 where t1.k1 = t.k1 and t1.k1 < 64 and t1.k1 > 0'
    runner.check(sql1, ['set enable_bucket_shuffle_join = true'], True, sql2)
    sql1 = 'select * from test t, test1 t1 where t1.k1 = t.k1 and t1.k1 <= 64 and t1.k1 > 0'
    runner.check_join_op(sql1, 2, 'INNER JOIN(BROADCAST)')
    sql2 = 'select * from test t, baseall t1 where t1.k1 = t.k1 and t1.k1 <= 64 and t1.k1 > 0'
    runner.check(sql1, ['set enable_bucket_shuffle_join = true'], True, sql2)
    sql = 'drop table test1'
    runner.query_palo.do_sql(sql)


def test_shuffle_join_bug():
    """
    {
    "title": "test_shuffle_join_1",
    "describe": "开启shuffle join，执行join，验证查询计划及结果",
    "tag": "function,p0"
    }
    """
    sql = 'drop table if exists test_bucket'
    LOG.info(L('execute sql', sql=sql))
    runner.query_palo.do_sql(sql)
    sql = 'CREATE TABLE `test_bucket` ( ' \
          '`k1` tinyint(4) NULL COMMENT "", ' \
          '`k2` bigint(20) NULL COMMENT "", ' \
          '`k3` int(11) NULL COMMENT "", ' \
          '`k4` bigint(20) NULL COMMENT "", ' \
          '`k5` decimal(9, 3) NULL COMMENT "", ' \
          '`k6` char(5) NULL COMMENT "", ' \
          '`k10` date NULL COMMENT "", ' \
          '`k11` datetime NULL COMMENT "", ' \
          '`k7` varchar(20) NULL COMMENT "", ' \
          '`k8` double NULL COMMENT "", ' \
          '`k9` float NULL COMMENT "" ' \
          ') ENGINE=OLAP ' \
          'DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`) ' \
          'COMMENT "OLAP" ' \
          'DISTRIBUTED BY HASH(`k1`) BUCKETS 2 ' \
          'PROPERTIES ( ' \
          '"replication_num" = "1", ' \
          '"in_memory" = "false", ' \
          '"storage_format" = "V2" ' \
          ')'
    LOG.info(L('execute sql', sql=sql))
    runner.query_palo.do_sql(sql)
    sql = 'insert into test_bucket select * from test'
    LOG.info(L('execute sql', sql=sql))
    runner.query_palo.do_sql(sql)
    sql = 'SELECT snvd.k2, snvd.k3 FROM ' \
          ' (SELECT snv.k2, snv.k3, snv.k1 FROM test snv ' \
          '   JOIN test_bucket detail ' \
          '   ON detail.k1 = snv.k1 where snv.k1 = 0) snvd ' \
          ' JOIN [broadcast] ' \
          ' (SELECT snvc.k2, snvc.k3, ds.k1 FROM test_bucket snvc ' \
          '   JOIN ' \
          '   (SELECT agsdd.k1, agsdd.k2, agsdd.k3 FROM test_bucket agsdd) ds ' \
          '   ON ds.k1 = snvc.k1 AND ds.k2 = snvc.k2) detail ' \
          ' ON detail.k2 = snvd.k2 AND detail.k3 = snvd.k3'
    LOG.info(L('set variable', cmd='set disable_colocate_plan = true'))
    LOG.info(L('execute sql', sql=sql))
    ret1 = runner.query_palo.do_set_properties_sql(sql, ['set disable_colocate_plan = true'])
    LOG.info(L('set variable', cmd='set disable_colocate_plan = false'))
    LOG.info(L('execute sql', sql=sql))
    ret2 = runner.query_palo.do_set_properties_sql(sql, ['set disable_colocate_plan = false'])
    util.check_same(ret1, ret2, True)


if __name__ == '__main__':
    setup_module()
    test_shuffle_join()


