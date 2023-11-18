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
file: stream_test_simple.py
测试 stream load hll & null 
"""

import os
import sys
import time

file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)

from data import schema as DATA
from lib import palo_config
from lib import palo_client
from lib import common

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L

host = config.fe_host
query_port = config.fe_query_port
http_port = config.fe_http_port
user = config.fe_user
password = config.fe_password


class TestHll(object):
    """test hll type"""
    database_name = 'stream_load_hll_test_db'

    def setup_class(self):
        """setup class"""
        client = palo_client.get_client(host, query_port, user=user, password=password)
        client.clean(self.database_name)
        ret = client.create_database(self.database_name)
        print('drop db')
        assert ret
        self.client = client
 
    def setUp(self):
        """setUp"""
        pass

    def test_hll_tinyint(self):
        """
        {
        "title": "test_stream_simple1.test_hll_tinyint",
        "describe": "test hll tinyint",
        "tag": "function,p1"
        }
        """
        """test hll tinyint"""
        table_name = 'tinyint_hll_test'
        insert_table_name = 'tinyint_hll_insert'
        self.client.use(self.database_name)
        column_list = DATA.hll_tinyint_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type)
        assert ret
        assert self.client.create_table(insert_table_name, column_list, 
                                        distribution_info=distribution_type)
        column_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3', 'dirty4']
        data_file = "%s/data/STREAM_LOAD/test_hash_tinyint.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port, 
                                      column_name_list=column_list)
        assert ret
        self.check(table_name)
        sql = 'insert into %s  select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        time.sleep(3)
        self.check(insert_table_name) 

    def test_hll_smallint(self):
        """
        {
        "title": "test_stream_simple1.test_hll_smallint",
        "describe": "test hll smallint",
        "tag": "function,p1"
        }
        """
        """test hll smallint"""
        table_name = 'smallint_hll_test'
        insert_table_name ='smallint_hll_insert'
        self.client.use(self.database_name)
        column_list = DATA.hll_smallint_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type)
        assert ret
        assert self.client.create_table(insert_table_name, column_list, 
                                        distribution_info=distribution_type)
        column_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3', 'dirty4']
        data_file = "%s/data/STREAM_LOAD/test_hash_smallint.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port, 
                                      column_name_list=column_list)
        assert ret
        self.check(table_name)
        sql = 'insert into %s  select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        self.check(insert_table_name)

    def test_hll_int(self):
        """
        {
        "title": "test_stream_simple1.test_hll_int",
        "describe": "test hll int",
        "tag": "function,p1"
        }
        """
        """test hll int"""
        table_name = 'int_hll_test'
        insert_table_name = 'int_hll_insert'
        self.client.use(self.database_name)
        column_list = DATA.hll_int_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type)
        assert ret
        assert self.client.create_table(insert_table_name, column_list, 
                                        distribution_info=distribution_type)
        column_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3', 'dirty4']
        data_file = "%s/data/STREAM_LOAD/test_hash_int.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port, 
                                      column_name_list=column_list)
        assert ret
        self.check(table_name)
        sql = 'insert into %s  select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        self.check(insert_table_name)

    def test_hll_bigint(self):
        """
        {
        "title": "test_stream_simple1.test_hll_bigint",
        "describe": "test hll bigint",
        "tag": "function,p1"
        }
        """
        """test hll bigint"""
        table_name = 'bigint_hll_test'
        insert_table_name = 'bigint_hll_insert'
        self.client.use(self.database_name)
        column_list = DATA.hll_bigint_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type)
        assert self.client.create_table(insert_table_name, column_list, 
                                        distribution_info=distribution_type)
        assert ret
        column_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3', 'dirty4']
        data_file = "%s/data/STREAM_LOAD/test_hash_bigint.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port, 
                                      column_name_list=column_list)
        assert ret
        self.check(table_name)
        sql = 'insert into %s  select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        self.check(insert_table_name)

    def test_hll_largeint(self):
        """
        {
        "title": "test_stream_simple1.test_hll_largeint",
        "describe": "test hll largeint",
        "tag": "function,p1"
        }
        """
        """test hll largeint"""
        table_name = 'largeint_hll_test'
        insert_table_name = 'largeint_hll_insert'
        self.client.use(self.database_name)
        column_list = DATA.hll_largeint_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type)
        assert ret
        assert self.client.create_table(insert_table_name, column_list, 
                                        distribution_info=distribution_type)
        column_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3', 'dirty4']
        data_file = "%s/data/STREAM_LOAD/test_hash_largeint.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port, 
                                      column_name_list=column_list)
        assert ret
        self.check(table_name)
        sql = 'insert into %s select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        self.check(insert_table_name)
  
    def test_hll_char(self):
        """
        {
        "title": "test_stream_simple1.test_hll_char",
        "describe": "test hll char",
        "tag": "function,p1"
        }
        """
        """test hll char"""
        table_name = 'char_hll_test'
        insert_table_name = 'char_hll_insert'
        self.client.use(self.database_name)
        column_list = DATA.hll_char_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type)
        assert ret
        assert self.client.create_table(insert_table_name, column_list, 
                                        distribution_info=distribution_type)
        column_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1']
        data_file = "%s/data/STREAM_LOAD/test_hash_char_normal.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port, 
                                      column_name_list=column_list)
        assert ret
        self.check(table_name)
        sql = 'insert into %s select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        self.check(insert_table_name)

    def test_hll_varchar(self):
        """
        {
        "title": "test_stream_simple1.test_hll_varchar",
        "describe": "test hll varchar",
        "tag": "function,p1"
        }
        """
        """test hll varchar"""
        table_name = 'varchar_hll_test'
        insert_table_name = 'varchar_hll_insert'
        self.client.use(self.database_name)
        column_list = DATA.hll_varchar_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, 
                                       distribution_info=distribution_type)
        assert ret
        assert self.client.create_table(insert_table_name, column_list, 
                                        distribution_info=distribution_type)
        column_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1']
        data_file = "%s/data/STREAM_LOAD/test_hash_varchar_least.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port,
                                      column_name_list=column_list)
        assert ret
        self.check(table_name)
        sql = 'insert into %s select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        self.check(insert_table_name)

    def test_hll_date(self):
        """
        {
        "title": "test_stream_simple1.test_hll_date",
        "describe": "test hll date",
        "tag": "function,p1"
        }
        """
        """test hll date"""
        table_name = 'date_hll_test'
        insert_table_name = 'date_hll_insert'
        self.client.use(self.database_name)
        column_list = DATA.hll_date_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type)
        assert ret
        ret = self.client.create_table(insert_table_name, column_list, 
                                       distribution_info=distribution_type)
        assert ret
        column_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3']
        data_file = "%s/data/STREAM_LOAD/test_hash_date.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port, 
                                      column_name_list=column_list)
        assert ret
        self.check(table_name)
        sql = 'insert into %s select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        self.check(insert_table_name)

    def test_hll_datetime(self):
        """
        {
        "title": "test_stream_simple1.test_hll_datetime",
        "describe": "test hll datetime",
        "tag": "function,p1"
        }
        """
        """test hll datetime"""
        table_name = 'datetime_hll_test'
        self.client.use(self.database_name)
        column_list = DATA.hll_datetime_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type)
        assert ret
        column_name_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3']
        data_file = "%s/data/STREAM_LOAD/test_hash_datetime.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port, 
                                      column_name_list=column_name_list)
        assert ret
        self.check(table_name)
        insert_table_name = 'datetime_hll_insert'
        assert self.client.create_table(insert_table_name, column_list, 
                                        distribution_info=distribution_type)
        sql = 'insert into %s select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        self.check(insert_table_name)

    def test_hll_decimal(self):
        """
        {
        "title": "test_stream_simple1.test_hll_decimal",
        "describe": "test hll decimal",
        "tag": "function,p1"
        }
        """
        """test hll decimal"""
        table_name = 'decimal_hll_test'
        self.client.use(self.database_name)
        column_list = DATA.hll_decimal_column_list
        distribution_type = DATA.hash_distribution_info
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type)
        assert ret
        column_name_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3', 'dirty4']
        data_file = "%s/data/STREAM_LOAD/test_hash_decimal_normal.data" % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.1, port=http_port, 
                                      column_name_list=column_name_list)
        assert ret
        self.check(table_name)
        insert_table_name = 'decimal_hll_insert'
        assert self.client.create_table(insert_table_name, column_list, 
                                        distribution_info=distribution_type)
        sql = 'insert into %s  select k1, hll_hash(k1), v2 \
               from %s' % (insert_table_name, table_name)
        ret = self.client.execute(sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        self.check(insert_table_name)

    def check(self, table_name):
        """check"""
        sql = 'select hll_union_agg(v1) from %s' % table_name
        ret1 = self.client.execute(sql)
        sql = 'select count(k1) from %s' % table_name
        ret2 = self.client.execute(sql)
        LOG.info(L('CHECK RET', ret1=ret1, ret2=ret2))
        error = (int(ret2[0][0]) - int(ret1[0][0])) / float(ret2[0][0])
        assert error < 0.2


class TestNull(object):
    """test nulll"""
    database_name = 'stream_load_null_test_db'

    def setup_class(self):
        """setup class"""
        client = palo_client.get_client(host, query_port, user=user, password=password)
        client.clean(self.database_name)
        ret = client.create_database(self.database_name)
        assert ret
        client.use(self.database_name)
        self.client = client

    def test_null(self):
        """
        {
        "title": "test_stream_simple1.test_null",
        "describe": "stream load null",
        "tag": "function,p1"
        }
        """
        """stream load null"""
        table_name = 'base_null'
        self.client.use(self.database_name)
        column_list = DATA.types_kv_column_list
        distribution_type = DATA.baseall_distribution_info
        ret = self.client.create_table(table_name, column_list, 
                                       distribution_info=distribution_type, set_null=True)
        assert ret
        data_file = '%s/data/NULL/data_5' % file_dir
        expect_file = '%s/data/NULL/verify_5' % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.2)
        assert ret
        sql = 'select * from %s order by k2 nulls last' % table_name
        ret = self.client.execute(sql)
        assert common.check_by_file(expect_file, sql=sql, client=self.client)
        insert_table_name = 'insert_' + table_name
        ret = self.client.create_table(insert_table_name, column_list, 
                                       distribution_info=distribution_type, set_null=True)
        assert ret
        insert_sql = 'insert into %s  select * from %s' % (insert_table_name, table_name)
        ret = self.client.execute(insert_sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        assert common.check_by_file(expect_file, sql=sql, client=self.client)

    def test_not_null(self):
        """
        {
        "title": "test_stream_simple1.test_not_null",
        "describe": "test stream load not null",
        "tag": "function,p1"
        }
        """
        """test stream load not null"""
        table_name = 'base_not_null'
        self.client.use(self.database_name)
        column_list = DATA.types_kv_column_list
        distribution_type = DATA.baseall_distribution_info
        ret = self.client.create_table(table_name, column_list, 
                                       distribution_info=distribution_type, set_null=False)
        assert ret
        data_file = '%s/data/NULL/data_5' % file_dir
        expect_file = '%s/data/NULL/verify_5_2' % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.2)
        assert ret
        sql = 'select * from %s order by k2' % table_name
        ret = self.client.execute(sql)
        assert common.check_by_file(expect_file, sql=sql, client=self.client)
        insert_table_name = 'insert_' + table_name
        ret = self.client.create_table(insert_table_name, column_list, 
                                       distribution_info=distribution_type, set_null=True)
        assert ret
        insert_sql = 'insert into %s  select * from %s' % (insert_table_name, table_name)
        ret = self.client.execute(insert_sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        assert common.check_by_file(expect_file, sql=sql, client=self.client)

    def test_bloom_filter(self):
        """
        {
        "title": "test_stream_simple1.test_bloom_filter",
        "describe": "test stream load with bloom filter",
        "tag": "function,p1"
        }
        """
        """test stream load with bloom filter"""
        table_name = 'base_bloom_filter'
        self.client.use(self.database_name)
        column_list = DATA.types_kv_column_list
        distribution_type = DATA.baseall_distribution_info
        bloom_filter = ['k5', 'k7', 'k9']
        ret = self.client.create_table(table_name, column_list, distribution_info=distribution_type,
                                       set_null=True, bloom_filter_column_list=bloom_filter)
        assert ret
        data_file = '%s/data/NULL/data_5' % file_dir
        expect_file = '%s/data/NULL/verify_5' % file_dir
        ret = self.client.stream_load(table_name, data_file, max_filter_ratio=0.2)
        assert ret
        sql = 'select * from %s order by k2 nulls last' % table_name
        ret = self.client.execute(sql)
        assert common.check_by_file(expect_file, sql=sql, client=self.client)
        insert_table_name = 'insert_' + table_name
        ret = self.client.create_table(insert_table_name, column_list, 
                                       distribution_info=distribution_type, set_null=True)
        assert ret
        insert_sql = 'insert into %s  select * from %s' % (insert_table_name, table_name)
        ret = self.client.execute(insert_sql)
        LOG.info(L('Stream insert ret', ret=ret))
        assert ret == ()
        assert common.check_by_file(expect_file, sql=sql, client=self.client)

