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
/***************************************************************************
  *
  * @file test_sys_pull_load_external.py
  * @brief Test for create external table
  *
  **************************************************************************/
"""

import pytest
from data import support_null as DATA2
from data import schema as DATA1
from data import pull_load_apache as DATA
from lib import palo_client
from lib import util
from lib import palo_config
from lib import common

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_name = config.broker_name
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)


def test_mysql_external_table():
    """
    {
    "title": "test_sys_pull_load_external.test_mysql_external_table",
    "describe": "create mysql external table and select",
    "tag": "system,p1"
    }
    """
    """create mysql external table and select"""
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {"host": config.mysql_host, 
                "port": config.mysql_port, 
                "user": config.mysql_user, 
                "password": config.mysql_password, 
                "database": config.mysql_db, 
                "table": "baseall"}
    ret = client.create_external_table(table_name, DATA1.baseall_column_list, engine='mysql',
                                 property=property)
    assert ret
    sql1 = 'select * from %s.%s order by k1' % (database_name, table_name)
    sql2 = 'select * from %s.baseall order by k1' % config.palo_db
    ret1 = client.execute(sql1)
    ret2 = client.execute(sql2)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_mysql_external_issue_5066():
    """
    {
    "title": "test_sys_pull_load_external.test_mysql_external_table_select",
    "describe": "create mysql external table and select",
    "tag": "system,p1"
    }
    """
    """create mysql external table and select"""
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {"host": config.mysql_host,
                "port": config.mysql_port,
                "user": config.mysql_user,
                "password": config.mysql_password,
                "database": config.mysql_db,
                "table": "baseall"}
    tb1 = table_name + '_1'
    tb2 = table_name + '_2'
    tb3 = table_name + '_3'
    assert client.create_external_table(tb1, DATA1.baseall_column_list, engine='mysql',
                                        property=property)
    assert client.create_external_table(tb2, DATA1.baseall_column_list, engine='mysql',
                                        property=property)
    assert client.create_external_table(tb3, DATA1.baseall_column_list, engine='mysql',
                                        property=property)
    sql = 'create view v1 as select * from %s union all select * from %s union all select * from %s' % (tb1, tb2, tb3)
    assert client.execute(sql) == ()
    sql1 = 'select count(*) from v1'
    sql2 = 'select 45'
    common.check2(client, sql1=sql1, sql2=sql2)
    client.clean(database_name)


def test_mysql_external_alter_engine():
    """
    {
    "title": "test_sys_pull_load_external.test_mysql_external_table",
    "describe": "create mysql external table and alter external engine/driver, issue-6993",
    "tag": "system,p1"
    }
    """
    """create mysql external table and select"""
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {"host": config.mysql_host,
                "port": config.mysql_port,
                "user": config.mysql_user,
                "password": config.mysql_password,
                "database": config.mysql_db,
                "table": "baseall"}
    ret = client.create_external_table(table_name, DATA1.baseall_column_list, engine='mysql',
                                 property=property)
    assert ret, 'create table failed'
    sql = 'select engine from information_schema.tables where ' \
          'table_schema="%s" and table_name="%s"' % (database_name, table_name)
    ret = client.execute(sql)
    assert ret[0][0].upper() == "MYSQL", 'expect mysql engine, but %s' % ret[0][0]
    sql = 'alter table %s modify engine to odbc properties("driver"="odbc")' % table_name
    ret = client.execute(sql)
    sql = 'select engine from information_schema.tables where ' \
          'table_schema="%s" and table_name="%s"' % (database_name, table_name)
    ret = client.execute(sql)
    assert ret[0][0].upper() == "ODBC", 'expect odbc engine, but %s' % ret[0][0]
    client.clean(database_name)


def test_hdfs_external_table_disable():
    """
    {
    "title": "test_sys_pull_load_external.test_hdfs_external_table_disable",
    "describe": "test hdfs csv file external table",
    "tag": "system,p1"
    }
    """
    """test hdfs csv file external table"""
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {"broker_name": config.broker_name,
                "path": DATA.data_1,
                "column_separator": "\t",
                "format": "csv"}
    ret = client.create_external_table(table_name, DATA.schema_1, engine='broker',
                                       property=property, broker_property=config.broker_property)
    assert ret
    msg = 'Broker external table is not supported, try to use table function please'
    util.assert_return(False, msg, client.select_all, table_name)
    client.clean(database_name)


def test_hdfs_external_table_csv():
    """
    {
    "title": "test_sys_pull_load_external.test_hdfs_external_table_csv",
    "describe": "test hdfs csv file external table",
    "tag": "system,p1"
    }
    """
    """test hdfs csv file external table"""
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {
                "uri": DATA.data_1,
                "fs.defaultFS": config.defaultFS, 
                "hadoop.username": config.hdfs_username,
                "column_separator": "\t",
                "format": "csv"}
    sql = 'select * from hdfs %s' % util.convert_dict2property(property)
    common.check_by_file(DATA.verify_1[0], sql=sql, client=client)
    client.clean(database_name)


def test_hdfs_external_table_gz():
    """
    {
    "title": "test_sys_pull_load_external.test_hdfs_external_table_gz",
    "describe": "test hdfs gz file external file",
    "tag": "system,p1"
    }
    """
    """test hdfs gz file external file"""
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {
                "uri": DATA.data_1_gz,
                "fs.defaultFS": config.defaultFS,
                "hadoop.username": config.hdfs_username,
                "column_separator": "\t"
               }

    sql = 'select * from hdfs %s' % util.convert_dict2property(property)
    msg = 'format: is not supported.'
    util.assert_return(False, msg, client.execute, sql)
    # common.check_by_file(DATA.verify_1[0], sql=sql, client=client)
    client.clean(database_name)


@pytest.mark.skip()
def test_hdfs_external_table_bz2():
    """
    {
    "title": "test_sys_pull_load_external.test_hdfs_external_table_bz2",
    "describe": "test hdfs bz2 external table",
    "tag": "system,p1"
    }
    """
    """test hdfs bz2 external table"""
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {"broker_name": config.broker_name,
                "path": DATA.data_1_bz2,
               }
    ret = client.create_external_table(table_name, DATA.schema_1, engine='broker',
                                       property=property, broker_property=broker_info)
    assert ret
    assert client.verify(DATA.verify_1, table_name)
    client.clean(database_name)


@pytest.mark.skip()
def test_hdfs_external_table_lzo():
    """
    {
    "title": "test_sys_pull_load_external.test_hdfs_external_table_lzo",
    "describe": "test hdfs lzo external table",
    "tag": "system,p1"
    }
    """
    """test hdfs lzo external table"""
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {"broker_name": config.broker_name,
                "path": DATA.data_1_lzo,
               }
    ret = client.create_external_table(table_name, DATA.schema_1, engine='broker',
                                       property=property, broker_property=broker_info)
    assert ret
    assert client.verify(DATA.verify_1, table_name)
    client.clean(database_name)


@pytest.mark.skip()
def test_hdfs_external_table_lz4():
    """
    {
    "title": "test_sys_pull_load_external.test_hdfs_external_table_lz4",
    "describe": "test hdfs lz4 file external table",
    "tag": "system,p1"
    }
    """
    """test hdfs lz4 file external table"""
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {"broker_name": config.broker_name,
                "path": DATA.data_1_lz4,
               }
    ret = client.create_external_table(table_name, DATA.schema_1, engine='broker',
                                       property=property, broker_property=broker_info)
    assert ret
    assert client.verify(DATA.verify_1, table_name)
    client.clean(database_name)


def test_hdfs_external_table_parquet():
    """
    {
    "title": "test_sys_pull_load_external.test_hdfs_external_table_parquet",
    "describe": "test hdfs parquet file external table",
    "tag": "system,p1"
    }
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {
                "uri": DATA.data_1_parquet,
                "fs.defaultFS": config.defaultFS,
                "hadoop.username": config.hdfs_username,
                "column_separator": "\t",
                "format": "parquet"
               }

    sql = 'select tinyint_key, smallint_key, int_key, bigint_key, largeint_key, char_key, ' \
          'varchar_key, cast(decimal_key as decimal(27, 9)), date_key, datetime_key, ' \
          'tinyint_value_max, smallint_value_min, int_value_sum, bigint_value_sum, ' \
          'largeint_value_sum, largeint_value_replace, char_value_replace, ' \
          'varchar_value_replace, cast(decimal_value_replace as decimal(27, 9)), ' \
          'date_value_replace, datetime_value_replace, float_value_sum, double_value_sum ' \
          'from hdfs %s' % util.convert_dict2property(property)
    common.check_by_file(DATA.verify_6[0], sql=sql, client=client)
    client.clean(database_name)


def test_external_table_null():
    """
    {
    "title": "test_sys_pull_load_external.test_external_table_null",
    "describe": "test external table column null",
    "tag": "system,p1"
    }
    """
    """test external table column null"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)

    property = {
                "uri": DATA2.hdfs_file_5,
                "fs.defaultFS": config.defaultFS,
                "hadoop.username": config.hdfs_username,
                "column_separator": "\t",
                "format": "csv"
               }
    sql = 'select c1,c2,c3,c4,c5,c6,c7,c8,c9,cast(c10 as decimal(9,3)), ' \
          'c11,c12,c13,c14,c15,c16,c17,c18,c19,cast(c20 as decimal(27,9)) ' \
          'from hdfs%s' % util.convert_dict2property(property)
    common.check_by_file(DATA2.expected_file_external_1, sql=sql, client=client)

    sql = 'SELECT COUNT(*) FROM hdfs%s' % util.convert_dict2property(property)
    ret = client.execute(sql)
    assert int(ret[0][0]) == 12
    sql = 'SELECT COUNT(c1) FROM hdfs%s' % util.convert_dict2property(property)
    ret = client.execute(sql)
    assert int(ret[0][0]) == 10
    client.clean(database_name)
    

def test_external_table_not_null():
    """
    {
    "title": "test_sys_pull_load_external.test_external_table_not_null",
    "describe": "test external table column not null, filtered null value",
    "tag": "system,p1"
    }
    """
    """test external table column not null, filtered null value"""
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name, index_name=index_name))
    
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    property = {"broker_name": config.broker_name,
                "path": DATA2.hdfs_file_5,
                "column_separator": "\t",
                "format": "csv"}
    ret = client.create_external_table(table_name, DATA2.schema_2, engine='broker',
                                       property=property, broker_property=broker_info, 
                                       set_null=False)
    assert ret
    
    sql = 'SELECT * FROM %s.%s ' \
          'ORDER BY k2, k1, k3, k4, k5, k6, k7, k8, k9, k10' \
          % (database_name, table_name)
    assert common.check_by_file(DATA2.expected_file_external_2, sql=sql, client=client)
    client.clean(database_name)
    

if __name__ == '__main__':
    setup_module()
    # test_mysql_external_table()
    # test_hdfs_external_table_gz()
    # test_hdfs_external_table_bz2()
    # test_hdfs_external_table_lz4()
    # test_hdfs_external_table_lzo()
    test_hdfs_external_table_csv()
    # test_hdfs_external_table_parquet()
    test_external_table_not_null()
    # test_external_table_null()
