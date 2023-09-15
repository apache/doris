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
Date: 2020-05-20 17:17:42
brief: test for select ... into ..., output property set
"""
import sys
import time
import os
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from data import schema as DATA
from lib import palo_client
from lib import palo_config
from lib import util
from lib import palo_job

LOG = palo_client.LOG
L = palo_client.L
config = palo_config.config
broker_info = palo_config.broker_info
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

check_db = 'test_query_qa'
table_name = 'baseall'


def setup_module():
    """setup"""
    try:
        client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                        user=config.fe_user, password=config.fe_password)
        client.execute('select count(*) from %s.%s' % (check_db, table_name))
    except Exception as e:
        raise pytest.skip('test_query_qa select failed, check palo cluster')


def check_select_into(client, query, output_file, broker=None, property=None, format=None):
    """
    验证查询和导出的结果相一致
    """
    # 执行查询获取表的schema用作校验
    LOG.info(L('execute', sql=query))
    cursor = client.connection.cursor()
    rows1 = cursor.execute(query)
    ret1 = cursor.fetchall()
    description = cursor.description
    column_list = get_schema(description)
    table_name = 'select_into_check_table'
    # select into
    ret = client.select_into(query, output_file, broker_info, property, format)
    if format is None:
        file_format = 'csv'
    else:
        file_format = format
    assert len(ret) == 1, 'expect return select into info'
    select_into_info = palo_job.SelectIntoInfo(ret[0])
    file_number = select_into_info.get_file_number()
    rows2 = select_into_info.get_total_rows()
    load_file = []
    for i in range(1, int(file_number) + 1):
        load_file.append(select_into_info.get_url() + str(i - 1) + "." + format)
    assert rows2 == rows1, 'returned rows not expected, expected: %s' % rows1
    client.drop_table(table_name, if_exist=True)
    distribution = palo_client.DistributionInfo('HASH(k_0)', 13)
    ret = client.create_table(table_name, column_list, distribution_info=distribution, 
                              keys_desc='DUPLICATE KEY(k_0)', set_null=True)
    assert ret, 'create table failed'
    if load_file is None:
        load_file = output_file + '*'
    
    try:
        column_separator = property.get("column_separator")
    except Exception as e:
        column_separator = None
    load_data_list = palo_client.LoadDataInfo(load_file, table_name, format_as=format, 
                                              column_terminator=column_separator)
    ret = client.batch_load(util.get_label(), load_data_list=load_data_list, broker=broker_info, is_wait=True)
    assert ret, 'broker load failed'
    ret2 = client.select_all(table_name)
    util.check(ret1, ret2, True)


def get_schema(description):
    """
    get query schema
    schema = [("tinyint_key", "TINYINT")]
    """
    type_map = {1: 'TINYINT', 2: 'SMALLINT',
                3: 'INT', 8: 'BIGINT',
                0: 'DECIMAL(27, 9)', 4: 'FLOAT',
                5: 'DOUBLE', 10: 'DATE',
                12: 'DATETIME', 15: 'VARCHAR',
                246: 'DECIMAL(27, 9)', 253: 'VARCHAR',
                254: 'CHAR'}
    column_list = list()
    id = 0
    col_prefix = 'k_'
    for col in description:
        col_name = col_prefix + str(id)
        col_num_type = col[1]
        if col_num_type in [0, 1, 2, 3, 4, 5, 8, 10, 12, 246]:
            col_palo_type = type_map.get(col_num_type)
        elif col_num_type in [15, 253]:
            col_palo_type = '%s(%s)' % (type_map.get(col_num_type), col[2])
        elif col_num_type in [254]:
            if col[2] is None or col[2] >= 65533:
                col_palo_type = 'VARCHAR(65533)'
            elif col[2] > 255 and col[2] < 65533:
                col_palo_type = 'VARCHAR(%s)' % col[2]
            elif col[2] <= 0:
                col_palo_type = '%s(1)' % type_map.get(col_num_type)
            else:
                col_palo_type = '%s(%s)' % (type_map.get(col_num_type), col[2])
        else:
            print(col, col_name, col_num_type)
            assert 0 == 1
        column_list.append((col_name, col_palo_type))
        id += 1
    return column_list


def init_data(client, db_name, tb_name, column_list, local_data_file,
              distribution_info, sql=None):
    """init db data"""
    client.clean(db_name)
    ret = client.create_database(db_name)
    assert ret, 'create db failed'
    client.use(db_name)
    if sql is None:
        sql = 'select * from %s order by 1, 2'
    ret = client.create_table(tb_name, column_list,
                              distribution_info=distribution_info)
    assert ret, 'create table failed'
    ret = client.stream_load(tb_name, local_data_file, max_filter_ratio=0.1)
    assert ret, 'stream load failed'


def test_output_format_parquet():
    """
    {
    "title": "test_output_format_parquet",
    "describe": "导出指定format为parquet，验证结果正确，issue 5938",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.partition_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/PARTITION/partition_type" % file_dir
    init_data(client, database_name, table_name, column_list, local_data_file, distribution_info)
    # select into
    query = 'select k1 k_0, k2 k_1, k3 k_2, k4 k_3, k5 k_4, v1 k_5, v2 k_6, v3 k_7,' \
            ' v4 k_8, v5 k_9, v6 k_10 from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    parquet_schema = "required,int32,k_0;required,int32,k_1;required,int32,k_2;required,int64,k_3;" \
                     "required,int64,k_4;required,int64,k_5;required,byte_array,k_6;" \
                     "required,byte_array,k_7;required,float,k_8;required,double,k_9;" \
                     "required,byte_array,k_10"
    check_select_into(client, query, output_list, format='parquet', property={"schema": parquet_schema})
    client.clean(database_name)


def test_output_format_csv():
    """
    {
    "title": "test_output_format_csv",
    "describe": "导出指定format为csv，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.partition_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/PARTITION/partition_type" % file_dir
    init_data(client, database_name, table_name, column_list, local_data_file, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    check_select_into(client, query, output_list, format='csv')
    client.clean(database_name)


def test_output_csv_column_separator():
    """
    {
    "title": "test_output_csv_column_separator",
    "describe": "导出指定列分割符，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.partition_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/PARTITION/partition_type" % file_dir
    init_data(client, database_name, table_name, column_list, local_data_file, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    check_select_into(client, query, output_list, format='csv', property={"column_separator":"|"})
    client.clean(database_name)


def test_output_csv_line_delimiter():
    """
    {
    "title": "test_output_csv_line_delimiter",
    "describe": "导出指定行分割符，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.partition_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/PARTITION/partition_type" % file_dir
    init_data(client, database_name, table_name, column_list, local_data_file, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    property = {"column_separator":"|", "line_delimiter":"\\r\\n"}
    check_select_into(client, query, output_list, format='csv', property={"column_separator":"|"})
    client.clean(database_name)


def test_output_max_file_size_bytes():
    """
    {
    "title": "test_output_max_file_size_bytes",
    "describe": "导出指定最大导出文件，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'select * from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    # load_file = [output_list + '0.csv', output_list + '1.csv']
    property = {"column_separator":"|", "max_file_size": "5MB"}
    check_select_into(client, query, output_list, property=property, format='csv')
    client.clean(database_name)


def test_into_twice():
    """
    {
    "title": "test_into_twice",
    "describe": "验证重复导出，（可能和导出的介质有关，bos会覆盖，hdfs应该也是覆盖）",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'select * from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    property = {"column_separator":"|", "max_file_size": "5MB"}
    check_select_into(client, query, output_list, property=property, format='csv')
    time.sleep(5)
    check_select_into(client, query, output_list, property=property, format='csv')
    client.clean(database_name)   


def test_into_hdfs_not_exists():
    """
    {
    "title": "test_into_hdfs_not_exists",
    "describe": "导出的hdfs路径不存在，预期导出成功",
    "tag": "system,p2,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'select * from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    property = {"column_separator":"|", "max_file_size": "5MB"}
    check_select_into(client, query, output_list, property=property, format='csv')
    client.clean(database_name)


def test_select_into_cancel():
    """
    {
    "title": "test_select_into_cancel",
    "describe": "select into的连接被kill，导出被cancel，不做任何结果处理，重试可以成功",
    "tag": "system,p2,fuzz"
    }
    """
    pass


def test_select_into_privilege():
    """
    {
    "title": "test_select_into_privilege",
    "describe": "select导出需要select权限",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    root_client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    read_user = 'read_query_user'
    try:
        root_client.drop_user(read_user)
    except Exception as e:
        pass
    root_client.create_user(read_user)
    read_user_client = palo_client.get_client(config.fe_host, config.fe_query_port, user=read_user, password='')
    query = 'select * from %s.baseall' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    property = {"column_separator":"|", "max_file_size": "5MB"}
    flag = True
    try:
        read_user_client.select_into(query, output_list, broker_info, property, 'csv')
        flag = False
    except Exception as e:
        print(str(e))
    assert flag
    assert root_client.grant(read_user, ['SELECT_PRIV'], check_db)
    time.sleep(5)
    read_user_client.connect()
    ret = read_user_client.select_into(query, output_list, broker_info, property, 'csv')
    assert ret


def test_select_into_timeout():
    """
    {
    "title": "test_select_into_timeout",
    "describe": "select导出设置query_timeout，超时返回失败",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    sql = 'set query_timeout=1'
    assert client.execute(sql) == ()
    query = 'select * from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    property = {"column_separator":"|", "max_file_size": "5MB"}
    flag = True
    try:
        sql = 'show variables like "query_timeout"'
        ret = client.execute(sql)
        print(ret)
        client.select_into(query, output_list, broker_info, property, 'csv')
        flag = False
    except Exception as e:
        print(str(e))
    assert flag


def test_select_into_while_other_job():
    """
    {
    "title": "test_select_into_while_other_job",
    "describe": "select导出不影响其他任务，其他任务不影响select导出，同select",
    "tag": "system,p2,stability"
    }
    """
    pass


def test_invalid_select_into():
    """
    {
    "title": "test_invalid_select_into",
    "describe": "select into错误使用, max file size should between 5MB and 2GB",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    query = 'select * from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    flag = True
    try:
        property = {"column_separator":"|", "max_file_size": "1MB"}
        client.select_into(query, output_list, broker_info, property=property, format_as='csv')
        flag = False
    except Exception as e:
        LOG.info(L('select into msg', msg=str(e)))
    assert flag, 'expect select into fail'
    flag = True
    try:
        property = {"column_separator":"|", "max_file_size": "5GB"}
        client.select_into(query, output_list, broker_info, property=property, format_as='csv')
        flag = False
    except Exception as e:
        LOG.info(L('select into msg', msg=str(e)))
    assert flag, 'expect select into fail'


def test_invalid_select_into_1():
    """
    {
    "title": "test_invalid_select_into_1",
    "describe": "select into错误使用，broker信息错误",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    query = 'select * from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    flag = True
    try:
        property = {"column_separator":"|", "max_file_size": "5MB"}
        error_broker_info = palo_config.BrokerInfo('not_exsits', config.broker_property)
        client.select_into(query, output_list, error_broker_info, property=property, format_as='csv')
        flag = False
    except Exception as e:
        LOG.info(L('select into msg', msg=str(e)))
    assert flag, 'expect select into fail'
    try:
        property = {"column_separator":"|", "max_file_size": "5MB"}
        error_broker_info = palo_config.BrokerInfo(config.broker_name, '')
        client.select_into(query, output_list, error_broker_info, property=property, format_as='csv')
        flag = False
    except Exception as e:
        LOG.info(L('select into msg', msg=str(e)))


def test_invalid_select_into_2():
    """
    {
    "title": "test_invalid_select_into_1",
    "describe": "select into错误使用，format错误",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    query = 'select * from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    flag = True
    try:
        property = {"column_separator":"|", "max_file_size": "5MB"}
        error_broker_info = palo_config.BrokerInfo('not_exsits', config.broker_property)
        client.select_into(query, output_list, error_broker_info, property=property, format_as='txt')
        flag = False
    except Exception as e:
        LOG.info(L('select into msg', msg=str(e)))
    assert flag, 'expect select into fail'


def test_invalid_select_into_3():
    """
    {
    "title": "test_invalid_select_into_1",
    "describe": "select into错误使用，format错误",
    "tag": "system,p1,fuzz"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    query = 'select * from %s.test where t > 0' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    flag = True
    try:
        property = {"column_separator":"|", "max_file_size": "5MB"}
        client.select_into(query, output_list, broker_info, property=property, format_as='csv')
        flag = False
    except Exception as e:
        LOG.info(L('select into msg', msg=str(e)))
    assert flag, 'expect select into fail'


if __name__ == '__main__':
    setup_module()
    test_select_into_timeout()
