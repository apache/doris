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
brief: test for select ... into ..., datatype cases
"""
import sys
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
    LOG.info(L('execute', sql=query))
    cursor = client.connection.cursor()
    rows1 = cursor.execute(query)
    ret1 = cursor.fetchall()
    description = cursor.description
    column_list = get_schema(description)
    table_name = 'select_into_check_table'
    ret = client.select_into(query, output_file, broker_info, property, format)
    rows2 = palo_job.SelectIntoInfo(ret[0]).get_total_rows()
    assert rows2 != 0, 'select into affacted rows should not be 0'
    assert rows2 == rows1, 'select rows not equal to select into rows'
    client.drop_table(table_name, if_exist=True)
    distribution = palo_client.DistributionInfo('HASH(k_0)', 13)
    ret = client.create_table(table_name, column_list, distribution_info=distribution, 
                              keys_desc='DUPLICATE KEY(k_0)', set_null=True)
    assert ret
    load_file = output_file + '*'
    try:
        column_separator = property.get("column_separator")
    except Exception as e:
        column_separator = None
    load_data_list = palo_client.LoadDataInfo(load_file, table_name, format_as=format,
                                              column_terminator=column_separator)
    ret = client.batch_load(util.get_label(), load_data_list=load_data_list, broker=broker_info, is_wait=True)
    assert ret
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
            assert 0 == 1
        column_list.append((col_name, col_palo_type))
        id += 1
    return column_list


def init_data(client, db_name, tb_name, column_list, local_data_file, key_type,
              distribution_info, sql=None):
    """init db data"""
    client.clean(db_name)
    ret = client.create_database(db_name)
    assert ret
    client.use(db_name)
    if sql is None:
        sql = 'select * from %s order by 1, 2'
    ret = client.create_table(tb_name, column_list,
                              distribution_info=distribution_info,
                              keys_desc=key_type)
    assert ret
    ret = client.stream_load(tb_name, local_data_file, max_filter_ratio=0.1)
    assert ret


def test_select_tinyint_into():
    """
    {
    "title": "test_select_tinyint_into",
    "describe": "查询导出tinying类型，包括本身的tinyint和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.tinyint_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_tinyint.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_smallint_into():
    """
    {
    "title": "test_select_smallint_into",
    "describe": "查询导出smallint类型，包括本身的smallint和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.smallint_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_smallint.data" % file_dir
    key_type = DATA.duplicate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_int_into():
    """
    {
    "title": "test_select_int_into",
    "describe": "查询导出int类型，包括本身的int和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.int_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_int.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_bigint_into():
    """
    {
    "title": "test_select_bigint_into",
    "describe": "查询导出bigint类型，包括本身的bigint和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.bigint_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_bigint.data" % file_dir
    key_type = DATA.duplicate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_largeint_into():
    """
    {
    "title": "test_select_largeint_into",
    "describe": "查询导出largeint类型，包括本身的largeint和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.largeint_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_largeint.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_decimal_least_into():
    """
    {
    "title": "test_select_decimal_into",
    "describe": "查询导出decimal类型，包括本身的decimal(1,0)和cast，验证精度问题",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.decimal_least_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_least.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_decimal_normal_into():
    """
    {
    "title": "test_select_decimal_normal_into",
    "describe": "查询导出decimal类型，包括本身的decimal和cast，验证精度问题",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.decimal_normal_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_normal.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_decimal_most_into():
    """
    {
    "title": "test_select_decimal_most_into",
    "describe": "查询导出decimal类型，包括本身的decimal和cast，验证精度问题",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.decimal_most_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_decimal_most.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_char_least_into():
    """
    {
    "title": "test_select_char_leash_into",
    "describe": "查询导出char类型，包括本身的char和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.char_least_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_least.data" % file_dir
    key_type = DATA.duplicate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_char_normal_into():
    """
    {
    "title": "test_select_char_normal_into",
    "describe": "查询导出char类型，包括本身的char和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.char_normal_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_normal.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_char_most_into():
    """
    {
    "title": "test_select_char_most_into",
    "describe": "查询导出char类型，包括本身的char和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.char_most_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_char_normal.data" % file_dir
    key_type = DATA.duplicate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_varchar_least_into():
    """
    {
    "title": "test_select_varchar_least_into",
    "describe": "查询导出varchar类型，包括本身的varchar和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.varchar_least_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_least.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_varchar_normal_into():
    """
    {
    "title": "test_select_varchar_normla_into",
    "describe": "查询导出varchar类型，包括本身的varchar和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.varchar_normal_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_normal.data" % file_dir
    key_type = DATA.duplicate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_varchar_most_into():
    """
    {
    "title": "test_select_varchar_most_into",
    "describe": "查询导出varchar类型，包括本身的varchar和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.varchar_most_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_most.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_date_into():
    """
    {
    "title": "test_select_date_into",
    "describe": "查询导出date类型，包括本身的date和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.date_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_date.data" % file_dir
    key_type = DATA.duplicate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_datetime_into():
    """
    {
    "title": "test_select_datetime_into",
    "describe": "查询导出datetime类型，包括本身的datetime和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.datetime_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_datetime.data" % file_dir
    key_type = DATA.duplicate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_double_into():
    """
    {
    "title": "test_select_double_into",
    "describe": "查询导出double类型，包括本身的double和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.double_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_double_int.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_float_into():
    """
    {
    "title": "float",
    "describe": "查询导出float类型，包括本身的float和cast，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.float_column_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_float_int.data" % file_dir
    key_type = DATA.aggregate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_hll_into():
    """
    {
    "title": "hll",
    "describe": "查询导出hll类型，包括本身的hll和其他hll相关函数结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    column_list = DATA.hll_int_column_list
    distribution_type = DATA.hash_distribution_info
    ret = client.create_table(table_name, column_list, distribution_info=distribution_type)
    assert ret
    column_list = ['k1', 'v1=hll_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3', 'dirty4']
    data_file = "%s/data/STREAM_LOAD/test_hash_int.data" % file_dir
    ret = client.stream_load(table_name, data_file, max_filter_ratio=0.1,
                             column_name_list=column_list)
    assert ret
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_bitmap_into():
    """
    {
    "title": "bitmap",
    "describe": "查询导出bitmap类型，包括本身的bitmap和其他bitmap相关函数结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    column_list = DATA.bitmap_int_column_list
    distribution_type = DATA.hash_distribution_info
    ret = client.create_table(table_name, column_list, distribution_info=distribution_type)
    assert ret
    column_list = ['k1', 'v1=bitmap_hash(k1)', 'v2=1', 'dirty1', 'dirty2', 'dirty3', 'dirty4']
    data_file = "%s/data/STREAM_LOAD/test_hash_int.data" % file_dir
    ret = client.stream_load(table_name, data_file, max_filter_ratio=0.1,
                             column_name_list=column_list)
    assert ret
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_null():
    """
    {
    "title": "test_select_null",
    "describe": "查询导出null，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    column_list = DATA.types_kv_column_list
    distribution_type = DATA.baseall_distribution_info
    ret = client.create_table(table_name, column_list,
                                   distribution_info=distribution_type, set_null=True)
    assert ret
    data_file = '%s/data/NULL/data_5' % file_dir
    ret = client.stream_load(table_name, data_file, max_filter_ratio=0.2)
    assert ret
    query = 'select * from %s order by k2' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(), 
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_empty():
    """
    {
    "title": "test_select_empty",
    "describe": "查询结果返回空集，验证导出空文件",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'select * from %s.baseall where k1 = 0' % check_db
    output_file = palo_config.gen_remote_file_path('export/%s/%s/%s') % ('empty', util.get_label(), util.get_label())
    cursor = client.connection.cursor()
    rows1 = cursor.execute(query)
    ret1 = cursor.fetchall()
    description = cursor.description
    column_list = get_schema(description)
    table_name = 'select_into_check_table'
    ret = client.select_into(query, output_file, broker_info)
    rows2 = palo_job.SelectIntoInfo(ret[0]).get_total_rows()
    assert rows2 == rows1, 'expect select into total rows %s' % rows1
    client.drop_table(table_name, if_exist=True)
    distribution = palo_client.DistributionInfo('HASH(k_0)', 13)
    ret = client.create_table(table_name, column_list, distribution_info=distribution,
                              keys_desc='DUPLICATE KEY(k_0)', set_null=True)
    assert ret
    load_file = output_file + '*'
    try:
        column_separator = property.get("column_separator")
    except Exception as e:
        column_separator = None
    load_data_list = palo_client.LoadDataInfo(load_file, table_name, format_as=None, 
                                              column_terminator=column_separator)
    ret = client.batch_load(util.get_label(), load_data_list=load_data_list, broker=broker_info, is_wait=True, \
            max_filter_ratio=0)
    assert ret, "expect batch load success"
    ret2 = client.select_all(table_name)
    util.check(ret1, ret2)
    client.clean(database_name)


def test_select_chinese():
    """
    {
    "title": "test_select_chinese",
    "describe": "查询返回中文字符，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'select "中文字符", k1, k6 from %s.baseall' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % ('empty', util.get_label(), util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_special():
    """
    {
    "title": "test_select_special",
    "describe": "查询返回特殊字符，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                    user=config.fe_user, password=config.fe_password)
    # init data
    column_list = DATA.varchar_normal_column_no_agg_list
    distribution_info = DATA.hash_distribution_info
    local_data_file = "%s/data/STREAM_LOAD/test_hash_varchar_normal.data" % file_dir
    key_type = DATA.duplicate_key
    init_data(client, database_name, table_name, column_list, local_data_file, key_type, distribution_info)
    # select into
    query = 'select * from %s' % table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    
    check_select_into(client, query, output_list)
    client.clean(database_name)


if __name__ == '__main__':
    setup_module()
    test_select_special()
