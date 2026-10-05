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
brief: test for select ... into ..., query cases
"""
import sys
import os
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
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
    assert rows2 == rows1
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
    ret = client.batch_load(util.get_label(), load_data_list=load_data_list, broker=broker_info, 
                            is_wait=True)
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
                254: 'CHAR', 252: 'VARCHAR'}
    column_list = list()
    id = 0
    col_prefix = 'k_'
    for col in description:
        print(col)
        col_name = col_prefix + str(id)
        col_num_type = col[1]
        if id == 0 and col_num_type in [4, 5]:
            col_palo_type = 'DECIMAL(27, 9)'
            column_list.append((col_name, col_palo_type))
            id += 1
            continue
        print(col_num_type)
        if col_num_type in [0, 1, 2, 3, 4, 5, 8, 10, 12, 246]:
            col_palo_type = type_map.get(col_num_type)
        elif col_num_type in [15, 253]:
            col_palo_type = '%s(%s)' % (type_map.get(col_num_type), col[2])
        elif col_num_type in [254, 252]:
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


def test_select_function():
    """
    {
    "title": "test_select_function",
    "describe": "select中使用函数，验证结果正确。注意某些返回值特殊的函数",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # 数学函数
    query = 'select round(k8, 2), abs(k8),  atan(k3), bin(k3), ceil(k8), floor(k8),' \
            ' sin(k3) from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/numeric_function/%s') \
                  % (database_name, util.get_label())
    check_select_into(client, query, output_list)
    query = 'select conv(k1, 10, 16), negative(k2), power(k1, 2), hex(k4), ln(abs(k1)) ' \
            'from %s.baseall' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/numeric_function/%s') \
                  % (database_name, util.get_label())
    check_select_into(client, query, output_list)
    # 时间函数
    query = 'select UNIX_TIMESTAMP(k11), CURDATE(), date_add(k11, interval 2 day) from %s.test' \
            % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/date_function/%s') \
                  % (database_name, util.get_label())
    check_select_into(client, query, output_list)
    query = 'select k11, date_format(k11, "%%W %%M %%Y"), dayname(k11), hour(k11) from %s.test' \
            % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/date_function/%s') \
                  % (database_name, util.get_label())
    check_select_into(client, query, output_list)
    # 字符串函数
    query = 'select ascii(k1), money_format(k4), reverse(k6) from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/string_function/%s') \
                  % (database_name, util.get_label())
    check_select_into(client, query, output_list)
    # 地理函数
    query = 'select ST_Point(24.7, 56.7) from %s.baseall' % check_db
    # ST_Circle(111, 64, 10000), ST_GeometryFromText("LINESTRING (1 1, 2 2)"), 
    # ST_LineFromText("LINESTRING (1 1, 2 2)"), ST_Polygon("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), 
    output_list = palo_config.gen_remote_file_path('export/%s/spacial_function/%s') \
                  % (database_name, util.get_label())
    # check_select_into(client, query, output_list)
    query = 'SELECT ST_AsText(ST_Point(24.7, 56.7)), ' \
            'ST_Contains(ST_Polygon("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"), ' \
            'ST_Point(5, 5)), ' \
            'st_distance_sphere(116.35620117, 39.939093, 116.4274406433, 39.9020987219), ' \
            'ST_X(ST_Point(24.7, 56.7))'
    # cast
    query = 'select cast(k1 as string), cast(k5 as double), cast(k10 as bigint), ' \
            'cast(k10 as datetime) from %s.test' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/cast_function/%s') % (database_name, 
                                                                                  util.get_label())
    check_select_into(client, query, output_list)
    # bitmap函数
    query = 'select bitmap_and(to_bitmap(1), to_bitmap(2)), bitmap_contains(to_bitmap(1),2),' \
            'bitmap_count(bitmap_empty()), bitmap_to_string(to_bitmap(1)), BITMAP_HASH("hello")' \
            'from %s.baseall' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/bitmap_function/%s') % (database_name,
                                                                                    util.get_label())
    check_select_into(client, query, output_list)
    # hll函数
    query = 'select hll_hash(k2), hll_cardinality(hll_empty()), hll_union_agg(hll_hash(k1))' \
            'from %s.baseall group by k2' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/hll_function/%s') % (database_name, util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_operator():
    """
    {
    "title": "test_select_operator",
    "describe": "select中使用操作符，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # 数值运算
    query = 'select k1 + k2, k5/10, k3*k1, k4 - k8, k9 + k8 from %s.baseall' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    # 逻辑运算
    query = 'select true and true, true or false, true and false, k1 ^ k2, k1 | k2, k2 & k3 from %s.baseall' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    # 比较运算
    query = 'select k1 > k2, k3 < k2, k8 <=> k9, k5 is null, k5 is not null from %s.baseall' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_union():
    """
    {
    "title": "test_select_union",
    "describe": "导出union查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    # union all
    query = "(select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s.test where k1>0) " \
            "union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s.baseall where k2>0)" \
            " union all (select k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11 from %s.baseall where k3>0)" \
            " order by k1, k2, k3, k4" % (check_db, check_db, check_db)
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    # union distinct
    query = "select x.k1, k6, k7, k8, k9, k10 from ((select k1, k6, k7, k8, k9, k10 from %s.test where k1=1)" \
            " union distinct (select k1, k6, k7, k8, k9, k10 from %s.test where k9>0)) x " \
            "union distinct (select k1, k6, k7, k8, k9, k10 from %s.baseall) order by 1, 4, 5, 6 limit 10" \
            % (check_db, check_db, check_db)
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(), 
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_join():
    """
    {
    "title": "test_select_join",
    "describe": "导出join查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = "select j.*, d.* from %s.test j full outer join %s.baseall d on (j.k1=d.k1) order by " \
            "j.k1, j.k2, j.k3, j.k4, d.k2 limit 100" % (check_db, check_db)
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    query = "select a.k4 from %s.test a inner join %s.baseall b on (a.k1=b.k1)" \
            " where a.k2>0 and b.k1=1 and a.k1=1 order by 1" % (check_db, check_db)
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_intercept():
    """
    {
    "title": "test_select_intercept",
    "describe": "导出intercept查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    pass
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'SELECT * FROM (SELECT k2 FROM %s.test INTERSECT SELECT k2 FROM %s.baseall) a ORDER BY k2' \
            % (check_db, check_db)
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_except():
    """
    {
    "title": "test_select_except",
    "describe": "导出except查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    pass
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'SELECT * FROM (SELECT k2 FROM %s.test EXCEPT SELECT k2 FROM %s.baseall) a ORDER BY k2' \
            % (check_db, check_db)
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_group():
    """
    {
    "title": "test_select_group",
    "describe": "导出group查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    line = "select k10, count(*) from %s.test where k5 is not null group by k10" \
           " having k10<cast('2010-01-01 01:05:20' as datetime) order by 1, 2" % (check_db)
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, line, output_list)
    client.clean(database_name)


def test_select_order():
    """
    {
    "title": "test_select_order",
    "describe": "导出order查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    line = 'select k1, k2 from (select k1, max(k2) as k2 from %s.test where k1 > 0 group by k1 \
            order by k1)a left join (select k1 as k3, k2 as k4 from %s.baseall) b on a.k1 = b.k3 \
            where k1 > 0 and k1 < 10 order by k1, k2' % (check_db, check_db)
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, line, output_list)
    client.clean(database_name)


def test_select_limit_offset():
    """
    {
    "title": "test_select_limit_offset",
    "describe": "导出limit offset查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    pass
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'select * from %s.test order by k1, k2, k3, k4 limit 100 offset 10' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_subselect():
    """
    {
    "title": "test_select_subselect",
    "describe": "导出子查询查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    pass
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'select * from %s.test where k2 in (select distinct k1 from %s.baseall) and k4>0 and' \
            ' round(k5) in (select k2 from %s.test) order by k1, k2, k3, k4' % (check_db, check_db, check_db)
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_win():
    """
    {
    "title": "test_select_win",
    "describe": "导出窗口函数查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    pass
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'select k1, sum(k2) over (partition by k2) as wj from %s.test order by k1, wj' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name,
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_with():
    """
    {
    "title": "test_select_with",
    "describe": "导出with查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    pass
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    query = 'with t as (select k3 x, k4 y from %s.test) select count(x), count(y) from t' % check_db
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_view():
    """
    {
    "title": "test_select_view",
    "describe": "导出view查询结果，验证结果正确",
    "tag": "system,p1"
    }
    """
    database_name, table_name, rollup_table_name = util.gen_name_list()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    client.execute('drop view if exists %s' % rollup_table_name)
    query = 'create view %s as with t as (select k3 x, k4 y from %s.test) select count(x),' \
            'count(y) from t' % (rollup_table_name, check_db)
    client.execute(query)
    query = 'select * from %s' % rollup_table_name
    output_list = palo_config.gen_remote_file_path('export/%s/%s/%s') % (database_name, 
                                                                       util.get_label(),
                                                                       util.get_label())
    check_select_into(client, query, output_list)
    client.clean(database_name)


def test_select_wrong():
    """
    {
    "title": "test_select_wrong",
    "describe": "查询报错，验证错误信息",
    "tag": "system,p1,fuzz"
    }
    """
    client = palo_client.get_client(config.fe_host, config.fe_query_port, check_db,
                                    user=config.fe_user, password=config.fe_password)
    query = 'select * from baseall where a = 0'
    output_file = palo_config.gen_remote_file_path('export/%s/%s/%s') % ('wrong', util.get_label(), util.get_label())
    flag = True
    try:
        client.select_into(query, output_file, broker_info)
        flag = False
    except Exception as e:
        print(str(e))
    assert flag


if __name__ == '__main__':
    setup_module()
    test_select_function()
