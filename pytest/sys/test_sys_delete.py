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
test delete on palo
Date: 2015/03/23 15:08:29
"""

import time
from decimal import Decimal
import datetime
from data import delete as DATA
from lib import palo_client
from lib import palo_config
from lib import util 
from lib import palo_task

client = None
config = palo_config.config
column_list = DATA.column_list
column_name_list = DATA.column_name_list
partition_info = DATA.random_partition_info
load_data_list = DATA.load_data_list
broker_info = palo_config.broker_info


def setup_module():
    """
    set up
    """
    # 需要修改配置文件中的max_tablet_data_size_bytes
    global client
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=config.fe_user, \
			password=config.fe_password)
    client.init()


def init(database_name, table_family_name, is_wait=True):
    """
    初始化
    """
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    client.use(database_name)
    ret = client.create_table(table_family_name, column_list, partition_info=partition_info)
    assert ret
    load_label = "%s_1" % database_name
    for data in load_data_list:
        data.table_name = table_family_name
    ret = client.batch_load(load_label, load_data_list, max_filter_ratio="0.5", is_wait=is_wait, broker=broker_info)
    assert ret
 
 
def test_bigint():
    """
    {
    "title": "test_sys_delete.test_bigint",
    "describe": "big int类型列上的删除条件",
    "tag": "system,p1"
    }
    """
    """
    功能点：big int类型列上的删除条件
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list() 
    print(database_name, table_family_name, rollup_table_name)
    init(database_name, table_family_name) 
    sql = "SELECT %s FROM %s.%s" % (",".join(column_name_list), database_name, table_family_name)
    data_1 = client.execute(sql)
    delete_condition_list = [("bigint_key", "<=", "2")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_1)
  
    data_2 = client.execute(sql)
    delete_condition_list = [("bigint_key", "<", "20"), ("bigint_key", "!=", "8")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_2)
  
    data_3 = client.execute(sql)
    delete_condition_list = [("bigint_key", "<", "200"), ("bigint_key", "!=", "8")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_3)

    data_4 = client.execute(sql)
    delete_condition_list = [("bigint_key", "<", "21225033234"), ("bigint_key", "!=", "8")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_4)

    data_5 = client.execute(sql)
    delete_condition_list = [("bigint_key", "=", "21225033234")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_5)

    sql = "SELECT %s FROM %s.%s" % (",".join(column_name_list), database_name, table_family_name)
    data_6 = client.execute(sql)
    delete_condition_list = [("bigint_key", ">", "94465395517212"), \
            ("bigint_key", ">=", "92080498116224")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_6)
    client.clean(database_name)


def test_date():
    """
    {
    "title": "test_sys_delete.test_date",
    "describe": "date类型列上的删除条件",
    "tag": "system,p1"
    }
    """
    """
    功能点：date类型列上的删除条件
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list() 
    init(database_name, table_family_name) 
    sql = "SELECT %s FROM %s.%s" % (",".join(column_name_list), database_name, table_family_name)
    data_1 = client.execute(sql)
    delete_condition_list = [("date_key", "!=", "1982-04-02")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_1)
    client.clean(database_name)
 

def test_datetime():
    """
    {
    "title": "test_sys_delete.test_datetime",
    "describe": "date类型列上的删除条件",
    "tag": "system,p1"
    }
    """
    """
    功能点：date类型列上的删除条件
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list() 
    init(database_name, table_family_name)
    sql = "SELECT %s FROM %s.%s" % (",".join(column_name_list), database_name, table_family_name)
    data_1 = client.execute(sql)
    delete_condition_list = [("datetime_key", "<=", "2000-12-03 16:50:21")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_1)

    data_2 = client.execute(sql)
    delete_condition_list = [("datetime_key", "=", "2002-10-31 18:11:03")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_2)

    data_3 = client.execute(sql)
    delete_condition_list = [("datetime_key", ">", "2013-09-05 11:16:04")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_3)
    client.clean(database_name)
 

def test_char():
    """
    {
    "title": "test_sys_delete.test_char",
    "describe": "char类型列上的删除条件",
    "tag": "system,p1"
    }
    """
    """
    功能点：char类型列上的删除条件
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list() 
    init(database_name, table_family_name)
    sql = "SELECT %s FROM %s.%s" % (",".join(column_name_list), database_name, table_family_name)
    data_1 = client.execute(sql)
    delete_condition_list = [("char_key", ">=", "y")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_1)

    data_2 = client.execute(sql)
    delete_condition_list = [("char_key", "<=", "0"), ("char_key", ">=", "0")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_2)

    data_3 = client.execute(sql)
    delete_condition_list = [("char_50_key", "!=", "AA6NZMc"), ("char_50_key", "<=", "m8CjCS")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_3)
    client.clean(database_name)


def test_varchar():
    """
    {
    "title": "test_sys_delete.test_varchar",
    "describe": "varchar类型列上的删除条件",
    "tag": "system,p1"
    }
    """
    """
    功能点：varchar类型列上的删除条件
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list() 
    init(database_name, table_family_name)
    sql = "SELECT %s FROM %s.%s" % (",".join(column_name_list), database_name, table_family_name)
    data_1 = client.execute(sql)
    delete_condition_list = [("varchar_key", "<=", "sk6S0")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_1)

    data_2 = client.execute(sql)
    delete_condition_list = [("varchar_most_key", ">=", "z"), ("varchar_key", "=", "9C6md")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_2)

    data_3 = client.execute(sql)
    delete_condition_list = [("varchar_key", "=", "9C6md")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_3)
    client.clean(database_name)


def test_decimal():
    """
    {
    "title": "test_sys_delete.test_decimal",
    "describe": "decimal类型列上的删除条件",
    "tag": "system,p1"
    }
    """
    """
    功能点：decimal类型列上的删除条件
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list() 
    init(database_name, table_family_name)
    sql = "SELECT %s FROM %s.%s" % (",".join(column_name_list), database_name, table_family_name)
    data_1 = client.execute(sql)
    delete_condition_list = [("decimal_key", "<=", "9035.16739")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_1)

    data_2 = client.execute(sql)
    delete_condition_list = [("decimal_key", ">", "9926614.32657"), \
            ("decimal_most_key", "=", "123456789012345678.123456789")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_2)

    data_3 = client.execute(sql)
    delete_condition_list = [("decimal_most_key", "=", "999999999999999999.999999999")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    check(database_name, table_family_name, delete_condition_list, data_3)
    client.clean(database_name)


def test_selecting():
    """
    {
    "title": "test_sys_delete.test_selecting",
    "describe": "删除不影响查询",
    "tag": "system,p1"
    }
    """
    """
    功能点：删除不影响查询
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list() 
    init(database_name, table_family_name)
    sql = "SELECT distinct(bigint_key) FROM %s.%s \
            WHERE bigint_key >= 92036854775807 \
            order by bigint_key" % \
            (database_name, table_family_name)
    select_task = palo_task.SelectTask(config.fe_host, config.fe_query_port, sql, 
                                       database_name=database_name)
    select_thread = palo_task.TaskThread(select_task)
    select_thread.start()
    sql = "SELECT %s FROM %s.%s" % (",".join(column_name_list), database_name, table_family_name)
    data_1 = client.execute(sql)
    delete_conditions_list = list()
    sql = "SELECT bigint_key FROM %s.%s order by bigint_key desc limit 100" % \
            (database_name, table_family_name)
    bigint_key_list = client.execute(sql)
    for value in bigint_key_list:
        delete_conditions_list.append([("bigint_key", ">", "%d" % value)])
    delete_task = palo_task.DeleteTask(config.fe_host, config.fe_query_port, database_name, 
                                       table_family_name, delete_conditions_list)
    delete_thread = palo_task.TaskThread(delete_task)
    delete_thread.start()
    time.sleep(30)
    select_thread.stop()
    delete_thread.stop()
    delete_thread.join()
    print(delete_task.delete_conditions_index)
    if delete_task.delete_conditions_index > 100:
        idx = -1
    else:
        idx = delete_task.delete_conditions_index - 1
    delete_condition_list = delete_conditions_list[idx]
    check(database_name, table_family_name, delete_condition_list, data_1)
    client.clean(database_name)


def test_two_condition():
    """
    {
    "title": "test_sys_delete.test_two_condition",
    "describe": "一次删除中在不同列上(k1, k2)带有条件",
    "tag": "system,p1"
    }
    """
    """
    功能点：一次删除中在不同列上(k1, k2)带有条件
    验证：
    1. 查询中不包含条件
    2. 查询中在k1或k2上带有条件
    3. 查询中在k1和k2上都带有条件
    """
    database_name, table_family_name, rollup_table_name = util.gen_name_list() 
    init(database_name, table_family_name)
    delete_condition_list = [("tinyint_key", "=", "1"), ("smallint_key", ">", "0")]
    ret = client.delete(table_family_name, delete_condition_list)
    assert ret
    sql = "SELECT DISTINCT(tinyint_key) FROM \
            (SELECT tinyint_key, smallint_key FROM %s.%s) t ORDER BY tinyint_key" % \
            (database_name, table_family_name)
    expected_data = client.execute(sql)
    sql = "SELECT DISTINCT(tinyint_key) FROM %s.%s ORDER BY tinyint_key" % \
            (database_name, table_family_name)
    actual_data = client.execute(sql)
    assert actual_data == expected_data, "query: %s\nexpected: %s\nactual: %s" % \
            (sql, str(expected_data), str(actual_data))
    client.clean(database_name)


def test_largeint_column():
    """
    {
    "title": "test_sys_delete.test_largeint_column",
    "describe": "列存表上largeint类型列上的删除",
    "tag": "system,p1"
    }
    """
    """
    列存表上largeint类型列上的删除
    """
    largeint("column")


def largeint(storage_type):
    """
    largeint类型列上使用delete条件测试
    """
    database_name, table_name, rollup_name = util.gen_name_list()
    database_name = "%s_%s" % (database_name, storage_type)
    client.clean(database_name)
    ret = client.create_database(database_name)
    assert ret
    
    ret = client.create_table(table_name, DATA.largeint_column_list, storage_type=storage_type, 
                              distribution_info=DATA.largeint_distribution_info)
    assert ret

    ret = client.stream_load(table_name, DATA.local_largeint_file_path,
                             database_name=database_name, max_filter_ratio=0.05)
    assert ret
    
    ret = client.verify("./data/delete/largeint.expected", table_name)
    assert ret

    result = client.execute("SELECT * from %s.%s" % (database_name, table_name))
    delete_condition_list = [("k2", "=", "-102208859176565441383255581740494103204")] 
    ret = client.delete(table_name, delete_condition_list)
    assert ret
    check(database_name, table_name, delete_condition_list, result)
    
    result = client.execute("SELECT * from %s.%s" % (database_name, table_name))
    delete_condition_list = [("k2", "=", "-18446744073709551616")] 
    ret = client.delete(table_name, delete_condition_list)
    assert ret
    check(database_name, table_name, delete_condition_list, result)
    
    result = client.execute("SELECT * from %s.%s" % (database_name, table_name))
    delete_condition_list = [("k2", "<", "-18446744073709551616")] 
    ret = client.delete(table_name, delete_condition_list)
    assert ret
    check(database_name, table_name, delete_condition_list, result)
    
    result = client.execute("SELECT * from %s.%s" % (database_name, table_name))
    ret = client.delete(table_name, delete_condition_list)
    assert ret
    check(database_name, table_name, delete_condition_list, result)
    
    result = client.execute("SELECT * from %s.%s" % (database_name, table_name))
    delete_condition_list = [("k2", "=", "18446744073709551616")] 
    ret = client.delete(table_name, delete_condition_list)
    assert ret
    check(database_name, table_name, delete_condition_list, result)
    
    result = client.execute("SELECT * from %s.%s" % (database_name, table_name))
    delete_condition_list = [("k2", ">=", "82824766624071150396653061164852204443")] 
    ret = client.delete(table_name, delete_condition_list)
    assert ret
    check(database_name, table_name, delete_condition_list, result)
    
    result = client.execute("SELECT * from %s.%s" % (database_name, table_name))
    delete_condition_list = [("k2", ">=", "31875034539506183418166553393355242463"), \
            ("k2", "<=", "31875034539506183418166553393355242463"), ("k1", "=", "1")] 
    ret = client.delete(table_name, delete_condition_list)
    assert ret
    check(database_name, table_name, delete_condition_list, result)
    
    result = client.execute("SELECT * from %s.%s" % (database_name, table_name))
    delete_condition_list = [("k2", "!=", "0")] 
    ret = client.delete(table_name, delete_condition_list)
    assert ret
    check(database_name, table_name, delete_condition_list, result)
    client.clean(database_name)


def check(database_name, table_family_name, delete_condition_list, data_before=None):
    """
    校验删除正确性
    只能有一个delete条件
    """
    table_schema = client.get_index_schema(table_family_name, database_name=database_name)
    column_name_list = [column[0] for column in table_schema]
    sql = "SELECT %s FROM %s.%s" % (",".join(column_name_list), database_name, table_family_name)
    if data_before:
        #用于比较
        op = {"<": _lt, ">": _gt, "<=": _le, ">=": _ge, "=": _eq, "!=": _ne}
        #用于类型转化
        type_info = {'tinyint': int, 'smallint': int, 'int': int, 'bigint': int, 'largeint': int, \
                'float': float, 'double': float, 'decimal': Decimal, 'char': str, 'varchar': str, \
                "character": str, 'datetime': _datetime, 'date': _date, 'decimalv3': Decimal}
        checker_list = []
        for c in delete_condition_list:
            column_index = column_name_list.index(c[0])
            column_type = table_schema[column_index][1]
            if column_type.find("(") == -1:
                column_type = type_info.get(column_type.lower())
            else:
                column_type = type_info.get(column_type[:column_type.find("(")].lower())
            #第1个元素为比较运算符，第2元素为删除条件所在的列，第3个元素为类型转化后的value
            checker_list.append([c[1], column_index, column_type(c[2])]) 
             
        data_after = client.execute(sql)
        #验证删除的数据都是符合删除条件的
        for data in data_before:
            if data not in data_after:
                for c in checker_list:
                    d = data[c[1]]
                    if table_schema[c[1]][1].lower().startswith("largeint"):
                        d = int(d)
                    assert op.get(c[0])(d, c[2]), "column %s: %s not %s %s" % (column_name_list[c[1]], d, 
                                                                               str(c[0]), c[2])
        
        #验证剩下的数据正确性
        for data in data_after:
            assert data in data_before, "error data: %s" % str(data)
        
    #验证符合删除条件的数据都被删除
    delete_string_list = list()
    for c in delete_condition_list:
        column_index = column_name_list.index(c[0])
        column_type = table_schema[column_index][1]
        if column_type.find("(") != -1:
            column_type = column_type[:column_type.find("(")]
        c = list(c)
        if column_type.lower() in ["char", "varchar", "datetime", "character", "date"]:
            c[2] = "\"%s\"" % c[2]
        delete_string_list.append(" ".join(c))
    sql = "%s WHERE %s" % (sql, " and ".join(delete_string_list))
    result = client.execute(sql)
    assert result == (), result
   

def _lt(v1, v2):
    return v1 < v2


def _le(v1, v2):
    return v1 <= v2


def _gt(v1, v2):
    return v1 > v2


def _ge(v1, v2):
    return v1 >= v2


def _eq(v1, v2):
    return v1 == v2


def _ne(v1, v2):
    return v1 != v2
    
    
def _datetime(d):
    return datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S")


def _date(d):
    d = datetime.datetime.strptime(d, "%Y-%m-%d")
    return datetime.date(d.year, d.month, d.day)



def teardown_module():
    """
    tear down
    """
    pass


if __name__ == '__main__':
    setup_module()
    test_varchar()
    # test_largeint_column()
