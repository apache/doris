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
util模块，提供一些辅助函数
Date: 2014/08/05 17:19:26
"""
import datetime
import os
import sys
import struct
import random
import inspect
import time
import subprocess
import pexpect
from decimal import Decimal
import hashlib
import functools
import palo_logger

LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


def pretty(data):
    """
    将data转成string方便打印，需要list中的元素实现__str__方法
    """
    result = ""
    if isinstance(data, dict):
        result += '{'
        for key, value in data.iteritems():
            result += '%s:%s' % (str(key), pretty(value))
            result += ', '
        result = result.rstrip(", ")
        result += '}'
    elif isinstance(data, list):
        for val in data:
            result += '['
            result += pretty(val)
            result += ', '
        result = result.rstrip(", ")
        result += ']'
    else:
        result += str(data)
    return result


def gen_name_list(prefix=""):
    """
    根据调用的文件名和函数名生成database_name, table_name, index_name
    """
    file_name = ""
    for command in sys.argv:
        if command.endswith(".py"):
            file_name = command
    dir_name, file_name = os.path.split(os.path.abspath(file_name))
    file_name = file_name[:-3]
    function_name = inspect.stack()[1][3]
    database_name = "%s_%s_%s_db" % (prefix, file_name, function_name)
    LOG.info(L('test case', file_name=file_name, case=function_name))
    if len(database_name) > 60:
        md5_str = get_md5(database_name)
        database_name =  'd' + md5_str[-4:] + '_' + database_name[-50:]
    database_name = database_name.lstrip("_")
    table_name = "%s_%s_%s_tb" % (prefix, file_name, function_name)
    if len(table_name) > 60:
        md5_str = get_md5(table_name)
        table_name = 't' + md5_str[-4:] + '_' + table_name[-50:]
    table_name = table_name.lstrip("_")
    index_name = "%s_%s_%s_index" % (prefix, file_name, function_name)
    if len(index_name) > 60:
        md5_str = get_md5(index_name)
        index_name = 'i' + md5_str[-4:] + '_' + index_name[-50:]
    index_name = index_name.lstrip("_")
    return database_name, table_name, index_name


def gen_num_format_name_list(prefix=""):
    """
    根据调用的文件名和函数名生成database_name, table_name, index_name
    在文件名和函数名生成的字符串过长时，取其md5值的一部分，尽可能减少case之间重名的问题
    """
    file_name = ""
    for command in sys.argv:
        if command.endswith(".py"):
            file_name = command
    dir_name, file_name = os.path.split(os.path.abspath(file_name))
    file_name = file_name[:-3]
    function_name = inspect.stack()[1][3]

    # 分别获取database_name, table_name, index_name
    suffixes = ['db', 'tb', 'index']
    names = []
    for suffix in suffixes:
        name = "%s_%s_%s_%s" % (prefix, file_name, function_name, suffix)
        if len(name) > 60:
            # palo里的数据库和表名必须以字符串开始
            md5_str = get_md5(name)
            name = 's' + md5_str[-4:] + '_' + name[-50:]
        name = name.lstrip("_")
        names.append(name)
    return names


def get_label():
    """
    生成label字符串
    """
    fmt = '%d_%H_%M_%S_%f'
    return "label_%s_%d" % (datetime.datetime.now().strftime(fmt), random.randint(0, 2 ** 31 - 1))


def get_snapshot_label(prefix=None):
    """生成snapshot label"""
    if prefix is None:
        prefix = 'random'
    fmt = '%d_%H_%M_%S_%f'
    return "%s_snapshot_%s_%d" % (prefix, datetime.datetime.now().strftime(fmt), 
                                  random.randint(0, 2 ** 31 - 1))


def column_to_sql(column, set_null=False):
    """
    将column 4元组转成palo格式的sql字符串
    (column_name, column_type, aggregation_type, default_value)
    """
    sql = "%s %s" % (column[0], column[1])
    #value列有聚合方法
    #key列有默认值时指定此项为None
    if len(column) > 2:
        if column[2]:
            sql = "%s %s" % (sql, column[2])
    if set_null is False:
        sql = sql + ' NOT NULL'
    elif set_null is True:
        sql = sql + ' NULL'
    else:
        pass
    #有默认值的列
    if len(column) > 3:
        if column[3] is None:
            sql = '%s DEFAULT NULL' % sql
        else:
            sql = '%s DEFAULT "%s"' % (sql, column[3])
    return sql


def column_to_no_agg_sql(column, set_null=False):
    """
    将column 4元组转成SQL字符串
    (column_name, column_type, aggregation_type, default_value)
    """
    sql = "%s %s" % (column[0], column[1])
    if set_null is False:
        sql = sql + ' NOT NULL'
    elif set_null is True:
        sql = sql + ' NULL'
    else:
        pass
    #有默认值的列
    if len(column) > 3:
        sql = '%s DEFAULT "%s"' % (sql, column[3])
    return sql


def convert_agg_column_to_no_agg_column(column_list):
    """
    将agg column 4元组转成no agg column 4元组
    (column_name, column_type, aggregation_type, default_value)
    """
    no_agg_column = []
    for i in column_list:
        if len(i) == 2:
            no_agg_column.append(i)
        elif len(i) == 3:
            no_agg_column.append((i[0], i[1]))
        elif len(i) == 4:
            no_agg_column.append((i[0], i[1], "", i[3]))
    return no_agg_column


def file_to_insert_sql_value(file_name, to_str=False):
    """
    将文件中的列转为insert into类型sql中value字段
    会将文件中的N转为NULL（broker load和insert中空值的差别）
    :param file_name:
    :param to_str: 默认是false，这样的话对数字不会加双引号
    :return:
    """
    fp = open(file_name, 'r')
    total_sql_list = []
    for line in fp.readlines():
        items = line.split('\n')[0].split('\t')
        str = ''
        for i in range(len(items)):
            item = items[i]
            if not to_str and is_number(item):
                str += item
            elif item == '\\N':
                str += 'NULL'
            else:
                str += '"' + item + '"'

            if i < len(items) - 1:
                str += ','
        total_sql_list.append('(' + str + ')')
    return ','.join(total_sql_list)


def is_number(s):
    """
    验证字符串是否为数字/浮点数
    :param s:
    :return:
    """
    try:
        if s == 'NaN':
            return False
        float(s)
        return True
    except ValueError:
        return False


def exec_cmd(cmd, user=None, password=None, host=None, timeout=30):
    """
    执行shell命令
    """
    if user is not None and password is not None and host is not None:
        if cmd.find("'") >= 0:
            raise Exception("We do not support quote ' now!")
        output, status = pexpect.run("ssh %s@%s '%s'" % (user, host, cmd),
                timeout=timeout, withexitstatus=True,
                events = {"continue connecting":"yes\n", "password:":"%s\n" % password})
        LOG.info(L('exec remote cmd', cmd=cmd, output=output, status=status))
    else:
        status, output = subprocess.getstatusoutput(cmd)
    return status, output


def compare(a, b):
    """compare data to None"""
    assert isinstance(a, (tuple, list))
    assert isinstance(b, (tuple, list))
    for i in range(0, len(a)):
        if a[i] is not None and b[i] is not None:
            if a[i] > b[i]:
                return 1
            elif a[i] < b[i]:
                return -1
            else:
                continue
        elif a[i] is None and b[i] is None:
            continue
        elif a[i] is None:
            return -1
        else:
            return 1
    return 0


def check(palo_result, mysql_result, force_order=False):
    """
    check the palo result and mysql result
    1. data format
    2. order
    """
    if force_order and palo_result != ():
        try:
            palo_result = list(palo_result)
            mysql_result = list(mysql_result)
            palo_result.sort()
            mysql_result.sort()
        except Exception as e:
            if isinstance(e, TypeError):
                palo_result.sort(key=functools.cmp_to_key(compare))
                mysql_result.sort(key=functools.cmp_to_key(compare))
    if mysql_result != palo_result:
        if len(palo_result) != len(mysql_result):
            LOG.error(L('check error', palo_length=len(palo_result), 
                                       expect_length=len(mysql_result)))
            assert 0 == 1, "\npalo_result length: %d\nmysql_result length:%d" % \
                           (len(palo_result), len(mysql_result))
        for palo_line, mysql_line in zip(palo_result, mysql_result):
            if len(palo_line) != len(mysql_line):
                LOG.error(L('check error, number of columns not match', 
                            palo_colums_number=len(palo_line), expect_columns_number=len(mysql_line)))
                LOG.error(L('check error', palo_line=str(palo_line)))
                LOG.error(L('check error', expect_line=str(mysql_line)))
                assert 0 == 1, "\npalo line: %s\nmysql line:%s" % (str(palo_line), str(mysql_line))
            if palo_line != mysql_line:
                for palo_data, mysql_data in zip(palo_line, mysql_line):
                    if palo_data != mysql_data:
                        same = False
                        # str vs bytes
                        if isinstance(palo_data, (str, bytes)) and isinstance(mysql_data, (str, bytes)):
                            if isinstance(palo_data, bytes):
                                palo_data = str(palo_data, "utf8")
                            if isinstance(mysql_data, bytes):
                                mysql_data = str(mysql_data, "utf8")
                            if palo_data == mysql_data:
                                return True
                        # chinese & palo largeint return unicode
                        if isinstance(palo_data, str):
                            if palo_data == str(mysql_data):
                                same = True
                        if palo_data == mysql_data:
                            same = True
                        # blank
                        if isinstance(palo_data, str):
                            if palo_data.strip() == "" and mysql_data == "":
                                same = True
                        # null string
                        if palo_data is None and mysql_data == "":
                            same = True
                        #float
                        elif isinstance(mysql_data, (Decimal, float)) \
                                and isinstance(palo_data, (Decimal, float)):
                            same = check_float(float(palo_data), float(mysql_data))
                        # list
                        elif isinstance(mysql_data, list):
                            same = check_list(palo_data, mysql_data)
                        if not same:
                            LOG.error(L('check error', palo_data=palo_data))
                            LOG.error(L('check error', expect_data=mysql_data))
                            LOG.error(L('check error', palo_line=palo_line))
                            LOG.error(L('check error', expect_line=mysql_line))
                            assert 0 == 1, "\ndiff data: \npalo:%s; \nexpect:%s;\npalo line:%s\nexpect line: %s" \
                                % (palo_data, mysql_data, palo_line, mysql_line)


def check_float(data1, data2):
    """
    check float
    """
    if float(data1) == float(data2):
        return True
    if abs(float(data1) - float(data2)) < 0.001:
        return True
    if data2 != 0 and abs(float(data1) / float(data2) - 1.0) < 0.001:
        return True
    return False


def check_list(data1, data2):
    """
    check list
    """
    if data1 is None and data2 is None:
        return True
    elif data1 is None or data2 is None:
        return False
    elif len(data2) > 0 and isinstance(data2[0], float):
        for d1, d2 in zip(data1, data2):
            if not check_float(d1, d2):
                return False
    else:
        return data1 == data2
    return True


def convert_dict2property(properties):
    """convert mat to property str"""
    sql = '('
    for k, v in properties.items():
        if v is not None:
            sql += ' "%s" = "%s", ' % (k, v)
    sql = sql.rstrip(', ')
    sql += ')'
    return sql


def get_timestamp(palo_data):
    """将datetime 转为时间戳"""
    timeArray = palo_data[0][0].timetuple()
    timestamp = time.mktime(timeArray)
    return timestamp


def check2_time_zone(palo_re, mysql_res, gap=2):
    """比较两个datetime时间，转为时间戳，允许的差值是gap"""
    palo_data = get_timestamp(palo_re)
    mysql_data = get_timestamp(mysql_res)
    assert abs(palo_data - mysql_data) <= 2 + gap, "palo_res %s, mysql_res %s, \
        excepted gap < %s" % (palo_data, mysql_data, gap)


def assert_return(expected_flag, expected_msg, func, *args, **kwargs):
    """
    验证一个函数的执行结果
    验证是否正确；如果错误的话，验证错误信息
    :param expected_flag:True/False
    :param expected_msg:
    :param func:
    :param args:
    :param kwargs:
    :return:
    """

    try:
        func(*args, **kwargs)
    except Exception as e:
        print(str(e))
        print(expected_msg)
        LOG.info(L('get an error', msg=str(e)))
        assert not expected_flag, "real sql status is False, expect %s" % expected_flag
        assert expected_msg in str(e), "expect:%s, doris:%s" % (expected_msg, str(e))
    else:
        assert expected_flag, "real sql status is True, expect %s" % expected_flag


def get_md5(target):
    """
    计算字符串的md5值
    :param target:
    :return:
    """
    obj = hashlib.md5(b"fkldsajlkfjlaksdjfkladsjfkladsjkldsjfklfjs")
    obj.update(target.encode("utf-8"))
    return obj.hexdigest()


def assert_return_flag(expected_flag, func, *args, **kwargs):
    """
    验证一个函数的返回值
    验证是否正确
    :param expected_flag:
    :param func:
    :param args:
    :param kwargs:
    :return:
    """
    return_flag = func(*args, **kwargs)
    assert expected_flag == return_flag


def bitmap_index_to_sql(bitmap_index, set_null=False):
    """
    bitmap_index: 由3元组(index_name, column_name, index_type)组成
    """
    sql = "INDEX %s (%s) USING %s" % (bitmap_index[0], bitmap_index[1], bitmap_index[2])
    return sql


def get_attr(ret, column_idx):
    """
    获取返回结果ret的第n列
    ret = client.show_backend()
    be_ip = get_attr(ret, palo_job.BackendShowInfo.IP)
    """
    return_list = list()
    for record in ret:
        return_list.append(record[column_idx])
    LOG.info(L('get column from ret', idx=column_idx, ret=return_list))
    return return_list


def get_attr_condition_value(ret, condition_col_idx, condition_value, retrun_col_idx=None):
    """寻找ret的第condition_col_idx列的值为condition_value的行，返回该行的第retrun_col_idx列，只返回一个符合条件的"""
    if retrun_col_idx is None:
        retrun_col_idx = condition_col_idx
    for record in ret:
        if record[condition_col_idx] == condition_value:
            LOG.info(L('get result from ret', searced_key=condition_value, value=record[retrun_col_idx]))
            return record[retrun_col_idx]
    LOG.info(L('can not get result from ret', searched_key=condition_value))
    return None


def get_attr_condition_list(ret, condition_col_idx, condition_value, retrun_col_idx=None):
    """寻找ret的第condition_col_idx列的值为condition_value的行，返回所有行的第retrun_col_idx列，返回说有符合条件的"""
    result_list = list()
    if retrun_col_idx is None:
        retrun_col_idx = condition_col_idx
    for record in ret:
        if record[condition_col_idx] == condition_value:
            result_list.append(record[retrun_col_idx])
    if len(result_list) == 0:
        return None
    else:
        LOG.info(L('get all result from ret', searced_key=condition_value, value=result_list))
        return result_list


def gen_tuple_num_str(begin, end):
    """gen_tuple_num_str(1, 3)  -> ('1', '2')"""
    return tuple(map(str, range(begin, end)))


def get_string_md5(st):
    """get string md5"""
    hl = hashlib.md5()
    hl.update(st.encode(encoding='utf-8'))
    return hl.hexdigest()


