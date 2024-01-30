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
import os
import random
import sys
import struct
import inspect
import time
import subprocess
import pexpect
import linecache
from decimal import Decimal
from collections.abc import Iterable
import functools

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


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


def get_label():
    """
    生成label字符串
    """
    return "label_%s" % str(time.time()).split('.')[0]


def compare(a, b):
    """ compare """
    assert isinstance(a, tuple)
    assert isinstance(b, tuple)
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


def check_same(palo_result, mysql_result, force_order=False):
    """
    check the palo result and mysql result
    1. data format
    2. order
    """
    if force_order:
        try:
            palo_result = list(palo_result)
            mysql_result = list(mysql_result)
            palo_result.sort()
            mysql_result.sort()
        except Exception as e:
            palo_result.sort(key=functools.cmp_to_key(compare))
            mysql_result.sort(key=functools.cmp_to_key(compare))
    
    if mysql_result != palo_result:
        assert len(palo_result) == len(mysql_result), \
                "palo_result length: %d \n mysql_result length:%d" % \
                (len(palo_result), len(mysql_result))
        for palo_line, mysql_line in zip(palo_result, mysql_result):
            if len(palo_line) != len(mysql_line):
                print("palo line: ")
                LOG.info(L('palo line', palo_sql=palo_line))
                print(palo_line)
                print("mysql_line: ")
                LOG.info(L('mysql line', mysql_sql=mysql_line))
                print(mysql_line)
            assert len(palo_line) == len(mysql_line), \
                    "palo line: %s\nmysql line:%s" % (str(palo_line), str(mysql_line))
            if palo_line != mysql_line:
                for palo_data, mysql_data in zip(palo_line, mysql_line):
                    same = check_data_same(palo_data, mysql_data)
                    if same == False:
                        print("diff:")
                        print("palo")
                        print(palo_line, type(palo_data))
                        LOG.error(L('palo data line:', sql=palo_line, format=type(palo_data)))
                        print("mysql")
                        print(mysql_line, type(mysql_data))
                        LOG.error(L('check data line:', sql=mysql_line, format=type(mysql_data)))
                        LOG.error(L('diff data:', palo_data=palo_data, mysql_data=mysql_data))
                    assert same, "diff data: palo:%s; mysql:%s\rpalo line:%s\rmysql line: %s" \
                                % (palo_data, mysql_data, palo_line, mysql_line)


def check_same_randomly(resultofpalo, resultofmysql, force_order=False):
    """
    check the palo result and mysql result randomly
    make the check time short
    """
    if force_order:
        resultofpalo = list(resultofpalo)
        resultofpalo.sort()
        resultofmysql = list(resultofmysql)
        resultofmysql.sort()
  
    if resultofmysql != resultofpalo:
        assert len(resultofpalo) == len(resultofpalo),\
            "palo result length: %d \n\r mysql result length: %d" %\
            (len(resultofpalo), len(resultofmysql))
        tmp = zip(resultofpalo, resultofmysql)
        lst_iter = iter(tmp)
        flag = 0
        for palo_line, mysql_line in zip(resultofpalo, resultofmysql):
            if palo_line != mysql_line:
                for palo_data, mysql_data in zip(palo_line, mysql_line):
                    flag = check_data_same(palo_data, mysql_data)
                    if not flag:
                        print("diff")
                        print("palo:")
                        print(palo_line, type(palo_data))
                        LOG.error(L('palo data line', sql=palo_line, formate=type(palo_data)))
                        print("mysql:")
                        print(mysql_line, type(mysql_data))
                        LOG.error(L('mysql data line', sql=mysql_line, formate=type(mysql_data)))
                        assert flag
            times = random.randint(0, 2)
            try:
                while times:
                    lst_iter.next()
                    times -= 1
            except StopIteration:
                pass


def check_data_same(dataofpalo, dataofmysql):
    """
    check the data of the record is same
    """
    if dataofpalo == dataofmysql:
        return True
    # encode
    if isinstance(dataofpalo, (str, bytes)) and isinstance(dataofmysql, (str, bytes)):
        if isinstance(dataofpalo, bytes):
            dataofpalo = str(dataofpalo, 'utf8')
        if isinstance(dataofmysql, bytes):
            dataofmysql = str(dataofmysql, "utf8")
        if dataofpalo == dataofmysql:
            return True
    # when largeint palo is str, python is int, need str()
    if isinstance(dataofpalo, str):
        if dataofpalo == str(dataofmysql):
            return True
    # mysql data is string, palo data may be not string, change palo data
    if isinstance(dataofmysql, str):
       if str(dataofpalo) == dataofmysql:
           return True
    # blank
    if isinstance(dataofpalo, str):
        if dataofpalo.strip() == "" and dataofmysql == "":
            return True
        elif isinstance(dataofmysql, int) and str(dataofmysql) == dataofpalo:
            return True
    # null string
    if dataofpalo is None and dataofmysql == "":
        return True
    # float
    if isinstance(dataofmysql, (Decimal, float)) and \
       isinstance(dataofpalo, (Decimal, float)):
        return check_float(float(dataofpalo), float(dataofmysql))
    # 空间函数
    # POLYGON ((-1 -1)) vs POLYGON((-1 -1))
    if isinstance(dataofpalo, str):
        if dataofpalo.find('POLYGON') != -1:
            if dataofpalo.replace(' ', '') == dataofmysql.replace(' ', ''):
                return True
        geo_type = ['POINT', 'LINESTRING', 'Circle']
        Find = False
        for tp in geo_type:
            if dataofpalo.startswith(tp):
                print("type", dataofpalo, tp)
                Find = True
                break
        if Find:
            return check_geodate(dataofpalo, dataofmysql)
    return False


def check_geodate(dataofpalo, dataofmysql):
    """
    比较空间函数的结果，结果包含空间关键字和数字，只对比数字部分，且精度要求在1e-13
    palo_data:(u'POINT (0.234999999404 0.234999999404)', 0.235)<type 'unicode'>
    mysql_data:('POINT(0.23499999940395355 0.23499999940395355)', 0.235) <type 'str'>
    Line(0 0, 0 0), Line(0 0,0 0)
    """
    if dataofpalo.startswith('LINESTRING'):
        palo = dataofpalo.replace(' ','')
        mysql = dataofmysql.replace(' ','')
        return palo == mysql
    palo = filter(lambda ch: ch in ' .0123456789', dataofpalo)
    mysql = filter(lambda ch: ch in ' .0123456789', dataofmysql)
    palo = list(palo)
    mysql = list(mysql)
    palo = ''.join(palo)
    mysql = ''.join(mysql)
    palo = palo.strip()
    mysql = mysql.strip()
    palo = palo.split(' ')
    mysql = mysql.split(' ')
    assert len(palo) == len(mysql)
    flag = False
    for i, j in zip(palo, mysql):
        flag = check_float(i,j, 1e-13)  
    return flag

    
def check_float(data1, data2, diff=0.001):
    """
    check float
    """
    if float(data1) == float(data2):
        return True
    if abs(float(data1) - float(data2)) < diff:
        return True
    if data2 != 0 and abs(float(data1) / float(data2) - 1.0) < diff:
        return True
    return False


def check_percent_diff(palo_result, mysql_result, accurate_column, force_order, percent=0.1):
    """用于估算函数，要和真实结果对比，对比的误差是percent
       palo_result：plao的结果
       mysql_result：mysql的结果
       accurate_column:需精算的列,1是精算，0是估算：[0,1,0,1]表示第一，三列估算
       force_order：结果是否要求有序
       percent：允许的误差占比0-1之间
       compute all result,finally check flag
    """
    if force_order:
        try:
            palo_result = list(palo_result)
            mysql_result = list(mysql_result)
            palo_result.sort()
            mysql_result.sort()
        except Exception as e:
            palo_result.sort(key=functools.cmp_to_key(compare))
            mysql_result.sort(key=functools.cmp_to_key(compare))
    # rows count
    if mysql_result != palo_result:
        assert len(palo_result) == len(mysql_result),\
                    "palo_result length %s, mysql_result length %s" % (len(palo_result),\
                     len(mysql_result))
    # content diff
    flag = True
    max_gap = 0.0
    if len(accurate_column) == 0:
        check_percent(palo_result, mysql_result, percent, force_order)
        return flag, max_gap

    for index in range(len(palo_result)):
        for i in range(len(accurate_column)):
            if accurate_column[i] == 1:
                # 精算列对比
                check_same((palo_result[index][i],), (mysql_result[index][i],), force_order)
            elif accurate_column[i] == 0:
                # 估算
                flag, gap = check_percent((palo_result[index][i],), (mysql_result[index][i],), percent, force_order)
                if gap > max_gap:
                    max_gap = gap
    return flag, max_gap


def check_percent(palo_result, mysql_result, percent, force_order=False):
    """allow diff percent is 0.1
        accurate_column:精确check的列,1是精算，0是估算：[0,1,0,1]表示第一，三列估算
    """
    if force_order:
        try:
            palo_result = list(palo_result)
            mysql_result = list(mysql_result)
            palo_result.sort()
            mysql_result.sort()
        except Exception as e:
            palo_result.sort(key=functools.cmp_to_key(compare))
            mysql_result.sort(key=functools.cmp_to_key(compare))

    flag = True
    gap = 0
    if mysql_result != palo_result:
        assert len(palo_result) == len(mysql_result), \
            "palo_result length: %d \n mysql_result length:%d" % \
            (len(palo_result), len(mysql_result))
        for palo_line, mysql_line in zip(palo_result, mysql_result):
            # 有len()，比较长度
            if len(palo_result) > 1 and len(mysql_result) > 1:
                # #result is(1L,) 不能继续line，len
                if len(palo_line) != len(mysql_line):
                    print("palo line: ")
                    print(palo_line)
                    LOG.error(L('palo line', palo_sql=palo_line))
                    print("mysql_line: ")
                    print(mysql_line)
                    LOG.error(L('mysql line', mysql_sql=mysql_line))
                assert len(palo_line) == len(mysql_line), \
                     "palo line: %s\nmysql line:%s" % (str(palo_line, mysql_line))
            
            if palo_line != mysql_line:
                if isinstance(palo_line,Iterable)  and isinstance(mysql_line,Iterable):
                    for palo_data, mysql_data in zip(palo_line, mysql_line):
                        # ##actucal vs estimate diff in 1%
                        if int(mysql_data) == 0:
                            # #k1 = 0
                            assert mysql_data == palo_data,"mysql_data is %s, palo_data %s"\
                                % (mysql_data, palo_data)
                            continue
                        if (mysql_data) is None or (palo_data) is None:                                
                            continue
                        gap = abs(float(palo_data) / float(mysql_data) - 1.0)
                        if gap > percent:
                            flag = False
                            print("hello, diff:")
                            LOG.error(L('diff:'))
                            print("palo:%s, mysql:%s, actual diff %s, except diff %s, flag %s" % \
                               (palo_data, mysql_data, gap, percent, flag))
                            LOG.error(L('palo line, mysql line', palo_line=palo_data, mysql_line=mysql_data))
                            return flag, gap
                else:
                    gap = abs(float(palo_line) / float(mysql_line) - 1.0)
                    if gap > percent:
                        flag = False
                        print("hello, diff:")
                        LOG.error(L('diff:'))
                        print("palo:%s, mysql:%s, actual diff %s, except diff %s, flag %s" % \
                           (palo_line, mysql_line, gap, percent, flag))
                        LOG.error(L('palo line, mysql line', palo_line=palo_line, mysql_line=mysql_line))
                        return flag, gap
    return flag, percent 


def read_file(file_name, line_count=1):
    """读取文件指定行的内容"""
    # print(linecache.getline(file_name, line_count))
    return linecache.getline(file_name, line_count)


if __name__ == '__main__':
    read_file('../data/query_string_money_format.py')
