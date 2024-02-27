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

###########################################################################
#
#   @file palo_types.py
#   @date 2022-10-26 15:11:52
#   @brief Palo types, 将csv或者返回的结果进行规范化处理
#
############################################################################
"""used for verify"""

import decimal
from datetime import datetime

# palo类型和对应的类型code，与mysql的cursor基本返回一致
BOOLEAN = 1
TINYINT = 1
SMALLINT = 2
INT = 3
BIGINT = 8
LARGEINT = 254
DECIMAL = 246
DOUBLE = 5
FLOAT = 4
DATE = 10
DATETIME = 12
CHAR = 254
VARCHAR = 254
STRING = 252
HLL = 254
BITMAP = 254

# palo复杂类型自定义的code
ARRAY_BOOLEAN = 1001
ARRAY_TINYINT = 1001
ARRAY_SMALLINT = 1001
ARRAY_INT = 1001
ARRAY_BIGINT = 1001
ARRAY_LARGEINT = 1006
ARRAY_DECIMAL = 1002
ARRAY_DOUBLE = 1005
ARRAY_FLOAT = 1005
ARRAY_DATE = 1003
ARRAY_DATETIME = 1004
ARRAY_CHAR = 1006
ARRAY_VARCHAR = 1006
ARRAY_STRING = 1006
# 如果后续有复杂类型，类型code需大于1000

palo_datetime = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
palo_date = lambda x: datetime.strptime(x, '%Y-%m-%d').date()

none_charact = ('NONE', 'NULL')
__int = lambda x: None if x.strip().upper() in none_charact else int(x)
__float = lambda x: None if x.strip().upper() in none_charact else float(x)
__date = lambda x: None if x.strip().upper() in none_charact else palo_date(x.strip())
__datetime = lambda x: None if x.strip().upper() in none_charact else palo_datetime(x.strip())
__decimal = lambda x: None if x.strip().upper() in none_charact else decimal.Decimal(x)
__str = lambda x: None if x.strip().upper() in none_charact else str(x)


def palo_array_int(data):
    """将字符串转为ARRAY_INT"""
    if data:
        data_list = data.strip('[').strip(']').split(',')
        if len(data_list) == 1:
            return []
        else:
            return map(__int, data_list)
    else:
        return data


def palo_array_float(data):
    """将字符串转为ARRAY_FLOAT"""
    if data:
        data_list = data.strip('[').strip(']').split(',')
        if len(data_list) == 1:
            return []
        else:
            return map(__float, data_list)
    else:
        return data


def palo_array_decimal(data):
    """将字符串转为ARRAY_DECIMAL"""
    if data:
        data_list = data.strip('[').strip(']').split(',')
        if len(data_list) == 1:
            return []
        else:
            return map(__decimal, data_list)
    else:
        return data


def palo_array_date(data):
    """将字符串转为ARRAY_DATE"""
    if data:
        data_list = data.strip('[').strip(']').replace('\'', '').split(',')
        if len(data_list) == 1:
            return []
        else:
            return map(__date, data_list)
    else:
        return data


def palo_array_datetime(data):
    """将字符串转为ARRAY_DATETIME"""
    if data:
        data_list = data.strip('[').strip(']').replace('\'', '').split(',')
        if len(data_list) == 1:
            return []
        else:
            return map(__datetime, data_list)
    else:
        return data


def palo_array_string(data):
    """将字符串转为ARRAY_STRING"""
    if data:
        data_list = data.strip('[').strip(']').split(',')
        if len(data_list) == 1:
            return []
        else:
            return map(__str, data_list)
    else:
        return data


# palo不同类型及对应的python转换函数
type_num_func_dict = {254: str, 
                   8: int, 
                   5: float, 
                   15: str,
                   0: decimal.Decimal, 
                   10: palo_date,
                   3: int,
                   1: int,
                   2: int,
                   4: float,
                   12: palo_datetime,
                   246: decimal.Decimal,
                   252: str,
                   1001: palo_array_int,
                   1002: palo_array_decimal,
                   1003: palo_array_date,
                   1004: palo_array_datetime,
                   1005: palo_array_float,
                   1006: palo_array_string}


def convert_csv_to_ret(csv, schema):
    """
    将csv按照对应的schema转化为对应的数据格式，[[],[],...]
    schema：对应的类型code list， 如csv中列类型为[tinyint, smallint, int],则schema为[1, 2, 3]
    csv：csv文件
    todo: 多个文件则需要按照key进行聚合，对照client.verify
    """
    convert_func = [type_num_func_dict[type_code] for type_code in schema]
    file_ret = list()
    with open(csv, 'r') as f:
        for line in f.readlines():
            col_data = line.strip('\n').strip('\r').split('\t')
            col_ret = list()
            for col, func in zip(col_data, convert_func):
                if col.upper() in ('NONE', 'NULL'):
                    col_ret.append(None)
                else:
                    col_ret.append(func(col))
            file_ret.append(col_ret)
    return file_ret


def convert_ret_complex_type(ret, schema):
    """
    当sql返回的结果中含有复杂类型时，则需要将对应的列转换为对应的类型。默认复杂类型返回的是字符串
    """
    convert_func = [type_num_func_dict[type_code] for type_code in schema]
    return_ret = list()
    for data in ret:
        data_line = list()
        for idx, func in zip(range(0, len(schema)), convert_func):
            if schema[idx] < 1000:
                data_line.append(data[idx])
            else:
                data_line.append(func(data[idx]))
        return_ret.append(data_line)
    return return_ret


