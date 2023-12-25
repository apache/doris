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
Translate sql to query variant
"""
import os
import pymysql
import re

def get_all_columns_info(connection, tables):
    """ 获取多个表的列信息 """
    all_columns_info = {}
    with connection.cursor() as cursor:
        for table in tables:
            sql_desc = f"DESC {table}"
            cursor.execute(sql_desc)
            for col in cursor.fetchall():
                col_type = col["Type"].upper()
                if "CHAR" in col_type or "VARCHAR" in col_type:
                    all_columns_info[col["Field"]] = "TEXT"
                elif "DECIMAL" in col_type:
                    all_columns_info[col["Field"]] = "DOUBLE"
                else:
                    all_columns_info[col["Field"]] = col_type
    return all_columns_info

def translate_sql(all_columns_info, sql):
    """ 翻译 SQL 查询 """
    # 简单的 SQL 解析：查找并替换列名
    def replace_column(match):
        column_name = match.group(0)
        if column_name in all_columns_info:
            return f'CAST(var["{column_name}"] AS {all_columns_info[column_name]})'
        return column_name

    # 使用正则表达式替换列名
    translated_sql = re.sub(r'\b(\w+)\b', replace_column, sql)

    return translated_sql

# 数据库连接配置
config = {
    'host': '127.0.0.1',
    'port': 9138,
    'user': 'root',
    'password': '',
    'db': 'regression_test_tpch_unique_sql_zstd_p0',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# 连接数据库
connection = pymysql.connect(**config)

def process_sql_files(connection, tables, sql_files):
    """ 处理多个 SQL 文件 """
    all_columns_info = get_all_columns_info(connection, tables)

    for sql_file in sql_files:
        with open(sql_file, 'r') as file:
            original_sql = file.read()

        translated_sql = translate_sql(all_columns_info, original_sql)

        output_file = os.path.splitext(sql_file)[0] + '_trans.sql'
        with open(output_file, 'w') as file:
            file.write(translated_sql)
        print(f"Translated SQL written to {output_file}")

try:
    # 涉及的表名
    tables = ["customer",
                  "lineitem",
                  "nation",
                  "orders",
                  "part",
                  "partsupp",
                  "region", 
                  "supplier"]
    sql_files = [f'q{str(i).zfill(2)}.sql' for i in range(1, 22)]



    # 获取所有表的列信息
    all_columns_info = get_all_columns_info(connection, tables)

    # 翻译 SQL
    process_sql_files(connection, tables, sql_files)
    print("Original SQL:", original_sql)
    print("Translated SQL:", translated_sql)

finally:
    connection.close()

