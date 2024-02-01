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
palo broker load info
"""


class LoadDataInfo(object):
    """
    load data description
    Attributes:
        file_path_list: 可以为string表示只有一个导入文件，或一个list表示多个导入文件
        column_name_list: 列名组成的list
        column_terminator: 数据文件列分隔符
        is_negative: 是否为数据恢复
        format_as: str，导入文件的格式，如csv，parquet
        columns_from_path: list，从路径中解析列的数据，如['k1', 'k2', 'city']
        where_clause: string, k1 > k2
    """
    def __init__(self, file_path_list, table_name, partition_list=None, temp_partition_list=None,
                 column_name_list=None, column_terminator=None, set_list=None, format_as=None,
                 is_negative=False, columns_from_path=None, where_clause=None, merge_type=None,
                 delete_on_predicates=None, order_by_list=None):
        if isinstance(file_path_list, list):
            self.file_path_list = file_path_list
        elif isinstance(file_path_list, str):
            self.file_path_list = [file_path_list]
        else:
            raise Exception("File path list should be list or string", file_path_list=file_path_list)
        self.table_name = table_name
        self.partition_list = partition_list
        self.temp_partition_list = temp_partition_list
        self.column_name_list = column_name_list
        self.column_terminator = column_terminator
        self.is_negative = is_negative
        self.set_list = set_list
        self.format_as = format_as
        self.columns_from_path = columns_from_path
        self.where_clause = where_clause
        self.merge_type = merge_type
        self.delete_on_predicates = delete_on_predicates
        self.order_by_list = order_by_list

    def __str__(self):
        if self.merge_type is None:
            self.merge_type = ''
        sql = '%s DATA INFILE("%s")' % (self.merge_type, '", "'.join(self.file_path_list))
        if self.is_negative:
            sql = '%s NEGATIVE' % sql
        sql = '%s INTO TABLE `%s`' % (sql, self.table_name)
        if self.partition_list is not None:
            sql = '%s PARTITION (%s)' % (sql, ', '.join(self.partition_list))
        if self.temp_partition_list is not None:
            sql = '%s TEMPORARY PARTITION (%s)' % (sql, ', '.join(self.temp_partition_list))
        if self.column_terminator is not None:
            sql = '%s COLUMNS TERMINATED BY "%s"' % (sql, self.column_terminator)
        if self.format_as is not None:
            sql = '%s FORMAT AS "%s"' % (sql, self.format_as)
        if self.column_name_list is not None:
            sql = '%s (`%s`)' % (sql, '`, `'.join(self.column_name_list))
        if self.columns_from_path is not None:
            sql = '%s COLUMNS FROM PATH AS (%s)' % (sql, ','.join(self.columns_from_path))
        if self.set_list is not None:
            sql = '%s SET(%s)' % (sql, ', '.join(self.set_list))
        if self.where_clause is not None:
            sql = '%s WHERE %s' % (sql, self.where_clause)
        if self.delete_on_predicates is not None:
            sql = '%s DELETE ON %s' % (sql, self.delete_on_predicates)
        if self.order_by_list is not None:
            sql = '%s ORDER BY %s' % (sql, ','.join(self.order_by_list))
        return sql

    def set(self, k, v):
        """设置属性"""
        if hasattr(self, k):
            setattr(self, k, v)
        else:
            raise Exception('data descriptor set error: %s' % k)


