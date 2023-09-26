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
palo partition info
"""


class Partition(object):
    """
    一个partition
    """
    def __init__(self, partition_name, partition_value,
                 storage_medium=None, storage_cooldown_time=None):
        self.partition_name = partition_name
        self.partition_value = None
        self.storage_medium = storage_medium
        self.storage_cooldown_time = storage_cooldown_time
        self.get_partition_value(partition_value)

    def get_partition_value(self, partition_value):
        """get partition value"""
        if isinstance(partition_value, tuple):
            s = ''
            for v in partition_value:
                if v.upper() == 'MAXVALUE' or v.upper() == 'MINVALUE':
                    s += v
                else:
                    s = '%s "%s"' % (s, v)
            self.partition_value = s
        else:
            self.partition_value = partition_value
        return self.partition_value

    def __str__(self):
        partition_value = self.partition_value
        if partition_value != 'MAXVALUE':
            partition_value = '(\"%s\")' % self.partition_value
        sql = 'PARTITION %s VALUES LESS THAN %s' % (self.partition_name, partition_value)
        if self.storage_medium is not None:
            sql = '%s ( "storage_medium"="%s"' % (sql, self.storage_medium)
            if self.storage_cooldown_time is not None:
                sql = '%s, "storage_cooldown_time"="%s"' % (sql, self.storage_cooldown_time)
            sql = '%s )' % sql
        return sql


class PartitionInfo(object):
    """
    partition即一级分区信息
    """

    def __init__(self, partition_by_column=None, partition_name_list=None,
                 partition_value_list=None, partition_list=None, partition_type='RANGE'):
        """

        Args:
            partition_by_column: list or str eg: 'k1' or ['k1','k2']
            partition_name_list: list e.g:['p1','p2', 'p3']
            partition_value_list: list e.g['1', '2', '3', '4'] or [('1','2','3'), ('2','2','3'),] or [(('1', '1'),('2', '2')), ((),())]
            partition_list: partition_list
            partition_type: range/list
        """
        self.partition_by_column = partition_by_column
        self.partition_name_list = partition_name_list
        self.partition_value_list = partition_value_list
        self.partition_list = partition_list
        self.partition_type = partition_type
        if self.partition_type.upper() == 'RANGE':
            self.values_partitioned = 'LESS THAN'
        elif self.partition_type.upper() == 'LIST':
            self.values_partitioned = 'IN'
        else:
            self.values_partitioned = ''

    def get_partition_value(self, partition_value):
        """to string"""
        if isinstance(partition_value, str) and partition_value.upper() == 'MAXVALUE':
            partition_value = 'VALUES %s %s' % (self.values_partitioned, partition_value)
            return partition_value
        if isinstance(partition_value, tuple):
            # ('1','2','3') RANGE多分区列 or LIST单列分区
            if isinstance(partition_value[0], str):
                partition_value = '(%s)' % ','.join('"{0}"'.format(v) for v in partition_value)
                partition_value.replace('"MAXVALUE"', 'MAXVALUE')
                partition_value.replace('"MINVALUE"', 'MINVALUE')
                partition_value = 'VALUES %s %s' % (self.values_partitioned, partition_value)
                return partition_value
            # (('1', '1'),('2', '2'))RANGE分区，区间表示
            elif self.partition_type.upper() == 'RANGE' and isinstance(partition_value[0], tuple) \
                    and len(partition_value) == 2:
                down_range = ','.join('"{0}"'.format(v) for v in partition_value[0])
                down_range.replace('"MAXVALUE"', 'MAXVALUE')
                up_range = ','.join('"{0}"'.format(v) for v in partition_value[1])
                up_range.replace('"MAXVALUE"', 'MAXVALUE')
                partition_value = 'VALUES [(%s), (%s))' % (down_range, up_range)
                return partition_value
            # LIST多列分区[(('1', '1'),('2', '2'),('3','3')), (...)]
            elif self.partition_type.upper() == 'LIST' and isinstance(partition_value[0], tuple):
                val_list = []
                for val in partition_value:
                    val_list.append('(%s)' % ','.join('"{0}"'.format(v) for v in val))
                partition_value = 'VALUES IN (%s)' % ','.join(val_list)
                return partition_value
            else:
                print("not support")
        else:
            # RANGE单分区列
            partition_value = 'VALUES %s ("%s")' % (self.values_partitioned, partition_value)
            return partition_value
        print('partition value error')
        return None

    def __str__(self):
        if isinstance(self.partition_by_column, list):
            sql = 'PARTITION BY %s(%s) (' % (self.partition_type, ','.join(self.partition_by_column))
        else:
            sql = 'PARTITION BY %s(%s) (' % (self.partition_type, self.partition_by_column)
        if self.partition_list is not None:
            for partition in self.partition_list:
                sql = '%s %s,' % (sql, str(partition))
            sql = sql.rstrip(',')
        else:
            for partition_name, partition_value in \
                    zip(self.partition_name_list, self.partition_value_list):
                partition_value = self.get_partition_value(partition_value)
                sql = '%s PARTITION %s %s,' % (sql, partition_name, partition_value)
        sql = "%s )" % sql.rstrip(",")
        return sql
