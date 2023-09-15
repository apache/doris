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
palo routine load info
"""


class RoutineLoadProperty(object):
    """routine load property"""
    def __init__(self):
        self.load_property = self.LoadProperty()
        self.job_property = self.JobProperty()
        self.data_source_property = self.DataSourceProperty()

    class LoadProperty(object):
        """routine load property"""
        def __init__(self):
            """
            load property:
                column_separator: string, like '\t'
                column_mapping: list, like ['k1', 'k2', 'v', 'v1=v+100']
                where_predicates: strine like 'k1 > 0 and k3 is not Null'
                partitions: list, like ['p1', 'p2', 'p3']
            """
            self.column_separator = None
            self.column_mapping = None
            self.where_predicates = None
            self.partitions = None
            self.merge_type = None
            self.delete_on_predicates = None

        def __str__(self):
            subsql_list = list()
            if self.column_separator is not None:
                sql = 'COLUMNS TERMINATED BY "%s"' % self.column_separator
                subsql_list.append(sql)
            if self.column_mapping is not None:
                sql = 'COLUMNS (%s)' % (', '.join(self.column_mapping))
                subsql_list.append(sql)
            if self.where_predicates is not None:
                sql = 'WHERE %s' % self.where_predicates
                subsql_list.append(sql)
            if self.partitions is not None:
                sql = 'PARTITION(%s)' % ', '.join(self.partitions)
                subsql_list.append(sql)
            if self.delete_on_predicates is not None:
                sql = 'DELETE ON %s' % self.delete_on_predicates
                subsql_list.append(sql)
            if self.merge_type is not None:
                return 'WITH %s %s' % (self.merge_type, ','.join(subsql_list))
            else:
                return ','.join(subsql_list)

    class JobProperty(object):
        """routine load job property"""
        def __init__(self):
            """
            job property:
                desired_concurrent_number: string/int
                max_batch_interval: string/int
                max_batch_rows: string/int
                max_batch_size: string/int
                max_error_number: string/int
                timezone : 指定导入作业所使用的时区
                strict_mode : true/false
                format: csv/json
                jsonpaths: string
                strip_outer_array: true/false
                json_root: string
            """
            self.job_property = {}

        def __str__(self):
            sql = 'PROPERTIES('
            for k, v in self.job_property.items():
                if v is not None:
                    sql += ' "%s" = "%s", ' % (k, v)
            sql = sql.rstrip(', ')
            sql += ')'
            if sql == 'PROPERTIES()':
                sql = ''
            return sql

    class DataSourceProperty(object):
        """routine load datasource property"""
        def __init__(self):
            """
            data source property:
                kafka_broker_list: string, like "broker1:9092,broker2:9092"
                kafka_topic: string, like 'topic'
                kafka_partitions: list, like [0,1,2,3]
                kafka_offsets: list, like ['0', '2', 'OFFSET_BEGINNING', 'OFFSET_END']
            还可能有kafka访问的鉴权参数
            """
            self.data_source_property = {}

        def __str__(self):
            sql = '('
            for k, v in self.data_source_property.items():
                if v is not None:
                    sql += ' "%s" = "%s", ' % (k, v)
            sql = sql.rstrip(', ')
            sql += ')'
            return sql

    def set_column_mapping(self, column_mapping):
        """set column mapping"""
        self.load_property.column_mapping = column_mapping

    def set_column_separator(self, column_separator):
        """set column separator"""
        self.load_property.column_separator = column_separator

    def set_partitions(self, partitions):
        """this is palo_sql table partition"""
        self.load_property.partitions = partitions

    def set_where_predicates(self, where_predicates):
        """set where predicates"""
        self.load_property.where_predicates = where_predicates

    def set_merge_type(self, merge_type):
        """set merge type"""
        self.load_property.merge_type = merge_type

    def set_delete_on_predicates(self, delete_on_predicates):
        """set delete on predicates"""
        self.load_property.delete_on_predicates = delete_on_predicates

    def set_desired_concurrent_number(self, desired_concurrent_num):
        """set desired concurrent number"""
        self.set_job_property("desired_concurrent_number", desired_concurrent_num)

    def set_max_batch_interval(self, max_batch_interval):
        """max batch interval"""
        self.set_job_property("max_batch_interval", max_batch_interval)

    def set_max_batch_rows(self, max_batch_rows):
        """set max batch rows"""
        self.set_job_property("max_batch_rows", max_batch_rows)

    def set_max_batch_size(self, max_batch_size):
        """set max batch size"""
        self.set_job_property("max_batch_size", max_batch_size)

    def set_max_error_number(self, max_error_number):
        """set max error number"""
        self.set_job_property("max_error_number", max_error_number)

    def set_timezone(self, time_zone):
        """set max batch rows"""
        self.set_job_property("timezone", time_zone)

    def set_kafka_broker_list(self, kafka_broker_list):
        """set kafka broker list"""
        self.set_data_source_property("kafka_broker_list", kafka_broker_list)

    def set_kafka_topic(self, kafka_topic):
        """set kafka topic"""
        self.set_data_source_property("kafka_topic", kafka_topic)

    def set_kafka_partitions(self, kafka_partitions):
        """set kafka partitions"""
        self.set_data_source_property("kafka_partitions", kafka_partitions)

    def set_kafka_offsets(self, kafka_offsets):
        """set kafka offsets"""
        self.set_data_source_property("kafka_offsets", kafka_offsets)

    def set_job_property(self, key, value):
        """set job property"""
        self.job_property.job_property[key] = value

    def set_data_source_property(self, key, value):
        """set data source property"""
        self.data_source_property.data_source_property[key] = value
