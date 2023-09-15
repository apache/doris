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
palo dynamic partition info
"""


class DynamicPartitionInfo(object):
    """
    dynamic_partition动态分区信息:
        enable: 是否开启动态分区，默认为true
        time_unit: 调度单位，可指定为HOUR、DAY、WEEK、MONTH
        time_zone: 时区，默认为系统时区
        start: 起始偏移，默认为-2147483648
        end: 结束偏移
        prefix: 分区名前缀
        buckets: 分桶数
        replication_num: 副本数
        start_day_of_week: 指定每周的起点 1--7
        start_day_of_month: 指定每月的起点 1--28
        create_history_partition: 是否创建历史分区，默认为false
        history_partition_num: 指定创建历史分区数量，默认为-1即未设置
        hot_partition_num: 指定最新的多少个分区为热分区
    """
    def __init__(self, dynamic_partition_info={}):
        self.value_list = dynamic_partition_info

    def to_string(self):
        """to string"""
        properties_value = ''
        for key in self.value_list:
            if self.value_list[key] is not None:
                properties_value = '%s "dynamic_partition.%s"="%s",' % (properties_value, key, self.value_list[key])
        return properties_value

    def __str__(self):
        return self.to_string()
