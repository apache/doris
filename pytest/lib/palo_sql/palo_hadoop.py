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
palo hadoop info, deprecated
"""


class HadoopInfo(object):
    """
    自定义etl
    """
    def __init__(self, cluster_name, hadoop_palo_path=None,
                 hadoop_configs=None, hadoop_http_port=None):
        self.hadoop_palo_path = hadoop_palo_path
        self.hadoop_configs = hadoop_configs
        self.hadoop_http_port = hadoop_http_port
        self.cluster_name = cluster_name

    def __str__(self):
        if self.hadoop_palo_path is None and self.hadoop_configs is None \
                and self.hadoop_http_port is None:
            return ''
        sql = '"cluster"="%s",' % self.cluster_name
        if self.hadoop_palo_path is not None:
            sql = '%s "hadoop_palo_path"="%s",' % (sql, self.hadoop_palo_path)
        if self.hadoop_http_port is not None:
            sql = '%s "hadoop_http_port"="%s",' % (sql, self.hadoop_http_port)
        if self.hadoop_configs is not None:
            sql = '%s "hadoop_configs"="%s",' % (sql, self.hadoop_configs)

        return sql