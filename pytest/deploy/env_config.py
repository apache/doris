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
This module describe Palo environment config.

Date:    2015/10/07 17:23:06
"""

master = 'fe_master'
follower_list = [
                'fe_follower1',
                'fe_follower2'
                ]

observer_list = [
                'fe_observer1',
                'fe_observer2'
                ]

be_list = [
          'be_host1',
          'be_host2',
          'be_host3',
          'be_host4',
          'be_host5'
          ]

dynamic_add_fe_list = []
dynamic_add_be_list = []

palo_path = '/home/work/dir'

host_username = 'work'
host_password = 'xxxx'

dpp_config_str=r""

fe_query_port = 9030
JAVA_HOME = '/home/work/jdk/jdk-1.8'
fe_password = 'xxxx'

broker_list = {'hdfs': 'host:ip',
               'ahdfs': 'host:ip'}

node_ipv4s = 'xxxx'
fe_config_map = {'priority_networks': node_ipv4s}
be_config_map = {}

# 这些可以使用默认配置
fe_path = '%s/%s' % (palo_path, 'PALO-FE')
be_path = '%s/%s' % (palo_path, 'PALO-BE')
be_data_path_list = ['%s/be/data/data.HDD' % (be_path), '%s/be/data/data.SSD' % (be_path)]

# be_port
be_port = fe_query_port + 30
webserver_port = fe_query_port - 1000 + 10
heartbeat_service_port = fe_query_port + 20
be_rpc_port = fe_query_port + 10
brpc_port = be_port - 1000

# fe_port
http_port = fe_query_port - 1000
rpc_port = fe_query_port - 10
edit_log_port = fe_query_port - 20

