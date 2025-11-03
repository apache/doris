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
This module create dir on hdfs for Palo runtime environment.

Date:    2015/10/07 17:23:06
"""

import json
import env_config


def gen_add_load_cluster_sql(user='root'):
    """create hdfs dir
    """
    dpp_config_str = env_config.dpp_config_str
    configs = ('{' + dpp_config_str.lstrip("{palo_dpp : {").rstrip('}').\
            replace("hadoop_palo_path", "'hadoop_palo_path'").\
            replace("hadoop_http_port", "'hadoop_http_port'").\
            replace("hadoop_configs", "'hadoop_configs'") + '}').replace("'", "\"")
    configs_dict = json.loads(configs)

    cluster_name = dpp_config_str.split(':')[0].strip().lstrip('{').lstrip()
    hadoop_palo_path = configs_dict['hadoop_palo_path']
    hadoop_http_port = configs_dict['hadoop_http_port']
    hadoop_configs = configs_dict['hadoop_configs']

    sql = "SET PROPERTY FOR '%s' 'load_cluster.%s.hadoop_palo_path' = '%s'," % \
            (user, cluster_name, hadoop_palo_path)
    sql += " 'load_cluster.%s.hadoop_http_port' = '%s'," % \
            (cluster_name, hadoop_http_port)
    sql += " 'load_cluster.%s.hadoop_configs' = '%s'" % \
            (cluster_name, hadoop_configs)

    sql_1 = sql
    sql_2 = "SET PROPERTY FOR '%s' 'default_load_cluster' = '%s'" % (user, cluster_name)

    return (sql_1, sql_2)
    

if __name__ == '__main__':
    sql_1, sql_2 = gen_add_load_cluster_sql()
    print(sql_1)
    print(sql_2)
