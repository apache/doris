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
import os

import env_config


def create_hdfs_dir():
    """create hdfs dir
    """
    dpp_config_str = env_config.dpp_config_str
    configs = ('{' + dpp_config_str.lstrip("{palo_dpp : {").rstrip('}').
               replace("hadoop_palo_path", "'hadoop_palo_path'").
               replace("hadoop_http_port", "'hadoop_http_port'").
               replace("hadoop_configs", "'hadoop_configs'") + '}').replace("'", "\"")

    configs_dict = json.loads(configs)
    hadoop_configs = configs_dict['hadoop_configs']
    hadoop_configs_list = hadoop_configs.split(';')

    hadoop_palo_path = configs_dict['hadoop_palo_path']
    fs_default_name = hadoop_configs_list[0].split('=')[1]
    mapred_job_tracker = hadoop_configs_list[1].split('=')[1]
    hadoop_job_ugi = hadoop_configs_list[2].split('=')[1]

    cmd = '%s fs -D mapred.job.tracker=%s -D fs.default.name=%s -D hadoop.job.ugi=%s -mkdir %s' % (
            './output/fe/lib/hadoop-client/hadoop/bin/hadoop',
            mapred_job_tracker, fs_default_name, hadoop_job_ugi, hadoop_palo_path)

    os.system(cmd)


if __name__ == '__main__':
    create_hdfs_dir()

