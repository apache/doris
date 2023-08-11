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

############################################################################
#
#   @file test_sys_show.py
#   @date 2021/09/14 14:29:00
#   @brief This file is a test file for show function.
#
#############################################################################

"""
test for show function
"""

import sys
import random
sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import util
from data import partition as DATA

client = None
config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)


def test_show_data_skew():
    """
    {
    "title": "test_sys_show.test_show_data_skew",
    "describe": "show data skew from table_name partition(partition_name), github issue 6219",
    "tag": "function,p1"
    }
    """    
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = [10, 20, 30, 40]
    random_bucket_num = random.randrange(1, 20)
    LOG.info(L('', random_bucket_num=random_bucket_num))
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('RANDOM', random_bucket_num)
    client.create_table(table_name, DATA.schema_1, partition_info, distribution_info)
    assert client.show_tables(table_name)
    for partition_name in partition_name_list:
        sql = "SHOW DATA SKEW FROM %s PARTITION(%s)" % (table_name, partition_name)
        ret = client.execute(sql)
        assert len(ret) == random_bucket_num, "show data skew info false"
        sql = "ADMIN SHOW REPLICA DISTRIBUTION FROM %s PARTITION(%s)" % (table_name, partition_name)
        ret = client.execute(sql)
        assert len(ret[0]) == 7, "show replica distribution info false"
        assert ret[0][2] is not None, "No replicasize"
        bucket_num = 0
        for i in ret:
            bucket_num += int(i[1])
        assert bucket_num == random_bucket_num * 3, "replica num false"
    client.clean(database_name)
