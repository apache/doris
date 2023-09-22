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
Date:    2015/11/17 17:23:06
"""

from lib import palo_client
from lib import util
from lib import palo_config

LOG = palo_client.LOG
L = palo_client.L
file_path_1 = palo_config.gen_hdfs_file_path('sys/partition/partition_type')
config = palo_config.config


def setup_module():
    """
    setUp
    """
    pass


def test_set_max_user_connections():
    """
    {
    "title": "test_sys_user_property.test_set_max_user_connections",
    "describe": "set max user connections",
    "tag": "function,p1,fuzz"
    }
    """
    """set max user connections
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    user = 'jack'
    LOG.info(L('', database_name=database_name,
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    assert client
    try:
        client.drop_user(user)
    except:
        pass
    ret = False
    try:
        ret = client.set_max_user_connections(10, user)
    except:
        pass
    assert not ret
    assert client.create_user(user, is_superuser=False)
    assert client.set_max_user_connections(10, user)
    assert client.show_max_user_connections(user) == 10
    client_list = []
    for i in range(0, 10):
        client = palo_client.get_client(config.fe_host, config.fe_query_port, user=user, password='')
        assert client
        client_list.append(client)
    flag = True
    try:
        client = palo_client.get_client(config.fe_host, config.fe_query_port,
                                        user=user, password='', retry=False)
        flag = False
        # assert not client
    except Exception as e:
        print('expect: %s' % str(e))
        ret = client_list[0].execute('show processlist')
        print(ret)
    assert flag, 'expect connect palo error'


def test_normal_set_max_user_connections():
    """
    {
    "title": "test_sys_user_property.test_normal_set_max_user_connections",
    "describe": "普通用户设置最大连接数，验证报错",
    "tag": "function,p1,fuzz"
    }
    """
    """
    普通用户设置最大连接数，验证报错
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    user = 'jack'
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    assert client
    try:
        client.drop_user(user)
    except:
        pass
    ret = False
    assert client.create_user(user, is_superuser=False)
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=user, password='')
    assert client
    ret = False
    try:
        ret = client.set_max_user_connections(10, user)
    except:
        pass
    assert not ret


def test_set_resource_cpu_share():
    """
    {
    "title": "ttest_sys_user_property.est_set_resource_cpu_share",
    "describe": "普通用户设置cpu_share，验证报错",
    "tag": "function,p1,fuzz"
    }
    """
    """
    普通用户设置cpu_share，验证报错
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    user = 'jack'
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    assert client
    try:
        client.drop_user(user)
    except:
        pass
    ret = False
    assert client.create_user(user, is_superuser=False)
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=user, password='')
    assert client
    ret = False
    try:
        ret = client.set_resource_cpu_share(1000, user)
    except:
        pass
    assert not ret


def test_set_quota():
    """
    {
    "title": "test_sys_user_property.test_set_quota",
    "describe": "普通用户为其他普通用户设置quota属性，验证报错",
    "tag": "function,p1,fuzz"
    }
    """
    """
    普通用户为其他普通用户设置quota属性，验证报错
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    user_1 = database_name
    user_2 = 'jack'
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    assert client
    try:
        client.drop_user(user_1)
    except:
        pass
    try:
        client.drop_user(user_2)
    except:
        pass
    assert client.create_user(user_1, is_superuser=False)
    assert client.create_user(user_2, is_superuser=False)
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_1, password='')
    assert client
    ret = False
    try:
        ret = client.set_quota_high(500, user_2)
    except:
        pass
    assert not ret


def test_super_set_max_user_connections():
    """
    {
    "title": "test_sys_user_property.test_super_set_max_user_connections",
    "describe": "超级用户为其他超级用户设置最大连接数",
    "tag": "function,p1,fuzz"
    }
    """
    """
    超级用户为其他超级用户设置最大连接数
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    user = 'jack'
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password)
    assert client
    try:
        client.drop_user(user)
    except:
        pass
    ret = False
    assert client.create_user(user, is_superuser=True)
    assert client.set_max_user_connections(10, user)


