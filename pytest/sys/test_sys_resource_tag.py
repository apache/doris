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
#   @file test_sys_resource_tag.py
#   @date 2021-11-02 15:18:00
#   @brief This file is a test file for resource tag
#
#############################################################################

"""
resource tag 测试，因修改了be节点标签，会导致默认副本建表失败，需单独运行测试
"""

import sys
import time
sys.path.append("../")
from data import schema
from data import load_file
from lib import palo_config
from lib import palo_client
from lib import palo_job
from lib import util
from lib import common
from lib import node_op
client = None
config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    setUp
    """
    global client
    global host_list
    global port
    global node_operator
    node_operator = node_op.Node()
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    ret = client.get_backend_list()
    port = util.get_attr(ret, palo_job.BackendProcInfo.HeartbeatPort)[0]
    try:
        resource_tag_initialize(node_operator.get_be_ip_list(), port)
    except Exception as e:
        LOG.info(L('init resource tag error', msg=str(e)))
        print(e)
    be_list = util.get_attr_condition_list(ret, palo_job.BackendProcInfo.Alive, 
                                           'false', palo_job.BackendProcInfo.Host)
    if be_list is not None:
        for be_host in be_list:
            node_operator.start_be(be_host)
            assert node_operator.is_be_alive(be_host)
    host_list = client.get_backend_host_ip()


def resource_tag_initialize(host_name_list, port):
    """初始化设置"""
    time.sleep(2)
    be_host_list = client.get_backend_host_ip()
    for host_name in host_name_list:
        if host_name not in be_host_list:
            try:
                client.add_backend(host_name, port)
            except Exception as e:
                LOG.info(L('add backend fail.', backend=host_name, port=port, msg=str(e)))
        tag = client.get_resource_tag(host_name)
        if tag != 'default':
            client.modify_resource_tag(host_name, port, "default")
    client.set_properties("'resource_tags.location'= ''", user='root')
    client.set_variables('exec_mem_limit', '2G', is_global=True)
    time.sleep(2)


def check_resource_tag(host_ip_list, tag_location_list):
    """验证be标签是否设置正确"""
    if not isinstance(host_ip_list, list):
        host_list = [host_ip_list,]
    if not isinstance(tag_location_list, list):
        tag_location_list = [tag_location_list,]
    check_tag_list = []
    for i in range(len(host_list)):
        check_tag_list.append(client.get_resource_tag(host_list[i]))
    assert check_tag_list == tag_location_list, \
           "backend resource tag is incorrect, expect %s, actural %s" % (tag_location_list, check_tag_list)
    

def check_resource_tag_by_id(backend_id_list, tag_location_list):
    """验证be标签是否设置正确"""
    if not isinstance(backend_id_list, list):
        host_name_list = [backend_id_list,]
    if not isinstance(tag_location_list, list):
        tag_location_list = [tag_location_list,]
    check_tag_list = []
    for i in range(len(backend_id_list)):
        check_tag_list.append(client.get_resource_tag_by_id(backend_id_list[i]))
    assert check_tag_list.sort() == tag_location_list.sort(), "backend resource tag is incorrect"


def host_port(host, port):
    """生成格式'host:port'"""
    return host + ":" + port


def create(table_name, replication_allocation=None, replication_num=None, column_list=None, \
            partition_info=None, keys_desc=None):
    """建表"""
    if not column_list:
        column_list = schema.baseall_column_list
    distribution_info = palo_client.DistributionInfo('HASH(k1)', 1)
    client.create_table(table_name, column_list, partition_info, distribution_info, replication_num=replication_num, \
                        keys_desc=keys_desc, replication_allocation=replication_allocation)
    assert client.show_tables(table_name), 'create table failed'


def check_data(table_name):
    """导入查询验证"""
    assert client.stream_load(table_name, load_file.baseall_local_file)
    sql_1 = 'select * from %s order by k1' % table_name
    sql_2 = 'select * from test_query_qa.baseall order by k1'
    util.check(client.execute(sql_1), client.execute(sql_2), True)


def check_data_2(table_name):
    """用于修改标签后的二次导入查询验证"""
    column = ["c1,c2,c3,c4,c5,c6,c10,c11,c7,c8,c9,k1=c1+15,k2=c2,k3=c3,k4=c4,k5=c5,k6=c6,k10=c10,k11=c11,k7=c7,\
                k8=c8,k9=c9"]
    assert client.stream_load(table_name, load_file.baseall_local_file, column_name_list=column)
    sql_1 = 'select * from %s order by k1' % table_name
    sql_2 = 'select * from (select * from test_query_qa.baseall union select k1+15 as k1,k2,k3,k4,k5,k6,k10,k11,k7,\
            k8,k9 from test_query_qa.baseall)a order by k1'
    util.check(client.execute(sql_1), client.execute(sql_2), True)


def wait_replica_transfer(table_name, backend_id):
    """等待副本完成迁移"""
    time_limit = 200
    LOG.info(L('WAIT TRANSFER', backend_id=backend_id))
    client.admin_repair_table(table_name)
    while time_limit > 0:
        new_backend_id = client.get_replica_backend_id(table_name)
        if new_backend_id != backend_id:
            LOG.info(L('TRANSFER RED', backend_id=new_backend_id))
            return True
        time_limit -= 1
        time.sleep(3)
    LOG.info(L('TRANSFER RET', backend_id=new_backend_id))
    LOG.info(L('REPLICA TRANSFER FAIL:TIMEOUT', backend_id=backend_id))
    return False


def test_add_tag():
    """
    {
    "title": "test_sys_resource_tag:test_add_tag",
    "describe": "drop be节点后，add be节点并设置标签和默认标签",
    "tag": "system,p0"
    }
    """
    #add tag
    resource_tag_initialize(host_list, port)
    for host in host_list:
        client.drop_backend_list(host_port(host, port))
        client.add_backend(host, port, "group_a")
        check_resource_tag(host, "group_a")
    #add default tag
    for host in host_list:
        client.drop_backend_list(host_port(host, port))
        client.add_backend(host, port, "default")
        check_resource_tag(host, "default")
    resource_tag_initialize(host_list, port)


def test_add_tag_twice():
    """
    {
    "title": "test_sys_resource_tag:test_add_tag_twice",
    "describe": "drop be节点后，add be节点并设置标签，设置相同标签再次add该节点失败",
    "tag": "system,p1,fuzz"
    }
    """
    resource_tag_initialize(host_list, port)
    host = host_list[0]
    client.drop_backend_list(host_port(host, port))
    client.add_backend(host, port, "group_a")
    check_resource_tag(host, "group_a")
    util.assert_return(False, 'Same backend already exists', client.add_backend, host, port, "group_a")
    resource_tag_initialize(host_list, port)


def test_add_two_tags():
    """
    {
    "title": "test_sys_resource_tag:test_add_two_tags",
    "describe": "drop be节点后，add be节点并设置两个标签，be节点添加成功",
    "tag": "system,p1"
    }
    """
    resource_tag_initialize(host_list, port)
    host = host_list[0]
    client.drop_backend_list(host_port(host, port))
    util.assert_return(False, 'Invalid tag', client.add_backend, host, port, "group_a,group_b")
    resource_tag_initialize(host_list, port)
    resource_tag_initialize(host_list, port)


def test_incorrect_host_port():
    """
    {
    "title": "test_sys_resource_tag:test_incorrect_host_port",
    "describe": "drop be节点后，add be节点并设置标签，设置的be host和port错误，be节点添加失败",
    "tag": "system,p1,fuzz"
    }
    """
    resource_tag_initialize(host_list, port)
    #incorrect host
    host = '0.0.0.0'
    client.add_backend(host, port, "group_a")
    backend_list = client.get_backend_list()
    for backend in backend_list:
        if palo_job.BackendProcInfo(backend).get_ip() == host:
            assert backend[palo_job.BackendProcInfo.Alive] == 'false'
    client.drop_backend_list(host_port(host, port))
    #incorrect port
    incorrect_port = '11000'
    host = host_list[0]
    client.add_backend(host, incorrect_port, "group_a")
    backend_list = client.get_backend_list()
    for backend in backend_list:
        if backend[palo_job.BackendProcInfo.HeartbeatPort] == incorrect_port:
            assert backend[palo_job.BackendProcInfo.Alive] == 'false'
    client.drop_backend_list(host_port(host, incorrect_port))
    resource_tag_initialize(host_list, port)


def test_tag_format():
    """
    {
    "title": "test_sys_resource_tag:test_tag_format",
    "describe": "drop be节点，add be节点并设置标签，验证标签格式，仅支持小写字母、数字和下划线，且必须为小写字母开头",
    "tag": "system,p1,fuzz"
    }
    """
    resource_tag_initialize(host_list, port)
    host = host_list[0]
    #good tag format
    tag_list = ['a', 'a_', 'a1', 'abcd_efgh_abcd_efgh_abcd_efgh_abc', 'aA']
    for tag in tag_list:
        client.drop_backend_list(host_port(host, port))
        client.add_backend(host, port, tag)
        check_resource_tag(host, tag)
    tag_list = ['_a', '1a', 'a*', '', ' ', 'abcd_efgh_abcd_efgh_abcd_efgh_abcd']
    for tag in tag_list:
        util.assert_return(False, 'Invalid tag', client.add_backend, host, port, tag)
    client.drop_backend_list(host_port(host, port))
    client.add_backend(host, port, 'default')
    check_resource_tag(host, 'default')
    client.drop_backend_list(host_port(host, port))


def test_modify_tag():
    """
    {
    "title": "test_sys_resource_tag:test_modify_tag",
    "describe": "修改be节点标签，再修改be节点为默认标签",
    "tag": "system,p0"
    }
    """
    resource_tag_initialize(host_list, port)
    for host in host_list:
        client.modify_resource_tag(host, port, 'group_a')
        check_resource_tag(host, 'group_a')
    #default tag
    for host in host_list:
        client.modify_resource_tag(host, port, 'default')
        check_resource_tag(host, 'default')
    resource_tag_initialize(host_list, port)


def test_modify_tag_format():
    """
    {
    "title": "test_sys_resource_tag:test_modify_tag",
    "describe": "修改be节点标签，验证标签格式，仅支持小写字母、数字和下划线，且必须为小写字母开头",
    "tag": "system,p1,fuzz"
    }   
    """
    resource_tag_initialize(host_list, port)
    host = host_list[0]
    #good tag format
    tag_list = ['a', 'a_', 'a1', 'abcd_efgh_abcd_efgh_abcd_efgh_abc', 'aA']
    for tag in tag_list:
        client.modify_resource_tag(host, port, tag)
        check_resource_tag(host, tag)
    # bad tag format
    tag_list = ['_a', '1a', 'a*', '', ' ', 'abcd_efgh_abcd_efgh_abcd_efgh_abcd']
    for tag in tag_list:
        util.assert_return(False, 'Invalid tag', client.modify_resource_tag, host, port, tag)
    client.modify_resource_tag(host, port, 'default')
    check_resource_tag(host, 'default')


def test_modify_tag_incorrect_host_port():
    """
    {
    "title": "test_sys_resource_tag:test_modify_tag_incorrect_host_port",
    "describe": "修改be节点标签，be节点的host和port配置错误，修改标签失败",
    "tag": "system,p1,fuzz"
    }
    """
    resource_tag_initialize(host_list, port)
    #incorrect host
    host = '0.0.0.0'
    util.assert_return(False, 'backend does not exists', client.modify_resource_tag, host, port, 'group_a')
    #incorrect port
    incorrect_port = '11000'
    host = host_list[0]
    util.assert_return(False, 'backend does not exists', client.modify_resource_tag, host, incorrect_port, 'group_a')
    resource_tag_initialize(host_list, port)


def test_modify_tag_after_drop():
    """
    {
    "title": "test_sys_resource_tag:test_modify_tag_after_drop",
    "describe": "drop be节点后修改be节点标签，修改标签失败",
    "tag": "system,p1,fuzz"
    }
    """
    resource_tag_initialize(host_list, port)
    host = host_list[0]
    client.drop_backend_list(host_port(host, port))
    util.assert_return(False, 'backend does not exists', client.modify_resource_tag, host, port, 'group_a')
    resource_tag_initialize(host_list, port)


def test_modify_same_tag():
    """
    {
    "title": "test_sys_resource_tag:test_modify_same_tag",
    "describe": "修改be节点标签与原标签相同，修改标签成功",
    "tag": "system,p1"
    }
    """
    resource_tag_initialize(host_list, port)
    for host in host_list:
        client.modify_resource_tag(host, port, 'group_a')
        check_resource_tag(host, 'group_a')
        client.modify_resource_tag(host, port, 'group_a')
        check_resource_tag(host, 'group_a')
    resource_tag_initialize(host_list, port)


def test_modify_tag_many_times():
    """
    {
    "title": "test_sys_resource_tag:test_modify_tag_many_times",
    "describe": "多次修改be节点标签，修改标签成功",
    "tag": "system,p1"
    }
    """
    resource_tag_initialize(host_list, port)
    host = host_list[0]
    for i in range(50):
        tag = 'a' + str(i)
        client.modify_resource_tag(host, port, tag)
        check_resource_tag(host, tag)
    resource_tag_initialize(host_list, port)


def test_restart_be_after_modify_tag():
    """
    {
    "title": "test_sys_resource_tag:test_restart_be_after_modify_tag",
    "describe": "修改be节点标签，重启be，be标签状态正确",
    "tag": "system,p1"
    }
    """
    resource_tag_initialize(host_list, port)
    host = host_list[0]
    client.modify_resource_tag(host, port, 'group_a')
    check_resource_tag(host, 'group_a')
    backend_list = client.get_backend_list()
    for backend in backend_list:
        if palo_job.BackendProcInfo(backend).get_ip() == host:
            host_name = backend[palo_job.BackendProcInfo.Host]
    assert node_operator.stop_be(host_name)
    assert node_operator.start_be(host_name)
    time.sleep(30)
    check_resource_tag(host, 'group_a')
    resource_tag_initialize(host_list, port)


def test_restart_be_after_add_tag():
    """
    {
    "title": "test_sys_resource_tag:test_restart_be_after_add_tag",
    "describe": "drop be节点后，add be节点并添加标签，重启be，be标签状态正确",
    "tag": "system,p1"
    }
    """
    resource_tag_initialize(host_list, port)
    host = host_list[0]
    client.drop_backend_list(host_port(host, port))
    client.add_backend(host, port, 'group_a')
    check_resource_tag(host, 'group_a')
    backend_list = client.get_backend_list()
    for backend in backend_list:
        if palo_job.BackendProcInfo(backend).get_ip() == host:
            host_name = backend[palo_job.BackendProcInfo.Host]
    assert node_operator.stop_be(host_name)
    assert node_operator.start_be(host_name)
    time.sleep(30)
    check_resource_tag(host, 'group_a')
    resource_tag_initialize(host_list, port)


def test_create_table_default_tag():
    """
    {
    "title": "test_sys_resource_tag:test_create_table_default_tag",
    "describe": "按照默认be标签建表，设置正确的副本数，建表成功，副本数量和标签正确，数据导入查询正确",
    "tag": "function,p0"
    }
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    replication_allocation = "tag.location.default:3"
    create(table_name, replication_allocation)
    backend_id_list = client.get_replica_backend_id(table_name)
    tag_location_list = ['default'] * 3
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    client.clean(database_name)


def test_create_table_default_tag_false():
    """
    {
    "title": "test_sys_resource_tag:test_create_table_default_tag_false",
    "describe": "按照默认be标签建表，设置错误的副本数，建表失败",
    "tag": "function,p1,fuzz"
    }
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    replication_allocation = "tag.location.default:10"
    util.assert_return(False, 'Failed to find 10 backend(s) for policy', create, table_name, replication_allocation)
    replication_allocation = "tag.location.default:-1"
    util.assert_return(False, 'Total replication num should between 1 and 32767', create, 
                       table_name, replication_allocation)
    replication_allocation = "tag.location.default:"
    util.assert_return(False, 'Invalid replication allocation property', create, table_name, replication_allocation)
    replication_allocation = "tag.location.default:a"
    util.assert_return(False, 'Unexpected exception', create, table_name, replication_allocation)
    client.clean(database_name)


def test_tag_location():
    """
    {
    "title": "test_sys_resource_tag:test_tag_location",
    "describe": "修改be节点标签，建表时设置正确的副本标签，建表成功，表副本标签正确，数据导入查询正确",
    "tag": "function,p0"
    }
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b', 'group_c']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:1,tag.location.group_c:1"
    create(table_name, replication_allocation)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_tag_location_false():
    """
    {
    "title": "test_sys_resource_tag:test_tag_location_false",
    "describe": "修改be节点标签，建表时设置错误的副本标签，建表失败",
    "tag": "function,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_a', 'group_b']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:2"
    util.assert_return(False, 'Failed to find 2 backend(s) for policy', create, table_name, replication_allocation)
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:1,tag.location.group_c:1"
    util.assert_return(False, 'Failed to find 1 backend(s) for policy', create, table_name, replication_allocation)
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:,"
    util.assert_return(False, 'Invalid replication allocation property', create, table_name, replication_allocation)
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:a,"
    util.assert_return(False, 'Unexpected exception: For input string: "a"', create, table_name, \
                    replication_allocation)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_create_modify_tag():
    """
    {
    "title": "test_sys_resource_tag:test_create_modify_tag",
    "describe": "修改be节点标签，建表时设置副本标签，建表成功，再修改be标签，数据导入查询正确",
    "tag": "function,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    host = host_list[0]
    client.modify_resource_tag(host, port, 'group_a')
    check_resource_tag(host, 'group_a')
    replication_allocation = "tag.location.group_a:1"
    #建表，设置副本标签
    create(table_name, replication_allocation)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, 'group_a')
    check_data(table_name)
    #修改BE标签
    client.modify_resource_tag(host, port, 'group_b')
    check_resource_tag(host, 'group_b')
    check_resource_tag_by_id(backend_id_list, 'group_b')
    check_data_2(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_replication_num_and_allocation():
    """
    {
    "title": "test_sys_resource_tag:test_replication_num_and_allocation",
    "describe": "修改be节点标签，建表时设置副本标签，同时指定replication_num和replication_allocation，\
                仅replication_num参数生效，数据导入查询正确",
    "tag": "function,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    host = host_list[0]
    client.modify_resource_tag(host, port, 'group_a')
    check_resource_tag(host, 'group_a')
    replication_allocation = "tag.location.group_a:1"
    create(table_name, replication_allocation, '2')
    backend_id_list = client.get_replica_backend_id(table_name)
    tag_location_list = ['default', 'default']
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_tag_aggregate():
    """
    {
    "title": "test_sys_resource_tag:test_tag_aggregate",
    "describe": "修改be节点标签，创建aggregate表，设置副本标签，建表成功，数据导入查询正确",
    "tag": "function,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b', 'group_c']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:1,tag.location.group_c:1"
    keys_desc = "aggregate key(k1,k2,k3,k4,k5,k6,k10,k11,k7)"
    create(table_name, replication_allocation, keys_desc=keys_desc)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_tag_unique():
    """
    {
    "title": "test_sys_resource_tag:test_tag_unique",
    "describe": "修改be节点标签，创建unique表，设置副本标签，建表成功，数据导入查询正确",
    "tag": "function,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b', 'group_c']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:1,tag.location.group_c:1"
    keys_desc = "unique key(k1)"
    create(table_name, replication_allocation, column_list=schema.baseall_column_no_agg_list, keys_desc=keys_desc)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_tag_duplicate():
    """
    {
    "title": "test_sys_resource_tag:test_tag_duplicate",
    "describe": "修改be节点标签，创建duplicate表，设置副本标签，建表成功，数据导入查询正确",
    "tag": "function,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b', 'group_c']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:1,tag.location.group_c:1"
    keys_desc = "duplicate key(k1)"
    create(table_name, replication_allocation, column_list=schema.baseall_column_no_agg_list, keys_desc=keys_desc)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_tag_range_partition():
    """
    {
    "title": "test_sys_resource_tag:test_tag_range_partition",
    "describe": "修改be节点标签，创建range分区表，设置副本标签，建表成功，数据导入查询正确",
    "tag": "function,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b', 'group_c']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    partition_name_list = ['partiiton_a', 'partition_b', 'partition_c']
    partition_value_list = ['10', '20', '40']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:1,tag.location.group_c:1"
    create(table_name, replication_allocation, partition_info=partition_info)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_tag_list_partition():
    """
    {
    "title": "test_sys_resource_tag:test_tag_list_partition",
    "describe": "修改be节点标签，创建list分区表，设置副本标签，建表成功，数据导入查询正确",
    "tag": "function,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b', 'group_c']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    partition_name_list = ['partiiton_a', 'partition_b']
    partition_value_list = [('true'), ('false')]
    partition_info = palo_client.PartitionInfo('k6', partition_name_list, partition_value_list, \
                    partition_type='LIST')
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:1,tag.location.group_c:1"
    create(table_name, replication_allocation, partition_info=partition_info)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_tag_composite_partition():
    """
    {
    "title": "test_sys_resource_tag:test_tag_composite_partition",
    "describe": "修改be节点标签，创建复合分区表，设置副本标签，建表成功，数据导入查询正确",
    "tag": "function,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b', 'group_c']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    partition_name_list = ['partiiton_a', 'partition_b', 'partition_c']
    partition_value_list = [('10', '1000'), ('20', '10000'), ('30', '32767')]
    partition_info = palo_client.PartitionInfo(['k1', 'k2'], partition_name_list, partition_value_list)
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:1,tag.location.group_c:1"
    create(table_name, replication_allocation, partition_info=partition_info)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_alter_partition_tag():
    """
    {
    "title": "test_sys_resource_tag:test_alter_partition_tag",
    "describe": "修改be节点标签，创建分区表，设置副本标签建表成功，修改分区表的副本配置正确，数据导入查询正确",
    "tag": "function,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_a', 'group_b']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    partition_name_list = ['partiiton_a', 'partition_b', 'partition_c']
    partition_value_list = ['5', '15', '35']
    partition_info = palo_client.PartitionInfo('k1', partition_name_list, partition_value_list)
    replication_allocation = "tag.location.group_a:1,tag.location.group_b:1"
    create(table_name, replication_allocation, partition_info=partition_info)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, ['group_a', 'group_b'])
    check_data(table_name)
    client.modify_partition(table_name, 'partition_b', replication_allocation='tag.location.group_a:2')
    assert client.get_partition_replica_allocation(table_name, 'partition_b') == 'tag.location.group_a: 2', \
            "partition replica allocation false"
    check_data_2(table_name)
    #副本数量设置错误
    util.assert_return(False, 'Failed to find enough host', client.modify_partition, table_name, 'partition_b', \
                        replication_allocation='tag.location.group_a:3')
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_create_be_restart():
    """
    {
    "title": "test_sys_resource_tag:test_create_be_restart",
    "describe": "修改be节点标签，建表时设置副本标签，建表成功，重启be，数据导入查询正确",
    "tag": "system,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_a', 'group_b']
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:2, tag.location.group_b:1"
    create(table_name, replication_allocation)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    #重启BE
    backend_list = client.get_backend_list()
    for backend in backend_list:
        if palo_job.BackendProcInfo(backend).get_ip() in host_list:
            host_name = backend[palo_job.BackendProcInfo.Host]
            assert node_operator.stop_be(host_name)
            assert node_operator.start_be(host_name)
    time.sleep(20)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data_2(table_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_replica_balance():
    """
    {
    "title": "test_sys_resource_tag:test_replica_balance",
    "describe": "修改be节点标签，设置副本标签创建30个表，验证表副本均匀分布在各个be",
    "tag": "system,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_a', 'group_a']
    be_id_list = []
    backend_list = client.get_backend_list()
    for backend in backend_list:
        if palo_job.BackendProcInfo(backend).get_ip() in host_list[:3]:
            be_id = backend[palo_job.BackendProcInfo.BackendId]
            if be_id not in be_id_list:
                be_id_list.append(be_id)
    table_nums = [0, 0, 0]
    for i in range(3):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:1"
    for i in range(60):
        table_name_s = '%s_%d' % (table_name, i)
        create(table_name_s, replication_allocation)
        backend_id_list = client.get_replica_backend_id(table_name_s)
        for i in range(3):
            if backend_id_list[0] == be_id_list[i]:
                table_nums[i] += 1
    for i in range(3):
        assert table_nums[i] >= 10 and table_nums[i] <= 30, "replica not balance"
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_rollup_tag():
    """
    {
    "title": "test_sys_resource_tag:test_tag_aggregate",
    "describe": "修改be节点标签，设置副本标签建表，建表成功，创建rollup表，验证rollup表副本标签正确",
    "tag": "function,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b']
    for i in range(2):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:1, tag.location.group_b:1"
    create(table_name, replication_allocation)
    backend_id_list = client.get_replica_backend_id(table_name)
    check_resource_tag_by_id(backend_id_list, tag_location_list)
    check_data(table_name)
    #创建rollup表
    rollup_name = table_name + '_r'
    client.create_rollup_table(table_name, rollup_name, ['k1', 'k2'], is_wait=True)
    backend_id_list = client.get_replica_backend_id(table_name)
    if backend_id_list[1] == backend_id_list[0]:
        assert backend_id_list[2] == backend_id_list[3]
    else:
        assert backend_id_list[2:].sort() == backend_id_list[:2].sort(), 'rollup table replica false'
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_usr_priv_tag():
    """
    {
    "title": "test_sys_resource_tag:test_usr_priv_tag",
    "describe": "修改be节点标签，建表，创建用户并设置用户资源权限，该用户查询不同标签表，验证资源权限设置正确",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b']
    for i in range(2):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation_1 = "tag.location.group_a:1"
    create(table_name, replication_allocation_1)
    replication_allocation_2 = "tag.location.group_b:1"
    check_data(table_name)
    table_name_2 = table_name + '_2'
    create(table_name_2, replication_allocation_2)
    check_data(table_name_2)
    #create user
    user_name = 'test_resource_tag'
    client.clean_user(user_name)
    client.create_user(user_name)
    privilege_list = ['SELECT_PRIV']
    client.grant(user_name, privilege_list, database_name)
    kv = "'resource_tags.location'= 'group_a'"
    client.set_properties(kv, user=user_name)
    client_tag = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name, password='', \
                http_port=config.fe_http_port)
    #对有权限的表查询验证
    client_tag.use(database_name)
    sql = "select * from %s order by k1" % table_name
    ret1 = client_tag.execute(sql)
    ret2 = client.execute('select * from test_query_qa.baseall order by k1')
    util.check(ret1, ret2)
    #对无权限的表查询验证
    sql = "select * from %s order by k1" % table_name_2
    util.assert_return(False, 'have no queryable replicas', client_tag.execute, sql)
    client.clean_user(user_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_user_set_property():
    """
    {
    "title": "test_sys_resource_tag:test_usr_set_property",
    "describe": "修改be节点标签，创建两个普通用户，使用其中一个用户对另一个设置用户资源权限失败",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    client.modify_resource_tag(host_list[0], port, 'group_a')
    check_resource_tag(host_list[0], 'group_a')
    user_name = 'test_resource_tag'
    client.clean_user(user_name)
    client.create_user(user_name)
    user_name_set = 'test_resource_tag_set'
    client.clean_user(user_name_set)
    client.create_user(user_name_set)
    client_tag = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name_set, password='', \
                http_port=config.fe_http_port)
    kv = "'resource_tags.location'= 'group_a'"
    util.assert_return(False, 'Access denied', client_tag.set_properties, kv, user=user_name)
    client.clean_user(user_name)
    client.clean_user(user_name_set)
    resource_tag_initialize(host_list, port)


def test_admin_user_set_property():
    """
    {
    "title": "test_sys_resource_tag:test_admin_user_set_property",
    "describe": "修改be节点标签，建表，创建普通用户和admin用户，使用admin用户对普通用户设置资源权限，验证权限设置正确",
    "tag": "system,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    #设置副本标签，建表
    tag_location_list = ['group_a', 'group_b']
    for i in range(2):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation_1 = "tag.location.group_a:1"
    create(table_name, replication_allocation_1)
    replication_allocation_2 = "tag.location.group_b:1"
    check_data(table_name)
    table_name_2 = table_name + '_2'
    create(table_name_2, replication_allocation_2)
    check_data(table_name_2)
    #设置admin用户和添加权限用户
    user_name = 'test_resource_tag'
    client.clean_user(user_name)
    client.create_user(user_name)
    privilege_list = ['SELECT_PRIV']
    client.grant(user_name, privilege_list, database_name)
    
    user_name_set = 'test_resource_tag_set'
    client.clean_user(user_name_set)
    client.create_user(user_name_set)
    privilege_list = ['ADMIN_PRIV']
    client.grant(user_name_set, privilege_list, database='*', table='*', catalog='*')
    #添加用户资源权限
    client_tag_set = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name_set, password='', \
                http_port=config.fe_http_port)
    kv = "'resource_tags.location'= 'group_a'"
    client_tag_set.set_properties(kv, user=user_name)
    #对有权限的表查询验证
    client_tag = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name, password='', \
                http_port=config.fe_http_port)
    client_tag.use(database_name)
    sql = "select * from %s order by k1" % table_name
    ret1 = client_tag.execute(sql)
    ret2 = client.execute('select * from test_query_qa.baseall order by k1')
    util.check(ret1, ret2)
    #对无权限的表查询验证
    sql = "select * from %s order by k1" % table_name_2
    util.assert_return(False, 'have no queryable replicas', client_tag.execute, sql)
    #清理用户和测试数据库，标签初始化
    client.clean_user(user_name)
    client.clean_user(user_name_set)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_set_property_for_root():
    """
    {
    "title": "test_sys_resource_tag:test_set_property_for_root",
    "describe": "修改be节点标签，建表，创建admin用户，使用admin用户对root用户设置资源权限，验证资源权限设置正确",
    "tag": "system,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    #设置副本标签，建表
    tag_location_list = ['group_a', 'group_b']
    for i in range(2):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation_1 = "tag.location.group_a:1"
    create(table_name, replication_allocation_1)
    replication_allocation_2 = "tag.location.group_b:1"
    check_data(table_name)
    table_name_2 = table_name + '_2'
    create(table_name_2, replication_allocation_2)
    check_data(table_name_2)
    #设置admin用户
    user_name_set = 'test_resource_tag_set'
    client.clean_user(user_name_set)
    client.create_user(user_name_set)
    privilege_list = ['ADMIN_PRIV']
    client.grant(user_name_set, privilege_list, database='*', table='*', catalog='*')
    #添加root用户资源权限
    client_tag_set = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name_set, \
                password='', http_port=config.fe_http_port)
    kv = "'resource_tags.location'= 'group_a'"
    client_tag_set.set_properties(kv, user='root')
    #对有权限的表查询验证
    client_root = palo_client.get_client(config.fe_host, config.fe_query_port, user='root', \
                password=config.fe_password, http_port=config.fe_http_port)
    client_root.use(database_name)
    sql = "select * from %s order by k1" % table_name
    ret1 = client_root.execute(sql)
    ret2 = client.execute('select * from test_query_qa.baseall order by k1')
    util.check(ret1, ret2)
    #对无权限的表查询验证
    sql = "select * from %s order by k1" % table_name_2
    util.assert_return(False, 'have no queryable replicas', client_root.execute, sql)
    #清理用户和测试数据库，标签初始化
    client.clean_user(user_name_set)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_set_property_for_role():
    """
    {
    "title": "test_sys_resource_tag:test_usr_priv_tag",
    "describe": "修改be节点标签，建表，创建role并设置用户资源权限，设置用户资源权限失败",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    client.modify_resource_tag(host_list[0], port, 'group_a')
    check_resource_tag(host_list[0], 'group_a')
    role_name = 'test_resource_tag_role'
    try:
        client.drop_role(role_name)
    except:
        pass
    client.create_role(role_name)
    kv = "'resource_tags.location'= 'group_a'"
    util.assert_return(False, 'Unknown user', client.set_properties, kv, role_name)
    client.drop_role(role_name)
    resource_tag_initialize(host_list, port)


def test_set_property_for_role_user():
    """
    {
    "title": "test_sys_resource_tag:test_set_property_for_role_user",
    "describe": "修改be节点标签，建表，创建用户并指定role，设置用户资源权限，验证资源权限设置正确",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b']
    for i in range(2):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation_1 = "tag.location.group_a:1"
    create(table_name, replication_allocation_1)
    replication_allocation_2 = "tag.location.group_b:1"
    check_data(table_name)
    table_name_2 = table_name + '_2'
    create(table_name_2, replication_allocation_2)
    check_data(table_name_2)
    #create role
    role_name = 'test_resource_tag_role'
    try:
        client.drop_role(role_name)
    except:
        pass
    client.create_role(role_name)
    privilege_list = ['SELECT_PRIV']
    client.grant(role_name, privilege_list, database_name, is_role=True)
    #create user
    user_name = 'test_resource_tag'
    client.clean_user(user_name)
    client.create_user(user_name, default_role=role_name)
    kv =  "'resource_tags.location'= 'group_a'"
    client.set_properties(kv, user_name)
    client_tag = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name, password='', \
                http_port=config.fe_http_port)
    #对有权限的表查询验证
    client_tag.use(database_name)
    sql = "select * from %s order by k1" % table_name
    ret1 = client_tag.execute(sql)
    ret2 = client.execute('select * from test_query_qa.baseall order by k1')
    util.check(ret1, ret2)
    #对无权限的表查询验证
    sql = "select * from %s order by k1" % table_name_2
    util.assert_return(False, 'have no queryable replicas', client_tag.execute, sql)
    client.clean_user(user_name)
    client.drop_role(role_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_set_property_empty():
    """
    {
    "title": "test_sys_resource_tag:test_set_property_empty",
    "describe": "修改be节点标签，建表，创建用户并设置用户资源权限为空值，验证用户具有所有资源权限",
    "tag": "system,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b']
    for i in range(2):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation_1 = "tag.location.group_a:1"
    create(table_name, replication_allocation_1)
    replication_allocation_2 = "tag.location.group_b:1"
    check_data(table_name)
    table_name_2 = table_name + '_2'
    create(table_name_2, replication_allocation_2)
    check_data(table_name_2)
    #create user
    user_name = 'test_resource_tag'
    client.clean_user(user_name)
    client.create_user(user_name)
    privilege_list = ['SELECT_PRIV']
    client.grant(user_name, privilege_list, database_name)
    kv =  "'resource_tags.location'= ''"
    client.set_properties(kv, user_name)
    client_tag = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name, password='', \
                http_port=config.fe_http_port)
    #用户具有所有标签表的权限
    client_tag.use(database_name)
    sql = "select * from %s order by k1" % table_name
    ret1 = client_tag.execute(sql)
    ret2 = client.execute('select * from test_query_qa.baseall order by k1')
    util.check(ret1, ret2)
    sql = "select * from %s order by k1" % table_name_2
    ret1 = client_tag.execute(sql)
    util.check(ret1, ret2)
    client.clean_user(user_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_set_property_and_alter_table_tag():
    """
    {
    "title": "test_sys_resource_tag:test_set_property_and_alter_table_tag",
    "describe": "修改be节点标签，建表，创建用户并设置用户资源权限，修改表的副本标签使用户对表有权限，验证权限正确",
    "tag": "system,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b']
    for i in range(2):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:1"
    create(table_name, replication_allocation)
    check_data(table_name)
    #create user
    user_name = 'test_resource_tag'
    client.clean_user(user_name)
    client.create_user(user_name)
    privilege_list = ['SELECT_PRIV']
    client.grant(user_name, privilege_list, database_name)
    kv =  "'resource_tags.location'= 'group_b'"
    client.set_properties(kv, user_name)
    client_tag = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name, password='', \
                http_port=config.fe_http_port)
    #用户对该表无权限
    client_tag.use(database_name)
    sql = "select * from %s order by k1" % table_name
    util.assert_return(False, 'have no queryable replicas', client_tag.execute, sql)
    #修改表的副本标签
    backend_id = client.get_replica_backend_id(table_name)
    client.schema_change(table_name, replication_allocation="tag.location.group_b:1")
    ret = wait_replica_transfer(table_name, backend_id)
    assert ret == True, 'REPLICA TRANSFER FAIL'
    #用户具有该表权限
    client_tag.use(database_name)
    ret1 = client_tag.execute(sql)
    ret2 = client.execute('select * from test_query_qa.baseall order by k1')
    util.check(ret1, ret2)
    client.clean_user(user_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_set_property_and_alter_table_tag_no_property():
    """
    {
    "title": "test_sys_resource_tag:test_set_property_and_alter_table_tag_no_property",
    "describe": "修改be节点标签，建表，创建用户并设置用户资源权限，修改表副本标签使用户对表无权限，验证权限设置正确",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b']
    for i in range(2):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation = "tag.location.group_a:1"
    create(table_name, replication_allocation)
    check_data(table_name)
    #create user
    user_name = 'test_resource_tag'
    client.clean_user(user_name)
    client.create_user(user_name)
    privilege_list = ['SELECT_PRIV']
    client.grant(user_name, privilege_list, database_name)
    kv =  "'resource_tags.location'= 'group_a'"
    client.set_properties(kv, user_name)
    client_tag = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name, password='', \
                http_port=config.fe_http_port)
    #用户对该表有权限
    client_tag.use(database_name)
    sql = "select * from %s order by k1" % table_name
    ret1 = client_tag.execute(sql)
    ret2 = client.execute('select * from test_query_qa.baseall order by k1')
    util.check(ret1, ret2)
    #修改表的副本标签
    backend_id = client.get_replica_backend_id(table_name)
    client.schema_change(table_name, replication_allocation="tag.location.group_b:1")
    ret = wait_replica_transfer(table_name, backend_id)
    assert ret == True, 'REPLICA TRANSFER FAIL'
    check_data_2(table_name)
    #bug,用户仍具有该表权限
    client_tag.use(database_name)
    ret1 = client_tag.execute(sql)
    ret2 = client.execute('select * from (select * from test_query_qa.baseall union select k1+15 as \
            k1,k2,k3,k4,k5,k6,k10,k11,k7,k8,k9 from test_query_qa.baseall)a order by k1')
    util.check(ret1, ret2)
    #util.assert_return(False, 'have no queryable replicas', client_tag.execute, sql)
    client.clean_user(user_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_alter_user_property():
    """
    {
    "title": "test_sys_resource_tag:test_alter_user_property",
    "describe": "修改be节点标签，建表，创建用户并设置用户资源权限，修改该用户资源权限，验证资源权限修改正确",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, table_name=table_name, index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    tag_location_list = ['group_a', 'group_b']
    for i in range(2):
        client.modify_resource_tag(host_list[i], port, tag_location_list[i])
        check_resource_tag(host_list[i], tag_location_list[i])
    replication_allocation_1 = "tag.location.group_a:1"
    create(table_name, replication_allocation_1)
    replication_allocation_2 = "tag.location.group_b:1"
    check_data(table_name)
    table_name_2 = table_name + '_2'
    create(table_name_2, replication_allocation_2)
    check_data(table_name_2)
    #create user
    user_name = 'test_resource_tag'
    client.clean_user(user_name)
    client.create_user(user_name)
    privilege_list = ['SELECT_PRIV']
    client.grant(user_name, privilege_list, database_name)
    kv =  "'resource_tags.location'= 'group_a'"
    client.set_properties(kv, user_name)
    client_tag = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name, password='', \
                http_port=config.fe_http_port)
    #验证
    client_tag.use(database_name)
    sql = "select * from %s order by k1" % table_name
    ret1 = client_tag.execute(sql)
    ret2 = client.execute('select * from test_query_qa.baseall order by k1')
    util.check(ret1, ret2)
    sql = "select * from %s order by k1" % table_name_2
    util.assert_return(False, 'have no queryable replicas', client_tag.execute, sql)
    #修改user权限
    kv =  "'resource_tags.location'= 'group_b'"
    client.set_properties(kv, user_name)
    client_tag = palo_client.get_client(config.fe_host, config.fe_query_port, user=user_name, password='', \
                http_port=config.fe_http_port)
    #验证
    client_tag.use(database_name)
    sql = "select * from %s order by k1" % table_name_2
    ret1 = client_tag.execute(sql)
    util.check(ret1, ret2)
    sql = "select * from %s order by k1" % table_name
    util.assert_return(False, 'have no queryable replicas', client_tag.execute, sql)
    client.clean_user(user_name)
    client.clean(database_name)
    resource_tag_initialize(host_list, port)


def test_set_exec_mem_limit():
    """
    {
    "title": "test_sys_resource_tag:test_set_exec_mem_limit",
    "describe": "设置exec_mem_limit，验证设置正确，对查询语句限制有效，仅在该连接内生效，仅支持正值",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    prefix = 'exec_mem_limit'
    client.set_variables(prefix, '1G')
    assert client.show_variables(prefix)[0][1] == '1073741824'
    client.set_variables(prefix, '1234567890')
    assert client.show_variables(prefix)[0][1] == '1234567890'
    client_new = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    print(client_new.show_variables(prefix)[0][1])
    assert client_new.show_variables(prefix)[0][1] == '2147483648'
    util.assert_return(False, 'invalid data volumn expression', client.set_variables, prefix, '-2147483648')
    util.assert_return(False, 'Data volumn must larger than 0', client.set_variables, prefix, '0')
    util.assert_return(False, 'invalid data volumn expression', client.set_variables, prefix, 'a')
    client.set_variables(prefix, '65536')
    sql = 'select * from test_query_qa.test order by k1 limit 1'
    util.assert_return(False, 'Memory limit exceeded', client.execute, sql)
    # util.assert_return(False, 'failed mem consume', client.execute, sql)
    client.set_variables(prefix, '2G')


def test_set_global_exec_mem_limit():
    """
    {
    "title": "test_sys_resource_tag:test_set_global_exec_mem_limit",
    "describe": "设置golbal exec_mem_limit，验证设置正确，对查询语句限制有效，全局生效，仅支持正值",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    prefix = 'exec_mem_limit'
    client.set_variables(prefix, '1G', is_global=True)
    client_new = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client_new.show_variables(prefix)[0][1] == '1073741824'
    util.assert_return(False, 'invalid data volumn expression', client_new.set_variables, prefix, '-2147483648', \
                is_global=True)
    client.set_variables(prefix, '65536', is_global=True)
    sql = 'select * from test_query_qa.test order by k1 limit 1'
    client_new = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    util.assert_return(False, 'Memory limit exceeded', client_new.execute, sql)
    # util.assert_return(False, 'failed mem consume', client_new.execute, sql)
    client.set_variables(prefix, '2G', is_global=True)


def test_set_sql_exec_mem_limit():
    """
    {
    "title": "test_sys_resource_tag:test_set_sql_exec_mem_limit",
    "describe": "查询语句中设置exec_mem_limit，验证设置正确，对查询语句限制有效，仅对该语句有效，仅支持正值",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    prefix = 'exec_mem_limit'
    client.set_variables(prefix, '2G')
    sql_1 = 'select /*+ SET_VAR(exec_mem_limit=1073741824) */ * from test_query_qa.test order by k1 limit 1'
    sql_2 = 'select * from test_query_qa.test order by k1 limit 1'
    common.check2(client, sql_1, sql2=sql_2)
    sql = 'select /*+ SET_VAR(exec_mem_limit=65536) */ * from test_query_qa.test order by k1 limit 1'
    util.assert_return(False, 'Memory limit exceeded', client.execute, sql)
    sql = 'select /*+ SET_VAR(exec_mem_limit=-2147483648) */ * from test_query_qa.test order by k1 limit 1'
    util.assert_return(False, 'Syntax error', client.execute, sql)
    client.set_variables(prefix, '2G')


def test_set_cpu_resource_limit():
    """
    {
    "title": "test_sys_resource_tag:test_set_cpu_resource_limit",
    "describe": "设置cpu_resource_limit，验证设置正确，仅支持正值和默认值-1",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    prefix = 'cpu_resource_limit'
    client.set_variables(prefix, '2')
    assert client.show_variables(prefix)[0][1] == '2'
    client.set_variables(prefix, '100')
    assert client.show_variables(prefix)[0][1] == '100'
    client.set_variables(prefix, '-1')
    assert client.show_variables(prefix)[0][1] == '-1'
    #bug待修复，参数不能设置为-1以外的负值
    util.assert_return(True, '', client.set_variables, prefix, '-100')
    #util.assert_return(False, 'Data volumn must larger than 0', client.set_variables, prefix, '-100')
    util.assert_return(False, 'Incorrect argument type', client.set_variables, prefix, 'a')


def test_set_cpu_resource_limit_user():
    """
    {
    "title": "test_sys_resource_tag:test_set_cpu_resource_limit_user",
    "describe": "创建用户，对用户设置cpu_resource_limit，验证设置正确，对该用户有效，仅支持正值和默认值-1",
    "tag": "system,p1,fuzz"
    } 
    """
    resource_tag_initialize(host_list, port)
    prefix = 'cpu_resource_limit'
    client.set_variables(prefix, '-1')
    assert client.show_variables(prefix)[0][1] == '-1'
    user_name = 'test_set_cpu_resource_limit'
    client.clean_user(user_name)
    client.create_user(user_name)
    kv = "'cpu_resource_limit' = '2'"
    client.set_properties(kv, user_name)
    assert client.show_property('cpu_resource_limit', user_name)[0][1] == '2', 'set user cpu_resource_limit false'
    kv = "'cpu_resource_limit' = '-1'"
    client.set_properties(kv, user_name)
    assert client.show_property('cpu_resource_limit', user_name)[0][1] == '-1', 'set user cpu_resource_limit false'

    kv = "'cpu_resource_limit' = 'a'"
    util.assert_return(False, "cpu_resource_limit is not number", client.set_properties, kv, user_name)
    kv = "'cpu_resource_limit' = '-100'"
    util.assert_return(False, "cpu_resource_limit is not valid", client.set_properties, kv, user_name)


def test_sql_exec_mem_limit():
    """
    {
    "title": "test_sys_resource_tag:test_sql_exec_mem_limit",
    "describe": "查询语句设置exec_mem_limit的语法测试，验证支持各种select语法和窗口函数",
    "tag": "system,p1"
    } 
    """
    resource_tag_initialize(host_list, port)
    prefix = 'exec_mem_limit'
    client.set_variables(prefix, '2G')
    sql_1 = 'select /*+ SET_VAR(exec_mem_limit=1073741824) */ k1,k2,k3,k7 from test_query_qa.test order by k2,k3 \
            limit 100'
    sql_2 = 'select k1,k2,k3,k7 from test_query_qa.test order by k2,k3 limit 100'
    util.check(client.execute(sql_1), client.execute(sql_2))
    sql_1 = 'select /*+ SET_VAR(exec_mem_limit=1073741824) */ * from test_query_qa.test where k3 > 0 and k2 < 0 \
            order by k2,k3,k4 desc'
    sql_2 = 'select * from test_query_qa.test where k3 > 0 and k2 < 0 order by k2,k3,k4 desc'
    util.check(client.execute(sql_1), client.execute(sql_2))
    sql_1 = 'select /*+ SET_VAR(exec_mem_limit=1073741824) */ count(*), max(k3) from test_query_qa.test group by k1 \
            order by k1'
    sql_2 = 'select count(*), max(k3) from test_query_qa.test group by k1 order by k1'
    util.check(client.execute(sql_1), client.execute(sql_2))
    sql_1 = 'select /*+ SET_VAR(exec_mem_limit=1073741824) */ * from test_query_qa.test t left join \
            test_query_qa.baseall b on t.k1 = b.k1 order by t.k2,t.k3,t.k4 desc'
    sql_2 = 'select * from test_query_qa.test t left join test_query_qa.baseall b on t.k1 = b.k1 \
            order by t.k2,t.k3,t.k4 desc'
    util.check(client.execute(sql_1), client.execute(sql_2))
    sql_1 = 'select /*+ SET_VAR(exec_mem_limit=1073741824) */ a.k1,a.k2 from (select * from test_query_qa.test \
            order by k2,k3) a order by a.k1,a.k2'
    sql_2 = 'select a.k1,a.k2 from (select * from test_query_qa.test order by k2,k3) a order by a.k1,a.k2'
    util.check(client.execute(sql_1), client.execute(sql_2))


def teardown_module():
    """
    tearDown
    """
    resource_tag_initialize(host_list, port)


