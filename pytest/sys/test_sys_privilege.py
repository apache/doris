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
test user privilege on palo
Date: 2015/08/10 11:07:32
"""
from data import privilege as DATA
from data import schema
from lib import palo_client
from lib import palo_config
from lib import util 
from lib import palo_task
from lib import palo_job

config = palo_config.config
root_client = None
super_client = None
user_client = None
backend_list = ["be_fake:9850"]


def setup_module():
    """
    set up
    """
    global root_client, super_client, user_client
    root_client = palo_client.PaloClient(config.fe_host, config.fe_query_port, 
            user="root", password=config.fe_password)
    assert root_client.init()
    super_user = "super_user"
    try:
        root_client.drop_user(super_user)
    except:
        pass
    ret = root_client.create_user(super_user, password=super_user, is_superuser=True)
    assert ret
    super_client = palo_client.PaloClient(config.fe_host, config.fe_query_port, 
            user=super_user, password=super_user)
    assert super_client.init()
    normal_user = "normal_user"
    try:
        super_client.drop_user(normal_user)
    except:
        pass
    ret = super_client.create_user(normal_user, password=normal_user)
    assert ret
    user_client = palo_client.PaloClient(config.fe_host, config.fe_query_port, 
            user=normal_user, password=normal_user)
    assert user_client.init()

   
def test_root():
    """
    {
    "title": "test_sys_privilege.test_root",
    "describe": "root权限：1. CREATE SUPERUSER 2. SET PASSWORD 3. DROP SUPERUSER",
    "tag": "function,p1,fuzz"
    }
    """
    """
    root权限：
    1. CREATE SUPERUSER
    2. SET PASSWORD
    3. DROP SUPERUSER
    """
    #CREATE SUPERUSER
    superuser = "test_root"
    try:
        root_client.drop_user(superuser)
    except:
        pass
    ret = root_client.create_user(superuser, is_superuser=True)
    assert ret
    #SET PASSWORD
    ret = root_client.set_password(superuser, superuser)
    assert ret
    assert palo_client.PaloClient(config.fe_host, config.fe_query_port, \
            user=superuser, password=superuser).init()
    #DROP SUPERUSER
    ret = root_client.drop_user(superuser)
    assert ret
    try:
        root_client.drop_backend_list(backend_list)
    except:
        pass
    ret = root_client.add_backend_list(backend_list)
    assert ret
    ret = root_client.drop_backend_list(backend_list)
    assert ret


def test_superuser_denied():
    """
    {
    "title": "test_sys_privilege.test_superuser_denied",
    "describe": "superuser无权限: 1. ALTER CLUSTER, superuser有权限：1. CREATE SUPERUSER,2. DROP SUPERUSE",
    "tag": "function,p1,fuzz"
    }
    """
    """
    superuser无权限:
    1. ALTER CLUSTER
    superuser有权限：
    1. CREATE SUPERUSER
    2. DROP SUPERUSE
    """
    #CREATE SUPERUSER
    superuser = "test_superuser_denied"
    try:
        root_client.drop_user(superuser)
    except Exception as e:
        pass
    ret = super_client.create_user(superuser, is_superuser=True)
    assert ret
    #DROP SUPERUSER
    ret = super_client.drop_user(superuser)
    assert ret
    #ALTER CLUSTER
    try:
        root_client.drop_backend_list(backend_list)
    except:
        pass
    try:
        super_client.add_backend_list(backend_list)
    except:
        pass
    else:
        assert False
    try:
        ret = root_client.add_backend_list(backend_list)
    except:
        pass
    # assert ret
    try:
        super_client.drop_backend_list(backend_list)
    except:
        pass
    else:
        assert False
    ret = root_client.drop_backend_list(backend_list)
    assert ret


def test_user_denied():
    """
    {
    "title": "test_sys_privilege.test_user_denied",
    "describe": "普通用户无权限：CREATE USER, DROP USER, CREATE DATABASE, DROP DATABASE, SHOW PROC",
    "tag": "function,p1,fuzz"
    }
    """
    """
    普通用户无权限：
    1. CREATE USER
    2. DROP USER
    3. CREATE DATABASE
    4. DROP DATABASE
    5. SHOW PROC
    """
    user = "test_user_denied"
    #CREATE USER
    try:
        root_client.drop_user(user)
    except:
        pass
    try:
        user_client.create_user(user)
    except:
        pass
    else:
        assert False
    ret = root_client.create_user(user)
    assert ret
    #DROP USER
    try:
        user_client.drop_user(user)
    except:
        pass
    else:
        assert False
    ret = root_client.drop_user(user)
    assert ret
    database_name = "test_user_denied"
    try:
        root_client.drop_database(database_name)
    except:
        pass
    #CREATE DATABASE
    try:
        user_client.create_database(database_name)
    except:
        pass
    else:
        assert False
    ret = root_client.create_database(database_name)
    assert ret
    #DROP DATABASE
    try:
        user_client.drop_database(database_name)
    except:
        pass
    else:
        assert False
    ret = root_client.drop_database(database_name)
    assert ret
    #SHOW PROC
    try:
        # user_client.get_database_list()
        user_client.execute('show proc "/"')
    except:
        pass
    else:
        assert False
 

def test_user_no_grant():
    """
    {
    "title": "test_sys_privilege.test_user_no_grant",
    "describe": "普通用户对没有grant的数据库没有权限",
    "tag": "function,p1,fuzz"
    }
    """
    """
    普通用户对没有grant的数据库没有权限
    """
    database_name, table_name, index_name = util.gen_name_list() 
    init(database_name)
    ret = user_client.use(database_name)
    assert not ret
    try:
        user_client.drop_database(database_name)
    except:
        pass
    else:
        assert False
    ret = root_client.drop_database(database_name)
    assert ret


def test_read_only():
    """
    {
    "title": "test_sys_privilege.test_read_only",
    "describe": "只读用户没有写权限",
    "tag": "function,p1,fuzz"
    }
    """
    """
    只读用户没有写权限
    """
    database_name, table_name, index_name = util.gen_name_list() 
    user = "test_read_only"
    try:
        super_client.drop_user(user)
    except:
        pass
    ret = super_client.create_user(user)
    assert ret
    client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=user, password='')
    assert client.init()
    init(database_name)
    ret = super_client.grant(user, "READ_ONLY", database_name)
    assert ret
    database_list = client.execute("SHOW DATABASES")
    assert (database_name, ) in database_list
    try:
        client.create_table(table_name, DATA.column_list, database_name=database_name)
    except:
        pass
    else:
        assert False
    ret = super_client.grant(user, "READ_WRITE", database_name)
    assert ret
    ret = client.create_table(table_name, DATA.column_list, database_name=database_name)
    assert ret


def init(database_name):
    """
    建库
    """
    root_client.clean(database_name)
    ret = root_client.create_database(database_name)
    assert ret


def test_roles():
    """
    {
    "title": "test_sys_privilege.test_roles",
    "describe": "1. 创建role,2. 给role赋权,3. 创建用户指定role,4. revoke role的权限,5. 删除role",
    "tag": "function,p1,fuzz"
    }
    """
    """
    1. 创建role
    2. 给role赋权
    3. 创建用户指定role
    4. revoke role的权限
    5. 删除role
    """
    database_name, table_name, index_name = util.gen_name_list()
    init(database_name=database_name)
    table1 = table_name + '_1'
    table2 = table_name + '_2'
    assert root_client.create_table(table1, DATA.column_list, database_name=database_name)
    assert root_client.create_table(table2, DATA.column_list, database_name=database_name)
    # create role
    roles1 = 'role_for_test1'
    roles2 = 'role_for_test2'
    try:
        root_client.drop_role(roles1)
        root_client.drop_role(roles2)
    except Exception as e:
        pass
    assert root_client.create_role(roles1)
    assert root_client.create_role(roles2)

    # grant role
    assert root_client.grant(roles1, ['SELECT_PRIV'], database_name, is_role=True)
    assert root_client.grant(roles2, ['SELECT_PRIV'], '%s.%s' % (database_name, table1), is_role=True)

    # create user with role
    user1 = 'test_role_user1'
    user2 = 'test_role_user2'
    try:
        root_client.drop_user(user1)
        root_client.drop_user(user2)
    except Exception as e:
        pass
    assert root_client.create_user(user1, password=user1, default_role=roles1)
    assert root_client.create_user(user2, password=user2, default_role=roles2)
    test_client1 = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=user1,
                                          password=user1, database_name=database_name)
    assert test_client1.init()
    test_client2 = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=user1,
                                          password=user1, database_name=database_name)
    assert test_client2.init()
    # check priv
    ret = test_client1.select_all(table1)
    assert ret == ()
    ret = test_client1.select_all(table2)
    assert ret == ()
    ret = test_client2.select_all(table1)
    assert ret == ()
    try:
        test_client2.select_all(table2)
        assert 0 == 1
    except Exception as e:
        pass
    # revoke
    ret = root_client.revoke(roles1, ['SELECT_PRIV'], database_name, is_role=True)
    assert ret
    # revoke check
    try:
        test_client1.connect()
        ret = test_client1.use(database_name)
        assert not ret
        ret = test_client1.select_all(table2)
        assert 0 == 1, 'can not select'
    except Exception as e:
        pass
    # drop
    root_client.drop_user(user1)
    root_client.drop_user(user2)
    root_client.drop_role(roles1)
    root_client.drop_role(roles2)
    root_client.clean(database_name)


def test_grant():
    """
    {
    "title": "test_sys_privilege.test_grant",
    "describe": "1. grant db,2. grant table,3. grant to user,4. grant to role,5. show grants",
    "tag": "function,p1"
    }
    """
    """
    1. grant db
    2. grant table
    3. grant to user
    4. grant to role
    5. show grants
    """
    database_name, table_name, index_name = util.gen_name_list()
    init(database_name=database_name)
    table1 = table_name + '_1'
    table2 = table_name + '_2'
    assert root_client.create_table(table1, DATA.column_list, database_name=database_name)
    assert root_client.create_table(table2, DATA.column_list, database_name=database_name)
    # create role and usesr; grant
    test_role = 'grant_to_role'
    test_user = 'grant_to_user'
    test_user1 = 'job'
    test_user2 = 'task'
    try:
        root_client.drop_role(test_role)
        root_client.drop_user(test_user)
        root_client.drop_user(test_user1)
        root_client.drop_user(test_user2)
    except Exception as e:
        pass
    assert root_client.create_role(test_role)
    assert root_client.create_user(test_user)
    assert root_client.grant(test_role, ['SELECT_PRIV', 'LOAD_PRIV', 'CREATE_PRIV'], 
                             '%s.*' % database_name, is_role=True)
    assert root_client.grant(test_user, ['SELECT_PRIV', 'LOAD_PRIV'], database_name)
    # check user grant
    ret = root_client.get_grant(test_user)
    db_privs = palo_job.GrantInfo(ret[0]).get_database_privs()
    tmp = '%s: Select_priv Load_priv' % (database_name)
    assert db_privs.find(tmp) != -1, 'expect contains: %s, actural: %s' % (tmp, db_privs)
    # CREATE USER ON ROLE, CHECK USER PRIV
    assert root_client.create_user(test_user1, password=test_user, default_role=test_role)
    ret = root_client.get_grant(test_user1)
    db_privs = palo_job.GrantInfo(ret[0]).get_database_privs()
    tmp = '%s: Select_priv Load_priv Create_priv' % (database_name)
    assert db_privs.find(tmp) != -1
    assert root_client.create_user(test_user2, password=test_user)
    assert root_client.grant(test_user2, ['SELECT_PRIV'], database_name, table1)
    ret = root_client.get_grant(test_user2)
    table_privs = palo_job.GrantInfo(ret[0]).get_table_privs()
    print(table_privs)
    tmp = '%s.%s: Select_priv' % (database_name, table1)
    assert table_privs.find(tmp) != -1
    # clean
    root_client.drop_role(test_role)
    root_client.drop_user(test_user)
    root_client.drop_user(test_user1)
    root_client.drop_user(test_user2)
    root_client.clean(database_name)


def test_revoke():
    """
    {
    "title": "test_sys_privilege.test_revoke",
    "describe": "1. revoke某个用户/role的table的权限, 2. revoke某个用户/role的db的权限, 3. show grant查看",
    "tag": "function,p1"
    }
    """
    """
    1. revoke某个用户/role的table的权限
    2. revoke某个用户/role的db的权限
    3. show grant查看
    """
    database_name, table_name, index_name = util.gen_name_list()
    init(database_name=database_name)
    table1 = table_name + '_1'
    table2 = table_name + '_2'
    assert root_client.create_table(table1, DATA.column_list, database_name=database_name)
    assert root_client.create_table(table2, DATA.column_list, database_name=database_name)
    # create user and grant
    test_role = 'revoke_to_role'
    test_user = 'revoke_to_user'
    test_user1 = 'revoke_job'
    test_user2 = 'revoke_task'
    try:
        root_client.drop_role(test_role)
        root_client.drop_user(test_user)
        root_client.drop_user(test_user1)
        root_client.drop_user(test_user2)
    except Exception as e:
        pass
    assert root_client.create_role(test_role)
    assert root_client.create_user(test_user)
    assert root_client.grant(test_role, ['SELECT_PRIV', 'LOAD_PRIV', 'CREATE_PRIV'], 
                             '%s.*' % database_name, is_role=True)
    assert root_client.grant(test_user, ['SELECT_PRIV', 'LOAD_PRIV'], database_name)
    # CHECK USER PRIV
    ret = root_client.get_grant(test_user)
    db_privs = palo_job.GrantInfo(ret[0]).get_database_privs()
    tmp = '%s: Select_priv Load_priv' % (database_name)
    assert db_privs.find(tmp) != -1
    # CREATE USER ON ROLE, CHECK USER PRIV
    assert root_client.create_user(test_user1, password=test_user, default_role=test_role)
    ret = root_client.get_grant(test_user1)
    db_privs = palo_job.GrantInfo(ret[0]).get_database_privs()
    tmp = '%s: Select_priv Load_priv Create_priv' % (database_name)
    assert db_privs.find(tmp) != -1
    # crate user, grant table priv and check
    assert root_client.create_user(test_user2)
    assert root_client.grant(test_user2, ['SELECT_PRIV', 'LOAD_PRIV'], 
                             database_name, table1)
    ret = root_client.get_grant(test_user2)
    tb_privs = palo_job.GrantInfo(ret[0]).get_table_privs()
    tmp = '%s.%s: Select_priv Load_priv' % (database_name, table1)
    assert tb_privs.find(tmp) != -1
    # revoke user priv
    assert root_client.revoke(test_user, ['LOAD_PRIV'], database_name)
    ret = root_client.get_grant(test_user)
    db_privs = palo_job.GrantInfo(ret[0]).get_database_privs()
    tmp = '%s: Select_priv' % (database_name)
    assert db_privs.find(tmp) != -1
    # revoke role priv
    assert root_client.revoke(test_role, ['SELECT_PRIV', 'LOAD_PRIV'], database_name, is_role=True)
    ret = root_client.get_grant(test_user1)
    db_privs = palo_job.GrantInfo(ret[0]).get_database_privs()
    tmp = '%s: Create_priv' % (database_name)
    assert db_privs.find(tmp) != -1
    # revoke user table priv
    assert root_client.revoke(test_user2, ['SELECT_PRIV'], '%s.%s' % (database_name, table1))
    ret = root_client.get_grant(test_user2)
    tb_privs = palo_job.GrantInfo(ret[0]).get_table_privs()
    tmp = '%s.%s: Load_priv' % (database_name, table1)
    assert tb_privs.find(tmp) != -1
    # clean
    root_client.drop_role(test_role)
    root_client.drop_user(test_user)
    root_client.drop_user(test_user1)
    root_client.drop_user(test_user2)
    root_client.clean(database_name)


def test_load_priv():
    """
    {
    "title": "test_sys_privilege.test_load_priv",
    "describe": "导入权限,不能进行其他操作",
    "tag": "function,p1,fuzz"
    }
    """
    """导入权限,不能进行其他操作"""
    database_name, table_name, index_name = util.gen_name_list()
    user = 'load_priv_user'
    init(database_name)
    table1 = table_name + '1'
    table2 = table_name + '2'
    assert root_client.create_table(table1, schema.partition_column_list)
    assert root_client.create_table(table2, schema.partition_column_list)
    try:
        root_client.drop_user(user)
    except Exception as e:
        pass
    assert root_client.create_user(user)
    assert root_client.grant(user, ['LOAD_PRIV'], database_name, table1)
    test_client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=user, password='')
    assert test_client.init()
    test_client.use(database_name)
    # load
    local_file = './data/PARTITION/partition_type'
    ret = test_client.stream_load(table1, local_file, database_name=database_name)
    assert ret
    # delete
    ret = test_client.delete(table1, [('k1', '=', '-1')])
    assert ret
    # load other table
    ret = test_client.stream_load(table2, local_file)
    assert not ret
    # select
    try:
        test_client.select_all(table1)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # schema change
    try:
        test_client.schema_change_add_column(table_name, [('add_v', 'int', 'replace', '1')])
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # create
    try:
        test_client.create_table(table_name, schema.partition_column_list)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # drop
    try:
        test_client.drop_table(table1)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # node 
    try:
        test_client.add_backend_list(backend_list)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    root_client.drop_user(user)
    root_client.clean(database_name)


def test_alter_priv():
    """
    {
    "title": "test_sys_privilege.test_alter_priv",
    "describe": "alter权限,执行其他操作",
    "tag": "function,p1,fuzz"
    }
    """
    """alter权限,执行其他操作"""
    database_name, table_name, index_name = util.gen_name_list()
    user = 'alter_priv_user'
    init(database_name)
    table1 = table_name + '1'
    table2 = table_name + '2'
    assert root_client.create_table(table1, schema.partition_column_list)
    assert root_client.create_table(table2, schema.partition_column_list)
    try:
        root_client.drop_user(user)
    except Exception as e:
        pass
    assert root_client.create_user(user)
    assert root_client.grant(user, ['ALTER_PRIV'], database_name, table1)
    test_client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=user, password='')
    assert test_client.init()
    test_client.use(database_name)
    # load
    local_file = './data/PARTITION/partition_type'
    ret = test_client.stream_load(table1, local_file)
    assert not ret
    # delete
    try:
        # delete priv??
        ret = test_client.delete(table1, [('k1', '=', '-1')])
    except Exception as e:
        print(str(e))
    else:
        0 == 1
    # select
    try:
        test_client.select_all(table1)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # schema change
    ret = test_client.schema_change_add_column(table1, [('add_v', 'int', 'replace', '1')], 
                                               database_name=database_name)
    assert ret
    # create
    try:
        test_client.create_table(table_name, schema.partition_column_list)
    except Exception as e:
        print(e)
    else:
        assert 0 == 1
    # drop
    try:
        test_client.drop_table(table1)
    except Exception as e:
        print(e)
    else:
        assert 0 == 1
    # node
    try:
        test_client.add_backend_list(backend_list)
    except Exception as e:
        print(e)
    else: 
        assert 0 == 1
    root_client.drop_user(user)
    root_client.clean(database_name)


def test_create_priv():
    """
    {
    "title": "test_sys_privilege.test_create_priv",
    "describe": "create权限,执行其他操作",
    "tag": "function,p1,fuzz"
    }
    """
    """create权限,执行其他操作"""
    database_name, table_name, index_name = util.gen_name_list()
    user = 'create_priv_user'
    init(database_name)
    table1 = table_name + '1'
    table2 = table_name + '2'
    assert root_client.create_table(table1, schema.partition_column_list)
    assert root_client.create_table(table2, schema.partition_column_list)
    try:
        root_client.drop_user(user)
    except Exception as e:
        pass
    assert root_client.create_user(user)
    assert root_client.grant(user, ['CREATE_PRIV'], database_name)
    test_client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=user, password='')
    assert test_client.init()
    test_client.use(database_name)
    # load
    local_file = './data/PARTITION/partition_type'
    ret = test_client.stream_load(table1, local_file)
    assert not ret
    # delete
    try:
        test_client.delete(table1, [('k1', '=', '-1')])
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # select
    try:
        test_client.select_all(table1)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # schema change
    try:
        test_client.schema_change_add_column(table_name, [('add_v', 'int', 'replace', '1')])
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # create
    ret = test_client.create_table(table_name, schema.partition_column_list)
    assert ret
    try:
        ret = test_client.create_database(database_name + '_1')
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # drop
    try:
        test_client.drop_table(table1)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # node
    try:
        test_client.add_backend_list(backend_list)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    root_client.drop_user(user)
    root_client.clean(database_name)


def test_drop_priv():
    """
    {
    "title": "test_sys_privilege.test_drop_priv",
    "describe": "drop权限,执行其他操作",
    "tag": "function,p1,fuzz"
    }
    """
    """drop权限,执行其他操作"""
    database_name, table_name, index_name = util.gen_name_list()
    user = 'drop_priv_user'
    init(database_name)
    table1 = table_name + '1'
    table2 = table_name + '2'
    assert root_client.create_table(table1, schema.partition_column_list)
    assert root_client.create_table(table2, schema.partition_column_list)
    try:
        root_client.drop_user(user)
    except Exception as e:
        pass
    assert root_client.create_user(user)
    assert root_client.grant(user, ['DROP_PRIV'], database_name)
    test_client = palo_client.PaloClient(config.fe_host, config.fe_query_port, user=user, password='')
    assert test_client.init()
    test_client.use(database_name)
    # load
    local_file = './data/PARTITION/partition_type'
    ret = test_client.stream_load(table1, local_file)
    assert not ret
    # delete
    try:
        test_client.delete(table1, [('k1', '=', '-1')])
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # select
    try:
        test_client.select_all(table1)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # schema change
    try:
        test_client.schema_change_add_column(table_name, [('add_v', 'int', 'replace', '1')])
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    # create 
    try:
        test_client.create_table(table_name, schema.partition_column_list)
    except Exception as e:
        print(str(e))
    else:
        assert 0 == 1
    try:
        ret = test_client.create_database(database_name + '_1')
        assert not ret
    except Exception as e:
        print(str(e))
    # drop
    ret = test_client.execute('DROP TABLE %s' % table1)
    assert ret == ()
    ret = test_client.drop_database(database_name)
    assert ret
    # node    
    try:
        test_client.add_backend_list(backend_list)
    except Exception as e:
        print(str(e))
    root_client.drop_user(user)
    root_client.clean(database_name)


def test_node_priv():
    """
    {
    "title": "test_sys_privilege.test_node_priv",
    "describe": "node 权限,执行其他操作",
    "tag": "function,p1,fuzz"
    }
    """
    """node 权限,执行其他操作"""
    database_name, table_name, index_name = util.gen_name_list()
    user = 'node_priv_user'
    init(database_name)
    assert root_client.create_table(table_name, schema.partition_column_list)
    try:
        root_client.drop_user(user)
    except Exception as e:
        pass
    assert root_client.create_user(user)
    try:
        root_client.grant(user, ['NODE_PRIV'], database_name)
        assert 0 == 1
    except Exception as e:
        pass

    root_client.drop_user(user)
    root_client.clean(database_name)


def teardown_module():
    """tear down"""
    pass
