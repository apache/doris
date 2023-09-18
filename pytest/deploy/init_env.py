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
This module init Palo runtime environment.
Date:    2015/10/07 17:23:06
"""
import threading
import env_config
import execute


def create_fe_dir():
    """create fe dir.
    """
    cmd = 'mkdir -p %s' % env_config.fe_path
    for host_name in [env_config.master] + env_config.follower_list + \
            env_config.observer_list + env_config.dynamic_add_fe_list:
        status, output = execute.exe_cmd(cmd, host_name)


def create_be_dir():
    """create be dir.
    """
    create_be_dir_threads = []
    for host_name in env_config.be_list + env_config.dynamic_add_be_list:
        t = threading.Thread(target=create_one_be_dir, args=(host_name,))
        t.start()
        create_be_dir_threads.append(t)
    for t in create_be_dir_threads:
        t.join()


def create_one_be_dir(host_name):
    """create be dir and data dir"""
    # if data_path not in be_path, will not backup, need to clean
    print("clean %s data path..." % host_name)
    # clean data path & create
    for dir in env_config.be_data_path_list:
        cmd = 'mkdir -p /home/%s/empty; rsync --delete-before -a /home/%s/empty/ %s/' % \
              (env_config.host_username, env_config.host_username, dir)
        status, output = execute.exe_cmd(cmd, host_name)
    data_path = ' '.join(env_config.be_data_path_list)
    cmd = 'rm -rf %s' % data_path
    status, output = execute.exe_cmd(cmd, host_name)

    print("%s mkdir be & data path" % host_name)
    cmd = 'mkdir -p %s' % env_config.be_path
    status, output = execute.exe_cmd(cmd, host_name)
    cmd = 'mkdir -p %s' % data_path
    status, output = execute.exe_cmd(cmd, host_name)
    

def create_palo_dir():
    """create palo dir.
    """
    create_fe_dir()
    create_be_dir()


def check_fe_port():
    """check whether fe port in use
    """
    query_port = env_config.fe_query_port
    http_port = query_port - 1000
    rpc_port = query_port - 10
    edit_log_port = query_port - 20
    port_list = [query_port, http_port, rpc_port, edit_log_port]
    for host_name in [env_config.master] + env_config.follower_list + \
            env_config.observer_list + env_config.dynamic_add_fe_list:
        for port in port_list:
            cmd = 'netstat -ntpl | grep "\:%s "' % (port)
            status, output = execute.exe_cmd(cmd, host_name)
            if status == 0:
                return False
    return True


def check_be_port():
    """check whether be port in use
    """
    query_port = env_config.fe_query_port
    be_port = query_port + 30
    webserver_port = query_port - 1000 + 10
    heartbeat_service_port = query_port + 20
    port_list = [be_port, webserver_port, heartbeat_service_port]
    for host_name in env_config.be_list + env_config.dynamic_add_be_list:
        for port in port_list:
            cmd = 'netstat -ntpl | grep "\:%s "' % (port)
            status, output = execute.exe_cmd(cmd, host_name)
            if status == 0:
                return False
    return True


def check_palo_port():
    """check whether palo port in use
    """
    if not check_fe_port():
        return False
    if not check_be_port():
        return False
    return True


def init_palo_env():
    """init palo env
    """
#   if not check_palo_port():
#       return False
    create_palo_dir()
    return True


if __name__ == '__main__':
    init_palo_env()
