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
This module generate configuration for fe.conf.out and be.conf.
Date:    2015/10/07 17:23:06
"""
import subprocess as commands
import os
import env_config


def modify_config(filepath, option, value):
    """修改配置
    """
    cmd = "grep -q '^{option}' {filepath} && "\
          "sed -i 's/^{option}[[:blank:]]*=.*/{option} = {value}/g' {filepath} || "\
          "echo -e '\n{option} = {value}' >> {filepath}".format( \
          option=option, value=value, filepath=filepath)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        return False
    cmd = "grep -q '^{} = {}' {}".format(option, value, filepath)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        return False
    return True


def process_fe_conf():
    """process fe config
    """
    os.system('cp fe.conf fe.conf.out')
    filepath = 'fe.conf.out'
    modify_config(filepath, 'http_port', env_config.http_port)
    modify_config(filepath, 'rpc_port', env_config.rpc_port)
    modify_config(filepath, 'query_port', env_config.fe_query_port)
    modify_config(filepath, 'edit_log_port', env_config.edit_log_port)
    try:
        for k, v in env_config.fe_config_map.items():
            modify_config(filepath, k, v)
            # os.system('echo "\n%s=%s" >> fe.conf.out' % (k, v))
    except AttributeError as e:
        pass


def process_be_conf():
    """
    process be config
    """
    os.system('cp be.conf be.conf.out')
    filepath = 'be.conf.out'
    storage_root_path = '; '.join(env_config.be_data_path_list).replace('/', '\/')
    modify_config(filepath, 'be_port', env_config.be_port)
    modify_config(filepath, 'webserver_port', env_config.webserver_port)
    modify_config(filepath, 'heartbeat_service_port', env_config.heartbeat_service_port)
    modify_config(filepath, 'be_rpc_port', env_config.be_rpc_port)
    modify_config(filepath, 'brpc_port', env_config.brpc_port)
    modify_config(filepath, 'storage_root_path', storage_root_path)
    try:
        for k, v in env_config.be_config_map.items():
            modify_config(filepath, k, v)
            # os.system('echo "\n%s=%s" >> be.conf.out' % (k, v))
    except AttributeError as e: 
        pass


def process_auditload_conf():
    """"""
    os.system('cp plugin_auditload.conf plugin_auditload.conf.out')
    os.system('sed -i "s/password=.*/password=%s/g" plugin_auditload.conf.out' % env_config.fe_password)
    os.system('sed -i "s/frontend_host_port=.*/frontend_host_port=%s:%s/g" plugin_auditload.conf.out' \
              % (env_config.master, env_config.fe_query_port - 1000))


def process_palo_conf():
    """process palo conf
    """
    process_fe_conf()
    process_be_conf()
    process_auditload_conf()


if __name__ == '__main__':
    process_palo_conf()


