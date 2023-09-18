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
This module config TDB.
Date:    2015/10/07 17:23:06
"""
import time
import threading

import env_config
import execute


def modify_config(host_name, filepath, option, value):
    """修改配置
    """
    cmd = "grep -q '^{option}' {filepath} && " \
          "sed -i 's/^{option}.*/{option} = {value}/g' {filepath} || " \
          "echo '{option} = {value}' >> {filepath}".format(
          option=option, value=value, filepath=filepath)
    status, output = execute.exe_cmd(cmd, host_name)
    if status != 0:
        return False
    cmd = "grep -q '^{} = {}' {}".format(option, value, filepath)
    status, output = execute.exe_cmd(cmd, host_name)
    if status != 0:
        return False
    return True


def remove_config(host_name, filepath, option):
    """删除配置
    """
    cmd = 'sed -i "s/^%s.*/ /" %s' % (option, filepath)
    execute.exe_cmd(cmd, host_name)
    cmd = "grep -q '^{} {}'".format(option, filepath)
    status, output = execute.exe_cmd(cmd, host_name)
    if status == 0:
        return False
    return True


def modify_be_config(host_name, path, option, value):
    """
    modify be config
    """
    filepath = '%s/conf/be.conf' % path
    return modify_config(host_name, filepath, option, value)


def config_one_be(host_name):
    """config one be
    """
    filepath = '%s/be/conf/be.conf' % env_config.be_path
    modify_config(host_name, filepath, 'be_port', env_config.be_port)
    modify_config(host_name, filepath, 'webserver_port', env_config.webserver_port)
    modify_config(host_name, filepath, 'heartbeat_service_port', env_config.heartbeat_service_port)
    modify_config(host_name, filepath, 'be_rpc_port', env_config.be_rpc_port)
    modify_config(host_name, filepath, 'brpc_port', env_config.brpc_port)
    time.sleep(3)


def config_be():
    """config be
    """
    config_be_threads = []
    for host_name in env_config.be_list + env_config.dynamic_add_be_list:
        t = threading.Thread(target=config_one_be, args=(host_name,))
        t.start()
        config_be_threads.append(t)

    for t in config_be_threads:
        t.join()


if __name__ == '__main__':
    config_be()
