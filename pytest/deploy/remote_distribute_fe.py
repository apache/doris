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
This module remote distribute Palo FE.
Date:    2015/10/07 17:23:06
"""
import os
import pexpect
import threading

import env_config


def exe_cmd(cmd, host_name, timeout=120):
    """exe
    """
    exec_cmd = 'ssh %s@%s "%s"' % (env_config.host_username, host_name, cmd)
    print(exec_cmd)
    output, status = pexpect.run(exec_cmd, timeout=timeout, withexitstatus=True,
                                 events={"continue connecting":"yes\n", "password:":"%s\n" % env_config.host_password})
    print(host_name, exec_cmd, status, output)
    return status, output


def scp_cmd(local_path, host_name, remote_path, timeout=3600):
    """scp
    """
    exec_cmd = 'scp -r %s %s@%s:%s' % (local_path, env_config.host_username, host_name, remote_path)
    print(exec_cmd)
    output, status = pexpect.run(exec_cmd, timeout=timeout, withexitstatus=True,
                                 events={"continue connecting":"yes\n", "password:":"%s\n" % env_config.host_password})
    print(host_name, exec_cmd, status, output)
    return status, output


def distribute_fe_package_on_remote():
    """distribute package on remote
    """
    def __extract_first_fe():
        cmd = 'cd %s;tar -zxf fe.tar.gz;cp fe.tar.gz fe' % (env_config.fe_path)
        os.system(cmd)

    def __distribute_package_to_other_fe(host_name):
        scp_cmd('fe.tar.gz', host_name, env_config.fe_path)
        cmd = 'cd %s;tar -zxf fe.tar.gz;mv fe.tar.gz fe' % (env_config.fe_path)
        status, output = exe_cmd(cmd, host_name)
    t1 = threading.Thread(target=__extract_first_fe)
    t1.start()
    fe_dis_threads_list = []
    for host_name in env_config.follower_list + \
            env_config.observer_list + env_config.dynamic_add_fe_list:
        t = threading.Thread(target=__distribute_package_to_other_fe, args=(host_name,))
        t.start()
        fe_dis_threads_list.append(t)

    t1.join()
    for t in fe_dis_threads_list:
        t.join()



if __name__ == '__main__':
    distribute_fe_package_on_remote()


