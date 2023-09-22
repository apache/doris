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
    output, status = pexpect.run(exec_cmd, timeout=timeout, withexitstatus=True,
                                 events={"continue connecting": "yes\n", "password:": "%s\n" % \
                                                                                      env_config.host_password})
    return status, output


def scp_cmd(local_path, host_name, remote_path, timeout=3600):
    """scp
    """
    exec_cmd = 'scp -r %s %s@%s:%s' % (local_path, env_config.host_username, host_name, remote_path)
    output, status = pexpect.run(exec_cmd, timeout=timeout, withexitstatus=True,
                                 events={"continue connecting": "yes\n", "password:": "%s\n" % \
                                                                                      env_config.host_password})
    return status, output


def distribute_be_package_on_remote():
    """dis be
    """
    def __extract_first_be():
        cmd = 'cd %s;tar -zxf be.tar.gz;cp be.tar.gz be' % (env_config.be_path)
        os.system(cmd)

    def __distribute_package_to_other_be(host_name):
        scp_cmd('be.tar.gz', host_name, env_config.be_path)
        cmd = 'cd %s && tar -zxf be.tar.gz' % (env_config.be_path)
        status, output = exe_cmd(cmd, host_name)

    t1 = threading.Thread(target=__extract_first_be)
    t1.start()
    be_dis_threads_list = []
    for host_name in env_config.be_list[1:] + env_config.dynamic_add_be_list:
        t = threading.Thread(target=__distribute_package_to_other_be, args=(host_name,))
        t.start()
        be_dis_threads_list.append(t)

    t1.join()
    for t in be_dis_threads_list:
        t.join()


if __name__ == '__main__':
    distribute_be_package_on_remote()


