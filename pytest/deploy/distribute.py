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
This module distribute Palo package.
Date:    2015/10/07 17:23:06
"""
import os
import threading

import env_config
import execute


def distribute_package_to_first_fe_be():
    """distribute to first fe and be
    """
    def __distribute_first_fe():
        execute.scp_cmd('fe.tar.gz', env_config.master, env_config.fe_path)
        execute.scp_cmd('env_config.py', env_config.master, env_config.fe_path)
        execute.scp_cmd('pexpect.tgz', env_config.master, env_config.fe_path)
        execute.scp_cmd('remote_distribute_fe.py', env_config.master, env_config.fe_path)

    def __distribute_first_be():
        if len(env_config.be_list) > 0:
            execute.scp_cmd('be.tar.gz', env_config.be_list[0], env_config.be_path)
            execute.scp_cmd('env_config.py', env_config.be_list[0], env_config.be_path)
            execute.scp_cmd('pexpect.tgz', env_config.be_list[0], env_config.be_path)
            execute.scp_cmd('remote_distribute_be.py', env_config.be_list[0], env_config.be_path)

    t1 = threading.Thread(target=__distribute_first_fe)
    t2 = threading.Thread(target=__distribute_first_be)

    t1.start()
    t2.start()

    t1.join()
    t2.join()


def distribute_package_to_other_fe_be():
    """distribute to other fe and be
    """
    def __dis_other_fe():
        cmd = 'cd %s;tar -zxf pexpect.tgz;python remote_distribute_fe.py' \
                % (env_config.fe_path)
        status, output = execute.exe_cmd(cmd, env_config.master)

    def __dis_other_be():
        if len(env_config.be_list) > 0:
            cmd = 'cd %s;tar -zxf pexpect.tgz;python remote_distribute_be.py' \
                    % (env_config.be_path)
            status, output = execute.exe_cmd(cmd, env_config.be_list[0])

    t1 = threading.Thread(target=__dis_other_fe)
    t2 = threading.Thread(target=__dis_other_be)

    t1.start()
    t2.start()

    t1.join()
    t2.join()


def distribute_package():
    """distribute package
    """
    distribute_package_to_first_fe_be()
    distribute_package_to_other_fe_be()


if __name__ == '__main__':
    distribute_package()


