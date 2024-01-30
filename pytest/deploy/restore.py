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

import sys
import threading

import execute
import env_config


def restore_one_fe(host_name, restore_name):
    """restore one fe
    """
    cmd = 'cd {0};mv backup/{1}/{2} .'.format(
            env_config.palo_path, restore_name, 'PALO-FE')
    status, output = execute.exe_cmd(cmd, host_name)


def restore_fe(restore_name):
    """restore fe
    """
    restore_fe_threads = []
    for host_name in [env_config.master] + env_config.follower_list + \
            env_config.observer_list + env_config.dynamic_add_fe_list:
        t = threading.Thread(target=restore_one_fe, args=(host_name, restore_name))
        t.start()
        restore_fe_threads.append(t)

    for t in restore_fe_threads:
        t.join()


def restore_one_be(host_name, restore_name):
    """restore one be
    """
    cmd = 'cd {0};mv backup/{1}/{2} .'.format(
            env_config.palo_path, restore_name, 'PALO-BE')
    status, output = execute.exe_cmd(cmd, host_name)


def restore_be(restore_name):
    """restore be
    """
    restore_be_threads = []
    for host_name in env_config.be_list + env_config.dynamic_add_be_list:
        t = threading.Thread(target=restore_one_be, args=(host_name, restore_name))
        t.start()
        restore_be_threads.append(t)

    for t in restore_be_threads:
        t.join()


def restore_palo(restore_name):
    """restore palo
    """
    restore_fe(restore_name)
    restore_be(restore_name)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        restore_palo(sys.argv[1])
    else:
        print('Usage: %s restore_name' % (sys.argv[0]))
        sys.exit(1)
