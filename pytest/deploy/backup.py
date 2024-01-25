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
import datetime
import sys
import threading

import execute
import env_config
import stop

fmt = '%Y-%m-%d-%H-%M-%S-%f'


def dump_fe(host_name):
    """dump fe
    """
    cmd = 'cd %s/fe;jmap -dump:live,format=b,file=heap.bin `cat bin/fe.pid`' \
            % env_config.fe_path
    status, output = execute.exe_cmd(cmd, host_name)


def backup_one_fe(host_name, backup_name):
    """backup one fe
    """
    dump_fe(host_name)
    stop.stop_one_fe(host_name)
    cmd = 'cd {0};mkdir -p backup/{1};mv {2} backup/{1}'.format(
            env_config.palo_path, backup_name, 'PALO-FE')
    status, output = execute.exe_cmd(cmd, host_name)


def backup_fe(backup_name=None):
    """backup fe
    """
    if not backup_name:
        backup_name = datetime.datetime.now().strftime(fmt)
    backup_fe_threads = []
    for host_name in [env_config.master] + env_config.follower_list + \
            env_config.observer_list + env_config.dynamic_add_fe_list:
        t = threading.Thread(target=backup_one_fe, args=(host_name, backup_name))
        t.start()
        backup_fe_threads.append(t)

    for t in backup_fe_threads:
        t.join()


def backup_one_be(host_name, backup_name):
    """backup one be
    """
    stop.stop_one_be(host_name)
    cmd = 'cd {0};mkdir -p backup/{1};mv {2} backup/{1}'.format(
            env_config.palo_path, backup_name, 'PALO-BE')
    status, output = execute.exe_cmd(cmd, host_name)


def backup_be(backup_name=None):
    """backup be
    """
    if not backup_name:
        backup_name = datetime.datetime.now().strftime(fmt)
    backup_be_threads = []
    for host_name in env_config.be_list + env_config.dynamic_add_be_list:
        t = threading.Thread(target=backup_one_be, args=(host_name, backup_name))
        t.start()
        backup_be_threads.append(t)

    for t in backup_be_threads:
        t.join()


def backup_palo(backup_name=None):
    """backup palo
    """
    if not backup_name:
        backup_name = datetime.datetime.now().strftime(fmt)
    backup_fe(backup_name)
    backup_be(backup_name)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        backup_palo(sys.argv[1])
    else:
        backup_palo()
