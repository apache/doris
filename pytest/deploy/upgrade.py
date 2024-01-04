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
This module provide Palo upgrade.
Date:    2017-11-23 16:34:26
"""
import os
import threading
import start
import stop
import execute
import env_config


def upgrade_be():
    """upgrade be"""
    stop.stop_be()
    prepare_be_lib()
    start.start_be()


def upgrade_fe():
    """upgrade fe"""
    stop.stop_fe()
    prepare_fe_lib()
    start.start_master()
    start.start_other_fe()


def prepare_be_lib():
    """prepare be/lib"""
    t_thread = []
    os.system('cd output/be; rm be_lib.tar.gz; tar -cf - lib | gzip --fast > be_lib.tar.gz; cd -')
    for host in env_config.be_list:
        t = threading.Thread(target=replace_be_lib, args=(host,))
        t.start()
        t_thread.append(t)

    for t in t_thread:
        t.join()


def replace_be_lib(host):
    """replace be/lib"""
    be_path = "%s/be" % env_config.be_path
    execute.scp_cmd('output/be/be_lib.tar.gz', host, be_path)
    cmd = 'cd %s; rm -rf lib; tar -xzf be_lib.tar.gz; rm be_lib.tar.gz' % be_path
    execute.exe_cmd(cmd, host)


def prepare_fe_lib():
    """prepare fe/lib"""
    t_thread = []
    os.system('cd output/fe; rm fe_lib.tar.gz; tar -cf - lib | gzip --fast > fe_lib.tar.gz; cd -')
    for host in [env_config.master] + env_config.follower_list + env_config.observer_list:
        t = threading.Thread(target=replace_fe_lib, args=(host,))
        t.start()
        t_thread.append(t)

    for t in t_thread:
        t.join()


def replace_fe_lib(host):
    """replace fe/lib"""
    fe_path = "%s/fe/" % env_config.fe_path
    execute.scp_cmd('output/fe/fe_lib.tar.gz', host, fe_path)
    cmd = 'cd %s; rm -rf lib; tar -xzf fe_lib.tar.gz; rm fe_lib.tar.gz' % fe_path
    execute.exe_cmd(cmd, host)


def upgrade_palo():
    """upgrade palo"""
    t1 = threading.Thread(target=upgrade_fe)
    t2 = threading.Thread(target=upgrade_be)
    t1.start()
    t2.start()
    t1.join()
    t2.join()


if __name__ == '__main__':
    upgrade_palo()

