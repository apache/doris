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
This module start Palo.
Date:    2015/10/07 17:23:06
"""
import threading

import env_config
import execute
import start
import stop


def clean_one_fe(host_name):
    """clean fe
    """
    cmd = 'cd %s/fe;rm -rf log palo-meta/image palo-meta/bdb temp' % env_config.fe_path
    status, output = execute.exe_cmd(cmd, host_name)


def clean_one_be(host_name):
    """clean one be
    """
    base_be_port = env_config.fe_query_port + 30
    cmd = 'cd %s/be;rm -rf data log unused bin/be.pid' % env_config.be_path
    status, output = execute.exe_cmd(cmd, host_name)


def clean_fe():
    """clean fe
    """
    clean_fe_threads = []
    for host_name in [env_config.master] + env_config.follower_list + \
            env_config.observer_list + env_config.dynamic_add_fe_list:
        t = threading.Thread(target=clean_one_fe, args=(host_name,))
        t.start()
        clean_fe_threads.append(t)

    for t in clean_fe_threads:
        t.join()


def clean_be():
    """clean be
    """
    clean_be_threads = []
    for host_name in env_config.be_list + env_config.dynamic_add_be_list:
        t = threading.Thread(target=clean_one_be, args=(host_name,))
        t.start()
        clean_be_threads.append(t)

    for t in clean_be_threads:
        t.join()


def clean_palo():
    """clean palo
    """
    clean_fe()
    clean_be()


def clean_start_palo():
    """clean start palo
    """
    stop.stop_palo()
    clean_palo()
    start.start_palo(init_state=True)


if __name__ == '__main__':
    clean_start_palo()

