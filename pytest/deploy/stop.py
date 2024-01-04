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
This module stop Palo.
Date:    2015/10/07 17:23:06
"""
import threading

import env_config
import execute


def stop_one_fe(host_name):
    """stop one fe
    """
    cmd = 'cd %s/fe;sh bin/stop_fe.sh' % env_config.fe_path
    status, output = execute.exe_cmd(cmd, host_name)


def stop_one_be(host_name):
    """stop one be
    """
    cmd = 'cd %s/be; sh bin/stop_be.sh' % env_config.be_path
    status, output = execute.exe_cmd(cmd, host_name)


def stop_fe():
    """stop fe
    """
    stop_fe_threads = []
    for host_name in [env_config.master] + env_config.follower_list + \
            env_config.observer_list + env_config.dynamic_add_fe_list:
        t = threading.Thread(target=stop_one_fe, args=(host_name,))
        t.start()
        stop_fe_threads.append(t)

    for t in stop_fe_threads:
        t.join()


def stop_be():
    """stop be
    """
    stop_be_threads = []
    for host_name in env_config.be_list + env_config.dynamic_add_be_list:
        t = threading.Thread(target=stop_one_be, args=(host_name,))
        t.start()
        stop_be_threads.append(t)

    for t in stop_be_threads:
        t.join()


def stop_palo():
    """stop palo
    """
    stop_fe()
    stop_be()


if __name__ == '__main__':
    stop_palo()
