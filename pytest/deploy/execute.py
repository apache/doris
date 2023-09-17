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
This module provide exe func.
Date:    2015/10/07 17:23:06
"""
import pexpect

import env_config


def exe_cmd(cmd, host_name, timeout=120):
    """exe cmd
    """
    exe_cmd = 'ssh %s@%s "%s"' % (env_config.host_username, host_name, cmd)
    output, status = pexpect.run(exe_cmd, timeout=timeout, withexitstatus=True,
            events = {"continue connecting":"yes\n", "password:":"%s\n" % env_config.host_password})
    if status:
        print(exe_cmd)
        print(host_name, status, output)
    return status, output


def scp_cmd(local_path, host_name, remote_path, timeout=3600):
    """scp cmd
    """
    exe_cmd = 'scp -r %s %s@%s:%s' % (local_path, env_config.host_username, host_name, remote_path)
    output, status = pexpect.run(exe_cmd, timeout=timeout, withexitstatus=True,
            events={"continue connecting":"yes\n", "password:":"%s\n" % env_config.host_password})
    if status:
        print(exe_cmd)
        print(status, output)
    return status, output

