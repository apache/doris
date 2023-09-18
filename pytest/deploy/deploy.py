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
This module provide Palo deployer.
Date:    2015/10/07 17:23:06
"""
import os
import init_env
import backup
import config_be
import process_config_file
import prepare_package
import hadoop_mkdir
import distribute
import start
import time


def deploy_palo():
    """deploy palo
    """
    if os.path.exists('./output/audit_loader/auditloader.zip'):
        deploy_audit = True
    else:
        deploy_audit = False
    # 集群停止，备份
    backup.backup_palo()
    # 初始化fe/be目录等
    if not init_env.init_palo_env():
        return False
    # 更新conf文件
    process_config_file.process_palo_conf()
    # 准备部署表
    prepare_package.prepare_palo_package(deploy_audit=deploy_audit)
    # hadoop_mkdir.create_hdfs_dir()
    # 分发部署包到fe/be机器
    distribute.distribute_package()
    time.sleep(100)
    # config_be.config_be()
    start.start_palo(init_state=True, deploy_audit=deploy_audit)


if __name__ == '__main__':
    deploy_palo()
