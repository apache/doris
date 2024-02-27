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
\This module generate configuration for fe.conf and be.conf.
Date:    2015/10/07 17:23:06
"""
import os
import env_config


def prepare_palo_package(deploy_audit=False):
    """prepare palo package
    """
    os.system('mv fe.conf.out output/fe/conf/fe.conf')
    os.system('mv be.conf.out output/be/conf/be.conf')
    os.system('mkdir output/fe/palo-meta > /dev/null 2>&1')
    os.system('echo clusterId=%s > output/fe/palo-meta/VERSION' % env_config.fe_query_port)
    os.system('sed -i "s/nohup/export LC_ALL=\\"C\\"\\n    ' \
              'ulimit -c unlimited\\n    nohup/g" output/be/bin/start_be.sh')
    os.system('sed -i "s/limit3 -c 0 -n/limit3 -n/g" output/be/bin/start_be.sh')
    os.system('mkdir -p output/be/var/pull_load > /dev/null 2>&1')
    if deploy_audit:
        os.system('unzip -q -u -d output/fe/plugin_auditloader output/audit_loader/auditloader.zip')
        os.system('mv plugin_auditload.conf.out output/fe/plugin_auditloader/plugin.conf')
    os.system('cd output;tar -cf - fe | gzip --fast > fe.tar.gz; cd -')
    os.system('cd output;tar -cf - be | gzip --fast > be.tar.gz; cd -')
    os.system('mv output/fe.tar.gz .')
    os.system('mv output/be.tar.gz .')


if __name__ == '__main__':
    prepare_palo_package()
