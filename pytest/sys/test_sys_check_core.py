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
Date: 2020-05-20 20:01:14
Brief: check if be has core
"""
import sys

sys.path.append('../lib')
import node_op
import common
import palo_logger

LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


def setup_module():
    """setup"""
    pass


def teardown_module():
    """teardown"""
    pass


def test_check_be_core():
    """
    {
    "title": "test_check_be_core",
    "describe": "查看be是否有core",
    "tag": "fuzz,p1"
    }
    """
    client = common.get_client()
    ret = client.show_variables("version_comment")
    if len(ret) == 1:
        print("Doris Version is : %s" % ret[0][1])
        LOG.info(L("GET VERSION", verion=ret[0][1]))
    node = node_op.Node()
    be_list = node.get_be_list()
    core_list = list()
    for host in be_list:
        ret = node.is_be_core(host)
        if ret:
            core_list.append(host)
    if len(core_list) > 0:
        assert 0 == 1, 'check be core file, there is be core: %s' % core_list

