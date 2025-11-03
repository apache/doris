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
/***************************************************************************
  *
  * @file test_sys_base64.py
  * @date 2016/04/01 15:26:21
  * @brief
  * 
  **************************************************************************/
"""
import base64
import sys
import string

sys.path.append('..')
from lib import palo_client
from lib import util
from lib import palo_config

config = palo_config.config
LOG = palo_client.LOG
L = palo_client.L


def setup_module():
    """
    setUp
    """
    pass


def test_base64_random():
    """
    base64 random
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client

    raw_string = string.ascii_letters + string.digits
    raw_bytes = bytes(raw_string, encoding='utf-8')
    base64_string = client.execute('SELECT TO_BASE64("%s")' % (raw_string))[0][0]
    assert base64_string == base64.standard_b64encode(raw_bytes).decode(), \
             'expect %s' % base64.standard_b64encode(raw_string)
    function_string = client.execute('SELECT FROM_BASE64("%s")' % (base64_string))[0][0]
    assert function_string == raw_string, 'expect %s' % raw_string


def test_base64_crypt_random():
    """
    base64 crypt random
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client

    raw_string = string.ascii_letters + string.digits
    key = string.ascii_letters + string.digits
    sql = 'SELECT TO_BASE64(AES_ENCRYPT("%s", "%s"))' % (raw_string, key)
    base64_cypt_string = client.execute(sql)[0][0]
    sql = 'SELECT AES_DECRYPT(FROM_BASE64("%s"), "%s")' % (base64_cypt_string, key)
    function_string = client.execute(sql)[0][0]
    assert function_string == raw_string


def test_base64_special():
    """
    base64 special
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client

    raw_string = ''
    base64_string = client.execute('SELECT TO_BASE64("%s")' % (raw_string))[0][0]
    #assert base64_string == base64.standard_b64encode(raw_string)
    #function_string = client.execute('SELECT FROM_BASE64("%s")' % (base64_string))[0][0]
    #assert function_string == raw_string

    raw_string = u'中文'
    base64_string = client.execute('SELECT TO_BASE64("%s")' % (raw_string))[0][0]
    function_string = client.execute('SELECT FROM_BASE64("%s")' % (base64_string))[0][0]
    assert function_string == raw_string


def test_base64_crypt_special():
    """
    base64 crypt special
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client

    raw_string = ''
    key = string.ascii_letters + string.digits
    sql = 'SELECT TO_BASE64(AES_ENCRYPT("%s", "%s"))' % (raw_string, key)
    base64_cypt_string = client.execute(sql)[0][0]
    sql = 'SELECT AES_DECRYPT(FROM_BASE64("%s"), "%s")' % (base64_cypt_string, key)
    function_string = client.execute(sql)[0][0]
    # assert function_string == raw_string

    raw_string = u'中文'
    key = string.ascii_letters + string.digits
    sql = 'SELECT TO_BASE64(AES_ENCRYPT("%s", "%s"))' % (raw_string, key)
    base64_cypt_string = client.execute(sql)[0][0]
    sql = 'SELECT AES_DECRYPT(FROM_BASE64("%s"), "%s")' % (base64_cypt_string, key)
    function_string = client.execute(sql)[0][0]
    assert function_string == raw_string


def test_base64_crypt_special_key():
    """
    base64 crypt special key
    """
    database_name, table_name, index_name = util.gen_num_format_name_list()
    LOG.info(L('', database_name=database_name, \
        table_name=table_name, index_name=index_name)) 
    client = palo_client.get_client(config.fe_host, config.fe_query_port, user=config.fe_user,
                                    password=config.fe_password, http_port=config.fe_http_port)
    assert client

    raw_string = string.ascii_letters + string.digits
    key = '' 
    sql = 'SELECT TO_BASE64(AES_ENCRYPT("%s", "%s"))' % (raw_string, key)
    base64_cypt_string = client.execute(sql)[0][0]
    sql = 'SELECT AES_DECRYPT(FROM_BASE64("%s"), "%s")' % (base64_cypt_string, key)
    function_string = client.execute(sql)[0][0]
    # assert function_string == raw_string

    raw_string = string.ascii_letters + string.digits
    key = u'中文' 
    sql = 'SELECT TO_BASE64(AES_ENCRYPT("%s", "%s"))' % (raw_string, key)
    base64_cypt_string = client.execute(sql)[0][0]
    sql = 'SELECT AES_DECRYPT(FROM_BASE64("%s"), "%s")' % (base64_cypt_string, key)
    function_string = client.execute(sql)[0][0]
    assert function_string == raw_string

    raw_string = ''
    key = '' 
    sql = 'SELECT TO_BASE64(AES_ENCRYPT("%s", "%s"))' % (raw_string, key)
    base64_cypt_string = client.execute(sql)[0][0]
    sql = 'SELECT AES_DECRYPT(FROM_BASE64("%s"), "%s")' % (base64_cypt_string, key)
    function_string = client.execute(sql)[0][0]
    # assert function_string == raw_string


