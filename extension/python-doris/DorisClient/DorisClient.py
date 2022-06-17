#!/usr/bin/env python3
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

import base64
import json
import logging
import time
import pymysql
import requests


def Logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s', "%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


logger = Logger(__name__)


def retry(func):
    max_retry = 3

    def run(*args, **kwargs):
        for i in range(max_retry + 1):
            if i > 0:
                logger.warning(f"will retry after 3 seconds，retry times : {i}/3")
            time.sleep(3)
            flag = func(*args, **kwargs)
            if flag:
                return flag

    return run


class DorisSession:

    def __init__(self, fe_servers, database, user, passwd, mysql_port=9030):
        """
        :param fe_servers: fe servers list, like: ['127.0.0.1:8030', '127.0.0.2:8030', '127.0.0.3:8030']
        :param database:
        :param user:
        :param passwd:
        :param mysql_port: port for sql client, default:9030
        """
        assert fe_servers
        assert database
        assert user
        assert passwd
        self.fe_servers = fe_servers
        self.database = database
        self.Authorization = base64.b64encode((user + ':' + passwd).encode('utf-8')).decode('utf-8')
        self.conn = pymysql.connect(host=fe_servers[0].split(':')[0],
                                    port=mysql_port,
                                    database=database,
                                    user=user,
                                    passwd=passwd)

    def _label(self, table):
        return f"{table}_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}"

    def _columns(self, keys):
        return ','.join([f'`{column}`' for column in keys])

    def _get_be(self, table, headers):
        for fe_server in self.fe_servers:
            host, port = fe_server.split(':')
            url = f'http://{host}:{port}/api/{self.database}/{table}/_stream_load'
            response = requests.put(url, '', headers=headers, allow_redirects=False)
            if response.status_code == 307:
                return response.headers['Location']
        else:
            raise Exception("No available BE nodes can be obtained. Please check configuration")

    @retry
    def streamload(self, table, json_array, **kwargs):
        """
        :param table: target table
        :param json_array: json_array list
        :param kwargs:
             merge_type：The merge type of data, which supports three types: APPEND, DELETE, and MERGE. Among them,
                         APPEND is the default value, which means that this batch of data needs to be appended to the
                         existing data, and DELETE means to delete all the data with the same key as this batch of data.
                         Line, the MERGE semantics need to be used in conjunction with the delete condition,
                         which means that the data that meets the delete condition is processed according to the DELETE
                         semantics and the rest is processed according to the APPEND semantics,
                         for example: -H "merge_type: MERGE" -H "delete: flag=1"
             delete：Only meaningful under MERGE, indicating the deletion condition of the data function_column.
             sequence_col: Only applicable to UNIQUE_KEYS. Under the same key column,
                           ensure that the value column is REPLACEed according to the source_sequence column.
                           The source_sequence can be a column in the data source or a column in the table structure.
                           for example: -H "function_column.sequence_col: col3"
        :return:
        """
        headers = {
            'Expect': '100-continue',
            'Authorization': 'Basic ' + self.Authorization,
            'format': 'json',
            'strip_outer_array': 'true',
            'fuzzy_parse': 'true',
        }
        if kwargs.get('sequence_col'):
            headers['function_column.sequence_col'] = kwargs.get('sequence_col')
        if kwargs.get('merge_type'):
            headers['merge_type'] = kwargs.get('merge_type')
        if kwargs.get('delete'):
            headers['delete'] = kwargs.get('delete')

        url = self._get_be(table, headers)
        headers['label'] = self._label(table)
        headers['columns'] = self._columns(json_array[0].keys())
        response = requests.put(url, json.dumps(json_array), headers=headers, allow_redirects=False)
        if response.status_code == 200:
            res = response.json()
            if res.get('Status') == 'Success':
                logger.info(res)
                return True
            else:
                logger.error(res)
                return False
        else:
            logger.error(response.text)
            return False

    def execute(self, sql, args=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, args)
            self.conn.commit()
        return True

    def read(self, sql, args=None):
        with self.conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(sql, args)
            return cur.fetchall()

    def __del__(self):
        try:
            self.conn.close()
        except:
            ...
