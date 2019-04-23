#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import os
import time
import subprocess
import requests
import json
from requests.auth import HTTPBasicAuth
import logging

logger = logging.getLogger("root")


class PaloMiniLoadClient(object):
    """ load file to palo """

    def __init__(self, mini_load_host, mini_load_port, db_name, 
                db_user, db_password, file_name, table, load_timeout):
        """
        init
        :param mini_load_host:mini_load服务地址
        :param mini_load_port:mini_load服务端口号
        :param db_name:数据库名称
        :param db_user:数据库用户名
        :param db_password:数据库密码
        :param file_name:待load的本地文件path
        :param table:数据库表名，向该表load数据
        :param load_timeout:mini load超时时间
        """
        self.file_name = file_name
        self.table = table
        self.load_host = mini_load_host
        self.load_port = mini_load_port
        self.load_database = db_name
        self.load_user = db_user
        self.load_password = db_password
        self.load_timeout = load_timeout

    def get_label(self):
        """
        获取label前缀
        :return: lable
        """

        return '_'.join([self.table, os.path.basename(self.file_name)])

    def load_palo_request(self):
        """
        WARN: This func won't work if doris upgrade curl's implement.
        load file to palo by python-requests, allow 3 times to retry
        """

        retry_time = 0
        label = self.get_label()

        while retry_time < 3:
            upload_url = """http://%s:%s/api/%s/%s/_load""" % (self.load_host, self.load_port,
                                                               self.load_database, self.table)
            payload = {'label': label}
            upload_file = open(self.file_name, 'rb')
            headers = {'Expect': '100-continue', 'content-type': 'text/plain'}

            load_session = requests.session()
            load_session.auth = HTTPBasicAuth(self.load_user, self.load_password)
            load_session.headers = headers
            initial_response = load_session.put(upload_url,
                                                data=upload_file,
                                                params=payload,
                                                headers=headers,
                                                allow_redirects=False)

            response = initial_response
            if initial_response.status_code == 307:
                redirect_url = initial_response.headers["location"]
                redirect_response = load_session.put(redirect_url,
                                                     data=upload_file,
                                                     params=payload)
                response = redirect_response

            if (response.status_code == requests.codes.ok
                    and json.loads(response.content)['status'] == 'Success'
                    and json.loads(response.content)['msg'] == 'OK'):
                logger.info("Load to palo success! LABEL is %s, Retry time is %d ",
                            label, retry_time)
                break
            else:
                logger.error("Load to palo failed! LABEL is %s, Retry time is %d ",
                             label, retry_time)
            retry_time += 1

        return label

    def load_palo(self):
        """
        load file to palo by curl, allow 3 times to retry.
        :return: mini load label
        """
        retry_time = 0
        label = self.get_label()

        while retry_time < 3:
            load_cmd = "curl"
            param_location = "--location-trusted"
            param_user = "%s:%s" % (self.load_user, self.load_password)
            param_file = "%s" % self.file_name
            param_url = "http://%s:%s/api/%s/%s/_load?label=%s&timeout=" % (self.load_host, self.load_port,
                                                                   self.load_database,
                                                                   self.table, label, self.load_timeout)

            load_subprocess = subprocess.Popen([load_cmd, param_location,
                                                "-u", param_user, "-T", param_file, param_url])

            # Wait for child process to terminate.  Returns returncode attribute
            load_subprocess.wait()

            # check returncode;
            # If fail, retry 3 times
            if load_subprocess.returncode != 0:
                logger.error("Load to palo failed! LABEL is %s, Retry time is %d ",
                             label, retry_time)
                retry_time += 1
            # If success, print log, and break retry loop
            if load_subprocess.returncode == 0:
                logger.info("Load to palo success! LABEL is %s, Retry time is %d ",
                            label, retry_time)
                break

        return label

    @classmethod
    def check_load_status(cls, label, db_conn):
        """
        check async palo load process status.
        :param label:mini_load label
        :param db_conn:数据库连接
        :return:
        """

        check_status_sql = "show load where label = '%s' order by CreateTime desc limit 1" % label

        # timeout config: 60 minutes.
        time_out = 60 * 60

        while time_out > 0:
            load_status_data = db_conn.execute(check_status_sql).data

            if load_status_data is None or len(load_status_data) == 0:
                logger.error("Load label: %s doesn't exist", label)
                return

            load_status = load_status_data[0]['State']
            if load_status == 'FINISHED':
                logger.info("Async load to palo success! label is %s", label)
                break

            if load_status == 'CANCELLED':
                logger.error("Async load to palo failed! label is %s", label)
                break

            time_out -= 5
            time.sleep(5)

        if time_out <= 0:
            logger.warning("Async load to palo timeout! timeout second is: %s, label is %s",
                           time_out, label)

