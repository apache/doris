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
import subprocess


class DorisMiniLoadClient(object):
    """ load file to doris """

    def __init__(self, db_host, db_port, db_name, 
                db_user, db_password, file_name, table, load_timeout):
        """
        init
        :param db_host: db host
        :param db_port: db port
        :param db_name: db name
        :param db_user: db user
        :param db_password: db password
        :param file_name: local file path
        :param table: db table
        :param load_timeout:mini load timeout, defalut 86400 seconds.
        """
        self.file_name = file_name
        self.table = table
        self.load_host = db_host
        self.load_port = db_port
        self.load_database = db_name
        self.load_user = db_user
        self.load_password = db_password
        self.load_timeout = load_timeout

    def get_label(self):
        """
        获取label前缀
        :return: label
        """

        return '_'.join([self.table, os.path.basename(self.file_name)])

    def load_doris(self):
        """
        load file to doris by curl, allow 3 times to retry.
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
                print """Load to doris failed! LABEL is %s, Retry time is %d """ % (label, retry_time)
                retry_time += 1
            # If success, print log, and break retry loop
            if load_subprocess.returncode == 0:
                print """Load to doris success! LABEL is %s, Retry time is %d """ % (label, retry_time)
                break

        return label

    @classmethod
    def check_load_status(cls, label, host, port, user, password, database):
        """
        check async mini load process status.
        :param label:mini load label
        :param host: db host
        :param port: db port
        :param user: db user
        :param password: db password
        :param database: db database
        :return: check async mini load process status.
        """

        db_conn = MySQLdb.connect(host=host,port=port,user=user,passwd=password,db=database)

        db_cursor = db_conn.cursor()
        check_status_sql = "show load where label = '%s' order by CreateTime desc limit 1" % label

        db_cursor.execute(check_status_sql)
        rows = db_cursor.fetchall()

        # timeout config: 60 minutes.
        timeout = 60 * 60

        while timeout > 0:
            if len(rows) == 0:
                print """Load label: %s doesn't exist""" % label
                return 
            load_status = rows[0][2]
            print "mini load status: " + load_status
            if load_status == 'FINISHED':
                print """Async mini load to db success! label is %s""" % label
                break
            if load_status == 'CANCELLED':
                print """Async load to db failed! label is %s""" % label
                break
            timeout = timeout - 5
            time.sleep(5)
            db_cursor.execute(sql)
            rows = db_cursor.fetchall()

        if time_out <= 0:
            print """Async load to db timeout! timeout second is: %s, label is %s""" % (time_out, label)


if __name__ == '__main__':
    """
    mini_load demo.
    There is no need to install subprocess in Python 2.7. It is a standard module that is built in.
    You need input db config & load param.
    """
    db_host = "db_conn_host"
    db_port = "port"
    db_name = "db_name"
    db_user = "db_user"
    db_password = "db_password"
    file_name = "file_name"
    table = "db_table"
    # default load_time_out, seconds
    load_timeout = 86400
    doris_client = DorisMiniLoadClient(
        db_host, db_port, db_name, db_user, db_password, file_name, table, load_timeout)
    doris_client.check_load_status(doris_client.load_doirs(), db_host, db_port, db_user, db_password, db_name)
    print "load to doris end"
