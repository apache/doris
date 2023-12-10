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
一个Palo的Task， 
用于测试两类任务的相互影响时，持续执行一类任务
Date: 2015/01/22 10:49:31
"""
import random
import time
import threading
from lib import palo_client


class PaloTask(object):
    """
    所有Task的父类
    """
    def __init__(self, client):
        self.client = client

    def do_task(self):
        """
        执行task, 在子类中实现
        """
        pass

    def wait_task(self):
        """
        等待task执行结束，用于异步任务的状态监控, 需在子类中实现
        """
        pass

    def clean(self):
        """清理task中的残留"""
        pass


class TaskThread(threading.Thread):
    """
    启动一个线程循环去执行task
    """
    def __init__(self, task):
        self._exit_event = threading.Event()
        self.task = task
        threading.Thread.__init__(self)
        self.setDaemon(True)

    def stop(self):
        """
        设置结束标记，会结束所有对象线程
        """
        self._exit_event.set() 

    def run(self):
        """
        启动线程
        """
        while not self._exit_event.is_set():
            self.task.wait_task()
            self.task.do_task()


class SelectTask(PaloTask):
    """
    查询任务
    """
    def __init__(self, host, port, sql, database_name=None, expected_file_path=None,
            user="root", password="", charset="utf8", delay=None, interval=None):
        self.client = palo_client.get_client(host, port, database_name=database_name, user=user,
                                             password=password, charset=charset)
        self.sql = sql
        if database_name is not None:
            self.client.use(database_name)
        self.expected_file_path = expected_file_path
        self.delay = delay
        if interval is None:
            self.interval = 1
        else:
            self.interval = interval

    def do_task(self):
        """
        发送查询
        """
        result = None
        if self.delay is not None:
            try_time = 0
            while try_time < self.delay:
                try:
                    result = self.client.execute(self.sql)
                except:
                    try_time += self.interval
                    time.sleep(self.interval)
                else:
                    break
        else:
            result = self.client.execute(self.sql)

        if self.expected_file_path:
            pass
        return result


class BatchLoadTask(PaloTask):
    """
    连续提交导入任务
    """
    def __init__(self, host, port, database_name, load_label, load_data_list, 
                 max_filter_ratio=None, timeout=None, is_wait=False, interval=None, 
                 user="root", password="", charset="utf8", broker=None):
        self.client = palo_client.get_client(host, port, database_name=database_name, user=user,
                                             password=password, charset=charset)
        self.client.use(database_name)
        self.load_label = load_label
        self.load_num = 0
        self.load_data_list = load_data_list
        self.max_filter_ratio = max_filter_ratio
        self.timeout = timeout
        self.is_wait = is_wait
        self.broker = broker
        if interval is None:
            self.interval = 0
        else:
            self.interval = interval
   
    def do_task(self):
        """
        做导入任务
        """
        load_label = "%s_%d" % (self.load_label, self.load_num)
        ret = self.client.batch_load(load_label, self.load_data_list, 
                                     max_filter_ratio=self.max_filter_ratio, 
                                     timeout=self.timeout, is_wait=self.is_wait, 
                                     broker=self.broker)
        assert ret
        self.load_num += 1
        time.sleep(self.interval)


class BulkLoadTask(PaloTask):
    """
    连续提交小批量导入任务
    """
    def __init__(self, host, port, be_host, webserver_port, database_name, table_family_name,
            load_label, data_file, max_filter_ratio=None, timeout=None, is_wait=False,
            user="root", password="", be_user="root", be_password="", charset="utf8", interval=0):
        self.client = palo_client.get_client(host, port, database_name=database_name, user=user,
                                             password=password, charset=charset)
        self.be_host = be_host
        self.webserver_port = webserver_port
        self.database_name = database_name
        self.table_family_name = table_family_name
        self.load_label = load_label
        self.load_num = 1
        self.data_file = data_file
        self.max_filter_ratio = max_filter_ratio
        self.timeout = timeout
        self.is_wait = is_wait
        self.be_user = be_user
        self.be_password = be_password
        self.interval = interval

    def do_task(self):
        """
        做小批量导入任务
        """
        load_label = "%s_%d" % (self.load_label, self.load_num)
        ret = self.client.bulk_load(self.table_family_name, load_label, self.data_file,
                self.max_filter_ratio, self.timeout, self.database_name, self.be_host,
                self.webserver_port, self.is_wait, user=self.be_user, password=self.be_password) 
        assert ret
        self.load_num += 1
        time.sleep(self.interval)


class RollupTask(PaloTask):
    """
    连续提交
    """
    def __init__(self, host, port, database_name, table_family_name, rollup_table_name,
            rollup_column_name_list, user="root", password="", charset="utf8", **kwargs):
        self.client = palo_client.get_client(host, port, database_name=database_name, user=user,
                                             password=password, charset=charset)
        self.client.use(database_name)
        self.table_family_name = table_family_name
        self.rollup_table_name = rollup_table_name
        self.rollup_num = 1
        self.rollup_column_name_list = rollup_column_name_list
        self.kwargs = kwargs

    def do_task(self):
        """
        做rollup
        """
        rollup_table_name = "%s_%d" % (self.rollup_table_name, self.rollup_num)
        self.rollup_num += 1
        self.client.create_rollup_table(self.table_family_name, rollup_table_name,
                self.rollup_column_name_list, is_wait=True)
       
       
class DeleteTask(PaloTask):
    """
    数据删除任务, 循环使用delete_conditions_list中的删除条件，向palo发送数据删除命令
    """
    def __init__(self, host, port, database_name, table_family_name, delete_conditions_list,
            user="root", password="", charset="utf8", **kwargs):
        """
        Parameters：
            delete_conditions_list：由delete_condition_list(见PaloClient.delete)组成的list
        """
        self.client = palo_client.get_client(host, port, database_name=database_name, user=user,
                                             password=password, charset=charset)
        self.client.use(database_name)
        self.table_family_name = table_family_name
        self.delete_conditions_list = delete_conditions_list
        self.delete_conditions_index = 0
        self.kwargs = kwargs

    def do_task(self):
        """
        执行一次数据删除，delete_conditions_index递增
        """
        delete_condition_list = self.delete_conditions_list[self.delete_conditions_index % \
                len(self.delete_conditions_list)]
        self.delete_conditions_index += 1
        try:
            ret = self.client.delete(self.table_family_name, delete_condition_list, **self.kwargs)
        except palo_client.PaloClientException as error:
            print(str(error))
            #TODO
            pass


class SyncTask(PaloTask):
    """
    执行同步任务，如查询，同步的insert、delete、update等
    todo: 每个任务的参数相同，每个任务的结果不校验
    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.succ_count = 0
        self.error_count = 0
        self.interval = None

    def do_task(self):
        """
        执行一次任务
        """
        try:
            self.func(*self.args, **self.kwargs)
            self.succ_count += 1
        except Exception as e:
            self.error_count += 1
            print(str(e))
        if self.interval is None:
            time.sleep(random.randint(0, 10) / 10.0)
        else:
            time.sleep(self.interval)



