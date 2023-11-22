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
file:stream_test_load_sc_job.py
测试stream load和schema change任务并发
"""
import threading
import os
import sys
import time

file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(file_dir)
file_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(file_dir)
sys.path.append("../")
from lib import palo_config
from lib import palo_client
from lib import palo_task
from lib import util
from lib import palo_job
from data import stream_load as DATA
from data import load_file as FILE

LOG = palo_client.LOG
L = palo_client.L
client = None
config = palo_config.config
user = config.fe_user
password = config.fe_password
host = config.fe_host
query_port = config.fe_query_port
http_port = config.fe_http_port
broker_info = palo_config.broker_info


def setup_module():
    """
    setUp
    """
    global client
    client = palo_client.get_client(host, query_port, user=user, password=password)
    init_check_env()


def init_check_env():
    """for verify"""
    global check_client
    global check_table_name
    check_client = palo_client.get_client(host, query_port, user=user, password=password)
    database_name = 'for_stream_db'
    check_client.clean(database_name)
    check_client.create_database(database_name)
    check_client.use(database_name)
    check_table_name = 'partition_type'
    check_client.execute('drop table if exists %s' % check_table_name)
    distribution_info = palo_client.DistributionInfo('Hash(k1, k2, k3)', 5)
    ret = check_client.create_table(check_table_name, DATA.schema_1, distribution_info=distribution_info,
                        keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)')
    assert ret
    #local_file = DATA.partition_data
    #ret = check_client.bulk_load(check_table_name, util.get_label(), data_file=local_file, 
    #                             is_wait=True)
    data_desc = palo_client.LoadDataInfo(FILE.partition_hdfs_file, check_table_name)
    ret = check_client.batch_load(util.get_label(), data_desc, is_wait=True, broker=broker_info)
    assert ret


def check(palo_sql, mysql_sql):
    """check palo result with mysql result"""
    palo_ret = client.execute(palo_sql)
    mysql_ret = check_client.execute(mysql_sql)
    util.check(palo_ret, mysql_ret)


class BatchLoadTask(palo_task.PaloTask):
    """
    连续提交导入任务, 每隔一段时间提交一个导入任务,提交失败抛出异常
    """
    def __init__(self, host, port, database_name, load_label, load_data_list,
            max_filter_ratio=None, timeout=None, is_wait=False, interval=None,
            user="root", password="", charset="utf8"):
        self.client = palo_client.PaloClient(host, port, user=user,
                password=password, charset=charset)
        self.client.init()
        self.client.use(database_name)
        self.load_label = load_label
        self.load_num = 0
        self.load_data_list = load_data_list
        self.max_filter_ratio = max_filter_ratio
        self.timeout = timeout
        self.is_wait = is_wait
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
                timeout=self.timeout, is_wait=self.is_wait, broker=broker_info)
        if ret:
            self.load_num += 1
        else:
            raise Exception('Batch load error')
        time.sleep(self.interval)


class BatchLoadThread(threading.Thread):
    """
    导入线程
    """
    def __init__(self, database_name, data_desc_list, label, max_filter_ratio=None):
        threading.Thread.__init__(self)
        self.database_name = database_name
        self.data_desc_list = data_desc_list
        self.label = label
        self.max_filter_ratio = max_filter_ratio

    def run(self):
        """
        run
        """
        thread_client = palo_client.PaloClient(host, query_port, self.database_name, user, password)
        thread_client.init()
        thread_client.batch_load(self.label, self.data_desc_list,
                max_filter_ratio=self.max_filter_ratio, is_wait=False, broker=broker_info)


class StreamLoadThread(threading.Thread):
    """
    导入线程,只执行一次导入
    """
    def __init__(self, table_name, data_file, database_name, label=None, max_filter_ratio=None, 
                 column_name_list=None, column_separator=None, partition_list=None, 
                 where_filter=None):
        threading.Thread.__init__(self)
        self.database_name = database_name
        self.table_name = table_name
        self.data_file = data_file
        self.label = label
        self.max_filter_ratio = max_filter_ratio
        self.column_name_list = column_name_list
        self.column_separator = column_separator
        self.partition_list = partition_list
        self.where_filter = where_filter

    def run(self):
        """
        run
        """
        thread_client = palo_client.PaloClient(host, query_port, self.database_name, user, password)
        thread_client.init()
        ret = thread_client.stream_load(table_name=self.table_name,
                                            data_file=self.data_file,
                                            max_filter_ratio=self.max_filter_ratio,
                                            load_label=self.label,
                                            column_name_list=self.column_name_list,
                                            database_name=self.database_name,
                                            host=host,
                                            port=http_port,
                                            column_separator=self.column_separator,
                                            partition_list=self.partition_list,
                                            where_filter=self.where_filter
                                            )
        if ret == 2:
            time.sleep(2)
        # assert ret


class StreamLoadTask(palo_task.PaloTask):
    """
    连续提交导入任务, 每隔一段时间提交一个导入任务,提交失败抛出异常
    """
    def __init__(self, database_name, table_name, data_file, label=None, column_name_list=None, 
                 column_seperator=None, partition_list=None, where_filter=None,
                 max_filter_ratio=None, timeout=None, interval=None):
        self.client = palo_client.PaloClient(host, query_port, user=user,
                                             password=password)
        self.client.init()
        self.client.use(database_name)
        self.label = label
        self.load_num = 0
        self.fail_num = 0
        self.data_file = data_file
        self.max_filter_ratio = max_filter_ratio
        self.timeout = timeout
        self.table_name = table_name
        self.column_name_list = column_name_list
        self.column_separator = column_seperator
        self.partition_list = partition_list
        self.where_filter = where_filter
        self.publish_timeout = False
        if interval is None:
            self.interval = 0
        else:
            self.interval = interval

    def do_task(self):
        """
        做导入任务
        """
        ret = self.client.stream_load(table_name=self.table_name,
                                             data_file=self.data_file,
                                             max_filter_ratio=self.max_filter_ratio,
                                             load_label=self.label,
                                             column_name_list=self.column_name_list,
                                             host=host,
                                             port=http_port,
                                             column_separator=self.column_separator,
                                             partition_list=self.partition_list,
                                             where_filter=self.where_filter
                                             )
        if ret > 0:
            self.load_num += 1
        else:
            self.fail_num += 1
        if ret == 2:
            self.publish_timeout = True
        if self.load_num > 500:
            raise Exception('steam load run too muck task.')
        time.sleep(self.interval)


class AddColumnThread(threading.Thread):
    """add column线程"""
    def __init__(self, table_name, column_list, to_table_name=None, after_column_name=None, 
                 client=None, db_name=None, is_cancel=False):
        threading.Thread.__init__(self)
        self.table_name = table_name
        self.column_list = column_list
        self.alter_column_name = after_column_name
        self.to_table_name = to_table_name
        self.client = client
        self.db_name = db_name
        self.is_cancel = is_cancel

    def run(self):
        """"""
        if self.client is None:
            self.client = palo_client.PaloClient(host, query_port, self.db_name, user, password)
            self.client.init()
        self.client.schema_change_add_column(table_name=self.table_name,
                                             column_list=self.column_list,
                                             after_column_name=self.alter_column_name)
        if self.is_cancel:
            stop_flag = False
            retry_times = 10
            while stop_flag is False and retry_times > 0:
                schema_change_job = self.client.show_schema_change_job(database_name=self.db_name,
                                                                       table_name=self.table_name)
                if len(schema_change_job) == 0:
                       retry_times -= 1
                       LOG.info(L('show alter table column is empty'))
                       time.sleep(1)
                       continue
                for job in schema_change_job:
                    sc = palo_job.SchemaChangeJob(job)
                    state =  sc.get_state()
                    if state == 'RUNNING':
                        stop_flag = True
                        break
                    if state == 'CANCELLED' or state == 'FINISHED':
                        assert 0 == 1
            #    time.sleep(1)
            self.client.cancel_schema_change(table_name=self.table_name,
                                             database_name=self.db_name)


class DropColumnThread(threading.Thread):
    """
    drop column thread
    """
    def __init__(self, table_name, column_name_list, db_name=None, client=None, is_cancel=False):
        threading.Thread.__init__(self)
        self.table_name = table_name
        self.column_name_list = column_name_list
        self.client = client
        self.db_name = db_name
        self.is_cancel = is_cancel

    def run(self):
        if self.client is None:
            self.client = palo_client.PaloClient(host, query_port, self.db_name, user, password)
            self.client.init()
        self.client.schema_change_drop_column(table_name=self.table_name,
                                              column_name_list=self.column_name_list)
        if self.is_cancel:
            stop_flag = False
            retry_times = 10
            while not stop_flag and retry_times > 0:
                schema_change_job = self.client.show_schema_change_job(database_name=self.db_name,
                                                                       table_name=self.table_name) 
                if len(schema_change_job) == 0:
                    time.sleep(1)
                    retry_times -= 1 
                    LOG.info(L('show alter table column is empty'))
                    continue
                for job in schema_change_job:
                    sc = palo_job.SchemaChangeJob(job)
                    state = sc.get_state()
                    if state == 'RUNNING':
                        stop_flag = True
                        break
                    if state == 'CANCELLED' or state == 'FINISHED':
                        print('drop column is over, stop')
                        assert 0 == 1, 'drop column is done'

                # time.sleep(1)
            self.client.cancel_schema_change(table_name=self.table_name,
                                             database_name=self.db_name)


class OrderThread(threading.Thread):
    """
    order thread
    """
    def __init__(self):
        threading.Thread.__init__(self)
        pass


class ModifyColumnThread(threading.Thread):
    """modify column thread"""
    def __init__(self, table_name, column_name, column_type, client=None, db_name=None, 
                 is_cancel=False):
        threading.Thread.__init__(self)
        self.table_name = table_name
        self.column_name = column_name
        self.column_type = column_type
        self.client = client
        self.db_name = db_name
        self.is_cancel = is_cancel

    def run(self):
        if self.client is None:
            self.client = palo_client.PaloClient(host, query_port, self.db_name, user, password)
            self.client.init()
        self.client.schema_change_modify_column(table_name=self.table_name, 
                                                database_name=self.db_name,
                                                column_name=self.column_name,
                                                column_type=self.column_type)

        if self.is_cancel:
            stop_flag = False
            retry_times = 10
            while not stop_flag and retry_times > 0:
                schema_change_job = self.client.show_schema_change_job(database_name=self.db_name,
                                                                       table_name=self.table_name)
                if len(schema_change_job) == 0:
                    time.sleep(1)
                    retry_times -= 1
                    LOG.info(L('show alter table column is empty'))
                    continue
                for job in schema_change_job:
                    sc = palo_job.SchemaChangeJob(job)
                    state = sc.get_state()
                    LOG.info(L('modify job state', state=state))
                    if state == 'RUNNING':
                        stop_flag = True
                        break
                    if state == 'FINISHED' or state == 'CANCELLED':
                        assert 0 == 1
            self.client.cancel_schema_change(table_name=self.table_name,
                                             database_name=self.db_name)


def partition_check(table_name, column_name, partition_name_list,
                    partition_value_list, distribution_type, bucket_num, storage_type):
    """
    检查，验证
    """
    partition_info = palo_client.PartitionInfo(column_name,
                                               partition_name_list, partition_value_list)
    distribution_info = palo_client.DistributionInfo('Hash(k1, k2, k3)', bucket_num)
    client.create_table(table_name, DATA.schema_1, partition_info,
                        distribution_info, keys_desc='AGGREGATE KEY (k1, k2, k3, k4, k5)')
    assert client.show_tables(table_name)


def verify_drop_table(table_name):
    """verify drop table"""
    try:
        client.select_all(table_name)
    except Exception as e:
        assert str(e).find('Unknown table') != -1
        return True


def verify_drop_database(database_name):
    """verify drop database"""
    try:
        db_list = client.get_database_list()
        if database_name in db_list:
            LOG.warning(L('Database should be dropped, but still be shown', database_name=database_name))
            return False
        return True
    except Exception as e:
        LOG.warning(L('GET db list error', msg=str(e)))
        return False


def test_loading_select():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_select",
    "describe": "功能点：导入不影响查询",
    "tag": "p1,system,stability"
    }
    """
    """
    功能点：导入不影响查询
    """

    class SelectTask(palo_task.PaloTask):
        """
        导入线程
        """

        def __init__(self, database_name, table_name):
            self.database_name = database_name
            self.table_name = table_name
            self.thread_client = palo_client.PaloClient(host, query_port, database_name, user, 
                                                        password)
            self.thread_client.init()

        def do_task(self):
            """
            run
            """
            ret = self.thread_client.select_all(self.table_name)
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'Hash(k1, k2, k3)', 13, 'column')
    select_task = SelectTask(database_name, table_name)
    select_thread = palo_task.TaskThread(select_task)
    select_thread.start()

    data_file = DATA.partition_data
    sub_partition_list = partition_name_list[0:3]
    load_thread_1 = StreamLoadThread(table_name, data_file, database_name, max_filter_ratio=0.1, 
                                     partition_list=sub_partition_list)
    load_thread_2 = StreamLoadThread(table_name, data_file, database_name, max_filter_ratio=0.1,
                                     partition_list=sub_partition_list)
    load_thread_3 = StreamLoadThread(table_name, data_file, database_name, max_filter_ratio=0.1)
    load_thread_4 = StreamLoadThread(table_name, data_file, database_name, max_filter_ratio=0.1)
    load_thread_1.start()
    load_thread_2.start()
    load_thread_3.start()
    load_thread_4.start()
    load_thread_1.join()
    load_thread_2.join()
    load_thread_3.join()
    load_thread_4.join()
    assert client.verify(list(DATA.expected_data_file_list_1) * 4, table_name)
    select_thread.stop()
    client.clean(database_name)


def test_loading_drop_table():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_drop_table",
    "describe": "导入的时候,drop table",
    "tag": "p1,stability,system"
    }
    """
    """
    导入的时候,drop table
    """
    class SelectTask(palo_task.PaloTask):
        """
        导入线程
        """

        def __init__(self, database_name, table_name):
            self.database_name = database_name
            self.table_name = table_name
            self.thread_client = palo_client.PaloClient(host, query_port, database_name, user, 
                                                        password)
            self.thread_client.init()
            self.error = None

        def do_task(self):
            """
            run
            """
            try:
                result = self.thread_client.select_all(self.table_name)
            except Exception as e:
                self.error = str(e)
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']
    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'RANDOM', 13, 'column')

    load_thread_list = list()
    data_file = DATA.partition_data
    # 多个导入线程并发, 一直查询结果
    for num in range(0, 10):
        load_thread = StreamLoadThread(table_name, data_file, database_name)
        load_thread_list.append(load_thread)
    for load_thread in load_thread_list:
        load_thread.start()
    select_task = SelectTask(database_name, table_name)
    select_thread = palo_task.TaskThread(select_task)
    select_thread.start()
    client.drop_table(table_name)
    # 多次查询,直到表droppd,查询失败,停止导入
    while select_task.error is None:
        time.sleep(1)
    for load_thread in load_thread_list:
        load_thread.join()
    select_thread.stop()
    assert verify_drop_table(table_name)
    client.clean(database_name)


def test_loading_drop_database():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_drop_database",
    "describe": "导入的时候,drop database",
    "tag": "p1,stability,system"
    }
    """
    """
    导入的时候,drop database
    """
    class SelectTask(palo_task.PaloTask):
        """
        导入线程
        """

        def __init__(self, database_name, table_name):
            self.database_name = database_name
            self.table_name = table_name
            self.thread_client = palo_client.PaloClient(host, query_port, database_name, user, 
                                                        password)
            self.thread_client.init()
            self.error = None

        def do_task(self):
            """
            run
            """
            try:
                result = self.thread_client.select_all(self.table_name)
            except Exception as e:
                self.error = str(e)
            time.sleep(1)
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    # 并发导入任务
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']
    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'RANDOM', 13, 'column')
    data_file = DATA.partition_data
    load_thread_list = list()
    for num in range(0, 10):
        load_thread = StreamLoadThread(table_name, data_file, database_name)
        load_thread_list.append(load_thread)
    for load_thread in load_thread_list:
        load_thread.start()
    select_task = SelectTask(database_name, table_name)
    select_thread = palo_task.TaskThread(select_task)
    select_thread.start()
    client.drop_database(database_name)
    for load_thread in load_thread_list:
        load_thread.join()
    # 多次查询,直到表droppd,查询失败,停止导入,停止查询
    while select_task.error is None:
        time.sleep(1)
    select_thread.stop()
    assert verify_drop_database(database_name)


def test_loading():
    """
    {
    "title": "test_stream_load_sc_job.test_loading",
    "describe": "功能点：并行导入",
    "tag": "p1,stability,system"
    }
    """
    """
    功能点：并行导入
    测试方法：
    1)	建表，初始化数据
    2)	向该表建立10个导入任务
    验证：
    1)	导入均执行成功，且导入的loading部分可以并行
    2)	数据正确
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    # 并发导入任务
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k2)', 13, 'column')
    load_thread_list = list()
    data_file = DATA.partition_data
    for num in range(0, 10):
        load_thread = StreamLoadThread(table_name, data_file, database_name)
        load_thread_list.append(load_thread)
    for load_thread in load_thread_list:
        load_thread.start()
    for load_thread in load_thread_list:
        load_thread.join()
    assert client.verify(list(DATA.expected_data_file_list_1) * 10, table_name)
    client.clean(database_name)


def test_loading_add_column_value():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_add_column_value",
    "describe": "test_loading_add_column_value",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_add_column_value
    测试方法：
    1)	建表，初始化数据
    2)	每隔一段时间建立一个导入任务（比如20秒），直至add column操作结束
    3)	当有loading状态的导入任务时，执行add column操作
    4)  等待add column完成,不再导入,等待所有导入结束
    验证：
    1)	所有导入finished，add column操作正常结束
    2)	验证数据
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    # 持续执行导入
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']
    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k3)', 13, 'column')
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file, 
                               column_name_list=column_name_list)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    # 执行add column操作
    column_list = [('add_v1', 'INT', 'REPLACE', '-1')]
    add_column_thread = AddColumnThread(table_name=table_name, column_list=column_list,
                                        db_name=database_name)
    add_column_thread.start()
    add_column_thread.join()
    # add column操作执行完成后, 停止导入任务
    ret = client.wait_table_schema_change_job(table_name=table_name, database_name=database_name)
    load_thread.stop()
    load_thread.join()
    assert ret
    # 等待所有导入结束后,验证数据
    print('load round: %s' % load_task.load_num)
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    # 新增列的默认值，为-1
    ret = client.desc_table(table_name, database_name)
    assert len(ret) == 12
    sql1 = 'SELECT * FROM %s.%s order by k1' % (database_name, table_name)
    sql2 = 'SELECT k1, k2, k3, k4, k5, v1, v2, v3, v4 * {num}, v5 * {num}, v6 * {num}, -1 \
            FROM {table} order by k1'.format(num=load_task.load_num, table=check_table_name)
    check(sql1, sql2)
    client.clean(database_name)


def test_loading_add_column_key():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_add_column_key",
    "describe": "test_loading_add_column_key",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_add_column_key
    测试方法：
    1)  建表，初始化数据
    2)  每隔一段时间建立一个导入任务（比如20秒），直至add column操作结束
    3)  当有loading状态的导入任务时，执行add column操作
    4)  等待add column完成,不再导入,等待所有导入结束
    验证：
    1)  所有导入finished，add column操作正常结束
    2)  验证数据
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    # 持续执行导入
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']
    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'RANDOM', 13, 'column')
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file, 
                               column_name_list=column_name_list)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    # add key column
    column_list = [('add_k6', 'INT', None, '-1')]
    add_column_thread = AddColumnThread(table_name=table_name, column_list=column_list,
                                        after_column_name='k4', db_name=database_name)
    add_column_thread.start()
    add_column_thread.join()
    # add column操作执行完成后, 停止导入任务
    ret = client.wait_table_schema_change_job(table_name=table_name, database_name=database_name)
    load_thread.stop()
    load_thread.join()
    assert ret
    print(load_task.load_num)
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    # 修改verify,加了一列
    ret = client.desc_table(table_name, database_name)
    assert len(ret) == 12
    sql1 = 'SELECT * FROM %s.%s order by k1' % (database_name, table_name)
    sql2 = 'SELECT k1, k2, k3, k4, -1, k5, v1, v2, v3, v4 * {num}, v5 * {num}, v6 * {num} FROM {table} \
            order by k1'.format(num=load_task.load_num, table=check_table_name)
    check(sql1, sql2)
    client.clean(database_name)


def test_loading_add_column_cancel():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_add_column_cancel",
    "describe": "test_loading_add_column_cancel",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_add_column_cancel
    测试方法：
    1)	建表，初始化数据
    2)	每隔一段时间建立一个导入任务（比如20秒），直至add column操作结束
    3)	当有loading状态的导入任务时，执行add column操作
    4)  当alter job为running状态时, 执行cancel操作,取消add column操作
    验证：
    1)	add column操作正常结束
    2)	验证数据
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    # 持续执行导入
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k1)', 13, 'column')
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    # add column and cancel
    column_list = [('add_v1', 'INT', 'SUM', '0')]
    add_column_thread = AddColumnThread(table_name=table_name, column_list=column_list,
                                        db_name=database_name, is_cancel=True)
    add_column_thread.start()
    add_column_thread.join()
    # 等待add column操作cancelled后,停止导入
    ret = client.wait_table_schema_change_job(table_name=table_name, database_name=database_name)
    load_thread.stop()
    load_thread.join()
    assert ret is False
    print(load_task.load_num)
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    # 验证表的schema 不变
    ret = client.desc_table(table_name, database_name)
    assert len(ret) == 11
    client.clean(database_name)


def test_loading_drop_column():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_drop_column",
    "describe": "test_loading_drop_column",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_drop_column
    同add column
    table中有replace类型value列，不能drop key列
    """
    # create db & table
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k3)', 13, 'column')
    # 持续导入
    column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file, 
                               column_name_list=column_name_list)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    # drop column
    column_list = ['v1', 'v2', 'v5']
    drop_column_thread = DropColumnThread(table_name=table_name, column_name_list=column_list, 
                                          db_name=database_name)
    drop_column_thread.start()
    drop_column_thread.join()
    # drop column结束后，停止导入
    ret = client.wait_table_schema_change_job(table_name=table_name, database_name=database_name)
    time.sleep(5)
    load_thread.stop()
    load_thread.join()
    assert ret
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    # verify file should be changed, drop columns v1, v2
    sql1 = 'SELECT * FROM %s.%s order by k1' % (database_name, table_name)
    sql2 = 'SELECT k1, k2, k3, k4, k5, v3, v4 * {num}, v6 * {num} FROM {table} \
            order by k1'.format(num=load_task.load_num, table=check_table_name)
    check(sql1, sql2)
    ret = client.desc_table(table_name, database_name)
    assert len(ret) == 8
    client.clean(database_name)


def test_loading_drop_column_cancel():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_drop_column_cancel",
    "describe": "test_loading_drop_column_cancel",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_drop_column_cancel
    同add column cancel
    """
    # create db & table
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k3)', 13, 'column')
    # 持续导入
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    # drop column 并cancel 
    column_list = ['v1', 'v2']
    drop_column_thread = DropColumnThread(table_name=table_name, column_name_list=column_list, 
                                          db_name=database_name, is_cancel=True)
    drop_column_thread.start()
    drop_column_thread.join()
    ret = client.wait_table_schema_change_job(table_name=table_name, database_name=database_name)
    load_thread.stop()
    load_thread.join()
    assert ret is False
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    # verify file should be changed, drop columns v1, v2
    ret = client.desc_table(table_name, database_name)
    assert len(ret) == 11
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    client.clean(database_name)


def test_loading_modify_column():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_modify_column",
    "describe": "test_loading_modify_column",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_modify_column
    同add column
    """
    # create db & table
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k1)', 13, 'column')
    # 持续导入
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    # modify column
    modify_column_thread = ModifyColumnThread(table_name=table_name, column_name='v2', 
                                              column_type='CHAR(10)', db_name=database_name)
    modify_column_thread.start()
    modify_column_thread.join()
    ret = client.wait_table_schema_change_job(table_name=table_name, database_name=database_name)
    load_thread.stop()
    load_thread.join()
    assert ret
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    # 校验数据,验证schema
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    ret = client.desc_table(table_name, database_name)
    for column_info in ret:
        if column_info[0] == 'v2':
            assert column_info[1] == 'CHAR(10)'
    client.clean(database_name)


def test_loading_modify_cancel():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_modify_cancel",
    "describe": "test_loading_modify_cancel",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_modify_cancel
    同add column cancel
    """
    # create db & table
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k1)', 13, 'column')
    ret1 = client.desc_table(table_name, database_name)
    # modify column & cancel
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    modify_column_thread = ModifyColumnThread(table_name=table_name, column_name='v2', 
                                              column_type='CHAR(10)', is_cancel=True, 
                                              db_name=database_name)
    modify_column_thread.start()
    modify_column_thread.join()
    ret = client.wait_table_schema_change_job(table_name=table_name, database_name=database_name)
    load_thread.stop()
    load_thread.join()
    assert ret is False
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    # 验证数据，和表结构
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    ret2 = client.desc_table(table_name, database_name)
    util.check(ret1, ret2)
    client.clean(database_name)


class CreateRollupThread(threading.Thread):
    """create rollup thread"""
    def __init__(self, table_name, rollup_name, column_name_list, db_name=None, client=None, 
                 is_cancel=False):
        threading.Thread.__init__(self)
        self.table_name = table_name
        self.rollup_name = rollup_name
        self.column_name_list = column_name_list
        self.client = client
        self.db_name = db_name
        self.is_cancel = is_cancel

    def run(self):
        if self.client is None:
            self.client = palo_client.PaloClient(host, query_port, self.db_name, user, password)
            self.client.init()
        self.client.create_rollup_table(table_name=self.table_name,
                                        rollup_table_name=self.rollup_name,
                                        column_name_list=self.column_name_list,
                                        database_name=self.db_name)
        if self.is_cancel:
            while True:
                schema_change_job = self.client.show_rollup_job(database_name=self.db_name,
                                                    table_name=self.table_name)
                job = palo_job.RollupJob(schema_change_job[-1])
                state = job.get_state()
                if state == 'RUNNING':
                    self.client.cancel_rollup(table_name=self.table_name,
                                              database_name=self.db_name)
                    break
                elif state == 'FINISHED' or state == 'CANCELLED':
                    break
                else:
                    pass
                time.sleep(3)


def test_loading_create_rollup():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_create_rollup",
    "describe": "test_loading_create_rollup",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_create_rollup 同add column
    create rollup后，导入失败cancelled，需更新dpp
    """
    # create db & table
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k1)', 13, 'column')
    # 持续导入
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    # create rollup
    rollup_column_list = ['k2', 'k3', 'v5', 'v4']
    create_rollup_thread = CreateRollupThread(table_name=table_name,
                                              rollup_name=index_name,
                                              column_name_list=rollup_column_list,
                                              db_name=database_name)
    create_rollup_thread.start()
    create_rollup_thread.join()
    # rollup结束后，停止导入任务
    ret = client.wait_table_rollup_job(table_name=table_name, database_name=database_name)
    load_thread.stop()
    load_thread.join()
    assert ret
    print(load_task.load_num)
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    # 验证表的rollup
    assert index_name in client.get_index_list(table_name, database_name)
    client.clean(database_name)


def test_loading_add_rollup_cancel():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_add_rollup_cancel",
    "describe": "test_loading_add_rollup_cancel",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_add_rollup_cancel 同add column cancel
    """
    # create db & table
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k1)', 13, 'column')
    # 持续导入
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    time.sleep(1)
    # create rollup & cancel
    rollup_column_list = ['k2', 'k3', 'v5', 'v4']
    create_rollup_thread = CreateRollupThread(table_name=table_name,
                                              rollup_name=index_name,
                                              column_name_list=rollup_column_list,
                                              db_name=database_name, is_cancel=True)
    create_rollup_thread.start()
    create_rollup_thread.join()
    client.wait_table_rollup_job(table_name=table_name, database_name=database_name)
    load_thread.stop()
    load_thread.join()
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    # 验证表的rollup
    assert index_name not in client.get_index_list(table_name, database_name)
    client.clean(database_name)


class DropRollupThread(threading.Thread):
    """Drop Rollup thread"""
    def __init__(self, table_name, rollup_name, db_name=None, client=None):
        threading.Thread.__init__(self)
        self.table_name = table_name
        self.rollup_name = rollup_name
        self.client = client
        self.db_name = db_name

    def run(self):
        if self.client is None:
            self.client = palo_client.PaloClient(host, query_port, self.db_name, user, password)
            self.client.init()
        self.client.drop_rollup_table(table_name=self.table_name,
                                      rollup_table_name=self.rollup_name,
                                      database_name=self.db_name)


def test_loading_drop_rollup():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_drop_rollup",
    "describe": "test_loading_drop_rollup",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_drop_rollup 同add column
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'Hash(k1)', 13, 'column')
    ret = client.create_rollup_table(table_name=table_name,
                                    rollup_table_name=index_name,
                                    column_name_list=['k2', 'v4'],
                                    database_name=database_name, is_wait=True)
    assert ret
    assert index_name in client.get_index_list(table_name, database_name)
    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    drop_rollup_thread = DropRollupThread(table_name=table_name,
                                            rollup_name=index_name,
                                            db_name=database_name)
    drop_rollup_thread.start()
    drop_rollup_thread.join()
    # 30s后停止load
    time.sleep(30)
    load_thread.stop()
    load_thread.join()
    # assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    print('have some stream load job failed, check log. fail num is %s' % load_task.fail_num)
    if load_task.publish_timeout:
        time.sleep(300)
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    assert index_name not in client.get_index_list(table_name, database_name)
    client.clean(database_name)


class TaskJob(palo_task.TaskThread):
    """palo task job"""
    def run(self):
        """
        启动线程
        """
        while not self._exit_event.is_set():
            try:
                self.task.wait_task()
                self.task.do_task()
            except Exception as e:
                print(str(e))
                self.stop()


def test_loading_rename_table():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_rename_table",
    "describe": "test_loading_rename_table",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_rename_table 同 add column
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k1)', 13, 'column')

    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = TaskJob(load_task)
    load_thread.start()
    time.sleep(10)
    new_table_name = 'new_' + table_name
    ret = client.rename_table(new_table_name, table_name, database_name)
    assert ret
    if not ret:
        load_thread.stop()
        assert ret
    # 等待60s如果还活着就stop loading进程, 该时间值需要确定
    # 预期rename table成功后,loading中后面的导入会失败
    time.sleep(50)
    if load_thread.is_alive():
        print('stop load thread')
        load_thread.stop()
        load_thread.join()
    assert load_task.fail_num != 0, 'expect some stream load failed'
    if load_task.publish_timeout:
        time.sleep(300)
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, new_table_name)
    client.clean(database_name)


def test_loading_rename_rollup():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_rename_rollup",
    "describe": "test_loading_rename_rollup",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_rename_rollup 同 add column
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k1)', 13, 'column')
    ret = client.create_rollup_table(table_name=table_name,
                                     rollup_table_name=index_name,
                                     column_name_list=['k2', 'v6'],
                                     database_name=database_name, is_wait=True)
    assert ret

    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    new_rollup_name = 'new_' + index_name
    time.sleep(1)
    ret = client.rename_rollup(new_rollup_name, index_name, table_name, database_name)
    if not ret:
        load_thread.stop()
        load_thread.join()
        assert ret
    # 判断什么时候停止load
    timeout = 600
    while client.get_index(table_name, new_rollup_name, database_name) is None and timeout > 0:
        time.sleep(1)
        timeout -= 1
    if timeout == 0:
        assert 0 == 1, 'can not get rename rollup index name'
    load_thread.stop()
    load_thread.join()
    if load_task.publish_timeout:
        time.sleep(300)
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    # 等待所有导入结束
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    client.clean(database_name)


def test_loading_rename_partition():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_rename_partition",
    "describe": "test_loading_rename_partition",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_rename_partition 同 add column
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'hash(k1)', 13, 'column')

    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    time.sleep(1)
    new_partition_name = 'rename_partition'
    ret = client.rename_partition(new_partition_name, 'partition_b', table_name, database_name)
    if not ret:
        load_thread.stop()
        load_thread.join()
        assert ret
    # 判断什么时候停止load
    while client.get_partition(table_name, new_partition_name, database_name) is None:
        time.sleep(1)
    load_thread.stop()
    load_thread.join()
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        time.sleep(300)
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    client.clean(database_name)


def test_loading_add_parititon():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_add_parititon",
    "describe": "test_loading_add_parititon",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_add_parititon同add column
    增加分区后,即可访问,非异步过程
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c']
    partition_value_list = ['5', '10', '20']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'HASH(k1)', 13, 'column')

    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file, max_filter_ratio=0.5)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    time.sleep(1)
    added_partition = 'add_partition'
    print(added_partition)
    ret = client.add_partition(table_name=table_name, partition_name=added_partition,
                               value='31', database_name=database_name)
    if not ret:
        load_thread.stop()
        load_thread.join()
        assert ret
    # 判断什么时候停止load
    while client.get_partition(table_name, added_partition, database_name) is None:
        time.sleep(1)
    print(load_task.load_num)
    time.sleep(10)
    print('stop load')
    load_thread.stop()
    load_thread.join()
    print(load_task.load_num)
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    if load_task.publish_timeout:
        LOG.info(L('STREAM LOAD RETURN PUBLISH TIMEOUT. waiting...'))
        time.sleep(300)
    print(load_task.load_num)
    sql1 = 'SELECT * FROM %s.%s where k1 < 20 order by k1' % (database_name, table_name)
    sql2 = 'SELECT k1, k2, k3, k4, k5, v1, v2, v3, v4 * {num}, v5 * {num}, v6 * {num} FROM {table} \
            where k1 < 20 order by k1'.format(num=load_task.load_num, table=check_table_name)
    check(sql1, sql2)

    client.clean(database_name)


def test_loading_drop_partition():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_drop_partition",
    "describe": "test_loading_drop_partition",
    "tag": "p1,stability,system"
    }
    """
    """
    test_loading_drop_partition同add column
    drop partition后 load caccelled，需更新dpp
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)

    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['5', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'Hash(k1)', 13, 'column')

    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file, max_filter_ratio=0.5)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    time.sleep(1)
    dropped_partition = 'partition_d'
    ret = client.drop_partition(table_name=table_name, partition_name=dropped_partition,
                                database_name=database_name)
    if not ret:
        load_thread.stop()
        load_thread.join()
        assert ret
    # 判断什么时候停止load
    while client.get_partition(table_name, dropped_partition, database_name):
        time.sleep(1)
    load_thread.stop()
    load_thread.join()
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    assert client.verify(list(DATA.expected_data_file_list_1) * load_task.load_num, table_name)
    client.clean(database_name)


def test_loading_sync_delete():
    """
    {
    "title": "test_stream_load_sc_job.test_loading_sync_delete",
    "describe": "执行delete from，show delete返回空，该部分元数据可能会修改",
    "tag": "p1,stability,system"
    }
    """
    """
    delete from
    执行delete from，show delete返回空，该部分元数据可能会修改
    """
    database_name, table_name, index_name = util.gen_name_list()
    LOG.info(L('', database_name=database_name,
               table_name=table_name,
               index_name=index_name))
    client.clean(database_name)
    client.create_database(database_name)
    client.use(database_name)
    partition_name_list = ['partition_a', 'partition_b', 'partition_c', 'partition_d']
    partition_value_list = ['10', '20', '31', 'MAXVALUE']

    partition_check(table_name, 'k1',
                    partition_name_list,
                    partition_value_list,
                    'Hash(k1)', 13, 'column')

    data_file = DATA.partition_data
    load_task = StreamLoadTask(database_name, table_name, data_file, max_filter_ratio=0.5)
    load_thread = palo_task.TaskThread(load_task)
    load_thread.start()
    time.sleep(3)
    delete_condition_list = [('k1', '>', '0')]
    partition_name = 'partition_a'
    ret = client.delete(table_name=table_name, delete_conditions=delete_condition_list,
                        partition_name=partition_name)
    print('after delete')
    load_thread.stop()
    load_thread.join()
    assert ret
    assert load_task.fail_num == 0, 'have some stream load job failed, check log'
    # check data
    client.execute('select * from %s' % table_name)
    ret = client.execute('show delete');
    assert len(ret) == 1
    state = palo_job.DeleteJob(ret[0]).get_state()
    assert ret[0][-1] == 'FINISHED'
    sql1 = 'SELECT * FROM %s.%s where k1 > 10 order by k1' % (database_name, table_name)
    sql2 = 'SELECT k1, k2, k3, k4, k5, v1, v2, v3, v4 * {num}, v5 * {num}, v6 * {num} FROM {table} \
            where k1 > 10 order by k1'.format(num=load_task.load_num, table=check_table_name)
    check(sql1, sql2)

    client.clean(database_name)


def teardown_module():
    """
    tearDown
    """
    check_client.clean('for_stream_db')


if __name__ == '__main__':
    setup_module()
    test_loading_select()


