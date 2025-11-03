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
线程池
Date: 2014/08/05 17:19:26
"""

import threading
import queue

from lib import palo_logger
L = palo_logger.StructedLogMessage
LOG = palo_logger.Logger.getLogger() 

# 轮询队列的超时时间
POOL_TIMEOUT = 1.0 


class WorkThread(threading.Thread):
    """
    工作线程，一直处理任务直到任务队列为空
    Attributes:
        name: 线程名字
        task_queue: 任务队列
    """
    def __init__(self, name, task_queue):
        threading.Thread.__init__(self)
        self._task_queue = task_queue
        self._exit_event = threading.Event() 
        self.name = name
        self.setDaemon(True)

    def exit(self):
        """
        设置线程退出标志 
        """
        self._exit_event.set()

    def run(self):
        """
        启动线程
        """
        LOG.info(L("Work thread start.", name=self.name, ident=self.ident))
        while not self._exit_event.is_set():
            try:
                callable_item = self._task_queue.get(block=True, timeout=POOL_TIMEOUT)
                if callable_item:
                    try:
                        callable_item()
                    except Exception as error:
                        LOG.exception(L("Work thread error.", error=error))
                    self._task_queue.task_done()
            except queue.Empty:
                LOG.debug("task queue is empty, wait new task")
            except:
                LOG.error("Work thread error.")
        LOG.info(L("Work thread exit.", name=self.name, ident=self.ident))


class ThreadPool(object):
    """
    一个工作线程池
    Attributes:
        thread_num: 线程个数
    """
 
    def __init__(self, name, thread_num):
        """
        初始化，建立线程池
        """
        self.name = name
        self._thread_num = thread_num
        self._task_queue = queue.Queue()
        self._threads = []
        for i in range(thread_num):
            work_thread = WorkThread("%s_%d" % (self.name, i), self._task_queue)
            work_thread.start()
            self._threads.append(work_thread)
                
    def __del__(self):
        """
        析构时首先停止线程
        """
        for thread in self._threads:
            thread.exit()

        for thread in self._threads:
            thread.join()

    def join(self):
        """
        等待所有的工作项结束, 该函数不会结束线程
        """
        self._task_queue.join()

    def add(self, callable_item, *args, **kwargs):
        """
        向工作队列中增加工作项
        Args:
            callable_item: 一个可以被调用的对象
        """
        self._task_queue.put(callable_item)

