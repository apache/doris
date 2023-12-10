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
log模块，palo测试所有日志通过该模块打印
Date: 2014/11/24 17:19:26
"""

import logging
import os
import json
import pprint

class Logger(object):
    """
    定义了一个log，包含了所有需要的log输出方式
    """

    logger = None
    title_format = "[%(levelname)s][%(asctime)s][%(thread)d]"
    body_format = "[%(filename)s:%(lineno)s][%(funcName)s] %(message)s"
    format = "%s%s" % (title_format, body_format) 
    pathList = ['./log/palo_test.log']
    logLevel = logging.INFO
    stdLogLevel = logging.ERROR

    @classmethod
    def ensureDir(cls, dirName):
        """
        确定输入目录存在，如果没有就创建
        Attributes:
            dirName: 日志输入位置
        """
        ret = 0
        if not dirName.strip():
            ret = 0
        elif not os.path.exists(dirName):
            ret = cls.ensureDir(os.path.dirname(dirName))
            if 0 == ret:
                try:
                    os.mkdir(dirName)
                except Exception as error:
                    ret = -1
                    print("mkdir %s fail, Exception : %s" % (dirName, str(error)))
        elif not os.path.isdir(dirName):
            ret = -1
            print("%s exists, but not dir" % dirName)
        return ret

    @classmethod
    def initLogger(cls):
        """
        初始化日志模块
        """
        if Logger.logger is None:
            Logger.logger = logging.getLogger("GTTS")
            Logger.logger.setLevel(Logger.logLevel)
            logFormat = logging.Formatter(Logger.format)
            for filePath in Logger.pathList:
                if "std" != filePath:
                    ret = cls.ensureDir(os.path.dirname(filePath))
                    if 0 != ret:
                        continue
                    if not Logger.logger.handlers:
                        handler = logging.FileHandler(filePath, encoding='utf-8')
                        handler.setFormatter(logFormat)
                        Logger.logger.addHandler(handler)
                else:
                    handler = logging.StreamHandler()
                    handler.setFormatter(logFormat)
                    handler.setLevel(Logger.stdLogLevel)
                    Logger.logger.addHandler(handler)

    @classmethod
    def getLogger(cls):
        """
        获得一个log模块
        """
        if Logger.logger is None:
            cls.initLogger()
        return Logger.logger

    @classmethod
    def setLogFormat(cls, format):
        """
        设置log的输出格式
        Attributes:
            format: log输出格式
        """
        Logger.format = format

    @classmethod
    def addLogPath(cls, path):
        """
        添加日志输出位置
        """
        Logger.pathList.append(path)

    @classmethod
    def setLogPath(cls, path):
        """
        设置日志输出位置
        """
        Logger.pathList = [path]


class StructedLogMessage(object):
    """
    定义一种日志消息
    可以支持格式化打印日志
    """
    def __init__(self, message, **kwargs):
        self.message = message
        self.kwargs = kwargs

    def _pretty(self, data, indent=0):
        """
        支持格式化打印日志
        """
        result = ""
        TAB = "  "
        if isinstance(data, dict):
            result += '{\n'
            for key, value in data.iteritems():
                result += TAB * indent
                result += '%s: ' % str(key) + self._pretty(value, indent + 1) 
                result += '\n'
            result += TAB * (indent - 1) + '}'
        elif isinstance(data, list):
            for val in data:
                result += '[\n'
                result += TAB * indent + self._pretty(val, indent + 1) + '\n'
                result += TAB * (indent - 1) + ']\n'
        else:
            result += str(data)
        return result                

    def __str__(self):
        """
        支持普通的字符串形式
        """
        return "[%s][%s]" % (self.message, str(self.kwargs))
 
