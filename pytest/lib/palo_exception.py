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
所有自定义Exception的基类
"""


class PaloException(Exception):
    """Benchmark所有的异常的基类

    Attributes:
        message: 关于异常的说明
        kwargs: 给异常附带的参数 
    Example:
        为了方便后续的日志等格式化处理，在抛出异常时都附带一段解释性说明字符串，
        并附带可选的一组Key-value值。所有的说明字符串不要使用格式化内容，而是
        使用固定的内容。例如：

        DBException(PaloException):
            pass 

        Good:
        raise DBException("Connect to database fail.", host=HOST, port=Port)

        Bad:
        raise DBException("Connect to database fail. host=%s, port=%d"
            % (HOST, PORT))
    """

    def __init__(self, message="", **kwargs):
        self.message = message
        self.kwargs = kwargs

    def __repr__(self):
        return "%s(message=%r, **%r)" % (
                self.__class__.__name__, self.message, self.kwargs)

    def __str__(self):
        return "%s %s" % (self.message, self.kwargs)


class PaloClientException(PaloException):
    """palo client exception"""
    pass