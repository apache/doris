#!/usr/bin/env python
# encoding: utf-8

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

class DorisColumnItem:
    def __init__(self, col_name, col_type, col_comment, col_default):
        self._col_name = col_name
        self._col_type = col_type
        self._col_comment = col_comment
        self._col_default = col_default

    def get_col_name(self):
        return self._col_name

    def get_col_type(self):
        return self._col_type

    def get_col_comment(self):
        return self._col_comment

    def get_col_default(self):
        return self._col_default

    def get_view_column_constraint(self):
        res = ""
        if self._col_comment != "":
            res  = f"`{self._col_name}` COMMENT '{self._col_comment}'"
        else:
            res = f"`{self._col_name}`"
        return res

    def get_table_column_constraint(self):
        res = ""
        if self._col_type is not None:
            res = f"cast(`{self._col_name}` as {self._col_type}) as `{self._col_name}`"
        else:
            res = f"`{self._col_name}`"
        return res
