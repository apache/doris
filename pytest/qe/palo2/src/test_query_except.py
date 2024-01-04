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

############################################################################
#
#   @file test_query_except.py
#   @date 2020-04
#   @brief 测试except的相关功能
#
#############################################################################

"""
测试except的相关功能
"""

import random
import sys

sys.path.append('../lib')
from palo_qe_client import QueryBase

test_tb = "test"
baseall_tb = "baseall"
bigtable_tb = "bigtable"


def setup_module():
    """
    init config
    """
    global runner, db
    runner = QueryBase()
    db = runner.query_db


def test_except_base():
    """
    {
    "title": "test_EXCEPT_base",
    "describe": "验证EXCEPT的基础功能，与union做diff",
    "tag": "autotest"
    }
    """
    table_name1 = "{0}.{1}".format(db, baseall_tb)
    table_name2 = "{0}.{1}".format(db, test_tb)

    for i in range(1, 12):
        key = 'k{0}'.format(i)
        print('key: ', key)
        line = """SELECT * FROM (SELECT {2} FROM {0} 
                    EXCEPT SELECT {2} FROM {1}) a ORDER BY {2}
                """.format(table_name1, table_name2, key)
        line2 = """SELECT DISTINCT {2} FROM {0} 
                    where {2} not in (SELECT {2} FROM {1}) ORDER BY {2}
                """.format(table_name1, table_name2, key)
        runner.check2_palo(line, line2)

        line1 = """SELECT * FROM (SELECT {2} FROM {0} 
                    EXCEPT SELECT NULL AS {2} FROM {1}) a ORDER BY {2}
                """.format(table_name1, table_name2, key)
        line2 = """SELECT DISTINCT {1} FROM {0} ORDER BY {1}""".format(table_name1, key)
        runner.check2_palo(line1, line2)

    where_conditions = ['k1 > 3', 'k3 < 0', 'k7 <> "wang"', 'k6="false"']
    for i in range(6):
        key = 'k%s' % random.randint(1, 11)
        condition1 = random.choice(where_conditions)
        condition2 = random.choice(where_conditions)
        print('condition: ', condition1, condition2)
        line = """SELECT * FROM (SELECT {2} FROM {0} WHERE {3}
                    EXCEPT SELECT {2} FROM {1} WHERE {4}) a ORDER BY {2}
                """.format(table_name1, table_name2, key, condition1, condition2)
        line2 = """SELECT DISTINCT {2} FROM {0} 
                    where  {3} AND {2} not in (SELECT {2} FROM {1} WHERE {4}) ORDER BY {2}
                """.format(table_name1, table_name2, key, condition1, condition2)
        runner.check2_palo(line, line2)


def teardown_module():
    """
    end
    """
    print("End")


if __name__ == '__main__':
    setup_module()
    test_except_base()
