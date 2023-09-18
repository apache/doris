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
add query case about bool
"""
import sys
sys.path.append("../lib/")
from palo_qe_client import QueryBase

table_name = "bool_tb"


def setup_module():
    """setup"""
    global runner
    runner = QueryBase()
    init_table()


def init_table():
    """create bool table and insert"""
    runner.init('DROP TABLE IF EXISTS %s' % table_name)
    p_sql = 'CREATE TABLE %s (k1 tinyint, k2 boolean, k3 boolean replace) DISTRIBUTED BY HASH(k1)' \
            ' BUCKETS 5' % table_name
    m_sql = 'CREATE TABLE %s (k1 tinyint, k2 boolean, k3 boolean)' % table_name
    runner.init(p_sql, m_sql)
    sql = 'insert into %s select k1, True, False from baseall' % table_name
    runner.init(sql)
    return True
    
    
def test_boolean_expression():
    """
    {
    "title": "test_boolean_expression",
    "describe": "验证boolean类型的各种表达式计算结果正确",
    "tag": "function,p1,fuzz"
    }
    """
    line = 'select k2 and k2, k2 or k2, not k2 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k2 and k3, k2 or k3 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k3 and k3, k3 or k3, not k3 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k2 + k3, k2 - k3, k2 / k3, k2 * k3 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k2 ^ k3, k2 | k3, k2 & k3, k2 %% k3 from %s order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k2 = 1 order by k1' % table_name
    runner.check(line) 
    line = 'select * from %s where k3 = 0 order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k2 != 1 order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k2 > 0 order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k2 < 2 order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k3 != 0 order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k3 > -1 order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k3 < 1 order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k2 <> 0 order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k3 <> 1 order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k2 = true and k3 = false order by k1' % table_name
    runner.check(line) 
    line = 'select * from %s where k2 = "true" or k3 = "false" order by k1' % table_name
    runner.checkwrong(line)
    
    
def test_boolean_function():
    """
    {
    "title": "test_boolean_function",
    "describe": "验证boolean类型在相应函数中结果正确",
    "tag": "function,p1,fuzz"
    }
    """
    line = 'select * from %s where k3 in (0, 2, 3, 4) and k2 in (0, 1, 2, 3) order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k2 like "true" order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k2 not like "true" order by k1' % table_name
    runner.check(line)
    line = 'select * from %s where k2 between 0 and 1 order by k1' % table_name
    runner.check(line)
    line = 'select k1, case when k2 then "True" when not k3 then "False" end c1 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k1, case when not k3 then "True" when k2 then "False" end c1 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k1, case when not k2 then "True" when not k3 then "False" end c1 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k1, case when k2 and k3 then "True" else "False" end c1 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k1, case k2 when true then 100 when false then -100 end c1 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k1, case k3 when true then 100 when false then -100 end c1 from %s order by k1' % table_name
    runner.check(line)
    line = 'select k1, if(k2, 1, 0) b, if(k3, 1, 0) c from %s order by k1' % table_name
    runner.check(line)
    line = 'select k1, nullif(k3, 100), nullif(k2, -100) from %s order by k1' % table_name
    runner.check(line)
    line = 'select k1, ifnull(k3, 100), ifnull(k2, -100) from %s order by k1' % table_name
    runner.check(line)
    line = 'select k1, coalesce(k2, k3) b from %s order by k1' % table_name
    runner.check(line)
    
    
def teardown_module():
    """teardown"""
    pass
    
    
