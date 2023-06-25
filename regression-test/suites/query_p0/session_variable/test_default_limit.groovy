// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite('test_default_limit') {
    sql 'use test_query_db'

    def res = sql 'select * from baseall'
    assertTrue(res.size() == 16)

    sql 'set default_order_by_limit = 10'
    sql 'set sql_select_limit = 5'

    res = sql 'select * from baseall'
    assertTrue(res.size() == 5)
    res = sql 'select * from baseall order by k1'
    assertTrue(res.size() == 5)
    res = sql 'select * from baseall limit 7'
    assertTrue(res.size() == 7)

    sql 'set default_order_by_limit = 5'
    sql 'set sql_select_limit = 10'

    res = sql 'select * from baseall'
    assertTrue(res.size() == 10)
    res = sql 'select * from baseall order by k1'
    assertTrue(res.size() == 5)
    res = sql 'select * from baseall limit 7'
    assertTrue(res.size() == 7)

    sql 'set sql_select_limit = -1'

    res = sql 'select * from baseall'
    assertTrue(res.size() == 16)
    res = sql 'select * from baseall limit 7'
    assertTrue(res.size() == 7)

    sql 'set sql_select_limit = -10'

    res = sql 'select * from baseall'
    assertTrue(res.size() == 16)
    res = sql 'select * from baseall limit 7'
    assertTrue(res.size() == 7)

    sql 'set sql_select_limit = 0'

    res = sql 'select * from baseall'
    assertTrue(res.size() == 0)
    res = sql 'select * from baseall limit 7'
    assertTrue(res.size() == 7)

    sql 'set sql_select_limit = 5'
    sql 'set default_order_by_limit = -1'

    res = sql 'select * from baseall order by k1'
    assertEquals(res.size(), 5)

    sql 'set default_order_by_limit = -10'

    res = sql 'select * from baseall order by k1'
    assertTrue(res.size() == 5)

    sql 'set default_order_by_limit = 0'

    res = sql 'select * from baseall order by k1'
    assertTrue(res.size() == 0)
}