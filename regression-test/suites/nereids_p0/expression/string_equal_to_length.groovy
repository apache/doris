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

suite("string_equal_to_length") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql 'drop table if exists test_string_equal_to_length;'

    sql '''create table test_string_equal_to_length (k0 int, k1 string, k2 varchar, k3 char(5)) distributed by hash(k0) buckets 3 properties('replication_num' = '1');'''

    // for string type
    sql '''
    select k1
    from test_string_equal_to_length
    where k1 <> "";
    '''
    def res = sql '''
    explain rewritten plan select k1
    from test_string_equal_to_length
    where k1 <> "";
    '''
    assertTrue(res.toString().contains("length"))

    // for varchar type
    sql '''
    select k2
    from test_string_equal_to_length
    where k2 <> "";
    '''
    def res1 = sql '''
    explain rewritten plan select k2
    from test_string_equal_to_length
    where k2 <> "";
    '''
    assertTrue(res1.toString().contains("length"))

    sql '''
    select k3
    from test_string_equal_to_length
    where k3 <> "";
    '''
    def res2 = sql '''
    explain rewritten plan select k3
    from test_string_equal_to_length
    where k3 <> "";
    '''
    assertTrue(res2.toString().contains("length"))

    // for equal
    sql '''
    select k1
    from test_string_equal_to_length
    where k1 == "";
    '''
    def res3 = sql '''
    explain rewritten plan select k1
    from test_string_equal_to_length
    where k1 == "";'''
    assertTrue(res3.toString().contains("length"))

}
