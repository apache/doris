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

suite("regression_test_variant_with_bf", ""){
    def table_name = "var_with_bloom_filter"
    sql "DROP TABLE IF EXISTS var_with_bloom_filter"
    sql """
        CREATE TABLE IF NOT EXISTS var_with_bloom_filter (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "bloom_filter_columns" = "v");
    """
    sql """insert into ${table_name} values (1, '{"a" : 123456}')"""
    sql """insert into ${table_name} values (2, '{"a" : 789111}')"""
    sql """insert into ${table_name} values (3, '{"a" : 789111}')"""

    sql """insert into ${table_name} values (1, '{"b" : "xxxxxxx"}')"""
    sql """insert into ${table_name} values (2, '{"b" : "yyyyyyy"}')"""
    sql """insert into ${table_name} values (3, '{"b" : "zzzzzzz"}')"""

    sql """insert into ${table_name} values (1, '{"b" : "xxxxxxx"}')"""
    sql """insert into ${table_name} values (2, '{"b" : "yyyyyyy"}')"""
    sql """insert into ${table_name} values (3, '{"b" : "zzzzzzz"}')"""

    qt_sql "select * from  var_with_bloom_filter where cast(v['a'] as int) = 789111"
    qt_sql "select * from  var_with_bloom_filter where cast(v['b'] as text) = 'yyyyyyy' ";
}