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

suite("test_simplify_in_predicate") {
    sql "set enable_nereids_planner=true"
    sql 'set enable_fallback_to_original_planner=false;'
    sql 'drop table if exists test_simplify_in_predicate_t'
    sql """CREATE TABLE IF NOT EXISTS `test_simplify_in_predicate_t` (
            a DATE NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY (`a`)
            DISTRIBUTED BY HASH(`a`) BUCKETS 120
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "compression" = "LZ4"
            );"""
    sql """insert into test_simplify_in_predicate_t values( "2023-06-06" );"""

    explain {
        sql "verbose select * from test_simplify_in_predicate_t where a in ('1992-01-31', '1992-02-01', '1992-02-02', '1992-02-03', '1992-02-04');"
        notContains "CAST"
    }
}