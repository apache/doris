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

// test array types with predicate
suite("regression_test_variant_array_with_predicate", "p0"){
    sql "DROP TABLE IF EXISTS array_with_predicate"
    sql """
        CREATE TABLE IF NOT EXISTS array_with_predicate (
            k bigint,
            v variant not null
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """
    sql """insert into array_with_predicate values (1, '{"arr" : ["1", "2", "3", "4"]}')"""
    sql """insert into array_with_predicate values (1, '{"arr" : "[]"}')"""
    sql """insert into array_with_predicate values (1, '{"arr1" : "[]"}')"""
    qt_sql """select count() from array_with_predicate where cast(v['arr'] as array<text>) is not null"""
    qt_sql """select count() from array_with_predicate where cast(v['arr'] as text) is not null"""
    qt_sql """select count() from array_with_predicate where cast(v['arr'] as array<text>) is null"""
    qt_sql """select count() from array_with_predicate where cast(v['arr'] as text) is null"""
}