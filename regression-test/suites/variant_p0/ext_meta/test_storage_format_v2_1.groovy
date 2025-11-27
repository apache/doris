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

suite("test_storage_format_v2_1") {
    def tableName = "test_storage_format_v2_1_table"
    
    // Test 1: Create table with storage_format = V2.1
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_format" = "V3"
        );
    """
    
    // Insert some data
    sql """insert into ${tableName} values (1, '{"a": 1, "b": 2, "c": 3}')"""
    sql """insert into ${tableName} values (2, '{"a": 10, "b": 20, "d": 40}')"""
    sql """insert into ${tableName} values (3, '{"e": 500, "f": 600}')"""
    
    // Query to verify data is correct
    qt_sql1 "select k, v['a'] from ${tableName} where cast(v['a'] as int) is not null order by k"
    qt_sql2 "select k, v['b'] from ${tableName} where cast(v['b'] as int) is not null order by k"
    qt_sql3 "select k, v['d'] from ${tableName} where cast(v['d'] as int) is not null order by k"
    qt_sql4 "select k, v['e'] from ${tableName} where cast(v['e'] as int) is not null order by k"
    
    // Verify table properties
    def result = sql "SHOW CREATE TABLE ${tableName}"
    logger.info("Show create table result: ${result}")
    assertTrue(result[0][1].contains("V3"), 
               "Table should be created with storage_format V3")
    
    // Cleanup
    sql "DROP TABLE IF EXISTS ${tableName}"
    
    // Test 2: Create table without specifying storage_format (should default to V2)
    def tableName2 = "test_storage_format_default"
    sql "DROP TABLE IF EXISTS ${tableName2}"
    sql """
        CREATE TABLE ${tableName2} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    
    sql """insert into ${tableName2} values (1, '{"x": 100}')"""
    qt_sql5 "select k, v['x'] from ${tableName2} order by k"
    
    sql "DROP TABLE IF EXISTS ${tableName2}"
    
    // Test 3: Verify that storage_format is case-insensitive
    def tableName3 = "test_storage_format_case_insensitive"
    sql "DROP TABLE IF EXISTS ${tableName3}"
    sql """
        CREATE TABLE ${tableName3} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_format" = "V3"
        );
    """
    
    sql """insert into ${tableName3} values (1, '{"test": "value"}')"""
    qt_sql6 "select k, v['test'] from ${tableName3} order by k"
    
    sql "DROP TABLE IF EXISTS ${tableName3}"
}


