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

suite("regression_test_variant_column_name", "variant_type"){
    def table_name = "var_column_name"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1 
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """ 

    sql """insert into ${table_name} values (1, '{"中文" : "中文", "\\\u4E2C\\\u6587": "unicode"}')"""
    qt_sql """select v:中文, v:`\\\u4E2C\\\u6587` from ${table_name}"""
    // sql """insert into ${table_name} values (2, '{}')"""
    sql """insert into ${table_name} values (3, '{"": ""}')"""
    qt_sql """select v:`` from ${table_name} order by k"""
    sql """insert into ${table_name} values (4, '{"!@#^&*()": "11111"}')"""
    qt_sql """select v:`!@#^&*()` from ${table_name} order by k"""
    sql """insert into ${table_name} values (5, '{"123": "456", "789": "012"}')"""
    qt_sql """select cast(v:`123` as string) from ${table_name} order by k"""
    // sql """insert into ${table_name} values (6, '{"\\n123": "t123", "\\\"123": "123"}')"""
    // qt_sql """select v:`\\n` from ${table_name} order by k"""
    sql """insert into ${table_name} values (7, '{"AA": "UPPER CASE", "aa": "lower case"}')"""
    qt_sql """select cast(v:`AA` as string), cast(v:`aa` as string) from ${table_name} order by k"""
    
}