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

suite("test_variant_predefine_index_type", "p0"){ 
    sql """ set describe_extend_variant_column = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """

    def tableName = "test_variant_predefine_index_type"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant <
            MATCH_NAME 'path.int' : int,
            MATCH_NAME 'path.decimal' : DECIMAL(15, 12)
        > NULL,
        INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="path.int", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_a_c (var) USING INVERTED PROPERTIES("field_pattern"="path.decimal") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true", "variant_max_subcolumns_count" = "0")"""

    sql """insert into ${tableName} values(1, '{"path" : {"int" : 123, "decimal" : 123.123456789012}}')"""
    sql """insert into ${tableName} values(2, '{"path" : {"int" : 456, "decimal" : 456.456789123456}}')"""
    sql """insert into ${tableName} values(3, '{"path" : {"int" : 789, "decimal" : 789.789123456789}}')"""
    sql """insert into ${tableName} values(4, '{"path" : {"int" : 100, "decimal" : 100.100123456789}}')"""
    sql """insert into ${tableName} values(5, '{"path" : {"int" : 111, "decimal" : 111.111111111111}}')"""
    
    qt_sql """ desc ${tableName} """
    qt_sql """select * from ${tableName} order by id"""
    
    qt_sql """ select count() from ${tableName} where cast(var['path']['int'] as int) = 789 """
    qt_sql """ select count() from ${tableName} where cast(var['path']['decimal'] as DECIMAL(15,12)) = 789.789123456789 """
}