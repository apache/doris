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

suite("test_variant_predefine_base", "p0"){ 
    sql """ set describe_extend_variant_column = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    def count = new Random().nextInt(5) + 1
    def tableName = "base_match_name_variant_test"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant<
            MATCH_NAME 'ab' : string,
            MATCH_NAME '*cc' : string,
            MATCH_NAME 'b?b' : string,
            MATCH_NAME_GLOB 'bb*' : string,
            MATCH_NAME_GLOB 'bx?' : string,
            properties("variant_max_subcolumns_count" = "${count}")
        > NOT NULL,
        INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_bb (var) USING INVERTED PROPERTIES("field_pattern"="*cc", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_b_b (var) USING INVERTED PROPERTIES("field_pattern"="b?b", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_bb_glob (var) USING INVERTED PROPERTIES("field_pattern"="bb*", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_bx_glob (var) USING INVERTED PROPERTIES("field_pattern"="bx?", "parser"="unicode", "support_phrase" = "true") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""

    sql """insert into ${tableName} values(1, '{"ab" : 123, "*cc" : 123, "b?b" : 123, "bb3" : 123, "bxx" : 123}')"""
    sql """insert into ${tableName} values(2, '{"ab" : 456, "*cc" : 456, "b?b" : 456, "bb3" : 456, "bxx" : 456}')"""
    sql """insert into ${tableName} values(3, '{"ab" : 789, "*cc" : 789, "b?b" : 789, "bb3" : 789, "bxx" : 789}')"""
    sql """insert into ${tableName} values(4, '{"ab" : 100, "*cc" : 100, "b?b" : 100, "bb3" : 100, "bxx" : 100}')"""
    sql """insert into ${tableName} values(5, '{"ab" : 111, "*cc" : 111, "b?b" : 111, "bb3" : 111, "bxx" : 111}')"""
    qt_sql """ select variant_type(var) from base_match_name_variant_test group by variant_type(var) """
    qt_sql """select * from ${tableName} order by id"""
    qt_sql """ select count() from ${tableName} where cast(var['ab'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['*cc'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['b?b'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['bb3'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['bxx'] as string) match '789' """
    

    trigger_and_wait_compaction(tableName, "full")

    qt_sql """select * from ${tableName} order by id"""
    qt_sql """ select variant_type(var) from base_match_name_variant_test group by variant_type(var) """
    qt_sql """ select count() from ${tableName} where cast(var['ab'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['*cc'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['b?b'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['bb3'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['bxx'] as string) match '789' """



    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant<
            MATCH_NAME 'a.b' : string,
            MATCH_NAME 'a.*' : string,
            MATCH_NAME_GLOB 'a.b[0-9]' : string,
            MATCH_NAME_GLOB 'a.b?c' : string,
            MATCH_NAME_GLOB 'a.c*' : string,
            properties("variant_max_subcolumns_count" = "${count}")
        > NOT NULL,
        INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="a.b", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_bb (var) USING INVERTED PROPERTIES("field_pattern"="a.*", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_b_b (var) USING INVERTED PROPERTIES("field_pattern"="a.b[0-9]", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_bb_glob (var) USING INVERTED PROPERTIES("field_pattern"="a.b?c", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_bx_glob (var) USING INVERTED PROPERTIES("field_pattern"="a.c*", "parser"="unicode", "support_phrase" = "true") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""

    sql """insert into ${tableName} values(1, '{"a" : {"b" : 789, "*" : 789, "b1" : 789, "bxc" : 789, "c2323" : 789}}')"""
    sql """insert into ${tableName} values(2, '{"a" : {"b" : 111, "*" : 111, "b1" : 111, "bxc" : 111, "c2323" : 111}}')"""
    sql """insert into ${tableName} values(3, '{"a" : {"b" : 222, "*" : 222, "b1" : 222, "bxc" : 222, "c2323" : 222}}')"""

    qt_sql """ select variant_type(var) from base_match_name_variant_test group by variant_type(var) """ 
    qt_sql """select * from ${tableName} order by id"""
    qt_sql """ select count() from ${tableName} where cast(var['a']['b'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['a']['*'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['a']['b1'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['a']['bxc'] as string) match '789' """
qt_sql """ select count() from ${tableName} where cast(var['a']['c2323'] as string) match '789' """
    
    trigger_and_wait_compaction(tableName, "full")

    qt_sql """select * from ${tableName} order by id"""
    qt_sql """ select variant_type(var) from base_match_name_variant_test group by variant_type(var) """
    qt_sql """ select count() from ${tableName} where cast(var['a']['b'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['a']['*'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['a']['b1'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['a']['bxc'] as string) match '789' """
    qt_sql """ select count() from ${tableName} where cast(var['a']['c2323'] as string) match '789' """
    
}
