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

suite("test_predefine_ddl", "p0"){ 

    def tableName = "test_ddl_table"
    boolean findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : string,
                MATCH_NAME '*cc' : string,
                MATCH_NAME 'b?b' : string
            > NOT NULL,
            INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_bb (var) USING INVERTED PROPERTIES("field_pattern"="*cc", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_b_b (var) USING INVERTED PROPERTIES("field_pattern"="b?b", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_bb_glob (var) USING INVERTED PROPERTIES("field_pattern"="bb*", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_bx_glob (var) USING INVERTED PROPERTIES("field_pattern"="bx?", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("can not find field pattern: bb* in column: var"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : string,
                MATCH_NAME '*cc' : string,
                MATCH_NAME 'b?b' : string
            > NOT NULL,
            INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        findException = true
    }
    assertFalse(findException)


    findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant NOT NULL,
            INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("can not find field pattern: ab in column: var"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        findException = true
    }
    assertFalse(findException)

    findException = false
    try {
         sql """ create index idx_ab on ${tableName} (var) using inverted properties("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") """
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not create index with field pattern"))
        findException = true
    }
    assertTrue(findException)   
    

    findException = false
    try {
        sql """ alter table ${tableName} add column var2 variant<'ab' : string> NULL """
    } catch (Exception e) {
        log.info(e.getMessage())
        findException = true
    }
    assertFalse(findException)

    findException = false
    try {
        sql """ alter table ${tableName} modify column var variant<'ab' : string> NULL """
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify variant column with children"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : string,
                MATCH_NAME '*cc' : string,
                MATCH_NAME 'b?b' : string
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        findException = true
    }
    assertFalse(findException)

    findException = false
    try {
        sql """ alter table ${tableName} modify column var variant NULL """
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not modify variant column with children"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql """ alter table ${tableName} drop index idx_ab """
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Can not drop index with field pattern"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql """ alter table ${tableName} drop column var """
    } catch (Exception e) {
        log.info(e.getMessage())
        findException = true
    }
    assertFalse(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : json
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("VARIANT unsupported sub-type: json"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : int,
                MATCH_NAME 'ab' : string
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("Duplicate field name ab in struct variant<MATCH_NAME 'ab':int,MATCH_NAME 'ab':text>"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : decimalv2(22, 2)
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("VARIANT unsupported sub-type: decimalv2(22,2)"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : datev1
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("VARIANT unsupported sub-type: date"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : datetimev1
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("VARIANT unsupported sub-type: datetime"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS test_ddl_table"
        sql """CREATE TABLE test_ddl_table (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : double
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        findException = true
    }
    assertFalse(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS test_ddl_table"
        sql """CREATE TABLE test_ddl_table (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : int
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("column: var cannot have multiple inverted indexes with field pattern: ab"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS test_ddl_table"
        sql """CREATE TABLE test_ddl_table (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : string
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        findException = true
    }
    assertFalse(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS test_ddl_table"
        sql """CREATE TABLE test_ddl_table (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : string
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("column: var cannot have multiple inverted indexes with field pattern: ab"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS test_ddl_table"
        sql """CREATE TABLE test_ddl_table (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : string
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("column: var cannot have multiple inverted indexes with field pattern: ab"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS test_ddl_table"
        sql """CREATE TABLE test_ddl_table (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : array<string>
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("column: var cannot have multiple inverted indexes with field pattern: ab"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql "DROP TABLE IF EXISTS test_ddl_table"
        sql """CREATE TABLE test_ddl_table (
            `id` bigint NULL,
            `var` string NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "v1")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("column: var cannot have multiple inverted indexes with file storage format: V1"))
        findException = true
    }
    assertTrue(findException)

    findException = false
    try {
        sql """CREATE TABLE test_ddl_table (
            `id` bigint NULL,
            `var` variant <'c' :char(10)> NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "v1")"""
    } catch (Exception e) {
        log.info(e.getMessage())
        assertTrue(e.getMessage().contains("VARIANT unsupported sub-type: char(10)"))
        findException = true
    }
    assertTrue(findException)
}