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

suite("test_predefine_ddl", "p0") { 

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { tableName, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(tableName + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def tableName = "test_ddl_table"

    test {
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
        exception("can not find field pattern: bb* in column: var")
    }

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

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant NOT NULL,
            INDEX idx_a_b (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("can not find field pattern: ab in column: var")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant<properties("variant_max_subcolumns_count" = "10")> NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""

    test {
         sql """ create index idx_ab on ${tableName} (var) using inverted properties("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") """
        exception("Can not create index with field pattern")
    }

    test {
         sql """ create index idx_ab on ${tableName} (var) using inverted properties("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") """
        exception("Can not create index with field pattern")
    }
    
    sql """ alter table ${tableName} add column var2 variant<'ab' : string, properties("variant_max_subcolumns_count" = "5")> NULL """

    sql """ alter table ${tableName} add column var3 variant<'ab' : string> NULL """

    // TODO(lihangyu) : uncomment
    // test {
    //     sql """ alter table ${tableName} modify column var variant<'ab' : string> NULL """
    //     exception("Can not modify variant column with children")
    // }


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

    test {
        sql """ alter table ${tableName} modify column var variant NULL """
        exception("Can not change variant schema templates")
    }

    test {
        sql """ alter table ${tableName} drop index idx_ab """
        exception("Can not drop index with field pattern")
    }

    sql """ alter table ${tableName} drop column var """

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : json
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("VARIANT unsupported sub-type: json")
    }

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : int,
                MATCH_NAME 'ab' : string,
                properties("variant_max_subcolumns_count" = "10", "variant_enable_typed_paths_to_sparse" = "true")
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("""Duplicate field name ab in variant variant<MATCH_NAME 'ab':int,MATCH_NAME 'ab':text,PROPERTIES ("variant_max_subcolumns_count" = "10","variant_enable_typed_paths_to_sparse" = "true")>""")
    }

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : decimalv2(22, 2)
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("VARIANT unsupported sub-type: decimalv2(22,2)")
    }

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : datev1
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("VARIANT unsupported sub-type: date")
    }

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : datetimev1
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("VARIANT unsupported sub-type: datetime")
    }

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : double
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("")
    }

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : int
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("column: var cannot have multiple inverted indexes with field pattern: ab")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant<
            MATCH_NAME 'ab' : string
        > NULL,
        INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
        INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : string
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("column: var cannot have multiple inverted indexes with field pattern: ab")
    }

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : string
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("column: var cannot have multiple inverted indexes with field pattern: ab")
    }

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant<
                MATCH_NAME 'ab' : array<string>
            > NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("field_pattern"="ab", "parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED PROPERTIES("field_pattern"="ab") COMMENT ''
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""
        exception("column: var cannot have multiple inverted indexes with field pattern: ab")
    }

    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` string NULL,
            INDEX idx_ab (var) USING INVERTED PROPERTIES("parser"="unicode", "support_phrase" = "true") COMMENT '',
            INDEX idx_ab_2 (var) USING INVERTED
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "v1")"""
        exception("column: var cannot have multiple inverted indexes with file storage format: V1")
    }

    test {
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var` variant <'c' :char(10)> NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true", "inverted_index_storage_format" = "v1")"""
        exception("VARIANT unsupported sub-type: char(10)")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant <'c' :text, properties("variant_max_subcolumns_count" = "10")> NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant <properties("variant_max_subcolumns_count" = "10")> NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""


    test {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (
            `id` bigint NULL,
            `var1` variant <properties("variant_max_subcolumns_count" = "10")> NULL,
            `var2` variant <properties("variant_max_subcolumns_count" = "0")> NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
        BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""
        exception("The variant_max_subcolumns_count must either be 0 in all columns, or greater than 0 in all columns")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "set default_variant_max_subcolumns_count = 10"
    sql "set default_variant_enable_typed_paths_to_sparse = false"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    qt_sql "desc ${tableName}"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `var` variant NULL,
        INDEX idx_ab (var) USING INVERTED PROPERTIES("parser"="unicode", "support_phrase" = "true") COMMENT ''
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql "create index idx_ab2 on ${tableName} (var) using inverted"
    wait_for_latest_op_on_table_finish("${tableName}", timeout)

    sql """alter table ${tableName} add column var2 variant<properties("variant_max_subcolumns_count" = "15")> NULL"""
    wait_for_latest_op_on_table_finish("${tableName}", timeout)

    test {
        sql """alter table ${tableName} add column var3 variant<properties("variant_max_subcolumns_count" = "0")> NULL"""
        exception("The variant_max_subcolumns_count must either be 0 in all columns or greater than 0 in all columns")
    }

    sql "alter table ${tableName} add column var3 variant NULL"
    wait_for_latest_op_on_table_finish("${tableName}", timeout)

    qt_sql "desc ${tableName}"

    sql "create index idx_ab3 on ${tableName} (var2) using inverted"
    wait_for_latest_op_on_table_finish("${tableName}", timeout)

    sql "create index idx_ab4 on ${tableName} (var2) using inverted properties(\"parser\"=\"unicode\")"
    wait_for_latest_op_on_table_finish("${tableName}", timeout)
}