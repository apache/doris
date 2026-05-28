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

    sql """ set default_variant_enable_doc_mode = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_max_subcolumns_count = 10 """
    sql """ set default_variant_max_sparse_column_statistics_size = 10 """
    sql """ set default_variant_sparse_hash_shard_count = 10 """
    sql """ set enable_add_index_for_new_data = true """

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
        exception("can not find field pattern: ab in column: var")
    }

    sql """insert into ${tableName} values(1, '{"content":"hello old", "ab":"alpha old"}')"""
    sql """ alter table ${tableName} modify column var variant<
                MATCH_NAME 'ab' : string,
                MATCH_NAME 'content' : string,
                properties("variant_max_subcolumns_count" = "10")
            > NULL """
    wait_for_latest_op_on_table_finish(tableName, timeout)
    sql """ create index idx_content on ${tableName} (var) using inverted
            properties("field_pattern"="content", "parser"="english", "support_phrase" = "true") """
    sql """ create index idx_ab on ${tableName} (var) using inverted
            properties("field_pattern"="ab", "parser"="english", "support_phrase" = "true") """
    def showCreateResult = sql """ show create table ${tableName} """
    assertTrue(showCreateResult.toString().contains("'content'"))
    assertTrue(showCreateResult.toString().contains("idx_content"))
    sql """insert into ${tableName} values(2, '{"content":"hello new", "ab":"alpha new"}')"""
    def matchResult = sql """ select id from ${tableName} where var['content'] match 'hello' order by id """
    assertEquals(2, matchResult.size())
    assertEquals("1", matchResult[0][0].toString())
    assertEquals("2", matchResult[1][0].toString())

    test {
         sql """ create index idx_missing on ${tableName} (var) using inverted properties("field_pattern"="missing", "parser"="unicode", "support_phrase" = "true") """
        exception("can not find field pattern: missing in column: var")
    }

    test {
         sql """ create index idx_content_dup on ${tableName} (var) using inverted properties("field_pattern"="content", "parser"="english", "support_phrase" = "true") """
        exception("field pattern 'content' with analyzer analyzer identity 'english'")
    }

    test {
         sql """ alter table ${tableName} modify column var variant<
                    MATCH_NAME 'ab' : string,
                    MATCH_NAME 'content' : string,
                    '*' : string,
                    'content*' : string,
                    properties("variant_max_subcolumns_count" = "10")
                > NULL """
        exception("can shadow it")
    }

    test {
        if (isCloudMode()) {
            sql """ build index on ${tableName} """
            exception("INVERTED index is not needed to build")
        } else {
            sql """ build index idx_content on ${tableName} """
            exception("because it is a variant type column")
        }
    }

    test {
         sql """ drop index idx_content on ${tableName} partition (p1) """
        exception("Can not drop index with field pattern")
    }

    sql """ drop index idx_content on ${tableName} """
    def showCreateAfterDropIndex = sql """ show create table ${tableName} """
    assertFalse(showCreateAfterDropIndex.toString().contains("idx_content"))

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
        exception("Can not reduce variant schema templates")
    }

    sql """ alter table ${tableName} drop index idx_ab """
    def showCreateAfterAlterDropIndex = sql """ show create table ${tableName} """
    assertFalse(showCreateAfterAlterDropIndex.toString().contains("idx_ab"))

    sql """ alter table ${tableName} drop column var """

    sql "DROP TABLE IF EXISTS test_variant_pattern_type_only_alter"
    sql """CREATE TABLE test_variant_pattern_type_only_alter (
        `id` bigint NULL,
        `var` variant<
            MATCH_NAME 'ab' : string
        > NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""

    test {
        sql """ alter table test_variant_pattern_type_only_alter modify column var variant<'ab' : string> NULL """
        exception("Can not change variant schema template pattern type")
    }

    sql "DROP TABLE IF EXISTS test_variant_comment_only_alter"
    sql """CREATE TABLE test_variant_comment_only_alter (
        `id` bigint NULL,
        `var` variant<
            MATCH_NAME 'ab' : string COMMENT 'old'
        > NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`)
    BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")"""

    test {
        sql """ alter table test_variant_comment_only_alter modify column var variant<
                    MATCH_NAME 'ab' : string COMMENT 'new'
                > NULL """
        exception("Can not change variant schema template comment")
    }

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
        exception("""Duplicate field name ab in variant""")
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
        exception("invalid INVERTED index")
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
        exception("invalid INVERTED index")
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
        exception("column: var cannot have multiple inverted indexes of the same type with field pattern: ab")
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
        exception("column: var cannot have multiple inverted indexes of the same type with field pattern: ab")
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
        exception("invalid INVERTED index")
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

    sql """alter table ${tableName} add column var3 variant<properties("variant_max_subcolumns_count" = "0")> NULL"""
    wait_for_latest_op_on_table_finish("${tableName}", timeout)

    qt_sql "desc ${tableName}"

    sql "create index idx_ab3 on ${tableName} (var2) using inverted"
    wait_for_latest_op_on_table_finish("${tableName}", timeout)

    sql "create index idx_ab4 on ${tableName} (var2) using inverted properties(\"parser\"=\"unicode\")"
    wait_for_latest_op_on_table_finish("${tableName}", timeout)
}
