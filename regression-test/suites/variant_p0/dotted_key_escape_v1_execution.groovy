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

suite("dotted_key_escape_v1_execution", "p0,nonConcurrent") {
    sql "set default_variant_enable_nested_group=false"
    sql "set group_commit='off_mode'"
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
    def json = '{"a.b":1,"a":{"b":2},"a%2Eb":3,"pct%":4}'
    def decode = { rows -> rows.collect { row ->
        [row[0], row[1] == null ? null : new groovy.json.JsonSlurper().parseText(row[1])]
    } }
    setFeConfigTemporary([enable_variant_v2:false, enable_variant_escape_dots_in_keys:true]) {
        // The same guard must cover materialized, sparse and doc-only paths.
        for (def layout : ['"variant_max_subcolumns_count"="10"',
                           '"variant_max_subcolumns_count"="1"',
                           '"variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="0"']) {
            sql "drop table if exists dotted_key_escape_v1_reads"
            sql """create table dotted_key_escape_v1_reads(id int, v variant<properties(${layout})>,
                w variant, padding int default '0') duplicate key(id) distributed by hash(id) buckets 1
                properties("replication_num"="1","variant_escape_dots_in_keys"="true",
                           "disable_auto_compaction"="true","light_schema_change"="true")"""
            [json, '{}', '[]', null].eachWithIndex { doc, id ->
                def value = doc == null ? 'NULL' : "parse_to_variant('${doc}')"
                sql "insert into dotted_key_escape_v1_reads(id,v,w) values (${id},${value},${value})"
            }
            def before = decode(sql("select id,v from dotted_key_escape_v1_reads order by id"))
            def ordinary = sql "select id,padding from dotted_key_escape_v1_reads order by id"
            def versions = sql_return_maparray("show partitions from dotted_key_escape_v1_reads")
                    .collect { it.VisibleVersion }
            setFeConfigTemporary([enable_variant_v2:false]) {
                assertEquals(before, decode(sql("select id,v from dotted_key_escape_v1_reads order by id")))
                assertEquals(before, decode(sql("select id,w from dotted_key_escape_v1_reads order by id")))
                assertEquals([[1, 2, 3, 4]], sql("""select cast(v['a.b'] as int),cast(v['a']['b'] as int),
                    cast(v['a%2Eb'] as int),cast(v['pct%'] as int)
                    from dotted_key_escape_v1_reads where id=0"""))
                assertEquals(ordinary, sql("select id,padding from dotted_key_escape_v1_reads order by id"))
                sql """insert into dotted_key_escape_v1_reads(id,v,w)
                    values (100,parse_to_variant('${json}'),parse_to_variant('${json}'))"""
                def published = sql_return_maparray("show partitions from dotted_key_escape_v1_reads")
                        .collect { it.VisibleVersion }
                assertTrue((published[0] as long) > (versions[0] as long))
                before = decode(sql("select id,v from dotted_key_escape_v1_reads order by id"))
                assertEquals(5, before.size())
                // Maintenance selects V2 from the persisted schema, independently of FE config.
                trigger_and_wait_compaction("dotted_key_escape_v1_reads", "full")
                // Changing a value type forces a physical rewrite even with light schema enabled.
                sql "alter table dotted_key_escape_v1_reads modify column padding bigint default '0'"
                waitForSchemaChangeDone {
                    sql "show alter table column where TableName='dotted_key_escape_v1_reads' order by CreateTime desc limit 1"
                    time 600
                }
                sql "alter table dotted_key_escape_v1_reads add column extra int default '7'"
                waitForSchemaChangeDone {
                    sql "show alter table column where TableName='dotted_key_escape_v1_reads' order by CreateTime desc limit 1"
                    time 600
                }
                sql "alter table dotted_key_escape_v1_reads drop column extra"
                waitForSchemaChangeDone {
                    sql "show alter table column where TableName='dotted_key_escape_v1_reads' order by CreateTime desc limit 1"
                    time 600
                }
            }
            assertEquals(before, decode(sql("select id,v from dotted_key_escape_v1_reads order by id")))
            assertEquals(before, decode(sql("select id,w from dotted_key_escape_v1_reads order by id")))
        }
        sql "drop table if exists dotted_key_escape_v1_rowstore"
        sql """create table dotted_key_escape_v1_rowstore(id int, v variant, padding int)
            unique key(id) distributed by hash(id) buckets 1 properties("replication_num"="1",
            "variant_escape_dots_in_keys"="true","store_row_column"="true",
            "enable_unique_key_merge_on_write"="true")"""
        sql "insert into dotted_key_escape_v1_rowstore values (1,parse_to_variant('${json}'),9)"
        sql "set enable_short_circuit_query=true"
        // The 4.2 planner excludes tables containing VARIANT from short-circuit queries.
        // Keep this fallback explicit; direct/cached point-query BE guards have UT coverage.
        explain {
            sql "select v from dotted_key_escape_v1_rowstore where id=1"
            notContains "SHORT-CIRCUIT"
        }
        def before = decode(sql("select id,v from dotted_key_escape_v1_rowstore where id=1"))
        setFeConfigTemporary([enable_variant_v2:false]) {
            assertEquals(before, decode(sql("select id,v from dotted_key_escape_v1_rowstore where id=1")))
            sql "select padding from dotted_key_escape_v1_rowstore where id=1"
            sql "set enable_short_circuit_query=false"
            sql "set enable_two_phase_read_opt=true"
            sql "set topn_lazy_materialization_threshold=1024"
            explain {
                sql "select v from dotted_key_escape_v1_rowstore order by padding limit 1"
                contains "MaterializeNode"
            }
            assertEquals(before, decode(sql("select id,v from dotted_key_escape_v1_rowstore order by padding limit 1")))
        }
        assertEquals(before, decode(sql("select id,v from dotted_key_escape_v1_rowstore where id=1")))
        sql "drop table if exists dotted_key_escape_v1_legacy"
        sql """create table dotted_key_escape_v1_legacy(id int,v variant) duplicate key(id)
            distributed by hash(id) buckets 1 properties("replication_num"="1","variant_escape_dots_in_keys"="false")"""
        def legacyJson = '{"nested":{"key":7},"pct%":4}'
        sql "insert into dotted_key_escape_v1_legacy values (1,'${legacyJson}')"
        def legacy = decode(sql("select id,v from dotted_key_escape_v1_legacy order by id"))
        setFeConfigTemporary([enable_variant_v2:false]) {
            assertEquals(legacy, decode(sql("select id,v from dotted_key_escape_v1_legacy order by id")))
            // V1's ordinary JSON-string insert preserves the destination Variant settings.
            sql "insert into dotted_key_escape_v1_legacy values (2,'${legacyJson}')"
            assertEquals(new groovy.json.JsonSlurper().parseText(legacyJson),
                    decode(sql("select id,v from dotted_key_escape_v1_legacy where id=2"))[0][1])
            sql "drop table if exists dotted_key_escape_v1_new_legacy"
            sql "create table dotted_key_escape_v1_new_legacy like dotted_key_escape_v1_legacy"
        }
    }
}
