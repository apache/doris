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

suite("variant_background_v2_execution", "p0,nonConcurrent") {
    sql "set default_variant_enable_nested_group=false"
    sql "set default_variant_enable_doc_mode=false"
    sql "set group_commit='off_mode'"
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
    def decode = { rows -> rows.collect { row ->
        [row[0], row[1] == null ? null : new groovy.json.JsonSlurper().parseText(row[1])]
    } }
    setFeConfigTemporary([enable_variant_v2:false, enable_variant_escape_dots_in_keys:true]) {
        for (def escaped : [false, true]) {
            for (def layout : ['"variant_max_subcolumns_count"="0"',
                               '"variant_max_subcolumns_count"="1"',
                               '"variant_enable_doc_mode"="true","variant_doc_materialization_min_rows"="0"']) {
                String name = "variant_background_v2_${escaped}"
                sql "drop table if exists ${name}"
                sql """create table ${name}(id int,v variant<properties(${layout})>,padding int default '0')
                    duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num"="1",
                    "variant_escape_dots_in_keys"="${escaped}","disable_auto_compaction"="true",
                    "light_schema_change"="true")"""
                def docs = [escaped ? '{"a.b":1,"a":{"b":2},"pct%":4}'
                                    : '{"nested":{"key":7},"pct%":4}', '{}', '[]', null]
                docs.eachWithIndex { doc, id ->
                    def value = doc == null ? 'NULL'
                            : (escaped ? "parse_to_variant('${doc}')" : "'${doc}'")
                    sql "insert into ${name}(id,v) values (${id},${value})"
                }
                def before = decode(sql("select id,v from ${name} order by id"))
                trigger_and_wait_compaction(name, "full")
                assertEquals(before, decode(sql("select id,v from ${name} order by id")))
                sql "alter table ${name} modify column padding bigint default '0'"
                waitForSchemaChangeDone {
                    sql "show alter table column where TableName='${name}' order by CreateTime desc limit 1"
                    time 600
                }
                assertEquals(before, decode(sql("select id,v from ${name} order by id")))
                assertEquals(escaped, sql("show create table ${name}")[0][1]
                        .contains('"variant_escape_dots_in_keys" = "true"'))
                assertFalse(getFeConfig("enable_variant_v2").toBoolean())
            }
        }

        // This source has no escaped property and no Variant column. The task must still use V2.
        sql "drop materialized view if exists variant_background_v2_mv"
        sql "drop table if exists variant_background_v2_text"
        sql """create table variant_background_v2_text(id int,j string) duplicate key(id)
            distributed by hash(id) buckets 1 properties("replication_num"="1")"""
        sql """insert into variant_background_v2_text values (1,'{"a":1}')"""
        String baseQuery = """select id,count(element_at(cast(j as variant),'a')) as c
            from variant_background_v2_text group by id"""
        def beforeMv = sql(baseQuery)
        assertEquals([[1,1L]], beforeMv)
        sql """create materialized view variant_background_v2_mv
            build deferred refresh complete on manual distributed by hash(id) buckets 1
            properties("replication_num"="1") as ${baseQuery}"""
        sql "refresh materialized view variant_background_v2_mv complete"
        def db = context.config.getDbNameByFile(context.file)
        waitingMTMVTaskFinished(getJobName(db,'variant_background_v2_mv'))
        // V2 casts the SQL text to a Variant string; field a is absent in that string.
        assertEquals([[1,0L]], sql("select id,c from variant_background_v2_mv"))
        sql "set enable_materialized_view_rewrite=true"
        assertEquals(beforeMv, sql(baseQuery))
        explain {
            sql baseQuery
            notContains "variant_background_v2_mv"
        }
        assertFalse(getFeConfig("enable_variant_v2").toBoolean())
    }
}
