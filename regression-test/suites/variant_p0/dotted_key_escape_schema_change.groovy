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

suite("dotted_key_escape_schema_change", "p0") {

    sql "set sql_mode=''"
    sql "set group_commit='off_mode'"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set batch_size=1024"
    sql "set default_variant_enable_doc_mode=false"
    sql "set default_variant_enable_nested_group=false"
    sql "set default_variant_max_subcolumns_count=0"
    sql "set default_variant_enable_typed_paths_to_sparse=false"
    sql "set default_variant_max_sparse_column_statistics_size=10000"
    sql "set enable_unique_key_partial_update=false"
    sql "set enable_variant_flatten_nested=false"
    sql "set enable_insert_strict=true"
    sql "set disable_nereids_rules=''"
    sql "set enable_inverted_index_query=true"
    sql "set enable_two_phase_read_opt=false"

    for (def storageFormat : ["V2", "V3"]) {
        for (def docMode : [false, true]) {
            def layout = docMode
                    ? '"variant_enable_doc_mode"="true", "variant_doc_hash_shard_count"="3", "variant_doc_materialization_min_rows"="1"'
                    : '"variant_max_subcolumns_count"="1", "variant_sparse_hash_shard_count"="3"'

            String name = "dotted_key_escape_schema_change_${storageFormat}_${docMode}"
            sql "drop table if exists ${name}"
            sql """create table ${name}(id int, v variant<properties("variant_escape_dots_in_keys"="true", ${layout}, "variant_enable_nested_group"="false")>, padding int default '0')
                duplicate key(id) distributed by hash(id) buckets 1
                properties("replication_num"="1", "storage_format"="${storageFormat}", "disable_auto_compaction"="true", "light_schema_change"="true")"""
            test {
                sql """ALTER TABLE ${name} MODIFY COLUMN v VARIANT<PROPERTIES(
                    "variant_escape_dots_in_keys"="false", ${layout}, "variant_enable_nested_group"="false")>"""
                exception "Can not change variant_escape_dots_in_keys"
            }
            def docs = ['{"a.b":1,"a":{"b":2},"a%2Eb":3,"50%":4,"":5}',
                        '{"hot":7,"a.b":8,"50%":9}', '{"hot":10,"":11}']
            docs.eachWithIndex { json, index ->
                sql "insert into ${name}(id,v) values (${index},CAST(CAST('${json}' AS JSON) AS VARIANT))"
            }
            def checkRows = {
                def rows = sql "select id,v from ${name} order by id"
                assertEquals(docs.size(), rows.size())
                rows.each { row -> assertEquals(new groovy.json.JsonSlurper().parseText(docs[row[0].toInteger()]),
                        new groovy.json.JsonSlurper().parseText(row[1])) }
            }
            checkRows()
            sql """alter table ${name} add column e variant<properties("variant_escape_dots_in_keys"="true",
                ${docMode ? layout : layout.replace('"1"', '"2"')}, 
                "variant_enable_nested_group"="false")>"""
            sql("select e from ${name}").each { assertEquals(null, it[0]) }
            def addedDoc = '{"e.f":12,"e":{"f":13},"e%2Ef":14,"":15}'
            docs.add('{"a.b":16,"50%":17,"":18}')
            sql "insert into ${name}(id,v,e) values (3,CAST(CAST('${docs[3]}' AS JSON) AS VARIANT),CAST(CAST('${addedDoc}' AS JSON) AS VARIANT))"
            def checkAdded = {
                sql("select e from ${name} where id<3").each { assertEquals(null, it[0]) }
                def row = sql("select e from ${name} where id=3")[0]
                assertEquals(new groovy.json.JsonSlurper().parseText(addedDoc),
                        new groovy.json.JsonSlurper().parseText(row[0]))
            }
            checkRows()
            checkAdded()
            sql "alter table ${name} modify column padding bigint default '0'"
            waitForSchemaChangeDone {
                sql "show alter table column where TableName='${name}' order by CreateTime desc limit 1"
                time 600
            }
            def jobs = sql_return_maparray "show alter table column where TableName='${name}' order by CreateTime desc limit 1"
            assertEquals("FINISHED", jobs[0].State)
            checkRows()
            checkAdded()
        }
    }
}
