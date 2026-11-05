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

suite("dotted_key_escape_compaction", "p0,nonConcurrent") {

    // Compaction preserves the visible version; cached query results would hide output corruption.
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
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

    def backendIPs = [:]
    def backendHttpPorts = [:]
    getBackendIpHttpPort(backendIPs, backendHttpPorts)
    for (def storageFormat : ["V2", "V3"]) {
        for (def docMode : [false, true]) {
            def layout = docMode
                    ? '"variant_enable_doc_mode"="true", "variant_doc_hash_shard_count"="3", "variant_doc_materialization_min_rows"="1"'
                    : '"variant_max_subcolumns_count"="1", "variant_sparse_hash_shard_count"="3"'

            String name = "dotted_key_escape_compaction_${storageFormat}_${docMode}"
            sql "drop table if exists ${name}"
            sql """create table ${name}(id int, v variant<properties(
                "variant_escape_dots_in_keys"="true", ${layout}, "variant_enable_nested_group"="false")>,
                index idx_v(v) using inverted properties("parser"="english"))
                duplicate key(id) distributed by hash(id) buckets 1
                properties("replication_num"="1", "storage_format"="${storageFormat}", "disable_auto_compaction"="true",
                           "inverted_index_storage_format"="V2")"""
            def expected = [:]
            def checkRows = {
                [true, false].each { useIndex ->
                    sql "set enable_inverted_index_query=${useIndex}"
                    def rows = sql "select id,v,cast(v['a.b'] as bigint),cast(v['a']['b'] as bigint),cast(v['a%2Eb'] as bigint),cast(v['50%'] as bigint) from ${name} order by id"
                    assertEquals(expected.size(), rows.size())
                    rows.each { row ->
                        def object = expected[row[0].toInteger()]
                        assertEquals(object, new groovy.json.JsonSlurper().parseText(row[1]))
                        assertEquals(object['a.b']?.toString(), row[2]?.toString())
                        assertEquals(object['a']?.get('b')?.toString(), row[3]?.toString())
                        assertEquals(object['a%2Eb']?.toString(), row[4]?.toString())
                        assertEquals(object['50%']?.toString(), row[5]?.toString())
                    }
                    def hits = sql "select id from ${name} where cast(v['a.b'] as bigint)>0 order by id"
                    assertEquals(expected.findAll { id, value -> value.containsKey('a.b') }.keySet().sort(),
                            hits.collect { it[0].toInteger() })
                }
            }
            int nextId = 1
            ["full", "full", "cumulative", "full"].eachWithIndex { kind, round ->
                ["a", "b", "c"].eachWithIndex { hot, batch ->
                    def first = ['a.b': 701 + round * 10 + batch, 'a': ['b': 2 + batch],
                                 'a%2Eb': 3 + batch, '50%': 4 + batch]
                    first["hot_${hot}".toString()] = 11
                    def second = [("hot_${hot}".toString()): 12]
                    [first, second].each { object ->
                        int id = nextId++
                        expected[id] = object
                        String json = groovy.json.JsonOutput.toJson(object)
                        sql "insert into ${name} values (${id},CAST(CAST('${json}' AS JSON) AS VARIANT))"
                    }
                }
                checkRows()
                def tablets = sql_return_maparray("SHOW TABLETS FROM ${name}")
                def readStatus = { tablet ->
                    def (code, out, err) = be_show_tablet_status(
                            backendIPs["${tablet.BackendId}"], backendHttpPorts["${tablet.BackendId}"], tablet.TabletId)
                    assertEquals(0, code, err)
                    return parseJson(out)
                }
                // Cloud keeps the empty initial [0-1] rowset outside full compaction.
                // Every other input must still be replaced by the new output.
                def rowsetIds = { status ->
                    status.rowsets.collect { it.split(/\s+/) }
                            .findAll { !(it[0] == "[0-1]" && it[1] == "0") }
                            .collect { it[4] }.toSet()
                }
                def inputs = tablets.collectEntries { tablet -> [(tablet.TabletId): rowsetIds(readStatus(tablet))] }
                // Six rows with five-row batches leave a one-row final batch in round 0.
                // Its hot_c value is dense in the input but loses promotion to the dotted paths.
                // Round 1 reads the multi-segment round-0 output together with new rowsets.
                // The two vertical rounds use group widths 1 and 2 respectively.
                setBeConfigTemporary([enable_vertical_compaction: round % 2 == 0,
                                      enable_vertical_compact_variant_subcolumns: true,
                                      enable_ordered_data_compaction: false,
                                      compaction_batch_size: 5,
                                      vertical_compaction_max_segment_size: 4096,
                                      vertical_compaction_num_columns_per_group: round.intdiv(2) + 1]) {
                    trigger_and_wait_compaction(name, kind)
                    for (def tablet : tablets) {
                        def status = readStatus(tablet)
                        assertEquals("[OK]", status["last ${kind} status"].toString())
                        def before = inputs[tablet.TabletId]
                        def after = rowsetIds(status)
                        assertTrue((before - after).size() >= 2, "No input replacement: ${before} -> ${after}")
                        assertEquals(1, (after - before).size(), "Missing new output: ${before} -> ${after}")
                        logger.info("${name} round ${round} ${kind}: replaced ${before - after} with ${after - before}")
                        if (kind == "full") {
                            assertTrue(before.intersect(after).isEmpty(), "Full compaction retained inputs: ${status}")
                        }
                        if (round == 0) {
                            assertTrue(status.rowsets.sum { it.split(/\s+/)[1].toInteger() } > 1,
                                    "Expected a multi-segment output: ${status}")
                        }
                    }
                }
                checkRows()
            }
        }
    }
}
