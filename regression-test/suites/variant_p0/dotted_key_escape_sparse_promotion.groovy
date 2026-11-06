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

import groovy.json.JsonOutput

suite("dotted_key_escape_sparse_promotion", "p0,nonConcurrent") {
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

    def basic = {
        def backendIPs = [:]
        def backendHttpPorts = [:]
        getBackendIpHttpPort(backendIPs, backendHttpPorts)
        def checkFullStatus = { String tableName ->
            // Waiting for completion alone also accepts a failed compaction with intact old
            // rowsets. Record its status so unchanged query results cannot hide that failure.
            for (def tablet : sql_return_maparray("SHOW TABLETS FROM ${tableName}")) {
                def (code, out, err) = be_show_tablet_status(
                    backendIPs["${tablet.BackendId}"], backendHttpPorts["${tablet.BackendId}"], tablet.TabletId)
                if (code != 0) {
                    throw new Exception("Unable to read compaction status: ${err}")
                }
                assert parseJson(out).containsKey("last full status")
                def status = parseJson(out)["last full status"].toString().replace("'", "''")
                assertEquals("[OK]", status)
                qt_full_status "SELECT '${status}'"
            }
        }
        for (def storageFormat : ["V2", "V3"]) {
            for (def shardCount : [1, 3]) {
                for (def keyModel : ["DUPLICATE", "UNIQUE"]) {
                    def tableName = "dotted_escape_promotion_${storageFormat}_${shardCount}_${keyModel}"
                    def mowProperty = keyModel == "UNIQUE" ? ', "enable_unique_key_merge_on_write"="true"' : ''
                    sql "DROP TABLE IF EXISTS ${tableName}"
                    sql """
                        CREATE TABLE ${tableName} (
                            id INT,
                            v VARIANT<PROPERTIES(
                                "variant_max_subcolumns_count"="1",
                                "variant_enable_nested_group"="false",
                                "variant_sparse_hash_shard_count"="${shardCount}")>
                        ) ${keyModel} KEY(id)
                        DISTRIBUTED BY HASH(id) BUCKETS 1
                        PROPERTIES("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true",
                                   "storage_format"="${storageFormat}"${mowProperty})
                    """
                    // Each rowset materializes hot_i (5 rows), leaving a (2) and a.b (1)
                    // sparse. Across rowsets a (6) wins over every hot_i (5) and is promoted.
                    // Its physical count must stay 6: a.b must not become another non-null a.
                    for (int batch = 0; batch < 3; ++batch) {
                        int id = batch * 5
                        int value = batch * 100
                        sql """INSERT INTO ${tableName} VALUES
                            (${id + 1}, CAST(CAST('{"a":${value + 1},"hot_${batch}":1}' AS JSON) AS VARIANT)),
                            (${id + 2}, CAST(CAST('{"a":${value + 2},"hot_${batch}":2}' AS JSON) AS VARIANT)),
                            (${id + 3}, CAST(CAST('{"hot_${batch}":3}' AS JSON) AS VARIANT)),
                            (${id + 4}, CAST(CAST('{"hot_${batch}":4}' AS JSON) AS VARIANT)),
                            (${id + 5}, CAST(CAST('{"a.b":${value+1005},"a%2Eb":${value+2005},"a":{"b":${value + 5}},"hot_${batch}":5}' AS JSON) AS VARIANT))
                        """
                    }
                    def projection = """SELECT id, CAST(v['a'] AS STRING), v['a']['b'],
                        v['a.b'], v['a%2Eb'], CAST(v AS STRING) FROM ${tableName} ORDER BY id"""
                    def checkPaths = {
                        sql(projection).each { row ->
                            def object = new groovy.json.JsonSlurper().parseText(row[5])
                            def a = object['a']
                            assertEquals(a == null ? null : groovy.json.JsonOutput.toJson(a), row[1]?.toString())
                            assertEquals(a instanceof Map ? a['b']?.toString() : null, row[2]?.toString())
                            assertEquals(object['a.b']?.toString(), row[3]?.toString())
                            assertEquals(object['a%2Eb']?.toString(), row[4]?.toString())
                        }
                    }
                    checkPaths()
                    qt_before projection
                    setBeConfigTemporary([cumulative_compaction_min_deltas:2, cumulative_compaction_max_deltas:2]) {
                        trigger_and_wait_compaction(tableName, "cumulative", 1800)
                    }
                    checkPaths()
                    qt_after_cumulative projection
                    trigger_and_wait_compaction(tableName, "full", 1800)
                    checkFullStatus(tableName)
                    checkPaths()
                    qt_after_first projection

                    // Add another source with a sparse ancestor and compact against the new
                    // materialized ancestor, exercising both sides of the promotion boundary.
                    sql """INSERT INTO ${tableName} VALUES
                        (16, CAST(CAST('{"a":301,"hot_3":1}' AS JSON) AS VARIANT)),
                        (17, CAST(CAST('{"a":302,"hot_3":2}' AS JSON) AS VARIANT)),
                        (18, CAST(CAST('{"hot_3":3}' AS JSON) AS VARIANT)),
                        (19, CAST(CAST('{"hot_3":4}' AS JSON) AS VARIANT)),
                        (20, CAST(CAST('{"a.b":1305,"a%2Eb":2305,"a":{"b":305},"hot_3":5}' AS JSON) AS VARIANT))
                    """
                    checkPaths()
                    qt_before_second projection
                    trigger_and_wait_compaction(tableName, "full", 1800)
                    checkFullStatus(tableName)
                    checkPaths()
                    qt_after_second projection
                }
            }
        }
    }
    basic()
    def extended = {
        def backendIPs = [:]
        def backendHttpPorts = [:]
        getBackendIpHttpPort(backendIPs, backendHttpPorts)
        def statusOf = { tablet ->
            def (code, out, err) = be_show_tablet_status(
                backendIPs["${tablet.BackendId}"], backendHttpPorts["${tablet.BackendId}"], tablet.TabletId)
            assert code == 0 : err
            parseJson(out)
        }
        def compact = { String tableName ->
            def tablet = sql_return_maparray("SHOW TABLETS FROM ${tableName}")[0]
            def before = statusOf(tablet)
            assert before.rowsets.size() > 1 : before
            trigger_and_wait_compaction(tableName, "full", 300)
            def after = statusOf(tablet)
            assert after.containsKey("last full status")
            // Query equality alone also passes when compaction fails and leaves inputs intact.
            assertEquals("[OK]", after["last full status"].toString())
            qt_compaction_status "SELECT '[OK]'"
            assert after.rowsets.size() == 1 : after
            assert after.rowsets.intersect(before.rowsets).isEmpty() : after
            logger.info("${tableName}: replaced ${before.rowsets} with ${after.rowsets}")
        }
        // The matrix above covers both storage formats and shard counts with DUP/MOW.
        // Add a deep-path case with heterogeneous values and three generations
        // of promotion, demotion and promotion, all on the same tablet.
        sql "DROP TABLE IF EXISTS dotted_escape_extended_deep"
        sql """CREATE TABLE dotted_escape_extended_deep (id INT, v VARIANT<PROPERTIES(
            "variant_max_subcolumns_count"="1",
            "variant_enable_nested_group"="false",
            "variant_sparse_hash_shard_count"="3")>)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true",
                       "storage_format"="V3")"""
        def wrap = { value -> [q: [a: value]] }
        def payloads = [[11, 'text', true], [[], [1, null, [x: 2]], 1.5],
                        [[:], null, [[x: 3], [x: 4]]]]
        for (int batch = 0; batch < 3; ++batch) {
            def rows = []
            for (int row = 0; row < 6; ++row) {
                def value = [("hot_${batch}".toString()): row]
                if (row < 3) {
                    value['p'] = wrap(payloads[batch][row])
                } else if (row == 3) {
                    value['p'] = wrap([child: [leaf: batch + 100]])
                } else if (row == 4) {
                    value['a'] = [b: batch + 1000]
                    value['a.b'] = batch + 2000
                    value['p.q.a'] = batch + 3000
                    value['a-'] = batch + 200
                    value['a%2Eb'] = batch + 300
                }
                rows.add("(${batch * 6 + row + 1}, CAST(CAST('${JsonOutput.toJson(value)}' AS JSON) AS VARIANT))")
            }
            sql "INSERT INTO dotted_escape_extended_deep VALUES ${rows.join(',')}"
        }
        def projection = """SELECT id, CAST(v['p']['q']['a'] AS STRING), v['p']['q']['a']['child']['leaf'],
            v['a.b'], v['a']['b'], v['p.q.a'], CAST(v AS STRING) FROM dotted_escape_extended_deep ORDER BY id"""
        qt_before projection
        setBeConfigTemporary([enable_vertical_compaction:true,
                vertical_compaction_num_columns_per_group:1]) {
            compact('dotted_escape_extended_deep')
            qt_promoted projection
            // Ten occurrences force the ancestor back into sparse storage.
            sql """INSERT INTO dotted_escape_extended_deep SELECT 100 + number,
                CAST(CAST(concat('{"takeover":', number, '}') AS JSON) AS VARIANT)
                FROM numbers("number"="10")"""
            compact('dotted_escape_extended_deep')
            qt_demoted projection
            // A third generation promotes the ancestor from both old sparse
            // output and newly written rowsets, while descendants stay sparse.
            for (int batch = 0; batch < 3; ++batch) {
                def rows = []
                for (int row = 0; row < 6; ++row) {
                    def value = [("new_hot_${batch}".toString()): row]
                    if (row < 4) {
                        value['p'] = wrap(batch * 10 + row + 400)
                    } else if (row == 4) {
                        value['p'] = wrap([child: [leaf: batch + 500]])
                    }
                    rows.add("(${200 + batch * 6 + row}, CAST(CAST('${JsonOutput.toJson(value)}' AS JSON) AS VARIANT))")
                }
                sql "INSERT INTO dotted_escape_extended_deep VALUES ${rows.join(',')}"
            }
            compact('dotted_escape_extended_deep')
            qt_repromoted projection
        }

        // Updates and delete predicates exercise paths where physical count equality
        // cannot replace checking the visible latest row and its complete root value.
        sql "DROP TABLE IF EXISTS dotted_escape_extended_mow"
        sql """CREATE TABLE dotted_escape_extended_mow (id INT, v VARIANT<PROPERTIES(
            "variant_max_subcolumns_count"="1",
            "variant_enable_nested_group"="false",
            "variant_sparse_hash_shard_count"="3")>) UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("variant_escape_dots_in_keys"="true", "replication_num"="1",
            "disable_auto_compaction"="true", "enable_unique_key_merge_on_write"="true",
            "storage_format"="V3")"""
        for (int batch = 0; batch < 3; ++batch) {
            sql """INSERT INTO dotted_escape_extended_mow VALUES
                (${batch*5+1},CAST(CAST('{"p":{"a":${batch+10}},"hot_${batch}":1}' AS JSON) AS VARIANT)),
                (${batch*5+2},CAST(CAST('{"p":{"a":${batch+20}},"hot_${batch}":2}' AS JSON) AS VARIANT)),
                (${batch*5+3},CAST(CAST('{"p":{"a":{"b":${batch+30}}},"hot_${batch}":3}' AS JSON) AS VARIANT)),
                (${batch*5+4},CAST(CAST('{"hot_${batch}":4}' AS JSON) AS VARIANT)),
                (${batch*5+5},CAST(CAST('{"hot_${batch}":5}' AS JSON) AS VARIANT))"""
        }
        def mowProjection = """SELECT id, CAST(v['p'] AS STRING), CAST(v['p']['a'] AS STRING),
            v['p']['a']['b'], CAST(v AS STRING) FROM dotted_escape_extended_mow ORDER BY id"""
        compact('dotted_escape_extended_mow')
        sql """INSERT INTO dotted_escape_extended_mow VALUES
            (1,CAST(CAST('{"p":{"a":{"b":901}}}' AS JSON) AS VARIANT)),
            (3,CAST(CAST('{"p":{"a":903}}' AS JSON) AS VARIANT)),
            (6,CAST(CAST('{"p":{"a":[]}}' AS JSON) AS VARIANT)),
            (8,CAST(CAST('{"p":{"a":null}}' AS JSON) AS VARIANT))"""
        sql "DELETE FROM dotted_escape_extended_mow WHERE id=2"
        qt_mow_before_second mowProjection
        compact('dotted_escape_extended_mow')
        qt_mow_second mowProjection
    }
    extended()
}
