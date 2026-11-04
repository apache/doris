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

suite("sparse_ancestor_promotion_extended", "p0,nonConcurrent") {
    sql "SET default_variant_enable_doc_mode = false"
    sql "SET describe_extend_variant_column = true"
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
        // Query equality alone also passes when compaction fails and leaves inputs intact.
        qt_compaction_status "SELECT '${after['last full status'].toString().replace("'", "''")}'"
        assert after.rowsets.size() == 1 : after
        assert after.rowsets.intersect(before.rowsets).isEmpty() : after
        qt_output_rows """SELECT count(*), sum(NUM_SEGMENTS), sum(ROWSET_NUM_ROWS)
            FROM information_schema.rowsets WHERE TABLET_ID=${tablet.TabletId} AND NUM_SEGMENTS>0"""
        logger.info("${tableName}: replaced ${before.rowsets} with ${after.rowsets}")
    }
    // The existing sparse_ancestor_promotion suite covers the format/shard/key-model
    // matrix. Add one deep-path case with heterogeneous values and three generations
    // of promotion, demotion and promotion, all on the same tablet.
    sql "DROP TABLE IF EXISTS sparse_ancestor_extended_deep"
    sql """CREATE TABLE sparse_ancestor_extended_deep (id INT, v VARIANT<PROPERTIES(
        "variant_max_subcolumns_count"="1", "variant_enable_nested_group"="false",
        "variant_sparse_hash_shard_count"="1")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
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
                value['a-'] = batch + 200
                value['a%2Eb'] = batch + 300
            }
            rows.add("(${batch * 6 + row + 1}, parse_to_variant('${JsonOutput.toJson(value)}'))")
        }
        sql "INSERT INTO sparse_ancestor_extended_deep VALUES ${rows.join(',')}"
    }
    def projection = """SELECT id, CAST(v['p']['q']['a'] AS STRING), v['p']['q']['a']['child']['leaf'],
        CAST(v AS STRING) FROM sparse_ancestor_extended_deep ORDER BY id"""
    qt_before projection
    setBeConfigTemporary([enable_vertical_compaction:true,
            enable_vertical_compact_variant_subcolumns:true,
            vertical_compaction_num_columns_per_group:1]) {
        compact('sparse_ancestor_extended_deep')
        qt_promoted projection
        order_qt_promoted_layout "DESC sparse_ancestor_extended_deep"
        // Ten occurrences force the ancestor back into sparse storage.
        sql """INSERT INTO sparse_ancestor_extended_deep SELECT 100 + number,
            parse_to_variant(concat('{"takeover":', number, '}'))
            FROM numbers("number"="10")"""
        compact('sparse_ancestor_extended_deep')
        qt_demoted projection
        order_qt_demoted_layout "DESC sparse_ancestor_extended_deep"
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
                rows.add("(${200 + batch * 6 + row}, parse_to_variant('${JsonOutput.toJson(value)}'))")
            }
            sql "INSERT INTO sparse_ancestor_extended_deep VALUES ${rows.join(',')}"
        }
        compact('sparse_ancestor_extended_deep')
        qt_repromoted projection
        order_qt_repromoted_layout "DESC sparse_ancestor_extended_deep"
    }

    // Updates and delete predicates exercise paths where physical count equality
    // cannot replace checking the visible latest row and its complete root value.
    sql "DROP TABLE IF EXISTS sparse_ancestor_extended_mow"
    sql """CREATE TABLE sparse_ancestor_extended_mow (id INT, v VARIANT<PROPERTIES(
        "variant_max_subcolumns_count"="1", "variant_enable_nested_group"="false",
        "variant_sparse_hash_shard_count"="1")>) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1",
        "disable_auto_compaction"="true", "enable_unique_key_merge_on_write"="true",
        "storage_format"="V3")"""
    for (int batch = 0; batch < 3; ++batch) {
        sql """INSERT INTO sparse_ancestor_extended_mow VALUES
            (${batch*5+1},parse_to_variant('{"p":{"a":${batch+10}},"hot_${batch}":1}')),
            (${batch*5+2},parse_to_variant('{"p":{"a":${batch+20}},"hot_${batch}":2}')),
            (${batch*5+3},parse_to_variant('{"p":{"a":{"b":${batch+30}}},"hot_${batch}":3}')),
            (${batch*5+4},parse_to_variant('{"hot_${batch}":4}')),
            (${batch*5+5},parse_to_variant('{"hot_${batch}":5}'))"""
    }
    def mowProjection = """SELECT id, CAST(v['p'] AS STRING), CAST(v['p']['a'] AS STRING),
        v['p']['a']['b'], CAST(v AS STRING) FROM sparse_ancestor_extended_mow ORDER BY id"""
    compact('sparse_ancestor_extended_mow')
    sql """INSERT INTO sparse_ancestor_extended_mow VALUES
        (1,parse_to_variant('{"p":{"a":{"b":901}}}')),
        (3,parse_to_variant('{"p":{"a":903}}')),
        (6,parse_to_variant('{"p":{"a":[]}}')),
        (8,parse_to_variant('{"p":{"a":null}}'))"""
    sql "DELETE FROM sparse_ancestor_extended_mow WHERE id=2"
    qt_mow_before_second mowProjection
    compact('sparse_ancestor_extended_mow')
    qt_mow_second mowProjection
}
