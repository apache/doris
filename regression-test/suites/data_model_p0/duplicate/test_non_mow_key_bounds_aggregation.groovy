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

import org.apache.doris.regression.util.Http

// Verifies that when `enable_aggregate_non_mow_key_bounds` is on, non-MOW
// rowsets collapse per-segment key bounds into a single [min, max] entry
// while MOW rowsets keep per-segment bounds regardless of the config.
suite("test_non_mow_key_bounds_aggregation", "nonConcurrent") {

    // see be/src/util/key_util.h:50
    def keyNormalMarker = new String(new Byte[]{2})

    def fetchRowsetMetaAtVersion = { String table, int version ->
        def metaUrl = sql_return_maparray("show tablets from ${table};").get(0).MetaUrl
        def jsonMeta = Http.GET(metaUrl, true, false)
        for (def meta : jsonMeta.rs_metas) {
            if (meta.end_version as int == version) {
                return meta
            }
        }
        if (isCloudMode()) {
            for (int i = 0; i < 60; i++) {
                Thread.sleep(1000)
                jsonMeta = Http.GET(metaUrl, true, false)
                for (def meta : jsonMeta.rs_metas) {
                    if (meta.end_version as int == version) {
                        return meta
                    }
                }
            }
        }
        return null
    }

    def dupTable = "test_non_mow_key_bounds_aggregation_dup"
    def mowTable = "test_non_mow_key_bounds_aggregation_mow"

    sql """ DROP TABLE IF EXISTS ${dupTable} force; """
    sql """ CREATE TABLE ${dupTable} (
        `k` varchar(65533) NOT NULL,
        `v` int)
        DUPLICATE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES("replication_num" = "1",
                "disable_auto_compaction" = "true"); """

    sql """ DROP TABLE IF EXISTS ${mowTable} force; """
    sql """ CREATE TABLE ${mowTable} (
        `k` varchar(65533) NOT NULL,
        `v` int)
        UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES("replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"); """

    int dupVersion = 1
    int mowVersion = 1

    // Disable truncation so bounds content equals the raw keys (with marker prefix).
    // Enable aggregation to exercise the new code path for non-MOW rowsets.
    def configOn = [
        segments_key_bounds_truncation_threshold: -1,
        enable_aggregate_non_mow_key_bounds     : true,
    ]

    setBeConfigTemporary(configOn) {
        // Case 1: non-MOW with config on -> aggregated single [min, max] entry.
        String dupK1 = "aaaaaaaa"
        String dupK2 = "mmmmmmmm"
        String dupK3 = "zzzzzzzz"
        sql """insert into ${dupTable} values("${dupK1}", 1), ("${dupK3}", 3), ("${dupK2}", 2);"""
        def dupMeta = fetchRowsetMetaAtVersion(dupTable, ++dupVersion)
        assertNotNull(dupMeta)
        logger.info("dup rowset meta (config on): ${dupMeta}")
        assertTrue(dupMeta.segments_key_bounds_aggregated as boolean)
        assertEquals(1, dupMeta.segments_key_bounds.size())
        assertEquals(keyNormalMarker + dupK1, dupMeta.segments_key_bounds.get(0).min_key)
        assertEquals(keyNormalMarker + dupK3, dupMeta.segments_key_bounds.get(0).max_key)

        // Case 2: MOW with config on -> per-segment bounds preserved, flag unset.
        String mowK1 = "mmmm1111"
        String mowK2 = "mmmm2222"
        sql """insert into ${mowTable} values("${mowK1}", 1), ("${mowK2}", 2);"""
        def mowMeta = fetchRowsetMetaAtVersion(mowTable, ++mowVersion)
        assertNotNull(mowMeta)
        logger.info("mow rowset meta (config on): ${mowMeta}")
        assertFalse((mowMeta.segments_key_bounds_aggregated ?: false) as boolean)
        assertEquals(mowMeta.num_segments as int, mowMeta.segments_key_bounds.size())
    }

    // Case 3: non-MOW with config off -> per-segment bounds (size == num_segments), flag unset.
    def configOff = [
        segments_key_bounds_truncation_threshold: -1,
        enable_aggregate_non_mow_key_bounds     : false,
    ]
    setBeConfigTemporary(configOff) {
        String dupK1 = "bbbb0000"
        String dupK2 = "bbbb9999"
        sql """insert into ${dupTable} values("${dupK1}", 10), ("${dupK2}", 20);"""
        def dupMeta = fetchRowsetMetaAtVersion(dupTable, ++dupVersion)
        assertNotNull(dupMeta)
        logger.info("dup rowset meta (config off): ${dupMeta}")
        assertFalse((dupMeta.segments_key_bounds_aggregated ?: false) as boolean)
        assertEquals(dupMeta.num_segments as int, dupMeta.segments_key_bounds.size())
    }

    // Case 4: rebuilding an already-aggregated non-MOW rowset through the index rewrite
    // path must preserve the aggregated flag and single-entry layout. Regression guard for
    // IndexBuilder clobbering the flag via set_segments_key_bounds's default parameter.
    // IndexBuilder runs on local rowsets only, so skip this case in cloud mode.
    if (!isCloudMode()) {
        def idxTable = "test_non_mow_key_bounds_aggregation_idx"
        sql """ DROP TABLE IF EXISTS ${idxTable} force; """
        sql """ CREATE TABLE ${idxTable} (
            `k` varchar(65533) NOT NULL,
            `v` int,
            INDEX idx_v (v) USING INVERTED)
            DUPLICATE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES("replication_num" = "1",
                    "disable_auto_compaction" = "true",
                    "light_schema_change" = "true"); """

        def waitAlterFinish = { String tbl, int timeoutSec ->
            for (int i = 0; i < timeoutSec; i++) {
                def rs = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tbl}" ORDER BY CreateTime DESC LIMIT 1;"""
                if (rs.size() == 0 || rs[0][9] == "FINISHED") {
                    return
                }
                if (rs[0][9] == "CANCELLED") {
                    throw new IllegalStateException("alter cancelled: ${rs}")
                }
                Thread.sleep(1000)
            }
            throw new IllegalStateException("waitAlterFinish timeout")
        }

        setBeConfigTemporary(configOn) {
            String idxK1 = "idx00000"
            String idxK2 = "idxzzzzz"
            sql """insert into ${idxTable} values("${idxK1}", 1), ("${idxK2}", 2);"""
            int idxVersion = 2
            def beforeMeta = fetchRowsetMetaAtVersion(idxTable, idxVersion)
            assertNotNull(beforeMeta)
            logger.info("idx rowset meta before DROP INDEX: ${beforeMeta}")
            assertTrue(beforeMeta.segments_key_bounds_aggregated as boolean)
            assertEquals(1, beforeMeta.segments_key_bounds.size())

            // DROP INDEX drives IndexBuilder, which rebuilds rowset meta in place.
            sql """ALTER TABLE ${idxTable} DROP INDEX idx_v;"""
            waitAlterFinish(idxTable, 120)

            def afterMeta = fetchRowsetMetaAtVersion(idxTable, idxVersion)
            assertNotNull(afterMeta)
            logger.info("idx rowset meta after DROP INDEX: ${afterMeta}")
            assertTrue(afterMeta.segments_key_bounds_aggregated as boolean)
            assertEquals(1, afterMeta.segments_key_bounds.size())
            assertEquals(beforeMeta.segments_key_bounds.get(0).min_key,
                         afterMeta.segments_key_bounds.get(0).min_key)
            assertEquals(beforeMeta.segments_key_bounds.get(0).max_key,
                         afterMeta.segments_key_bounds.get(0).max_key)
        }
    }
}
