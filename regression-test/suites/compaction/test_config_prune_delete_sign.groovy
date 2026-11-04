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

suite("test_config_prune_delete_sign", "nonConcurrent") {

    def customBeConfig = [
        enable_prune_delete_sign_when_base_compaction: false,
        compaction_promotion_version_count: 5,
        base_compaction_min_rowset_num: 1
    ]

    setBeConfigTemporary(customBeConfig) {
        def table1 = "test_config_prune_delete_sign"
        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "enable_mow_light_delete" = "false",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1"); """

        def getDeleteSignCnt = {
            def original = sql('select @@skip_delete_sign, @@skip_delete_bitmap')[0]
            try {
                sql "set skip_delete_sign=true;"
                sql "set skip_delete_bitmap=true;"
                qt_del_cnt "select count() from ${table1} where __DORIS_DELETE_SIGN__=1;"
            } finally {
                try {
                    sql "set skip_delete_sign=${original[0]};"
                } finally {
                    sql "set skip_delete_bitmap=${original[1]};"
                }
            }
        }

        def tabletKey = { tablet -> "${tablet.TabletId}@${tablet.BackendId}".toString() }
        def readTablet = { tablet ->
            def (code, out, err) = curl('GET', tablet.CompactionStatus.toString(), null, 5, '', '', 1)
            assertEquals(0, code, "cannot read tablet ${tabletKey(tablet)}: ${err}")
            parseJson(out)
        }
        def rowsetRanges = { state ->
            assertTrue(state.rowsets instanceof List, "missing rowsets: ${state}")
            state.rowsets.collect { it.toString().split(/\s+/)[0] }.sort()
        }
        def assertLayout = { state, ranges, point ->
            assertEquals(ranges.sort(false), rowsetRanges(state))
            assertTrue(state.missing_rowsets instanceof List && state.missing_rowsets.isEmpty(),
                    "tablet has a version gap: ${state}")
            assertEquals(point as long, state['cumulative point'] as long)
        }
        def compactAndCheck = { tablets, type, ranges, point ->
            def before = tablets.collectEntries { [(tabletKey(it)): readTablet(it)] }
            trigger_and_wait_compaction(table1, type)
            tablets.each { tablet ->
                def old = before[tabletKey(tablet)]
                def state = [:]
                try {
                    awaitUntil(60, 0.5) {
                        state = readTablet(tablet)
                        if (state["last ${type} failure time"] != old["last ${type} failure time"]) {
                            assertEquals('[OK]', state["last ${type} status"],
                                    "${type} compaction failed for ${tabletKey(tablet)}: ${state}")
                        }
                        // The shared helper may ignore E-2000/E-2010. Require this attempt
                        // to succeed and produce the intended physical rowsets on every BE.
                        state["last ${type} success time"] != old["last ${type} success time"] &&
                                state["last ${type} status"] == '[OK]' &&
                                rowsetRanges(state) == ranges.sort(false) &&
                                (state['cumulative point'] as long) == point
                    }
                } catch (Throwable t) {
                    throw new AssertionError("${type} compaction did not produce ${ranges} on " +
                            "${tabletKey(tablet)}: ${state}", t)
                }
                assertLayout(state, ranges, point)
            }
        }

        (1..30).each {
            sql "insert into ${table1} values($it,$it,$it);"
        }
        // The first 30 INSERTs advance the tablet from version 1 to version 31. Make that
        // version visible on the serving BE before cumulative compaction is triggered.
        def tablets = sql_return_maparray("show tablets from ${table1};")
        syncAndWaitTabletVersion(tablets, 31)
        compactAndCheck(tablets, 'cumulative', ['[0-1]', '[2-31]'], 32)

        sql "delete from ${table1} where k1<=20;"
        sql "sync;"
        qt_sql "select count() from ${table1};"
        getDeleteSignCnt()

        (31..59).each {
            sql "insert into ${table1} values($it,$it,$it);"
        }
        // The DELETE consumes version 32 and these 29 INSERTs consume versions 33 through 61.
        tablets = sql_return_maparray("show tablets from ${table1};")
        syncAndWaitTabletVersion(tablets, 61)
        compactAndCheck(tablets, 'cumulative', ['[0-1]', '[2-31]', '[32-61]'], 62)

        // Both cumulative outputs are promoted. Keep a newer rowset outside the base
        // inputs; its visibility does not require compacting it or moving the point again.
        sql "insert into ${table1} values(60,60,60);"
        tablets = sql_return_maparray("show tablets from ${table1};")
        syncAndWaitTabletVersion(tablets, 62)
        tablets.each { tablet ->
            assertLayout(readTablet(tablet), ['[0-1]', '[2-31]', '[32-61]', '[62-62]'], 62)
        }

        // Cloud excludes the empty [0-1] rowset. In either mode, base must consume the
        // two promoted data rowsets and leave version 62 in the cumulative layer.
        def baseRanges = isCloudMode() ? ['[0-1]', '[2-61]', '[62-62]'] : ['[0-61]', '[62-62]']
        compactAndCheck(tablets, 'base', baseRanges, 62)
        qt_sql "select count() from ${table1};"
        getDeleteSignCnt()
    }
}
