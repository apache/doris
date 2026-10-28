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

suite("test_sequence_delete_sign_retention_version_gap", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    for (def vertical : [false, true]) {
        setBeConfigTemporary([
                sequence_delete_sign_retention_version_gap: 31,
                enable_prune_delete_sign_when_base_compaction: true,
                enable_vertical_compaction: vertical,
                vertical_compaction_num_columns_per_group: 1,
                compaction_promotion_version_count: 5,
                base_compaction_min_rowset_num: 1]) {
            sql "DROP TABLE IF EXISTS test_sequence_delete_sign_retention_version_gap FORCE"
            sql """CREATE TABLE test_sequence_delete_sign_retention_version_gap (
                    k1 INT NOT NULL, c1 INT, seq BIGINT NOT NULL
                ) UNIQUE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "enable_mow_light_delete" = "false",
                    "disable_auto_compaction" = "true",
                    "function_column.sequence_col" = "seq")"""

            def readTablet = { tablet ->
                def (code, out, err) = curl('GET', tablet.CompactionStatus.toString(), null, 5, '', '', 1)
                assertEquals(0, code, "cannot read tablet ${tablet.TabletId}: ${err}")
                parseJson(out)
            }
            def rowsetRanges = { state ->
                assertTrue(state.rowsets instanceof List, "missing rowsets: ${state}")
                state.rowsets.collect { it.toString().split(/\s+/)[0] }.sort()
            }
            def compactAndCheck = { tablets, type, ranges, point ->
                def before = tablets.collectEntries { [(it.TabletId): readTablet(it)] }
                trigger_and_wait_compaction('test_sequence_delete_sign_retention_version_gap', type)
                tablets.each { tablet ->
                    def state = [:]
                    try {
                        awaitUntil(60, 0.5) {
                            state = readTablet(tablet)
                            if (state["last ${type} failure time"] != before[tablet.TabletId]["last ${type} failure time"]) {
                                assertEquals('[OK]', state["last ${type} status"], "compaction failed: ${state}")
                            }
                            // The shared helper may ignore no-suitable-version errors. Require
                            // an actual successful compaction and the intended physical layout.
                            state["last ${type} success time"] != before[tablet.TabletId]["last ${type} success time"] &&
                                    state["last ${type} status"] == '[OK]' &&
                                    rowsetRanges(state) == ranges.sort(false) &&
                                    (point == null || (state['cumulative point'] as long) == point)
                        }
                    } catch (Throwable t) {
                        throw new AssertionError("${type} compaction (vertical=${vertical}) did not produce " +
                                "${ranges} on ${tablet.TabletId}: ${state}", t)
                    }
                    assertTrue(state.missing_rowsets instanceof List && state.missing_rowsets.isEmpty(),
                            "tablet has a version gap: ${state}")
                }
            }
            def checkDelete = {
                def original = sql('select @@skip_delete_sign, @@skip_delete_bitmap, @@show_hidden_columns')[0]
                try {
                    sql 'set show_hidden_columns=true'
                    sql 'set skip_delete_sign=true'
                    sql 'set skip_delete_bitmap=true'
                    qt_delete_version """select __DORIS_SEQUENCE_COL__, __DORIS_VERSION_COL__
                        from test_sequence_delete_sign_retention_version_gap
                        where k1=1 and __DORIS_DELETE_SIGN__=1 order by __DORIS_VERSION_COL__"""
                } finally {
                    try {
                        sql "set skip_delete_sign=${original[0]}"
                    } finally {
                        try {
                            sql "set skip_delete_bitmap=${original[1]}"
                        } finally {
                            sql "set show_hidden_columns=${original[2]}"
                        }
                    }
                }
            }

            // Versions 2..31: promote the first cumulative output into the base layer.
            (1..30).each {
                sql "insert into test_sequence_delete_sign_retention_version_gap values (${it},${it},100)"
            }
            def tablets = sql_return_maparray('show tablets from test_sequence_delete_sign_retention_version_gap')
            syncAndWaitTabletVersion(tablets, 31)
            compactAndCheck(tablets, 'cumulative', ['[0-1]', '[2-31]'], 32)

            // Version 32 contains the sequence=100 delete sign. Cumulative compaction
            // must preserve its original published version when combining it with 33..61.
            sql 'delete from test_sequence_delete_sign_retention_version_gap where k1=1'
            (31..59).each {
                sql "insert into test_sequence_delete_sign_retention_version_gap values (${it},${it},100)"
            }
            syncAndWaitTabletVersion(tablets, 61)
            compactAndCheck(tablets, 'cumulative', ['[0-1]', '[2-31]', '[32-61]'], 62)
            checkDelete()
            qt_deleted_key "select k1,c1,seq from test_sequence_delete_sign_retention_version_gap where k1=1 order by k1"

            // max_version=62, row_version=32, gap=30 < 31: both base and full must retain it.
            sql 'insert into test_sequence_delete_sign_retention_version_gap values (60,60,100)'
            syncAndWaitTabletVersion(tablets, 62)
            compactAndCheck(tablets, 'base', ['[0-61]', '[62-62]'], 62)
            checkDelete()
            qt_deleted_key "select k1,c1,seq from test_sequence_delete_sign_retention_version_gap where k1=1 order by k1"
            compactAndCheck(tablets, 'full', ['[0-62]'], null)
            checkDelete()
            qt_deleted_key "select k1,c1,seq from test_sequence_delete_sign_retention_version_gap where k1=1 order by k1"

            // A lower-sequence write is rejected while the delete sign is physically present.
            sql 'insert into test_sequence_delete_sign_retention_version_gap values (1,90,90)'
            syncAndWaitTabletVersion(tablets, 63)
            checkDelete()
            qt_deleted_key "select k1,c1,seq from test_sequence_delete_sign_retention_version_gap where k1=1 order by k1"

            // max_version=63: the original DELETE is exactly 31 versions old. After full
            // compaction it is physically gone, so a subsequent lower-sequence write can enter.
            compactAndCheck(tablets, 'full', ['[0-63]'], null)
            checkDelete()
            sql 'insert into test_sequence_delete_sign_retention_version_gap values (1,90,90)'
            syncAndWaitTabletVersion(tablets, 64)
            qt_reinsert "select k1,c1,seq from test_sequence_delete_sign_retention_version_gap where k1=1 order by k1"
        }
    }
}
