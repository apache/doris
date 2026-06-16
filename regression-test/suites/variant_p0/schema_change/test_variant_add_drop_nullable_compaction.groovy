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

suite("test_variant_add_drop_nullable_compaction", "variant_type,nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_variant_add_drop_nullable_compaction"
    sql "set default_variant_enable_doc_mode = false"
    sql """
        CREATE TABLE test_variant_add_drop_nullable_compaction (
            k int,
            v variant,
            old_v variant
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        );
    """

    sql """
        INSERT INTO test_variant_add_drop_nullable_compaction VALUES
            (0, '{"a":"old-one"}', '{"gone":"drop-one"}'),
            (1, '{"a":"old-two"}', '{"gone":"drop-two"}');
    """

    sql "ALTER TABLE test_variant_add_drop_nullable_compaction DROP COLUMN old_v"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_variant_add_drop_nullable_compaction' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    sql "ALTER TABLE test_variant_add_drop_nullable_compaction ADD COLUMN v2 variant<'x':string> DEFAULT NULL"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_variant_add_drop_nullable_compaction' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    sql """
        INSERT INTO test_variant_add_drop_nullable_compaction VALUES
            (2, '{"a":"new-one"}', '{"x":"added-one"}'),
            (3, '{"a":"new-two"}', '{"x":"added-two"}');
    """
    sql "SYNC"

    order_qt_before_compaction """
        SELECT k, cast(v2['x'] as string)
        FROM test_variant_add_drop_nullable_compaction
        ORDER BY k
    """

    def tabletStats = sql_return_maparray "SHOW TABLETS FROM test_variant_add_drop_nullable_compaction"
    assertEquals(1, tabletStats.size())
    def tablet = tabletStats[0]
    def (code, out, err) = curl("GET", tablet.CompactionStatus)
    assertEquals(0, code)
    def tabletStatus = parseJson(out.trim())
    assert tabletStatus.rowsets instanceof List
    logger.info("tablet ${tablet.TabletId} rowsets before compaction: ${tabletStatus.rowsets}")

    def rowsetVersions = ((List<String>) tabletStatus.rowsets).collect { rowset ->
        def matcher = rowset =~ /\[(\d+)-(\d+)\]/
        assert matcher.find(): "Cannot parse rowset version from ${rowset}"
        [matcher.group(1).toInteger(), matcher.group(2).toInteger()]
    }.findAll { version -> version[1] >= 2 }
    assertTrue(rowsetVersions.size() >= 2,
            "Expected at least two data rowsets across add-variant schema change, got ${tabletStatus.rowsets}")
    int startVersion = rowsetVersions.first()[0]
    int endVersion = rowsetVersions.last()[1]
    logger.info("force cumulative compaction for tablet ${tablet.TabletId}, versions [${startVersion}, ${endVersion}]")

    try {
        GetDebugPoint().enableDebugPointForAllBEs(
                "SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                [tablet_id: "${tablet.TabletId}", start_version: "${startVersion}", end_version: "${endVersion}"])
        trigger_and_wait_compaction("test_variant_add_drop_nullable_compaction", "cumulative", 1800)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(
                "SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
    }

    order_qt_after_compaction """
        SELECT k, cast(v2['x'] as string)
        FROM test_variant_add_drop_nullable_compaction
        ORDER BY k
    """

    order_qt_count_after_compaction "SELECT count(*) FROM test_variant_add_drop_nullable_compaction"
}
