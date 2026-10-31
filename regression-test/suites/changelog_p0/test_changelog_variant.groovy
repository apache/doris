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

// A changelog keeps the AFTER and BEFORE images of every Variant column with all paths in the sparse
// column, whatever the source column's layout, and changelog reads never push a Variant sub-path into the
// changelog scan: sub-paths are taken from whole values, so an UPDATE_BEFORE row shows the old value.
suite("test_changelog_variant", "nonConcurrent") {
    setBeConfigTemporary([
        changelog_compaction_goal_size_mbytes: 0,
        changelog_compaction_file_count_threshold: 2,
        changelog_compaction_wait_timesec_after_visible: 0,
        changelog_compaction_time_threshold_seconds: 86400
    ]) {
        sql "DROP STREAM IF EXISTS test_changelog_variant_md_stream FORCE"
        sql "DROP STREAM IF EXISTS test_changelog_variant_detail_stream FORCE"
        sql "DROP TABLE IF EXISTS test_changelog_variant FORCE"
        // v: default layout, t: typed paths and one materialized path, d: doc mode
        sql """
            CREATE TABLE test_changelog_variant (
                k INT,
                v VARIANT,
                t VARIANT<'a':INT, 'b':STRING, PROPERTIES("variant_max_subcolumns_count" = "1")>,
                d VARIANT<PROPERTIES("variant_enable_doc_mode" = "true")>
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "changelog.enable" = "true",
                "changelog.need_historical_value" = "true"
            )
        """

        sql """
            INSERT INTO test_changelog_variant VALUES
                (1, parse_to_variant('{"a":1,"b":{"c":"x"},"arr":[1,2]}'), parse_to_variant('{"a":10,"b":"p","z":1.5}'), parse_to_variant('{"k":"d1","n":{"m":1}}')),
                (2, parse_to_variant('{"a":2}'), parse_to_variant('{"a":20}'), parse_to_variant('{"k":"d2"}')),
                (3, parse_to_variant('{"a":3,"b":{"c":"z"}}'), parse_to_variant('{"b":"q"}'), parse_to_variant('{"k":"d3"}'))
        """

        sql """
            CREATE STREAM test_changelog_variant_md_stream ON TABLE test_changelog_variant
            PROPERTIES ("type" = "min_delta", "show_initial_rows" = "false")
        """
        sql """
            CREATE STREAM test_changelog_variant_detail_stream ON TABLE test_changelog_variant
            PROPERTIES ("type" = "detail", "show_initial_rows" = "false")
        """

        sql """
            INSERT INTO test_changelog_variant VALUES
                (1, parse_to_variant('{"a":11,"b":{"c":"x2"},"new":true}'), parse_to_variant('{"a":11,"b":"p2"}'), parse_to_variant('{"k":"d1b"}')),
                (4, parse_to_variant('{"a":4}'), parse_to_variant('{"a":40}'), parse_to_variant('{"k":"d4"}'))
        """
        sql "DELETE FROM test_changelog_variant WHERE k = 2"
        sql "sync"
        sleep(1200)

        order_qt_base """
            SELECT k, v, t, d FROM test_changelog_variant
        """

        // both images of every Variant column, read whole from the changelog
        order_qt_raw_changelog """
            SELECT __DORIS_CHANGELOG_OP__ AS op, k, v, __DORIS_CHANGELOG_BEFORE_v__ AS before_v,
                   t, __DORIS_CHANGELOG_BEFORE_t__ AS before_t, d, __DORIS_CHANGELOG_BEFORE_d__ AS before_d
            FROM changelog("table" = "test_changelog_variant")
        """

        // sub-paths of both images
        order_qt_raw_changelog_sub_paths """
            SELECT __DORIS_CHANGELOG_OP__ AS op, k,
                   v['a'], __DORIS_CHANGELOG_BEFORE_v__['a'],
                   v['b']['c'], __DORIS_CHANGELOG_BEFORE_v__['b']['c'],
                   CAST(t['a'] AS INT), CAST(__DORIS_CHANGELOG_BEFORE_t__['a'] AS INT),
                   d['k'], __DORIS_CHANGELOG_BEFORE_d__['k']
            FROM changelog("table" = "test_changelog_variant")
        """

        // sub-paths over a UNION of the base table and its changelog
        order_qt_union_with_changelog_sub_paths """
            SELECT k, v['a'], v['b']['c']
            FROM (SELECT k, v FROM test_changelog_variant
                  UNION ALL
                  SELECT k, v FROM changelog("table" = "test_changelog_variant")) u
        """

        // A source column laid out like its images needs no cast over the union, and a CTE used twice keeps the
        // sub-paths above the union, where the changelog() child could not supply them.
        sql "DROP TABLE IF EXISTS test_changelog_variant_same_layout FORCE"
        sql """
            CREATE TABLE test_changelog_variant_same_layout (
                k INT,
                v VARIANT<PROPERTIES(
                    "variant_max_subcolumns_count" = "0",
                    "variant_enable_typed_paths_to_sparse" = "true"
                )>
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "changelog.enable" = "true"
            )
        """
        sql """INSERT INTO test_changelog_variant_same_layout VALUES (1, parse_to_variant('{"a":1,"b":"x"}'))"""
        sql "sync"
        sleep(1200)
        order_qt_union_same_layout_cte_used_twice """
            WITH c AS (
                SELECT k, v FROM test_changelog_variant_same_layout
                UNION ALL
                SELECT k, v FROM changelog("table" = "test_changelog_variant_same_layout")
            )
            SELECT l.k, l.v['a'], r.v['b'] FROM c l JOIN c r ON l.k = r.k
        """

        // an UPDATE_BEFORE row carries the old value of every sub-path
        order_qt_min_delta_stream """
            SELECT k, v, v['a'], v['b']['c'], CAST(t['a'] AS INT), t['b'], d['k'],
                   __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM test_changelog_variant_md_stream
        """

        // a filter on a sub-path sees the image of its own row: only the old value of k = 1 is below 10
        order_qt_min_delta_stream_filter """
            SELECT k, v['a'], __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM test_changelog_variant_md_stream
            WHERE CAST(v['a'] AS INT) < 10
        """

        order_qt_detail_stream """
            SELECT k, v['a'], CAST(t['a'] AS INT), d['k'], __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM test_changelog_variant_detail_stream
        """

        // compacting the changelog keeps both images intact
        sql """
            INSERT INTO test_changelog_variant VALUES
                (3, parse_to_variant('{"a":33,"b":{"c":"z3"}}'), parse_to_variant('{"a":33,"b":"q3"}'), parse_to_variant('{"k":"d3b"}'))
        """
        trigger_and_wait_compaction("test_changelog_variant", "cumulative")

        order_qt_raw_changelog_after_compaction """
            SELECT __DORIS_CHANGELOG_OP__ AS op, k, v, __DORIS_CHANGELOG_BEFORE_v__ AS before_v,
                   t, __DORIS_CHANGELOG_BEFORE_t__ AS before_t, d, __DORIS_CHANGELOG_BEFORE_d__ AS before_d
            FROM changelog("table" = "test_changelog_variant")
        """

        order_qt_min_delta_stream_after_compaction """
            SELECT k, v['a'], v['b']['c'], CAST(t['a'] AS INT), d['k'], __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM test_changelog_variant_md_stream
        """

        // a stream over history and incremental partitions reads one from the base table and the other
        // from the changelog; sub-paths still come out right on both sides
        sql "DROP STREAM IF EXISTS test_changelog_variant_mixed_stream FORCE"
        sql "DROP TABLE IF EXISTS test_changelog_variant_mixed FORCE"
        sql """
            CREATE TABLE test_changelog_variant_mixed (
                k INT,
                v VARIANT
            )
            UNIQUE KEY(k)
            PARTITION BY RANGE(k)
            (
                PARTITION p1 VALUES LESS THAN (10),
                PARTITION p2 VALUES [(10), (20))
            )
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "changelog.enable" = "true",
                "changelog.need_historical_value" = "true"
            )
        """
        sql """INSERT INTO test_changelog_variant_mixed VALUES (1, parse_to_variant('{"a":1,"b":"history"}'))"""
        sql """
            CREATE STREAM test_changelog_variant_mixed_stream ON TABLE test_changelog_variant_mixed
            PROPERTIES ("type" = "min_delta", "show_initial_rows" = "true")
        """
        sql """INSERT INTO test_changelog_variant_mixed VALUES (11, parse_to_variant('{"a":11,"b":"incremental"}'))"""
        sql """INSERT INTO test_changelog_variant_mixed VALUES (11, parse_to_variant('{"a":12,"b":"updated"}'))"""
        sql "sync"
        sleep(1200)

        order_qt_mixed_history_incremental """
            SELECT k, v['a'], v['b'], __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM test_changelog_variant_mixed_stream
        """

        // ADD COLUMN mirrors a new Variant column into the changelog
        sql "ALTER TABLE test_changelog_variant ADD COLUMN added VARIANT"
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN WHERE TableName = 'test_changelog_variant' ORDER BY CreateTime DESC LIMIT 1"""
            time 600
        }
        sql """INSERT INTO test_changelog_variant VALUES (4, parse_to_variant('{"a":44}'), parse_to_variant('{"a":44}'), parse_to_variant('{"k":"d4b"}'), parse_to_variant('{"x":1}'))"""
        sql """INSERT INTO test_changelog_variant VALUES (4, parse_to_variant('{"a":45}'), parse_to_variant('{"a":45}'), parse_to_variant('{"k":"d4c"}'), parse_to_variant('{"x":2}'))"""
        sql "sync"

        order_qt_added_column_changelog """
            SELECT __DORIS_CHANGELOG_OP__ AS op, k, added, __DORIS_CHANGELOG_BEFORE_added__ AS before_added,
                   added['x'], __DORIS_CHANGELOG_BEFORE_added__['x']
            FROM changelog("table" = "test_changelog_variant")
            WHERE k = 4
        """
    }
}
