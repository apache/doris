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

import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_analyze_long_string", "nonConcurrent") {
    // `analyze ... with sync` does not persist the job in the in-memory map,
    // so we use async analyze and poll SHOW ANALYZE / SHOW ANALYZE TASK STATUS.

    def findJobId = { String ctl, String db, String tbl ->
        def rows = sql """show analyze"""
        def match = -1L
        for (row in rows) {
            // columns: job_id, catalog_name, db_name, tbl_name, ...
            if (row[1].toString() == ctl
                    && row[2].toString() == db
                    && row[3].toString() == tbl) {
                long id = Long.parseLong(row[0].toString())
                if (id > match) {
                    match = id
                }
            }
        }
        return match == -1L ? null : match
    }

    def waitJobFinished = { long jobId ->
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until {
            def rows = sql """show analyze ${jobId}"""
            if (rows.isEmpty()) {
                return false
            }
            def state = rows[0][9].toString()
            logger.info("job ${jobId} state=${state}")
            return state == "FINISHED" || state == "FAILED"
        }
    }

    def collectTaskStatuses = { long jobId ->
        def rows = sql """show analyze task status ${jobId}"""
        def result = [:]
        for (row in rows) {
            // columns: task_id, col_name, index_name, message,
            //          last_state_change_time, time_cost_in_ms, state
            def colName = row[1].toString()
            def msg = row[3] == null ? "" : row[3].toString()
            def state = row[6].toString()
            result[colName] = [state: state, message: msg]
        }
        return result
    }

    def assertTaskSkipped = { long jobId, List<String> expectedSkipped, List<String> expectedOk ->
        def statuses = collectTaskStatuses(jobId)
        logger.info("job ${jobId} task statuses=${statuses}")
        for (c in expectedSkipped) {
            assertTrue(statuses.containsKey(c), "missing task for skipped col ${c}")
            assertEquals("FINISHED", statuses[c].state,
                    "expected FINISHED for skipped col ${c}, got ${statuses[c]}")
            assertTrue(statuses[c].message.contains("statistics_max_string_column_length")
                    || statuses[c].message.contains("exceeds"),
                    "expected skip reason visible for col ${c}, got msg=${statuses[c].message}")
        }
        for (c in expectedOk) {
            assertTrue(statuses.containsKey(c), "missing task for col ${c}")
            assertEquals("FINISHED", statuses[c].state,
                    "expected FINISHED for col ${c}, got ${statuses[c]}")
            assertEquals("", statuses[c].message,
                    "expected empty message for col ${c}, got ${statuses[c].message}")
        }
    }

    def collectedColumns = { String tbl ->
        def rows = sql """show column stats ${tbl}"""
        def set = [] as Set
        for (r in rows) {
            set.add(r[0].toString())
        }
        return set
    }

    sql """drop database if exists test_analyze_long_string"""
    sql """create database test_analyze_long_string"""
    sql """use test_analyze_long_string"""
    sql """set global enable_auto_analyze=false"""

    // ---------- Case 1: full analyze on a non-partitioned table ----------
    sql """drop table if exists test_analyze_long_string_full"""
    sql """
        CREATE TABLE test_analyze_long_string_full (
          `id` bigint,
          `name` varchar(100),
          `small_str` varchar(100),
          `fixed` char(16),
          `big_str` string
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """insert into test_analyze_long_string_full values(1, 'alice',   'aaa', 'abc',   NULL)"""
    sql """insert into test_analyze_long_string_full values(2, NULL,      'bbb', 'defg',  repeat('y', 100))"""
    sql """insert into test_analyze_long_string_full values(3, 'charlie', 'ccc', 'hij',   repeat('z', 2048))"""

    setFeConfigTemporary([statistics_max_string_column_length: 1024]) {
        sql """analyze table test_analyze_long_string_full"""
        def jobId = findJobId("internal", "test_analyze_long_string", "test_analyze_long_string_full")
        assertNotNull(jobId, "must find analyze job for test_analyze_long_string_full")
        waitJobFinished(jobId)
        assertTaskSkipped(jobId, ["big_str"], ["id", "name", "small_str", "fixed"])
        def collected = collectedColumns("test_analyze_long_string_full")
        logger.info("test_analyze_long_string_full collected=${collected}")
        assertFalse(collected.contains("big_str"),
                "big_str should be skipped, got ${collected}")
        assertTrue(collected.contains("id"))
        assertTrue(collected.contains("name"))
        assertTrue(collected.contains("small_str"))
        assertTrue(collected.contains("fixed"))
        def bigRows = sql """show column stats test_analyze_long_string_full (big_str)"""
        assertEquals(0, bigRows.size(),
                "expected no stats row for skipped column big_str, got ${bigRows}")
    }

    // ---------- Case 2: disabled (limit <= 0) must collect everything ----------
    setFeConfigTemporary([statistics_max_string_column_length: 0]) {
        sql """analyze table test_analyze_long_string_full with sync"""
        def bigRows = sql """show column stats test_analyze_long_string_full (big_str)"""
        assertEquals(1, bigRows.size(),
                "protection disabled: big_str should be collected, got ${bigRows}")
        assertEquals("3.0", bigRows[0][2].toString())
    }

    // ---------- Case 3: sample analyze (LINEAR / DUJ1 paths) ----------
    sql """drop table if exists test_analyze_long_string_sample"""
    sql """
        CREATE TABLE test_analyze_long_string_sample (
          `id` bigint,
          `small_str` varchar(100),
          `big_str` string
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """insert into test_analyze_long_string_sample values(1, 'aa', repeat('z', 2048))"""
    sql """insert into test_analyze_long_string_sample values(2, 'bb', 'short1')"""
    sql """insert into test_analyze_long_string_sample values(3, 'cc', 'short2')"""
    sql """insert into test_analyze_long_string_sample values(4, 'dd', 'short3')"""
    sql """insert into test_analyze_long_string_sample values(5, 'ee', 'short4')"""

    setFeConfigTemporary([statistics_max_string_column_length: 1024]) {
        sql """analyze table test_analyze_long_string_sample with sample percent 100"""
        def jobId = findJobId("internal", "test_analyze_long_string", "test_analyze_long_string_sample")
        assertNotNull(jobId, "must find sample analyze job for test_analyze_long_string_sample")
        waitJobFinished(jobId)
        assertTaskSkipped(jobId, ["big_str"], ["id", "small_str"])
        def collected = collectedColumns("test_analyze_long_string_sample")
        assertFalse(collected.contains("big_str"), "sample analyze: big_str should be skipped")
        assertTrue(collected.contains("small_str"))
    }

    // ---------- Case 4: partitioned table — long-string guard does NOT apply ----------
    // Partition-granularity analyze uses PARTITION_ANALYZE_TEMPLATE and commits per-batch
    // inserts into __partition_stats incrementally. To avoid leaving a mix of fresh and
    // stale partition rows on mid-loop abort, the long-string skip guard is intentionally
    // NOT wired into this path. Analyze should FINISH normally and produce statistics
    // for big_str even when rows exceed statistics_max_string_column_length.
    sql """drop table if exists test_analyze_long_string_part"""
    sql """
        CREATE TABLE test_analyze_long_string_part (
          `d` date NOT NULL,
          `id` bigint,
          `big_str` string
        ) ENGINE=OLAP
        DUPLICATE KEY(`d`)
        PARTITION BY RANGE(`d`) (
          PARTITION p1 VALUES LESS THAN ('2024-01-02'),
          PARTITION p2 VALUES LESS THAN ('2024-01-03')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """insert into test_analyze_long_string_part values('2024-01-01', 1, 'short_a'),
                                    ('2024-01-01', 2, repeat('x', 2048)),
                                    ('2024-01-02', 3, 'short_b')"""

    setFeConfigTemporary([statistics_max_string_column_length: 1024]) {
        sql """set global enable_partition_analyze = true"""
        try {
            sql """analyze table test_analyze_long_string_part"""
            def jobId = findJobId("internal", "test_analyze_long_string", "test_analyze_long_string_part")
            assertNotNull(jobId, "must find analyze job for test_analyze_long_string_part")
            waitJobFinished(jobId)
            // No task should be skipped: partition path bypasses the long-string guard.
            assertTaskSkipped(jobId, [], ["d", "id", "big_str"])
            def collected = collectedColumns("test_analyze_long_string_part")
            assertTrue(collected.contains("big_str"),
                    "partition analyze should still collect big_str (guard does not apply)")
            assertTrue(collected.contains("d"))
            assertTrue(collected.contains("id"))
        } finally {
            sql """set global enable_partition_analyze = false"""
        }
    }

    // ---------- Case 5: sample analyze forced onto DUJ1 template ----------
    // useLinearAnalyzeTemplate() normally returns true on small tables (scanFullTable),
    // so LINEAR covers the sample path. Use a FE debug point to force DUJ1 so its
    // ${lengthAssert} injection is exercised end-to-end.
    sql """drop table if exists test_analyze_long_string_duj1"""
    sql """
        CREATE TABLE test_analyze_long_string_duj1 (
          `id` bigint,
          `small_str` varchar(100),
          `big_str` string
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """insert into test_analyze_long_string_duj1 values(1, 'aa', repeat('z', 2048))"""
    sql """insert into test_analyze_long_string_duj1 values(2, 'bb', 'short1')"""
    sql """insert into test_analyze_long_string_duj1 values(3, 'cc', 'short2')"""
    sql """insert into test_analyze_long_string_duj1 values(4, 'dd', 'short3')"""
    sql """insert into test_analyze_long_string_duj1 values(5, 'ee', 'short4')"""

    setFeConfigTemporary([statistics_max_string_column_length: 1024]) {
        GetDebugPoint().enableDebugPointForAllFEs('OlapAnalysisTask.useDUJ1Template')
        try {
            sql """analyze table test_analyze_long_string_duj1 with sample rows 3"""
            def jobId = findJobId("internal", "test_analyze_long_string", "test_analyze_long_string_duj1")
            assertNotNull(jobId, "must find analyze job for test_analyze_long_string_duj1")
            waitJobFinished(jobId)
            assertTaskSkipped(jobId, ["big_str"], ["id", "small_str"])
            def collected = collectedColumns("test_analyze_long_string_duj1")
            assertFalse(collected.contains("big_str"),
                    "DUJ1 analyze: big_str should be skipped")
            assertTrue(collected.contains("small_str"))
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs('OlapAnalysisTask.useDUJ1Template')
        }
    }

    // ---------- Case 6: WITH SYNC — skipped column must have no stats ----------
    // Sync analyze never populates analysisJobIdToTaskMap, so SHOW ANALYZE cannot see
    // the skip reason. AnalysisManager.buildAndAssignJob surfaces skip messages via
    // ConnectContext OK-packet info for interactive visibility. We cannot easily read
    // that info string from JDBC in regression, so we verify the functional outcome:
    // the skipped column produces no row in column_statistics, while other columns do.
    sql """drop table if exists test_analyze_long_string_full"""
    sql """
        CREATE TABLE test_analyze_long_string_full (
          `id` bigint,
          `name` varchar(100),
          `big_str` string
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """insert into test_analyze_long_string_full values(1, 'alice', NULL)"""
    sql """insert into test_analyze_long_string_full values(2, 'bob',   repeat('y', 100))"""
    sql """insert into test_analyze_long_string_full values(3, 'chris', repeat('z', 2048))"""
    setFeConfigTemporary([statistics_max_string_column_length: 1024]) {
        sql """analyze table test_analyze_long_string_full with sync"""
        def bigRows = sql """show column stats test_analyze_long_string_full (big_str)"""
        assertEquals(0, bigRows.size(),
                "WITH SYNC: big_str should be skipped, got ${bigRows}")
        def nameRows = sql """show column stats test_analyze_long_string_full (name)"""
        assertEquals(1, nameRows.size(),
                "WITH SYNC: name should be collected, got ${nameRows}")
    }

    // ---------- Cleanup ----------
    sql """drop database if exists test_analyze_long_string"""
}
