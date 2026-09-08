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

import java.util.regex.Pattern

suite("test_bf_index_local_cloud_light_change", "p0") {
    def runShowIndex = { tag, tableName ->
        quickRunTest(tag, "SHOW INDEX FROM ${tableName}", false) { row ->
            [row[2], row[4], row[10], row[12]]
        }
    }

    def runDesc = { tag, tableName, all = false ->
        def statement = all ? "DESC ${tableName} ALL" : "DESC ${tableName}"
        quickRunTest(tag, statement, false) { row ->
            all ? [row[2], row[3], row[8]] : [row[0], row[1], row[5]]
        }
    }

    def getRowsBloomFilterFiltered = { profileString ->
        def matcher = Pattern.compile("RowsBloomFilterFiltered:\\s+(\\d+)").matcher(profileString)
        assertTrue(matcher.find(), "RowsBloomFilterFiltered is missing from profile: ${profileString}")
        return matcher.group(1).toInteger()
    }

    def waitForBuildIndexFinish = { tableName, beforeBuildJobIds ->
        def jobs = []
        for (int retry = 0; retry < 120; retry++) {
            jobs = sql """SHOW BUILD INDEX WHERE TableName = "${tableName}" ORDER BY JobId"""
            def newJobs = jobs.findAll { job -> !beforeBuildJobIds.contains(job[0].toString()) }
            if (!newJobs.isEmpty()) {
                assertTrue(newJobs.every { job -> job[7] != "CANCELLED" },
                        "build index job was cancelled: ${newJobs}")
                if (newJobs.every { job -> job[7] == "FINISHED" }) {
                    return
                }
            }
            sleep(1000)
        }
        assertTrue(false, "build index job did not finish: ${jobs}")
    }

    def waitForSchemaChangeFinish = { tableName ->
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1"""
            time 120
        }
    }

    def executeIndexChangeAndWait = { tableName, statement, waitForBuildIndex ->
        def beforeBuildJobIds = waitForBuildIndex
                ? (sql """SHOW BUILD INDEX WHERE TableName = "${tableName}" """)
                        .collect { job -> job[0].toString() }.toSet()
                : [] as Set
        sql statement
        if (waitForBuildIndex) {
            waitForBuildIndexFinish(tableName, beforeBuildJobIds)
        } else {
            waitForSchemaChangeFinish(tableName)
        }
    }

    def lightTable = "test_bf_index_local_cloud_light"
    sql "DROP TABLE IF EXISTS ${lightTable}"
    sql """
        CREATE TABLE ${lightTable} (
            k1 INT,
            v1 VARCHAR(32),
            v2 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """
    sql """INSERT INTO ${lightTable} VALUES (1, 'a', 0), (2, 'n', 100), (3, 'z', 200)"""
    sql "SYNC"

    sql "SET enable_add_index_for_new_data = true"
    sql "SET enable_condition_cache = false"
    sql "SET enable_sql_cache = false"
    sql "SET enable_profile = true"
    sql "SET profile_level = 2"
    sql "SET parallel_pipeline_task_num = 1"
    sql "SET parallel_scan_min_rows_per_scanner = 2097152"
    sql """CREATE INDEX idx_v2 ON ${lightTable}(v2) USING BLOOMFILTER
            PROPERTIES ("bloom_filter_fpp" = "0.0001")"""
    runShowIndex("show_index_after_light_add", lightTable)

    sql """INSERT INTO ${lightTable} VALUES (4, 'a', 40), (5, 'z', 60)"""
    sql "SYNC"
    order_qt_light_add_data "SELECT * FROM ${lightTable} ORDER BY k1"

    profile("light_bf_index_only_filters_new_rowset") {
        run {
            sql """/* light_bf_index_only_filters_new_rowset */
                    SELECT v2 FROM ${lightTable} WHERE v2 = 50"""
            sleep(1000)
        }
        check { profileString, exception ->
            if (exception != null) {
                throw exception
            }
            assertEquals(2, getRowsBloomFilterFiltered(profileString))
        }
    }

    test {
        sql isCloudMode() ? "BUILD INDEX ON ${lightTable}" : "BUILD INDEX idx_v2 ON ${lightTable}"
        exception "BLOOMFILTER index is not needed to build"
    }

    executeIndexChangeAndWait(lightTable, "DROP INDEX idx_v2 ON ${lightTable}", isCloudMode())
    runShowIndex("show_index_after_light_drop", lightTable)

    profile("light_bf_index_drop_removes_all_rowset_indexes") {
        run {
            sql """/* light_bf_index_drop_removes_all_rowset_indexes */
                    SELECT v2 FROM ${lightTable} WHERE v2 = 50"""
            sleep(1000)
        }
        check { profileString, exception ->
            if (exception != null) {
                throw exception
            }
            assertEquals(0, getRowsBloomFilterFiltered(profileString))
        }
    }

    def fullTable = "test_bf_index_local_cloud_schema_change"
    sql "DROP TABLE IF EXISTS ${fullTable}"
    sql """
        CREATE TABLE ${fullTable} (
            k1 INT,
            v2 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """
    sql "INSERT INTO ${fullTable} VALUES (1, 0), (2, 100), (3, 200)"
    sql "SYNC"

    sql "SET enable_add_index_for_new_data = false"
    executeIndexChangeAndWait(fullTable, "CREATE INDEX idx_v2 ON ${fullTable}(v2) USING BLOOMFILTER", false)

    profile("local_cloud_bf_index_schema_change_backfills_existing_rowsets") {
        run {
            sql """/* local_cloud_bf_index_schema_change_backfills_existing_rowsets */
                    SELECT v2 FROM ${fullTable} WHERE v2 = 50"""
            sleep(1000)
        }
        check { profileString, exception ->
            if (exception != null) {
                throw exception
            }
            assertEquals(3, getRowsBloomFilterFiltered(profileString))
        }
    }

    sql "SET enable_add_index_for_new_data = true"
    sql "INSERT INTO ${fullTable} VALUES (4, 40), (5, 60)"
    sql "SYNC"

    profile("local_cloud_bf_index_schema_change_indexes_new_rowsets") {
        run {
            sql """/* local_cloud_bf_index_schema_change_indexes_new_rowsets */
                    SELECT v2 FROM ${fullTable} WHERE v2 = 50"""
            sleep(1000)
        }
        check { profileString, exception ->
            if (exception != null) {
                throw exception
            }
            assertEquals(5, getRowsBloomFilterFiltered(profileString))
        }
    }

    def mixedTable = "test_bf_index_local_cloud_mixed"
    sql "DROP TABLE IF EXISTS ${mixedTable}"
    sql """
        CREATE TABLE ${mixedTable} (
            k1 INT,
            v1 VARCHAR(32),
            v2 INT,
            INDEX idx_v1 (v1) USING BLOOMFILTER PROPERTIES ("bloom_filter_fpp" = "0.02")
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "bloom_filter_columns" = "v2",
            "bloom_filter_fpp" = "0.03"
        )
    """
    sql """INSERT INTO ${mixedTable} VALUES (1, 'mixed-a', 10), (2, 'mixed-b', 20)"""
    sql "SYNC"
    runDesc("desc_mixed", mixedTable, true)
    runShowIndex("show_index_mixed", mixedTable)
    order_qt_mixed_data "SELECT * FROM ${mixedTable} ORDER BY k1"

    test {
        sql """CREATE INDEX idx_v2 ON ${mixedTable}(v2) USING BLOOMFILTER"""
        exception "v2 should have only one ngram bloom filter index or bloom filter index"
    }

    def conflictTable = "test_bf_index_local_cloud_conflict"
    sql "DROP TABLE IF EXISTS ${conflictTable}"
    sql """
        CREATE TABLE ${conflictTable} (
            k1 INT,
            v1 VARCHAR(32),
            INDEX idx_v1 (v1) USING BLOOMFILTER
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    test {
        sql """ALTER TABLE ${conflictTable} SET ("bloom_filter_columns" = "v1")"""
        exception "v1 should have only one ngram bloom filter index or bloom filter index"
    }

    def columnTable = "test_bf_index_local_cloud_column"
    sql "DROP TABLE IF EXISTS ${columnTable}"
    sql """
        CREATE TABLE ${columnTable} (
            k1 INT,
            v1 VARCHAR(32),
            INDEX idx_v1 (v1) USING BLOOMFILTER
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """ALTER TABLE ${columnTable} RENAME COLUMN v1 v1_new"""
    sql "SYNC"
    runShowIndex("show_index_after_rename", columnTable)

    sql """ALTER TABLE ${columnTable} DROP COLUMN v1_new"""
    sql "SYNC"
    runShowIndex("show_index_after_drop_column", columnTable)
}
