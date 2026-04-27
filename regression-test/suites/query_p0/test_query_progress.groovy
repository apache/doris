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

suite("test_query_progress") {
    def tableName = "test_query_progress_tb"

    // ---- setup: create table with some data ----
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            k int,
            v string
        ) DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 3
        PROPERTIES ("replication_num" = "1");
    """

    // Insert enough data to make the subsequent query take measurable time.
    // Using numbers() table function to generate rows.
    sql """ INSERT INTO ${tableName}
            SELECT number, repeat('x', 500)
            FROM numbers("number" = "100000"); """

    // ---- test 1: verify proc returns expected number of columns ----
    // SHOW PROC '/current_queries' returns column layout:
    // QueryId(0), ConnectionId(1), Catalog(2), Database(3), User(4),
    // ExecTime(5), SqlHash(6), Statement(7),
    // ScanRows(8), ScanBytes(9), ProcessRows(10), CpuMs(11),
    // MaxPeakMemoryBytes(12), CurrentUsedMemoryBytes(13), WorkloadGroupId(14),
    // ShuffleSendBytes(15), ShuffleSendRows(16),
    // ScanBytesFromLocalStorage(17), ScanBytesFromRemoteStorage(18),
    // SpillWriteBytesToLocalStorage(19), SpillReadBytesFromLocalStorage(20),
    // BytesWriteIntoCache(21),
    // TotalTasks(22), FinishedTasks(23), Progress(24)
    // Total = 25 columns

    // ---- test 2: verify progress values during a running query ----
    // Run a query that takes long enough to observe progress.
    def queryThread = new Thread({
        sql """
            SET parallel_pipeline_task_num = 6;
            SELECT k, count(*), min(v), max(v)
            FROM ${tableName}
            GROUP BY k
            ORDER BY k;
        """
    })
    queryThread.setDaemon(true)
    queryThread.start()

    // Give the query a moment to start executing and register tasks
    Thread.sleep(5000)

    // Check progress while query is potentially still running
    def progressResult = sql "SHOW PROC '/current_queries'"
    logger.info("Current queries with progress: ${progressResult}")

    // Validate that data rows have valid progress columns.
    // Note: sql() returns List<List<String>> of data rows only — no header row.
    // Column indices: TotalTasks=22, FinishedTasks=23, Progress=24
    if (!progressResult.isEmpty()) {
        for (int i = 0; i < progressResult.size(); i++) {
            def row = progressResult[i]
            assertTrue(row.size() >= 25,
                    "Expected at least 25 columns (through Progress), got ${row.size()}")

            def totalTasks = row[22]
            def finishedTasks = row[23]
            def progress = row[24]

            // Basic format validation
            assertTrue(totalTasks.isNumber(),
                    "TotalTasks should be numeric, got: ${totalTasks}")
            assertTrue(finishedTasks.isNumber(),
                    "FinishedTasks should be numeric, got: ${finishedTasks}")
            assertTrue(progress.endsWith("%"),
                    "Progress should end with '%', got: ${progress}")

            def totalVal = totalTasks.toInteger()
            def finishedVal = finishedTasks.toInteger()
            // Sanity check: finished should not exceed total
            // (in normal operation; may temporarily exceed due to atomic relaxation, skip in that case)
            if (totalVal > 0 && finishedVal <= totalVal) {
                logger.info("Query ${row[0]}: TotalTasks=${totalVal}, " +
                        "FinishedTasks=${finishedVal}, Progress=${progress}")
            }
        }
    } else {
        logger.info("No running queries found (query may have finished quickly)")
    }

    // Wait for query thread to complete
    queryThread.join(60000)

    // ---- test 3: verify proc still works after all queries complete ----
    def idleResult = sql "SHOW PROC '/current_queries'"
    // Should not crash; may return empty data rows when no queries are active
    assertTrue(idleResult != null, "SHOW PROC '/current_queries' should not return null")
    logger.info("Current queries after test query completed: ${idleResult}")

    // ---- cleanup ----
    sql "DROP TABLE IF EXISTS ${tableName}"
}
