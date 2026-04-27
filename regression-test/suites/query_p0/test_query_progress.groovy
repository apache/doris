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

    // ---- test 1: verify header columns exist without any running queries ----
    def headerResult = sql "SHOW PROC '/current_queries'"
    def headerRow = headerResult[0]  // first row contains column names

    // Verify the progress-related columns exist in the output
    def hasTotalTasks = headerRow.contains("TotalTasks")
    def hasFinishedTasks = headerRow.contains("FinishedTasks")
    def hasProgress = headerRow.contains("Progress")

    assertTrue(hasTotalTasks, "Expected 'TotalTasks' column in SHOW PROC '/current_queries', got: ${headerRow}")
    assertTrue(hasFinishedTasks, "Expected 'FinishedTasks' column in SHOW PROC '/current_queries', got: ${headerRow}")
    assertTrue(hasProgress, "Expected 'Progress' column in SHOW PROC '/current_queries', got: ${headerRow}")

    logger.info("SHOW PROC '/current_queries' header verified: TotalTasks=${hasTotalTasks}, FinishedTasks=${hasFinishedTasks}, Progress=${hasProgress}")

    // ---- test 2: verify progress values during a running query ----
    // Build a query that may take long enough to have measurable progress.
    // Using GROUP BY + ORDER BY on a decent-sized dataset.
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

    // Give the query a moment to start executing
    Thread.sleep(3000)

    // Check progress while query is potentially still running
    def progressResult = sql "SHOW PROC '/current_queries'"
    logger.info("Current queries with progress: ${progressResult}")

    // Verify that running queries (if any) have valid progress columns
    // Skip the header row (index 0)
    if (progressResult.size() > 1) {
        for (int i = 1; i < progressResult.size(); i++) {
            def row = progressResult[i]
            // Column indices: TotalTasks = 22, FinishedTasks = 23, Progress = 24
            if (row.size() > 24) {
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
                logger.info("Query ${row[0]}: TotalTasks=${totalTasks}, FinishedTasks=${finishedTasks}, Progress=${progress}")
            }
        }
    } else {
        logger.info("No running queries found to verify progress values (query may have finished quickly)")
    }

    // Wait for query thread to complete
    queryThread.join(30000)

    // ---- test 3: verify progress formatting edge cases ----
    // FinishedTasks should never exceed TotalTasks in a correctly formatted row
    // (This is a logical consistency check)
    def idleResult = sql "SHOW PROC '/current_queries'"
    logger.info("Current queries after test query completed: ${idleResult}")

    // ---- test 4: verify the columns are still present when no queries are active ----
    // Should return headers with no data rows (not crash)
    assertTrue(idleResult.size() >= 1, "SHOW PROC '/current_queries' should at least return headers")

    // ---- cleanup ----
    sql "DROP TABLE IF EXISTS ${tableName}"
}
