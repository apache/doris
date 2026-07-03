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

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

suite("test_show_load_insert_runtime_observation", "nonConcurrent,p0") {
    def dbName = "test_insert_runtime_observe_db"
    def dstTable = "runtime_observe_dst"
    def failedTable = "runtime_observe_dst_fail"
    def label = "runtime_observe_label_" + System.currentTimeMillis()
    def failedLabel = "runtime_observe_failed_label_" + System.currentTimeMillis()
    def expectedRows = 600572
    // Existing regression data used by stream-load tests; large enough to expose runtime rows/bytes reports.
    def s3Path = "http://${getS3BucketName()}.${getS3Endpoint()}/regression/tpch/sf0.1/lineitem.tbl.gz"

    def longValue = { value ->
        assertNotNull(value)
        return (value as Number).longValue()
    }

    def optionalLongValue = { value ->
        return value == null ? 0L : (value as Number).longValue()
    }

    def parseProgressInfo = { String progress ->
        def matcher = progress =~ /\((\d+)\/(\d+)\)/
        if (!matcher.find()) {
            return [valid: false, finished: 0, total: 0]
        }
        return [valid: true, finished: matcher.group(1) as int, total: matcher.group(2) as int]
    }

    def hasUsefulJobDetails = { json ->
        return json != null
                && optionalLongValue(json.ScannedRows) > 0
                && optionalLongValue(json.LoadBytes) > 0
                && optionalLongValue(json.FileNumber) > 0
                && optionalLongValue(json.FileSize) > 0
                && optionalLongValue(json.TaskNumber) > 0
                && json."All backends" != null
                && json."All backends".size() > 0
    }

    def assertUsefulJobDetails = { json, String phase ->
        assertTrue(longValue(json.ScannedRows) > 0, "${phase} ScannedRows should be > 0: ${json}")
        assertTrue(longValue(json.LoadBytes) > 0, "${phase} LoadBytes should be > 0: ${json}")
        assertTrue(longValue(json.FileNumber) > 0, "${phase} FileNumber should be > 0: ${json}")
        assertTrue(longValue(json.FileSize) > 0, "${phase} FileSize should be > 0: ${json}")
        assertTrue(longValue(json.TaskNumber) > 0, "${phase} TaskNumber should be > 0: ${json}")
        assertTrue(json."All backends" != null && json."All backends".size() > 0,
                "${phase} All backends should not be empty: ${json}")
    }

    def assertUsefulProgress = { String progress, String phase ->
        assertFalse(progress.contains("Unknown id"), "${phase} progress should not contain Unknown id: ${progress}")
        def progressInfo = parseProgressInfo(progress)
        assertTrue(progressInfo.valid, "${phase} progress should contain finished/total scan ranges: ${progress}")
        assertTrue(progressInfo.total > 0, "${phase} total scan ranges should be > 0: ${progress}")
    }

    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE DATABASE ${dbName}"""
    sql """USE ${dbName}"""

    sql """
        CREATE TABLE ${dstTable} (
            l_orderkey BIGINT NOT NULL,
            l_partkey BIGINT NOT NULL,
            l_suppkey BIGINT NOT NULL,
            l_linenumber INT NOT NULL,
            l_quantity DECIMAL(15, 2) NOT NULL,
            l_extendedprice DECIMAL(15, 2) NOT NULL,
            l_discount DECIMAL(15, 2) NOT NULL,
            l_tax DECIMAL(15, 2) NOT NULL,
            l_returnflag CHAR(1) NOT NULL,
            l_linestatus CHAR(1) NOT NULL,
            l_shipdate DATE NOT NULL,
            l_commitdate DATE NOT NULL,
            l_receiptdate DATE NOT NULL,
            l_shipinstruct CHAR(25) NOT NULL,
            l_shipmode CHAR(10) NOT NULL,
            l_comment VARCHAR(44) NOT NULL
        )
        DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
        DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE ${failedTable} (
            k INT NOT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    def executorService = Executors.newSingleThreadExecutor()
    def insertFuture = null

    try {
        GetDebugPoint().enableDebugPointForAllBEs("VTabletWriter.close.sleep", [sleep_sec: 30])
        GetDebugPoint().enableDebugPointForAllBEs("VTabletWriterV2.close.sleep", [sleep_sec: 30])

        insertFuture = executorService.submit({
            try {
                connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                    sql """USE ${dbName}"""
                    sql """
                        INSERT INTO ${dstTable} WITH LABEL ${label}
                        SELECT c1, c2, c3, c4, c5, c6, c7, c8,
                               c9, c10, c11, c12, c13, c14, c15, c16
                        FROM S3(
                            "uri" = "${s3Path}",
                            "ACCESS_KEY" = "${getS3AK()}",
                            "SECRET_KEY" = "${getS3SK()}",
                            "format" = "csv",
                            "column_separator" = "|",
                            "compress_type" = "GZ",
                            "region" = "${getS3Region()}",
                            "provider" = "${getS3Provider()}"
                        )
                    """
                }
                return "SUCCESS"
            } catch (Exception e) {
                logger.error("INSERT INTO SELECT FROM S3 failed: " + e.getMessage(), e)
                return "FAILED: " + e.getMessage()
            }
        } as Callable<String>)

        boolean observedInShowLoad = false
        boolean observedUsefulLoading = false
        def loadingProgress = null
        def loadingJobDetails = null
        def lastLoadingProgress = null
        def lastLoadingJobDetails = null

        // Poll from the main connection while the S3 INSERT runs on another JDBC connection.
        for (int attempt = 1; attempt <= 600 && !observedUsefulLoading; attempt++) {
            def result = sql """SHOW LOAD FROM ${dbName} WHERE LABEL = '${label}'"""
            if (result.size() > 0) {
                observedInShowLoad = true
                def jobRow = result[0]
                def jobState = jobRow[2].toString()
                def jobProgress = jobRow[3].toString()
                def jobType = jobRow[4].toString()
                def jobDetails = jobRow[14].toString()
                def jobDetailsJson = parseJson(jobDetails)

                logger.info("Attempt ${attempt}: State=${jobState}, Type=${jobType}, Progress=${jobProgress}, "
                        + "JobDetails=${jobDetails}")

                assertEquals(label, jobRow[1].toString())
                assertEquals("INSERT", jobType)
                assertFalse(jobProgress.contains("Unknown id"),
                        "runtime progress should not contain Unknown id: ${jobProgress}")
                def progressInfo = parseProgressInfo(jobProgress)
                assertTrue(progressInfo.valid,
                        "runtime progress should contain finished/total scan ranges: ${jobProgress}")
                assertNotNull(jobDetailsJson, "Runtime JobDetails should be valid JSON")

                if (jobState == "LOADING") {
                    lastLoadingProgress = jobProgress
                    lastLoadingJobDetails = jobDetails
                    boolean usefulProgress = progressInfo.total > 0
                    boolean usefulJobDetails = hasUsefulJobDetails(jobDetailsJson)
                    if (usefulProgress && usefulJobDetails) {
                        observedUsefulLoading = true
                        loadingProgress = jobProgress
                        loadingJobDetails = jobDetails
                        break
                    }
                    logger.info("LOADING sample is visible but not complete yet: progressUseful={}, "
                            + "jobDetailsUseful={}, Progress={}, JobDetails={}",
                            usefulProgress, usefulJobDetails, jobProgress, jobDetails)
                }
            }

            if (insertFuture.isDone()) {
                break
            }
            Thread.sleep(100)
        }

        if (!observedUsefulLoading && insertFuture.isDone()) {
            logger.warn("INSERT finished before a useful LOADING sample was observed. "
                    + "lastLoadingProgress={}, lastLoadingJobDetails={}",
                    lastLoadingProgress, lastLoadingJobDetails)
        }
        assertTrue(observedInShowLoad, "The running S3 INSERT should be visible in SHOW LOAD")
        assertTrue(observedUsefulLoading,
                "SHOW LOAD should observe LOADING with non-zero progress and JobDetails, "
                        + "lastLoadingProgress=${lastLoadingProgress}, lastLoadingJobDetails=${lastLoadingJobDetails}")
        logger.info("Observed useful LOADING progress=${loadingProgress}, jobDetails=${loadingJobDetails}")

        def insertResult = insertFuture.get(180, TimeUnit.SECONDS)
        assertEquals("SUCCESS", insertResult)
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("VTabletWriter.close.sleep")
        GetDebugPoint().disableDebugPointForAllBEs("VTabletWriterV2.close.sleep")
        executorService.shutdownNow()
    }

    def finalResult = sql """SHOW LOAD FROM ${dbName} WHERE LABEL = '${label}'"""
    assertEquals(1, finalResult.size())

    def finalRow = finalResult[0]
    def finalProgress = finalRow[3].toString()
    def finalJobDetails = finalRow[14].toString()
    def transactionId = finalRow[15].toString()
    logger.info("Final SHOW LOAD state={}, type={}, progress={}, transactionId={}, jobDetails={}",
            finalRow[2], finalRow[4], finalProgress, transactionId, finalJobDetails)

    assertEquals(label, finalRow[1].toString())
    assertEquals("FINISHED", finalRow[2].toString())
    assertUsefulProgress(finalProgress, "final")
    assertEquals("INSERT", finalRow[4].toString())
    assertTrue(transactionId.matches("\\d+"), "TransactionId should be set, got: ${transactionId}")

    def finalJson = parseJson(finalJobDetails)
    assertNotNull(finalJson, "Final JobDetails should be valid JSON")
    assertUsefulJobDetails(finalJson, "final")
    assertEquals(expectedRows as long, longValue(finalJson.ScannedRows))
    assertEquals(1L, longValue(finalJson.FileNumber))

    def countResult = sql """SELECT COUNT(*) FROM ${dstTable}"""
    assertEquals(expectedRows, countResult[0][0] as int)

    sql """
        INSERT INTO ${dstTable} VALUES
        (-1, -1, -1, 1, 1.00, 1.00, 0.00, 0.00, 'N', 'O',
         '1998-01-01', '1998-01-01', '1998-01-01', 'NONE', 'AIR', 'values check')
    """
    def afterValuesResult = sql """SHOW LOAD FROM ${dbName} WHERE LABEL = '${label}'"""
    assertEquals(1, afterValuesResult.size(), "INSERT INTO VALUES should not create a new SHOW LOAD entry")

    sql """ SET enable_insert_strict = true """
    try {
        sql """
            INSERT INTO ${failedTable} WITH LABEL ${failedLabel}
            SELECT CAST('abc' AS INT)
        """
    } catch (Exception e) {
        logger.info("Expected failed INSERT INTO SELECT: " + e.getMessage())
    } finally {
        sql """ SET enable_insert_strict = false """
    }

    def failedResult = sql """SHOW LOAD FROM ${dbName} WHERE LABEL = '${failedLabel}'"""
    assertEquals(1, failedResult.size(), "Failed INSERT INTO SELECT should be visible in SHOW LOAD")
    def failedRow = failedResult[0]
    assertEquals("CANCELLED", failedRow[2].toString())
    assertEquals("INSERT", failedRow[4].toString())
    assertTrue(failedRow[7].toString() != "\\N" && !failedRow[7].toString().isEmpty(),
            "Failed INSERT INTO SELECT should expose ErrorMsg")
    assertNotNull(parseJson(failedRow[14].toString()),
            "Failed INSERT INTO SELECT JobDetails should be valid best-effort JSON")
}
