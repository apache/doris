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

// ############################################################################
// A query whose attempt fails on an RPC error is retried by dispatching the
// same plan again (StmtExecutor.handleQueryWithRetry). Under a partitioned read
// the plan's ranges are tickets for the result streams of a remote query that
// ran while FE planned the scan, and the attempt that failed has drained them:
// the same tickets read again return nothing, and the query used to succeed
// with no rows. Such a plan must not be dispatched again, so the query fails
// instead. A statement range runs its query on every read, so that query is
// still retried and returns every row.
//
// The failure is the FE debug point ResultReceiver.getNext.dropDataBatch: it
// loses the first fetch_data response that carries rows -- by then the scans
// have read their input -- and reports THRIFT_RPC_ERROR, the status a failed
// fetch_data RPC maps to. Debug points are global, hence nonConcurrent.
//
// Setup is the same as test_adbc_catalog_scan -- see its header. In short:
// FE and every BE must be able to read libadbc_driver_flightsql.so at the same
// absolute path.
// ############################################################################

suite("test_adbc_partitioned_read_retry", "p0,external,nonConcurrent") {
    String repoRoot = new File(context.config.suitePath).getParentFile().getParentFile()
            .getAbsolutePath()
    String thirdparty = System.getenv("DORIS_THIRDPARTY")
    if (thirdparty == null || thirdparty.isEmpty()) {
        thirdparty = "${repoRoot}/thirdparty"
    }
    String driverPath = context.config.otherConfigs.get("adbcDriverPath")
    if (driverPath == null || driverPath.isEmpty()) {
        driverPath = "${thirdparty}/installed/lib64/libadbc_driver_flightsql.so"
    }

    if (!new File(driverPath).canRead()) {
        // Not a pass. Nothing about ADBC has been exercised by this run.
        logger.info("SKIPPED test_adbc_partitioned_read_retry: no readable ADBC Flight SQL driver at "
                + "${driverPath}. Install it with 'cd thirdparty && ./build-thirdparty.sh arrow_adbc', "
                + "or set adbcDriverPath in regression-conf.groovy. "
                + "THE RETRY OF AN ADBC PARTITIONED READ IS NOT BEING TESTED.")
        return
    }

    def frontends = sql "show frontends"
    String arrowPort = frontends[0][6]

    sql """DROP CATALOG IF EXISTS test_adbc_partitioned_read_retry_partitions"""
    sql """DROP CATALOG IF EXISTS test_adbc_partitioned_read_retry_statement"""
    sql """DROP DATABASE IF EXISTS test_adbc_partitioned_read_retry_db FORCE"""
    sql """CREATE DATABASE test_adbc_partitioned_read_retry_db"""

    sql """
        CREATE TABLE test_adbc_partitioned_read_retry_db.src (
          `k` bigint NOT NULL,
          `v` varchar(32) NOT NULL
        ) DISTRIBUTED BY HASH(`k`) BUCKETS 8
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_adbc_partitioned_read_retry_db.src
        SELECT number, concat('v', number) FROM numbers("number" = "100000")
    """

    // 'required', not the default 'auto': a driver that stopped partitioning would be downgraded to the
    // statement path silently, and the partitioned half of this suite would test the statement half.
    sql """
        CREATE CATALOG test_adbc_partitioned_read_retry_partitions PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${driverPath}",
            "sql_dialect" = "doris",
            "uri" = "grpc://127.0.0.1:${arrowPort}",
            "user" = "root",
            "password" = "",
            "partitioned_read" = "required"
        )
    """
    sql """
        CREATE CATALOG test_adbc_partitioned_read_retry_statement PROPERTIES (
            "type" = "adbc",
            "driver_url" = "${driverPath}",
            "sql_dialect" = "doris",
            "uri" = "grpc://127.0.0.1:${arrowPort}",
            "user" = "root",
            "password" = "",
            "partitioned_read" = "disabled"
        )
    """

    // Without a failure, both paths return every row.
    order_qt_partitions """
        SELECT count(k), sum(k), max(v)
        FROM test_adbc_partitioned_read_retry_partitions.test_adbc_partitioned_read_retry_db.src
    """
    order_qt_statement """
        SELECT count(k), sum(k), max(v)
        FROM test_adbc_partitioned_read_retry_statement.test_adbc_partitioned_read_retry_db.src
    """

    try {
        // A statement runs again when the retry reads its range: one response lost, every row returned.
        GetDebugPoint().enableDebugPointForAllFEs("ResultReceiver.getNext.dropDataBatch", [execute: 1])
        order_qt_statement_retried """
            SELECT count(k), sum(k), max(v)
            FROM test_adbc_partitioned_read_retry_statement.test_adbc_partitioned_read_retry_db.src
        """
        // The response lost was that query's, which proves its retry happened: armed for one hit, the debug
        // point would otherwise fail this partitioned read.
        order_qt_partitions_after_retry """
            SELECT count(k), sum(k), max(v)
            FROM test_adbc_partitioned_read_retry_partitions.test_adbc_partitioned_read_retry_db.src
        """

        // A partitioned read is not retried: the attempt that failed drained its tickets, so the query fails
        // rather than return what is left of them -- which is nothing, a count of 0.
        GetDebugPoint().enableDebugPointForAllFEs("ResultReceiver.getNext.dropDataBatch", [execute: 1])
        test {
            sql """
                SELECT count(k), sum(k), max(v)
                FROM test_adbc_partitioned_read_retry_partitions.test_adbc_partitioned_read_retry_db.src
            """
            exception "fetch result rpc failed"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("ResultReceiver.getNext.dropDataBatch")
    }
}
