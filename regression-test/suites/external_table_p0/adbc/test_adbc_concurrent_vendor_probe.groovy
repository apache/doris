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

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

suite("test_adbc_concurrent_vendor_probe", "p0,external") {
    String driver = context.config.otherConfigs.get("adbcDriverPath")
    if (driver == null || driver.isEmpty()) {
        String thirdparty = System.getenv("DORIS_THIRDPARTY")
        if (thirdparty == null || thirdparty.isEmpty()) {
            thirdparty = new File(context.config.suitePath).getParentFile().getParentFile()
                    .getAbsolutePath() + "/thirdparty"
        }
        driver = "${thirdparty}/installed/lib64/libadbc_driver_flightsql.so"
    }
    int port = (sql "SHOW FRONTENDS")[0][6] as int
    if (port <= 0 || !new File(driver).canRead()) {
        logger.info("SKIPPED test_adbc_concurrent_vendor_probe: Flight SQL and its ADBC driver are required")
        return
    }

    sql "DROP DATABASE IF EXISTS internal.test_adbc_concurrent_vendor_probe_db FORCE"
    sql "CREATE DATABASE internal.test_adbc_concurrent_vendor_probe_db"
    sql """CREATE TABLE internal.test_adbc_concurrent_vendor_probe_db.test_vendor_probe_table (
        id INT
    ) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    String expectedTables = (sql "SHOW TABLES FROM internal.test_adbc_concurrent_vendor_probe_db").toString()
    String expected = (sql """SHOW DATABASES FROM internal
        LIKE 'test_adbc_concurrent_vendor_probe_db'""").toString()
    int workers = 3
    for (int round = 0; round < 5; round++) {
        def catalogs = (0..<workers).collect { worker -> "test_adbc_concurrent_vendor_probe_${round}_${worker}" }
        def ready = new CountDownLatch(workers)
        def start = new CountDownLatch(1)
        def calls = []
        try {
            catalogs.each { catalog ->
                sql "DROP CATALOG IF EXISTS ${catalog}"
                // Metadata routing still needs a source identity with an explicit SQL rendering override.
                sql """CREATE CATALOG ${catalog} PROPERTIES (
                    "type" = "adbc", "driver_url" = "${driver}", "sql_dialect" = "doris",
                    "uri" = "grpc://127.0.0.1:${port}", "user" = "root", "password" = "")"""
            }
            String warmCatalog = catalogs[0]
            assertEquals(expected, (sql "SHOW DATABASES FROM ${warmCatalog} LIKE 'test_adbc_concurrent_vendor_probe_db'")
                    .toString())
            calls = catalogs.collect { catalog ->
                thread("vendor-probe-${catalog}") {
                    ready.countDown()
                    assertTrue(start.await(30, TimeUnit.SECONDS), "concurrent probe start timed out")
                    if (catalog == warmCatalog) {
                        // Opening a connection for a warm catalog reads driver info while cold catalogs probe vendors.
                        return (sql "SHOW TABLES FROM ${catalog}.test_adbc_concurrent_vendor_probe_db").toString()
                    }
                    return (sql "SHOW DATABASES FROM ${catalog} LIKE 'test_adbc_concurrent_vendor_probe_db'").toString()
                }
            }
            assertTrue(ready.await(30, TimeUnit.SECONDS), "concurrent probe callers did not start")
            start.countDown()
            calls.eachWithIndex { call, index ->
                assertEquals(index == 0 ? expectedTables : expected, call.get(60, TimeUnit.SECONDS))
            }
        } finally {
            start.countDown()
            calls.each { call -> call.cancel(true) }
            catalogs.each { catalog -> sql "DROP CATALOG IF EXISTS ${catalog}" }
        }
    }
}
