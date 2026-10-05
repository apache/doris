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

import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

suite("test_group_commit_stream_load_high_concurrency_async", "p0") {
    def tableName = "test_group_commit_stream_load_high_concurrency_async"
    int concurrentClients = 20
    int loadsPerClient = 20
    int expectedRows = concurrentClients * loadsPerClient
    def errors = Collections.synchronizedList(new ArrayList<String>())
    def stopRequested = new AtomicBoolean(false)
    def successLoads = new AtomicInteger(0)
    def getProperty = { property, userName ->
        def result = sql_return_maparray """SHOW PROPERTY FOR '${userName}'"""
        result.find {
            it.Key == property as String
        }
    }
    def originMaxUserConnections = getProperty("max_user_connections", "root").Value as long

    def waitRowCount = { expected ->
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql "select count(*) from ${tableName}"
            logger.info("table: ${tableName}, rowCount: ${result}, expected: ${expected}")
            return result[0][0] == expected
        })
    }

    def checkStreamLoadResult = { loadId, result, exception ->
        if (exception != null) {
            stopRequested.set(true)
            errors.add("load ${loadId} exception: ${exception.getMessage()}")
            return
        }
        def json = parseJson(result)
        if (!"success".equalsIgnoreCase(json.Status?.toString())) {
            stopRequested.set(true)
            errors.add("load ${loadId} status=${json.Status}, msg=${json.Message}")
            return
        }
        if (json.GroupCommit != true) {
            stopRequested.set(true)
            errors.add("load ${loadId} is not group commit: ${result}")
            return
        }
        if (json.NumberTotalRows != 1 || json.NumberLoadedRows != 1 ||
            json.NumberFilteredRows != 0 || json.NumberUnselectedRows != 0) {
            stopRequested.set(true)
            errors.add("load ${loadId} unexpected counters: ${result}")
            return
        }
        successLoads.incrementAndGet()
    }

    try {
        sql """SET PROPERTY FOR 'root' 'max_user_connections' = '1024'"""
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                id BIGINT NOT NULL,
                name STRING NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "group_commit_interval_ms" = "5"
            );
        """

        def threads = []
        for (int client = 0; client < concurrentClients; client++) {
            int clientId = client
            threads.add(Thread.startDaemon("group-commit-stream-load-${clientId}") {
                for (int round = 0; round < loadsPerClient; round++) {
                    if (stopRequested.get()) {
                        break
                    }
                    long loadId = clientId * loadsPerClient + round
                    try {
                        streamLoad {
                            table "${tableName}"
                            set 'column_separator', ','
                            set 'group_commit', 'async_mode'
                            unset 'label'
                            inputText "${loadId},name_${loadId}\n"
                            time 60000

                            check { result, exception, startTime, endTime ->
                                checkStreamLoadResult(loadId, result, exception)
                            }
                        }
                    } catch (Exception e) {
                        stopRequested.set(true)
                        errors.add("load ${loadId} streamLoad throws: ${e.getMessage()}")
                        break
                    }
                }
            })
        }

        threads.each { it.join() }

        assertTrue(errors.isEmpty(),
                "group commit stream load failures: " +
                        errors.subList(0, Math.min(errors.size(), 10)))
        assertEquals(expectedRows, successLoads.get())

        sql "sync"
        waitRowCount(expectedRows)

        def result = sql "select count(*) from ${tableName}"
        assertEquals(expectedRows, result[0][0])
    } finally {
        sql """SET PROPERTY FOR 'root' 'max_user_connections' = '${originMaxUserConnections}'"""
    }
}
