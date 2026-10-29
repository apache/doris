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

suite("test_audit_log_queue_time", "nonConcurrent") {
    // Check admin privilege
    def originalAuditPlugin = sql("show global variables like 'enable_audit_plugin'")[0][1]
    try {
        sql "set global enable_audit_plugin = true"
    } catch (Exception e) {
        log.warn("skip this case, because " + e.getMessage())
        assertTrue(e.getMessage().toUpperCase().contains("ADMIN"))
        return
    }

    def tableName = "audit_queue_time_test"
    def wgName = "test_queue_time_wg"
    def maxConcurrency = 1
    def queueTimeoutMs = 30000
    def guaranteedQueueTimeMs = 1000
    def blockerSleepTime = 10
    def markerPrefix = UUID.randomUUID().toString().substring(0, 8)
    def blockerMarker = "${markerPrefix}_blocker"
    def queuedMarker = "${markerPrefix}_queued"
    def threads = []
    def queryErrors = Collections.synchronizedList([])

    def getQueueState = {
        def row = sql("show workload groups").find { it[1].toString() == wgName }
        if (row == null) {
            return null
        }
        return [
                running: row[row.size() - 2] as int,
                waiting: row[row.size() - 1] as int
        ]
    }

    def waitForQueueState = { int expectedRunning, int expectedWaiting ->
        def state = null
        for (int i = 0; i < 100; i++) {
            state = getQueueState()
            if (state != null
                    && state.running == expectedRunning
                    && state.waiting == expectedWaiting) {
                return
            }
            sleep(100)
        }
        throw new RuntimeException("workload group ${wgName} did not reach "
                + "running=${expectedRunning}, waiting=${expectedWaiting}; last state=${state}")
    }

    try {
        // Cleanup environment
        sql "drop table if exists ${tableName}"
        sql "drop workload group if exists ${wgName}"

        // Create test table
        sql """
            CREATE TABLE `${tableName}` (
              `id` bigint,
              `name` varchar(32)
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """

        sql "insert into ${tableName} values (1, 'test')"

        // max_concurrency=1 ensures that the second query enters the queue.
        sql """
            create workload group ${wgName}
            properties (
                'max_concurrency' = '${maxConcurrency}',
                'max_queue_size' = '10',
                'queue_timeout' = '${queueTimeoutMs}'
            )
        """

        // Wait for workload group to take effect.
        Thread.sleep(5000)

        // Truncate audit_log for easier testing.
        sql "truncate table __internal_schema.audit_log"

        // Occupy the only running slot before submitting the query whose queue time is audited.
        threads << Thread.start {
            try {
                sql "set workload_group=${wgName}"
                sql """
                    select sleep(${blockerSleepTime}), '${blockerMarker}' as marker
                    from ${tableName} limit 1
                """
            } catch (Throwable t) {
                queryErrors.add(t)
            }
        }

        waitForQueueState(maxConcurrency, 0)

        threads << Thread.start {
            try {
                sql "set workload_group=${wgName}"
                sql """
                    select id, '${queuedMarker}' as marker
                    from ${tableName} limit 1
                """
            } catch (Throwable t) {
                queryErrors.add(t)
            }
        }

        waitForQueueState(maxConcurrency, 1)
        sleep(guaranteedQueueTimeMs)
        waitForQueueState(maxConcurrency, 1)

        threads.each { it.join() }
        assertTrue(queryErrors.isEmpty(), "query failures: ${queryErrors}")

        // Verify queue_time_ms column exists.
        def schemaResult = sql "desc internal.__internal_schema.audit_log"
        def hasQueueTimeMs = schemaResult.any { it[0] == "queue_time_ms" }
        assertTrue(hasQueueTimeMs)

        // Match only the queued query. Excluding audit_log statements prevents the lookup
        // queries themselves from matching the marker during retries.
        def query = """
            select query_id, queue_time_ms, stmt
            from __internal_schema.audit_log
            where stmt like '%${queuedMarker}%'
            and stmt not like '%__internal_schema.audit_log%'
            order by time
        """
        def auditResult = []
        for (int retry = 0; retry < 10 && auditResult.isEmpty(); retry++) {
            sql "call flush_audit_log()"
            sleep(1000)
            auditResult = sql "${query}"
        }

        assertFalse(auditResult.isEmpty(), "queued query was not found in audit log")
        logger.info("Queued query audit result: ${auditResult}")
        auditResult.each { row ->
            def queueTimeMs = row[1] as long
            assertTrue(queueTimeMs >= guaranteedQueueTimeMs,
                    "queue_time_ms ${queueTimeMs} is less than guaranteed wait ${guaranteedQueueTimeMs}")
            assertTrue(queueTimeMs < queueTimeoutMs,
                    "queue_time_ms ${queueTimeMs} reached queue timeout ${queueTimeoutMs}")
        }
    } finally {
        threads.each { thread ->
            if (thread.isAlive()) {
                thread.join(blockerSleepTime * 1000 + 5000)
            }
        }
        sql "drop table if exists ${tableName}"
        sql "drop workload group if exists ${wgName}"
        sql "set global enable_audit_plugin = ${originalAuditPlugin}"
    }
}
