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
    try {
        sql "set global enable_audit_plugin = true"
    } catch (Exception e) {
        log.warn("skip this case, because " + e.getMessage())
        assertTrue(e.getMessage().toUpperCase().contains("ADMIN"))
        return
    }

    def tableName = "audit_queue_time_test"
    def wgName = "test_queue_time_wg"
    def testMarker = UUID.randomUUID().toString().substring(0, 8)

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

    def maxConcurrency = 1
    // Create workload group: max_concurrency=1 ensures queries queue up
    sql """
        create workload group ${wgName}
        properties (
            'max_concurrency' = '${maxConcurrency}',
            'max_queue_size' = '10',
            'queue_timeout' = '30000'
        )
    """

    // Wait for workload group to take effect
    Thread.sleep(5000)

    // Truncate audit_log for easier testing
    sql "truncate table __internal_schema.audit_log"

    // Submit concurrent queries with marker for later lookup
    def sqlSleepTime = 5
    def queuedSqlCnt = 1
    def threads = []
    for (int i = 0; i < maxConcurrency + queuedSqlCnt; i++) {
        def idx = i
        threads << Thread.start {
            try {
                sql "set workload_group=${wgName}"
                // Use sleep function to simulate long query, ensuring subsequent queries need to queue
                sql """
                    select sleep(${sqlSleepTime}), '${testMarker}_${idx}' as marker
                    from ${tableName} limit 1
                """
            } catch (Exception e) {
                log.warn("Query ${idx} failed: ${e.getMessage()}")
            }
        }
    }

    // Wait for all queries to complete
    threads.each { it.join() }

    // Wait for audit log to flush
    Thread.sleep(5000)
    sql "call flush_audit_log()"
    Thread.sleep(5000)

    // Verify queue_time_ms column exists
    def schemaResult = sql "desc internal.__internal_schema.audit_log"
    def hasQueueTimeMs = schemaResult.any { it[0] == "queue_time_ms" }
    assertTrue(hasQueueTimeMs)

    // check result
    def retry = 10
    def query = """
        select query_id, queue_time_ms, stmt
        from __internal_schema.audit_log
        where stmt like '%${testMarker}%'
        and queue_time_ms > 0
        order by time
    """
    def auditResult = sql "${query}"

    while (auditResult.isEmpty()) {
        if (retry-- < 0) {
            throw new RuntimeException("It has retried a few but still failed, you need to check it")
        }
        sql "call flush_audit_log()"
        sleep(3000)
        auditResult = sql "${query}"
    }

    auditResult.each { row ->
        assertTrue(row[1] >= sqlSleepTime * 1000)
    }

    assertTrue(auditResult.size() >= queuedSqlCnt)

    // Cleanup
    sql "drop table if exists ${tableName}"
    sql "drop workload group if exists ${wgName}"
    sql "set global enable_audit_plugin = false"
}
