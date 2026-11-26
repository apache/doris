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

import org.apache.doris.regression.suite.ClusterOptions

// When a frontend becomes master, InternalSchemaInitializer brings __internal_schema.audit_log to the
// audit schema of its version, so a table an earlier version created gains the columns added since.
// A table that only lacks columns gets them in place, by a light schema change that rewrites nothing.
// A table whose columns are out of order is rewritten by an add + reorder job, which in cloud mode has
// to run on a compute group: the initializer's daemon thread used to have none, and the upgrade failed
// with CONNECT_CONTEXT_NOT_SET on every retry.
suite("test_audit_log_schema_upgrade_docker", "docker") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 1
    // Both modes: the in-place add applies to either, and the rewrite failed in cloud mode.
    options.cloudMode = null

    docker(options) {
        def columnNames = {
            return (sql "desc __internal_schema.audit_log").collect { it[0] as String }
        }
        // the name, type and nullability of each column, in order
        def columns = {
            return (sql "desc __internal_schema.audit_log").collect { "${it[0]} ${it[1]} ${it[2]}".toString() }
        }
        def tabletIds = {
            return (sql_return_maparray "show tablets from __internal_schema.audit_log")
                    .collect { it.TabletId as String } as Set
        }
        def latestAlterJob = {
            def jobs = sql_return_maparray """SHOW ALTER TABLE COLUMN FROM __internal_schema
                    WHERE TableName = 'audit_log' ORDER BY CreateTime DESC LIMIT 1"""
            return jobs.isEmpty() ? null : jobs[0]
        }

        // The first master creates the table with the full schema: the shape an upgrade must reach.
        def expectedColumns = null
        for (int i = 0; i < 60 && expectedColumns == null; i++) {
            try {
                expectedColumns = columns()
            } catch (Exception e) {
                logger.info("the audit table is not created yet: ${e.getMessage()}")
                sleep(1000)
            }
        }
        assertNotNull(expectedColumns)
        assertTrue(columnNames().containsAll(["catalog", "query_time", "queue_time_ms", "cpu_time_ms",
                "compute_group"]), "${expectedColumns}")

        // A restarted frontend becomes master again and runs InternalSchemaInitializer against the
        // table as it is now.
        def restartFrontend = {
            cluster.restartFrontends()
            for (int i = 0; i < 60; i++) {
                try {
                    context.reconnectFe()
                    sql "show frontends"
                    return
                } catch (Exception e) {
                    logger.info("the restarted frontend is not ready yet: ${e.getMessage()}")
                    sleep(1000)
                }
            }
            throw new IllegalStateException("cannot connect to the restarted frontend")
        }
        def waitForAuditSchema = { int timeoutSeconds ->
            def actual = columns()
            for (int i = 0; i < timeoutSeconds && actual != expectedColumns; i++) {
                sleep(1000)
                actual = columns()
            }
            assertEquals(expectedColumns, actual,
                    "the audit table was not upgraded; latest schema change job: ${latestAlterJob()}")
        }

        // 1. A table of an earlier version lacks the columns added since and has the others in order;
        // queue_time_ms, for one, came with 4.1.4. Drop it, the column after it, and one further on.
        sql """ALTER TABLE __internal_schema.audit_log
                DROP COLUMN queue_time_ms, DROP COLUMN cpu_time_ms, DROP COLUMN compute_group"""
        assertFalse(columnNames().contains("queue_time_ms"))
        def tabletsBefore = tabletIds()

        restartFrontend()
        waitForAuditSchema(120)
        // Added in place: the table keeps its tablets.
        def tabletsAfter = tabletIds()
        assertTrue(tabletsAfter.containsAll(tabletsBefore), "tablets before: ${tabletsBefore}, after: ${tabletsAfter}")

        // New audit rows fill the columns the upgrade added.
        sql "set global enable_audit_plugin = true"
        def marker = "audit_schema_upgrade_marker_5E1C9A"
        sql "select 1 as ${marker}"
        def countMarkedRows = {
            return (sql """select count(*) from __internal_schema.audit_log
                    where stmt like '%${marker}%' and stmt not like '%audit_log%'
                      and queue_time_ms is not null and cpu_time_ms is not null""")[0][0] as long
        }
        def found = countMarkedRows()
        for (int i = 0; i < 60 && found == 0; i++) {
            sleep(3000)
            sql "call flush_audit_log()"
            found = countMarkedRows()
        }
        assertTrue(found >= 1, "no audit row of the marked statement fills the added columns")

        // 2. A table created before #51363 moved the audit columns around has them out of order. Move
        // catalog to the end and drop queue_time_ms: the upgrade has to rewrite the table, which in
        // cloud mode runs on a compute group.
        def catalogType = (sql "desc __internal_schema.audit_log").find { it[0] == "catalog" }[1]
        sql "ALTER TABLE __internal_schema.audit_log DROP COLUMN catalog, DROP COLUMN queue_time_ms"
        sql "ALTER TABLE __internal_schema.audit_log ADD COLUMN catalog ${catalogType} NULL"
        assertEquals("catalog", columnNames().last())
        tabletsBefore = tabletIds()

        restartFrontend()
        waitForAuditSchema(300)
        def job = latestAlterJob()
        assertEquals("FINISHED", job.State, "${job}")
        // Rewritten: none of the old tablets is left.
        tabletsAfter = tabletIds()
        assertTrue(tabletsAfter.intersect(tabletsBefore).isEmpty(),
                "tablets before: ${tabletsBefore}, after: ${tabletsAfter}")
    }
}
