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

// The audit log names the protocol a statement came in over, as SHOW PROCESSLIST does: the
// protocol column of __internal_schema.audit_log is MySQL for a statement of a MySQL connection
// and ArrowFlightSQL for one of an Arrow Flight SQL session.
suite("test_audit_log_protocol", "nonConcurrent") {
    try {
        sql "set global enable_audit_plugin = true"
    } catch (Exception e) {
        log.warn("skip this case, because " + e.getMessage())
        assertTrue(e.getMessage().toUpperCase().contains("ADMIN"))
        return
    }

    try {
        // The column is there from the first start on this version, and is added to a table of an
        // earlier version.
        def schema = sql "desc internal.__internal_schema.audit_log"
        assertTrue(schema.any { it[0] == "protocol" }, "audit_log has no protocol column: ${schema}")

        // Unique markers so the exact statements can be located in the audit log.
        def marker = "audit_protocol_marker_7C2E9D"
        sql "truncate table __internal_schema.audit_log"

        sql "select 1 as ${marker}_mysql"
        arrow_flight_sql "select 1 as ${marker}_flight"

        Thread.sleep(6000)
        sql "call flush_audit_log()"

        // The two marked statements, and only they: the polling query below carries the marker in
        // its own text too, and names the audit_log table, which the marked statements do not.
        def query = """select protocol, stmt from __internal_schema.audit_log
                       where stmt like '%${marker}%' and stmt not like '%audit_log%'
                       order by stmt"""
        def retry = 60
        def rows = sql "${query}"
        while (rows.size() < 2) {
            if (retry-- < 0) {
                throw new RuntimeException("the audit_log rows of the marked statements were not found: ${rows}")
            }
            sleep(3000)
            sql "call flush_audit_log()"
            rows = sql "${query}"
        }
        logger.info("audit rows of the marked statements: ${rows}")
        assertEquals(2, rows.size(), "rows: ${rows}")
        def flightRow = rows.find { it[1].contains("${marker}_flight") }
        def mysqlRow = rows.find { it[1].contains("${marker}_mysql") }
        assertNotNull(flightRow, "the Flight statement is missing: ${rows}")
        assertNotNull(mysqlRow, "the MySQL statement is missing: ${rows}")
        assertEquals("ArrowFlightSQL", flightRow[0])
        assertEquals("MySQL", mysqlRow[0])
    } finally {
        sql "set global enable_audit_plugin = false"
    }
}
