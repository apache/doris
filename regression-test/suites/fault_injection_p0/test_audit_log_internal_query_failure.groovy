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

// Regression test for CIR-20019: when an internal query fails (for example
// the column-statistics gathering SQL that ANALYZE issues against a user
// table), the audit log entry must record state=ERR with a descriptive
// error_message instead of the previous misleading state=OK / return_rows=0.
suite('test_audit_log_internal_query_failure', 'nonConcurrent') {
    def tbl = 'test_audit_log_internal_query_failure_t1'

    setGlobalVarTemporary([enable_audit_plugin: true], {
        try {
            sql "drop table if exists ${tbl}"
            sql """
                create table ${tbl} (k int, v int)
                duplicate key(k)
                distributed by hash(k) buckets 1
                properties('replication_num'='1')
            """
            sql "insert into ${tbl} values(1,10),(2,20),(3,30)"

            // Limit the injected IO error to this table's tablet so concurrent
            // reads on system tables (such as audit_log itself) are unaffected.
            def tabletRows = sql_return_maparray "show tablets from ${tbl}"
            assertFalse(tabletRows.isEmpty(), "expected at least one tablet for ${tbl}")
            def tabletId = tabletRows[0].TabletId

            // Capture the FE-side current timestamp to filter audit rows so
            // stale entries from previous runs do not satisfy the assertion.
            def startTime = (sql_return_maparray "select now() as ts")[0].ts.toString()

            GetDebugPoint().clearDebugPointsForAllBEs()
            try {
                GetDebugPoint().enableDebugPointForAllBEs(
                        "LocalFileReader::read_at_impl.io_error",
                        [ sub_path: "/${tabletId}/" ])

                test {
                    sql "analyze table ${tbl} with sync"
                    exception "IO_ERROR"
                }
            } finally {
                GetDebugPoint().clearDebugPointsForAllBEs()
            }

            // Force a flush so the failed internal query is queryable from
            // __internal_schema.audit_log.
            // The failed gather SQL reads from our user table and runs as an
            // internal query; it must show up with state=ERR. Filter by start
            // time to avoid matching stale entries from previous runs.
            def query = """select state, error_code, error_message
                           from __internal_schema.audit_log
                           where is_internal = 1
                             and stmt like '%${tbl}%'
                             and state = 'ERR'
                             and `time` >= '${startTime}'
                           order by `time` desc limit 1"""
            def res = []
            int retry = 60
            while (res.isEmpty() && retry-- > 0) {
                sql "call flush_audit_log()"
                sleep(2000)
                res = sql_return_maparray "${query}"
            }
            assertFalse(res.isEmpty(),
                    "expected an audit_log entry with state=ERR for the failed gather query")
            assertEquals('ERR', res[0].state.toString())
            assertNotEquals('0', res[0].error_code.toString())
            assertNotNull(res[0].error_message)
            assertTrue(!res[0].error_message.toString().isEmpty(),
                    "audit_log error_message should not be empty, got: ${res[0].error_message}")
        } finally {
            try {
                sql "drop table if exists ${tbl}"
            } catch (Throwable ignored) {
            }
        }
    })
}
