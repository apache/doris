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

// A session variable changed by a per-query SET_VAR hint is temporary: it is reverted in
// StmtExecutor.execute()'s finally block. The audit log is built afterwards (in auditAfterExec),
// so without a pre-revert snapshot the hint value never reaches changed_variables. This case
// verifies that a per-query SET_VAR(session_context=...) is recorded in the audit log's
// changed_variables. The value is quoted so the hint parser keeps the ':' in the value.
suite("test_audit_log_hint_session_context", "nonConcurrent") {
    try {
        sql "set global enable_audit_plugin = true"
    } catch (Exception e) {
        log.warn("skip this case, because " + e.getMessage())
        assertTrue(e.getMessage().toUpperCase().contains("ADMIN"))
        return
    }

    def tbl = "audit_hint_session_context"
    sql "drop table if exists ${tbl}"
    sql """
        CREATE TABLE `${tbl}` (`id` bigint) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """
    sql "insert into ${tbl} values (1)"

    // unique markers so the exact statement can be located in the audit log
    def hintTrace = "trace_id:hint_sc_7F3A2B"
    def stmtMarker = "audit_hint_sc_marker_7F3A2B"

    sql "truncate table __internal_schema.audit_log"

    // per-query SET_VAR hint sets session_context for this statement only
    sql """select /*+ SET_VAR(session_context="${hintTrace}") */ id, '${stmtMarker}' as marker from ${tbl}"""

    Thread.sleep(6000)
    sql """call flush_audit_log()"""

    // The hint query's audit row must carry session_context in changed_variables. Match by the
    // marker AND the exact session_context value: the polling query below is itself audited and
    // its statement text also contains the marker, but it sets no session_context. Picking a
    // single row via "order by time desc limit 1" could therefore land on such a self-row (whose
    // session_context is null) depending on audit-flush ordering, which is flaky. Counting rows
    // whose session_context equals the expected value excludes those self-rows entirely.
    def retry = 60
    def query = """select count(*) from __internal_schema.audit_log
                   where stmt like '%${stmtMarker}%'
                   and ELEMENT_AT(changed_variables, 'session_context') = '${hintTrace}'"""
    def found = (sql "${query}")[0][0] as long
    while (found == 0) {
        if (retry-- < 0) {
            throw new RuntimeException("audit_log row for the hint query with the expected "
                    + "session_context was not found")
        }
        sleep(3000)
        sql """call flush_audit_log()"""
        found = (sql "${query}")[0][0] as long
    }

    // The per-query SET_VAR hint session_context must be visible in changed_variables.
    assertTrue(found >= 1)

    if (isCloudMode()) {
        def computeGroups = sql "SHOW COMPUTE GROUPS"
        if (computeGroups.size() < 2) {
            log.warn("skip Compute Group audit assertion because fewer than two Compute Groups exist")
        } else {
            def sessionComputeGroup = computeGroups[0][0]
            def hintComputeGroup = computeGroups[1][0]
            def computeGroupMarker = "audit_hint_cg_marker_7F3A2B"

            sql "use @${sessionComputeGroup}"
            sql "truncate table __internal_schema.audit_log"
            sql """select /*+ SET_VAR(cloud_cluster = '${hintComputeGroup}') */
                       1, '${computeGroupMarker}'"""

            def computeGroupRetry = 60
            def computeGroupQuery = """select compute_group
                                      from __internal_schema.audit_log
                                      where stmt like '%${computeGroupMarker}%'
                                        and compute_group = '${hintComputeGroup}'
                                      order by time desc limit 1"""
            def computeGroupResult = sql "${computeGroupQuery}"
            while (computeGroupResult.isEmpty()) {
                if (computeGroupRetry-- < 0) {
                    throw new RuntimeException("audit_log row for the hint query was not found in the "
                            + "hint Compute Group")
                }
                sleep(3000)
                sql "call flush_audit_log()"
                computeGroupResult = sql "${computeGroupQuery}"
            }

            assertEquals(hintComputeGroup, computeGroupResult[0][0].toString())
        }
    }

    sql "set global enable_audit_plugin = false"
}
