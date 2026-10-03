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

suite("test_audit_planning_diagnostics", "nonConcurrent") {
    def originalAudit = sql("show global variables like 'enable_audit_plugin'")[0][1]
    def originalProfile = sql("show variables like 'enable_profile'")[0][1]
    def marker = "planning_diagnostics_" + UUID.randomUUID().toString().replace("-", "")
    try {
        sql "set global enable_audit_plugin = true"
        sql "set enable_profile = false"
        sql "drop table if exists audit_planning_diagnostics_test"
        sql """create table audit_planning_diagnostics_test (id int)
               distributed by hash(id) buckets 1 properties ("replication_num"="1")"""
        sql "insert into audit_planning_diagnostics_test values (1), (2)"
        sql "select sum(id) as ${marker}_success from audit_planning_diagnostics_test"
        test {
            sql "select missing_column as ${marker}_failed from audit_planning_diagnostics_test"
            exception "Unknown column"
        }

        def filter = """stmt like '%${marker}%' and stmt not like '%__internal_schema%'"""
        def rows = []
        for (int i = 0; i < 30; i++) {
            sleep(1000)
            sql "call flush_audit_log()"
            rows = sql "select query_id from __internal_schema.audit_log where ${filter}"
            if (rows.size() == 2) {
                break
            }
        }
        if (rows.size() != 2) {
            throw new IllegalStateException("Expected successful and failed query audit rows, got ${rows}")
        }
        // Exercise the real MAP<STRING, INT> audit ingestion with profile collection disabled.
        order_qt_planning_audit """
            select if(stmt like '%_failed%', 'failed', 'success'),
                   plan_times_ms['planning_passes'],
                   plan_times_ms['planning_last_failed'],
                   plan_times_ms['planning_analyze'] >= 0,
                   plan_times_ms['planning_choose_plan'] >= 0,
                   plan_times_ms['planning_post_process'] >= 0,
                   plan_times_ms['preload_external_metadata'] is not null,
                   plan_times_ms['pre_rewrite_mv'] is not null,
                   ifnull(plan_times_ms['planning_analyze_failures'], 0)
            from __internal_schema.audit_log where ${filter}
        """
    } finally {
        sql "set enable_profile = ${originalProfile}"
        sql "set global enable_audit_plugin = ${originalAudit}"
    }
}
