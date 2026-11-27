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

suite("test_spm_manager_lifecycle", "spm") {
    sql """DROP TABLE IF EXISTS spm_manager_lifecycle"""
    sql """CREATE TABLE spm_manager_lifecycle (k INT, v INT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO spm_manager_lifecycle VALUES (1, 10), (2, 20), (3, 30)"""

    def dropOwnBaselines = {
        sql("SHOW BASELINE PLANS").findAll { it[1].toString().contains("spm_manager_lifecycle") }
                .each { row -> sql "DROP BASELINE PLAN ${row[0]}" }
    }
    dropOwnBaselines()
    sql "SET enable_spm_rewrite = true"
    sql "SET enable_spm_fallback = false"

    String query = "SELECT k, v FROM spm_manager_lifecycle WHERE k > 1 ORDER BY k"
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }

    long previousCapacity = sql("SHOW GLOBAL VARIABLES LIKE 'spm_baseline_cache_size'")[0][1] as Long
    sql "SET GLOBAL spm_baseline_cache_size = 1"
    try {
        long original = createBaseline(query)
        order_qt_initial_rows "SELECT k, v FROM spm_manager_lifecycle WHERE k > 1 ORDER BY k"
        boolean initialHit = sql("EXPLAIN ${query}").toString().contains("SPM baseline hit: id=${original}")
        qt_initial_hit "SELECT ${initialHit} AS baseline_hit"
        sql "ALTER BASELINE PLAN ${original} DISABLE"

        // Warm a different hash to evict the disabled row before its duplicate CREATE.
        createBaseline("SELECT k FROM spm_manager_lifecycle WHERE v > 10 ORDER BY k")
        order_qt_other_hash "SELECT k FROM spm_manager_lifecycle WHERE v > 10 ORDER BY k"

        // Duplicate CREATE preserves the existing incarnation and its disabled status.
        long duplicate = createBaseline(query)
        qt_disabled_duplicate "SELECT ${duplicate} = ${original} AS same_id"
        qt_disabled_row """SELECT status, COUNT(*) FROM __internal_schema.spm_baselines
            WHERE id = ${original} GROUP BY status ORDER BY status"""
        order_qt_disabled_rows "SELECT k, v FROM spm_manager_lifecycle WHERE k > 1 ORDER BY k"

        sql "DROP BASELINE PLAN ${original}"
        long recreated = createBaseline(query)
        qt_recreated_id "SELECT ${recreated} > ${original} AS newer_id"

        // Retrying the old DROP must preserve the newer allocation of the same SQL identity.
        sql "DROP BASELINE PLAN IF EXISTS ${original}"
        qt_recreated_row """SELECT status, COUNT(*) FROM __internal_schema.spm_baselines
            WHERE id = ${recreated} GROUP BY status ORDER BY status"""
        long recreatedDuplicate = createBaseline(query)
        qt_recreated_duplicate "SELECT ${recreatedDuplicate} = ${recreated} AS same_id"
        order_qt_recreated_rows "SELECT k, v FROM spm_manager_lifecycle WHERE k > 1 ORDER BY k"
        boolean recreatedHit = sql("EXPLAIN ${query}").toString().contains("SPM baseline hit: id=${recreated}")
        qt_recreated_hit "SELECT ${recreatedHit} AS baseline_hit"
        qt_old_row_absent "SELECT COUNT(*) FROM __internal_schema.spm_baselines WHERE id = ${original}"
    } finally {
        try {
            dropOwnBaselines()
        } finally {
            sql "SET GLOBAL spm_baseline_cache_size = ${previousCapacity}"
        }
    }
}
