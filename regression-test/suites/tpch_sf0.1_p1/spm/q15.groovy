/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("spm_tpch_sf01_q15", "spm") {
    if (isCloudMode()) {
        return
    }
    sql "USE regression_test_tpch_sf0_1_p1"
    sql "SET be_number_for_test = 3"
    sql "SET parallel_pipeline_task_num = 8"
    sql "SET forbid_unknown_col_stats = true"
    sql "SET enable_nereids_timeout = false"
    sql "SET spm_rewrite_timeout_ms = 60000"
    sql "SET enable_spm_fallback = false"
    sql "SET disable_nereids_rules = PRUNE_EMPTY_PARTITION"

    // Inline the revenue1 definition so SPM can capture and replay the Q15 computation.
    def q15 = { String startDate ->
        """WITH revenue1 AS (
            SELECT l_suppkey AS supplier_no,
                   sum(l_extendedprice * (1 - l_discount)) AS total_revenue
            FROM lineitem
            WHERE l_shipdate >= DATE '${startDate}'
              AND l_shipdate < DATE '${startDate}' + INTERVAL '3' MONTH
            GROUP BY l_suppkey
        )
        SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
        FROM supplier, revenue1
        WHERE s_suppkey = supplier_no
          AND total_revenue = (SELECT max(total_revenue) FROM revenue1)
        ORDER BY s_suppkey"""
    }

    String original = q15("1996-01-01")
    String variant = q15("1996-04-01")
    long id = sql("CREATE GLOBAL BASELINE PLAN \"${original}\" WITH \"${original}\"")[0][0] as Long
    try {
        order_qt_spm_bind_sql "SELECT bind_sql FROM __internal_schema.spm_baselines WHERE id = ${id}"
        order_qt_spm_bind_digest "SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id}"
        order_qt_spm_plan_sql "SELECT plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}"

        sql "SET enable_spm_rewrite = true"
        assertTrue(sql("EXPLAIN ${original}").toString().contains("SPM baseline hit: id=${id},"))
        assertTrue(sql("EXPLAIN ${variant}").toString().contains("SPM baseline hit: id=${id},"))
        order_qt_orig_with_spm original
        order_qt_similar_with_spm variant

        sql "SET enable_spm_rewrite = false"
        order_qt_orig_without_spm original
        order_qt_similar_without_spm variant
    } finally {
        sql "DROP BASELINE PLAN IF EXISTS ${id}"
    }
}
