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

suite("spm_tpcds_sf1_q09", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q09 query
    // (SQL unchanged from sql/q09.sql) against the REAL TPCDS sf1 data loaded
    // by load.groovy.
    // 1. CREATE BASELINE PLAN from the query text (bind + plan); the returned
    //    baseline id locates the own row in SHOW BASELINE PLANS and is used to
    //    drop the baseline afterwards.
    // 2. Match verification: the query itself and a similar query (same structure,
    //    different literal values, verified to return data) run through the SPM
    //    rewrite path (enable_spm_rewrite=true); each must produce the same result
    //    as without SPM (rewrite hit or safe fallback - never an error or a
    //    different result).

    String db = context.config.getDbNameByFile(new File(context.file.parent))
    if (isCloudMode()) {
        return
    }
    sql "use regression_test_tpcds_sf1_p1"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    // Pin the planning cost inputs so the SPM-optimized plan and the frozen
    // plan_sql text decompiled from it stay reproducible across FE restarts.
    // (enable_nereids_distribute_planner is intentionally NOT pinned: disabling
    // it makes SPM-replayed plans stall at execution - see comments.)
    sql 'set be_number_for_test=3'
    sql 'set parallel_pipeline_task_num=8'
    sql 'set forbid_unknown_col_stats=true'
    sql 'set enable_nereids_timeout=false'
    // SPM regression pins the fallback switch CLOSED: a rewritten-plan failure must
    // surface as an error, never silently re-run the original query.
    sql 'set enable_spm_fallback=false'
    sql 'set disable_nereids_rules=PRUNE_EMPTY_PARTITION'

    // CREATE BASELINE PLAN returns the created baseline id; the own row in
    // SHOW BASELINE PLANS is located by that id (duplicate CREATE is idempotent,
    // so no upfront cleanup is needed).

    // original query text (raw; used for the CREATE statement and the rewrite checks)
    def bindSql = """SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 20)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 21 AND 40)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 41 AND 60)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 61 AND 80)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 81 AND 100)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) END) bucket5
FROM
  reason
WHERE (r_reason_sk = 1)"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("store_sales"),
                "q09 plan_sql should reference store_sales: ${own[0][4]}")
        // print the exact baseline columns (bind_sql / bind_sql_digest / plan_sql)
        // into the .out file as three separate labeled blocks so the three values stay
        // readable and distinguishable. The real decompiled subquery aliases (t_0,
        // t_1, ...) are kept verbatim (no normalization)
        order_qt_spm_bind_sql """
            SELECT bind_sql FROM __internal_schema.spm_baselines WHERE id = ${id}
        """
        order_qt_spm_bind_digest """
            SELECT bind_sql_digest FROM __internal_schema.spm_baselines WHERE id = ${id}
        """
        order_qt_spm_plan_sql """
            SELECT plan_sql
            FROM __internal_schema.spm_baselines WHERE id = ${id}
        """
    
        // ===== match verification: the baseline query itself hits its own baseline =====
        sql 'set enable_spm_rewrite=true'
        sql 'set spm_rewrite_timeout_ms=60000'
    def origWithSpm = sql """SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 20)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 21 AND 40)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 41 AND 60)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 61 AND 80)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 81 AND 100)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) END) bucket5
FROM
  reason
    WHERE (r_reason_sk = 1)"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 20)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 21 AND 40)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 41 AND 60)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 61 AND 80)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 81 AND 100)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) END) bucket5
FROM
  reason
    WHERE (r_reason_sk = 1)"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q09 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 21)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 21)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 21)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 22 AND 41)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 22 AND 41)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 22 AND 41)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 42 AND 61)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 42 AND 61)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 42 AND 61)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 62 AND 81)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 62 AND 81)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 62 AND 81)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 82 AND 101)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 82 AND 101)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 82 AND 101)
) END) bucket5
FROM
  reason
    WHERE (r_reason_sk = 2)"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 21)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 21)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 21)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 22 AND 41)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 22 AND 41)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 22 AND 41)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 42 AND 61)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 42 AND 61)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 42 AND 61)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 62 AND 81)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 62 AND 81)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 62 AND 81)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 82 AND 101)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 82 AND 101)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 82 AND 101)
) END) bucket5
FROM
  reason
    WHERE (r_reason_sk = 2)"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q09 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 20)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 21 AND 40)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 41 AND 60)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 61 AND 80)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 81 AND 100)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) END) bucket5
FROM
  reason
    WHERE (r_reason_sk = 1)"""
    def explainSimilar = sql """EXPLAIN SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 21)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 21)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 21)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 22 AND 41)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 22 AND 41)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 22 AND 41)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 42 AND 61)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 42 AND 61)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 42 AND 61)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 62 AND 81)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 62 AND 81)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 62 AND 81)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 82 AND 101)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 82 AND 101)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 82 AND 101)
) END) bucket5
FROM
  reason
    WHERE (r_reason_sk = 2)"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q09 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q09 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
