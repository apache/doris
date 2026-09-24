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

suite("spm_tpcds_sf1_q28", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q28 query
    // (SQL unchanged from sql/q28.sql) against the REAL TPCDS sf1 data loaded
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
    def bindSql = """SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 0 AND 5)
      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 10)
      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 11 AND 15)
      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 16 AND 20)
      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 25)
      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 26 AND 30)
      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
LIMIT 100"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("store_sales"),
                "q28 plan_sql should reference store_sales: ${own[0][4]}")
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
    def origWithSpm = sql """SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 0 AND 5)
      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 10)
      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 11 AND 15)
      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 16 AND 20)
      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 25)
      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 26 AND 30)
      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 0 AND 5)
      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 10)
      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 11 AND 15)
      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 16 AND 20)
      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 25)
      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 26 AND 30)
      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
    LIMIT 100"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q28 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 6)
      AND ((ss_list_price BETWEEN 8 AND (8 + 11))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 11)
      AND ((ss_list_price BETWEEN 90 AND (90 + 11))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 12 AND 16)
      AND ((ss_list_price BETWEEN 142 AND (142 + 11))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 17 AND 21)
      AND ((ss_list_price BETWEEN 135 AND (135 + 11))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 22 AND 26)
      AND ((ss_list_price BETWEEN 122 AND (122 + 11))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 27 AND 31)
      AND ((ss_list_price BETWEEN 154 AND (154 + 11))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 6)
      AND ((ss_list_price BETWEEN 8 AND (8 + 11))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 11)
      AND ((ss_list_price BETWEEN 90 AND (90 + 11))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 12 AND 16)
      AND ((ss_list_price BETWEEN 142 AND (142 + 11))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 17 AND 21)
      AND ((ss_list_price BETWEEN 135 AND (135 + 11))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 22 AND 26)
      AND ((ss_list_price BETWEEN 122 AND (122 + 11))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 27 AND 31)
      AND ((ss_list_price BETWEEN 154 AND (154 + 11))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
    LIMIT 100"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q28 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 0 AND 5)
      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 10)
      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 11 AND 15)
      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 16 AND 20)
      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 25)
      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 26 AND 30)
      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
    LIMIT 100"""
    def explainSimilar = sql """EXPLAIN SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 6)
      AND ((ss_list_price BETWEEN 8 AND (8 + 11))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 11)
      AND ((ss_list_price BETWEEN 90 AND (90 + 11))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 12 AND 16)
      AND ((ss_list_price BETWEEN 142 AND (142 + 11))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 17 AND 21)
      AND ((ss_list_price BETWEEN 135 AND (135 + 11))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 22 AND 26)
      AND ((ss_list_price BETWEEN 122 AND (122 + 11))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 27 AND 31)
      AND ((ss_list_price BETWEEN 154 AND (154 + 11))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q28 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q28 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
