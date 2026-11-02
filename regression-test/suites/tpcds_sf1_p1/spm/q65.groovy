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

suite("spm_tpcds_sf1_q65", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q65 query
    // (SQL unchanged from sql/q65.sql) against the REAL TPCDS sf1 data loaded
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
  s_store_name
, i_item_desc
, sc.revenue
, i_current_price
, i_wholesale_cost
, i_brand
FROM
  store
, item
, (
   SELECT
     ss_store_sk
   , avg(revenue) ave
   FROM
     (
      SELECT
        ss_store_sk
      , ss_item_sk
      , sum(ss_sales_price) revenue
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
      GROUP BY ss_store_sk, ss_item_sk
   )  sa
   GROUP BY ss_store_sk
)  sb
, (
   SELECT
     ss_store_sk
   , ss_item_sk
   , sum(ss_sales_price) revenue
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
   GROUP BY ss_store_sk, ss_item_sk
)  sc
WHERE (sb.ss_store_sk = sc.ss_store_sk)
   AND (sc.revenue <= (CAST('0.1' AS DECIMAL(2,1)) * sb.ave))
   AND (s_store_sk = sc.ss_store_sk)
   AND (i_item_sk = sc.ss_item_sk)
ORDER BY s_store_name ASC, i_item_desc ASC
LIMIT 100"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("store"),
                "q65 plan_sql should reference store: ${own[0][4]}")
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
  s_store_name
, i_item_desc
, sc.revenue
, i_current_price
, i_wholesale_cost
, i_brand
FROM
  store
, item
, (
   SELECT
     ss_store_sk
   , avg(revenue) ave
   FROM
     (
      SELECT
        ss_store_sk
      , ss_item_sk
      , sum(ss_sales_price) revenue
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
      GROUP BY ss_store_sk, ss_item_sk
   )  sa
   GROUP BY ss_store_sk
)  sb
, (
   SELECT
     ss_store_sk
   , ss_item_sk
   , sum(ss_sales_price) revenue
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
   GROUP BY ss_store_sk, ss_item_sk
)  sc
WHERE (sb.ss_store_sk = sc.ss_store_sk)
   AND (sc.revenue <= (CAST('0.1' AS DECIMAL(2,1)) * sb.ave))
   AND (s_store_sk = sc.ss_store_sk)
   AND (i_item_sk = sc.ss_item_sk)
ORDER BY s_store_name ASC, i_item_desc ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """SELECT
  s_store_name
, i_item_desc
, sc.revenue
, i_current_price
, i_wholesale_cost
, i_brand
FROM
  store
, item
, (
   SELECT
     ss_store_sk
   , avg(revenue) ave
   FROM
     (
      SELECT
        ss_store_sk
      , ss_item_sk
      , sum(ss_sales_price) revenue
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
      GROUP BY ss_store_sk, ss_item_sk
   )  sa
   GROUP BY ss_store_sk
)  sb
, (
   SELECT
     ss_store_sk
   , ss_item_sk
   , sum(ss_sales_price) revenue
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
   GROUP BY ss_store_sk, ss_item_sk
)  sc
WHERE (sb.ss_store_sk = sc.ss_store_sk)
   AND (sc.revenue <= (CAST('0.1' AS DECIMAL(2,1)) * sb.ave))
   AND (s_store_sk = sc.ss_store_sk)
   AND (i_item_sk = sc.ss_item_sk)
ORDER BY s_store_name ASC, i_item_desc ASC
    LIMIT 100"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q65 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """SELECT
  s_store_name
, i_item_desc
, sc.revenue
, i_current_price
, i_wholesale_cost
, i_brand
FROM
  store
, item
, (
   SELECT
     ss_store_sk
   , avg(revenue) ave
   FROM
     (
      SELECT
        ss_store_sk
      , ss_item_sk
      , sum(ss_sales_price) revenue
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_month_seq BETWEEN 1177 AND (1177 + 11))
      GROUP BY ss_store_sk, ss_item_sk
   )  sa
   GROUP BY ss_store_sk
)  sb
, (
   SELECT
     ss_store_sk
   , ss_item_sk
   , sum(ss_sales_price) revenue
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1177 AND (1177 + 11))
   GROUP BY ss_store_sk, ss_item_sk
)  sc
WHERE (sb.ss_store_sk = sc.ss_store_sk)
   AND (sc.revenue <= (CAST('0.2' AS DECIMAL(2,1)) * sb.ave))
   AND (s_store_sk = sc.ss_store_sk)
   AND (i_item_sk = sc.ss_item_sk)
ORDER BY s_store_name ASC, i_item_desc ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """SELECT
  s_store_name
, i_item_desc
, sc.revenue
, i_current_price
, i_wholesale_cost
, i_brand
FROM
  store
, item
, (
   SELECT
     ss_store_sk
   , avg(revenue) ave
   FROM
     (
      SELECT
        ss_store_sk
      , ss_item_sk
      , sum(ss_sales_price) revenue
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_month_seq BETWEEN 1177 AND (1177 + 11))
      GROUP BY ss_store_sk, ss_item_sk
   )  sa
   GROUP BY ss_store_sk
)  sb
, (
   SELECT
     ss_store_sk
   , ss_item_sk
   , sum(ss_sales_price) revenue
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1177 AND (1177 + 11))
   GROUP BY ss_store_sk, ss_item_sk
)  sc
WHERE (sb.ss_store_sk = sc.ss_store_sk)
   AND (sc.revenue <= (CAST('0.2' AS DECIMAL(2,1)) * sb.ave))
   AND (s_store_sk = sc.ss_store_sk)
   AND (i_item_sk = sc.ss_item_sk)
ORDER BY s_store_name ASC, i_item_desc ASC
    LIMIT 100"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q65 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN SELECT
  s_store_name
, i_item_desc
, sc.revenue
, i_current_price
, i_wholesale_cost
, i_brand
FROM
  store
, item
, (
   SELECT
     ss_store_sk
   , avg(revenue) ave
   FROM
     (
      SELECT
        ss_store_sk
      , ss_item_sk
      , sum(ss_sales_price) revenue
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
      GROUP BY ss_store_sk, ss_item_sk
   )  sa
   GROUP BY ss_store_sk
)  sb
, (
   SELECT
     ss_store_sk
   , ss_item_sk
   , sum(ss_sales_price) revenue
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
   GROUP BY ss_store_sk, ss_item_sk
)  sc
WHERE (sb.ss_store_sk = sc.ss_store_sk)
   AND (sc.revenue <= (CAST('0.1' AS DECIMAL(2,1)) * sb.ave))
   AND (s_store_sk = sc.ss_store_sk)
   AND (i_item_sk = sc.ss_item_sk)
ORDER BY s_store_name ASC, i_item_desc ASC
    LIMIT 100"""
    def explainSimilar = sql """EXPLAIN SELECT
  s_store_name
, i_item_desc
, sc.revenue
, i_current_price
, i_wholesale_cost
, i_brand
FROM
  store
, item
, (
   SELECT
     ss_store_sk
   , avg(revenue) ave
   FROM
     (
      SELECT
        ss_store_sk
      , ss_item_sk
      , sum(ss_sales_price) revenue
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_month_seq BETWEEN 1177 AND (1177 + 11))
      GROUP BY ss_store_sk, ss_item_sk
   )  sa
   GROUP BY ss_store_sk
)  sb
, (
   SELECT
     ss_store_sk
   , ss_item_sk
   , sum(ss_sales_price) revenue
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1177 AND (1177 + 11))
   GROUP BY ss_store_sk, ss_item_sk
)  sc
WHERE (sb.ss_store_sk = sc.ss_store_sk)
   AND (sc.revenue <= (CAST('0.2' AS DECIMAL(2,1)) * sb.ave))
   AND (s_store_sk = sc.ss_store_sk)
   AND (i_item_sk = sc.ss_item_sk)
ORDER BY s_store_name ASC, i_item_desc ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q65 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q65 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
