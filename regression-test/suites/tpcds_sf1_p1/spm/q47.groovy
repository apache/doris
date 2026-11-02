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

suite("spm_tpcds_sf1_q47", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q47 query
    // (SQL unchanged from sql/q47.sql) against the REAL TPCDS sf1 data loaded
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
    def bindSql = """WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , s_store_name
   , s_company_name
   , d_year
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND ((d_year = 1999)
         OR ((d_year = (1999 - 1))
            AND (d_moy = 12))
         OR ((d_year = (1999 + 1))
            AND (d_moy = 1)))
   GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.s_store_name
   , v1.s_company_name
   , v1.d_year
   , v1.d_moy
   , v1.avg_monthly_sales
   , v1.sum_sales
   , v1_lag.sum_sales psum
   , v1_lead.sum_sales nsum
   FROM
     v1
   , v1 v1_lag
   , v1 v1_lead
   WHERE (v1.i_category = v1_lag.i_category)
      AND (v1.i_category = v1_lead.i_category)
      AND (v1.i_brand = v1_lag.i_brand)
      AND (v1.i_brand = v1_lead.i_brand)
      AND (v1.s_store_name = v1_lag.s_store_name)
      AND (v1.s_store_name = v1_lead.s_store_name)
      AND (v1.s_company_name = v1_lag.s_company_name)
      AND (v1.s_company_name = v1_lead.s_company_name)
      AND (v1.rn = (v1_lag.rn + 1))
      AND (v1.rn = (v1_lead.rn - 1))
)
SELECT *
FROM
  v2
WHERE (d_year = 1999)
   AND (avg_monthly_sales > 0)
   AND ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, 3 ASC
LIMIT 100"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("item"),
                "q47 plan_sql should reference item: ${own[0][4]}")
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
    def origWithSpm = sql """WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , s_store_name
   , s_company_name
   , d_year
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND ((d_year = 1999)
         OR ((d_year = (1999 - 1))
            AND (d_moy = 12))
         OR ((d_year = (1999 + 1))
            AND (d_moy = 1)))
   GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.s_store_name
   , v1.s_company_name
   , v1.d_year
   , v1.d_moy
   , v1.avg_monthly_sales
   , v1.sum_sales
   , v1_lag.sum_sales psum
   , v1_lead.sum_sales nsum
   FROM
     v1
   , v1 v1_lag
   , v1 v1_lead
   WHERE (v1.i_category = v1_lag.i_category)
      AND (v1.i_category = v1_lead.i_category)
      AND (v1.i_brand = v1_lag.i_brand)
      AND (v1.i_brand = v1_lead.i_brand)
      AND (v1.s_store_name = v1_lag.s_store_name)
      AND (v1.s_store_name = v1_lead.s_store_name)
      AND (v1.s_company_name = v1_lag.s_company_name)
      AND (v1.s_company_name = v1_lead.s_company_name)
      AND (v1.rn = (v1_lag.rn + 1))
      AND (v1.rn = (v1_lead.rn - 1))
)
SELECT *
FROM
  v2
WHERE (d_year = 1999)
   AND (avg_monthly_sales > 0)
   AND ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, 3 ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , s_store_name
   , s_company_name
   , d_year
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND ((d_year = 1999)
         OR ((d_year = (1999 - 1))
            AND (d_moy = 12))
         OR ((d_year = (1999 + 1))
            AND (d_moy = 1)))
   GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.s_store_name
   , v1.s_company_name
   , v1.d_year
   , v1.d_moy
   , v1.avg_monthly_sales
   , v1.sum_sales
   , v1_lag.sum_sales psum
   , v1_lead.sum_sales nsum
   FROM
     v1
   , v1 v1_lag
   , v1 v1_lead
   WHERE (v1.i_category = v1_lag.i_category)
      AND (v1.i_category = v1_lead.i_category)
      AND (v1.i_brand = v1_lag.i_brand)
      AND (v1.i_brand = v1_lead.i_brand)
      AND (v1.s_store_name = v1_lag.s_store_name)
      AND (v1.s_store_name = v1_lead.s_store_name)
      AND (v1.s_company_name = v1_lag.s_company_name)
      AND (v1.s_company_name = v1_lead.s_company_name)
      AND (v1.rn = (v1_lag.rn + 1))
      AND (v1.rn = (v1_lead.rn - 1))
)
SELECT *
FROM
  v2
WHERE (d_year = 1999)
   AND (avg_monthly_sales > 0)
   AND ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, 3 ASC
    LIMIT 100"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q47 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , s_store_name
   , s_company_name
   , d_year
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND ((d_year = 2000)
         OR ((d_year = (2000 - 1))
            AND (d_moy = 11))
         OR ((d_year = (2000 + 1))
            AND (d_moy = 2)))
   GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.s_store_name
   , v1.s_company_name
   , v1.d_year
   , v1.d_moy
   , v1.avg_monthly_sales
   , v1.sum_sales
   , v1_lag.sum_sales psum
   , v1_lead.sum_sales nsum
   FROM
     v1
   , v1 v1_lag
   , v1 v1_lead
   WHERE (v1.i_category = v1_lag.i_category)
      AND (v1.i_category = v1_lead.i_category)
      AND (v1.i_brand = v1_lag.i_brand)
      AND (v1.i_brand = v1_lead.i_brand)
      AND (v1.s_store_name = v1_lag.s_store_name)
      AND (v1.s_store_name = v1_lead.s_store_name)
      AND (v1.s_company_name = v1_lag.s_company_name)
      AND (v1.s_company_name = v1_lead.s_company_name)
      AND (v1.rn = (v1_lag.rn + 1))
      AND (v1.rn = (v1_lead.rn - 1))
)
SELECT *
FROM
  v2
WHERE (d_year = 2000)
   AND (avg_monthly_sales > 0)
   AND ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, 3 ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , s_store_name
   , s_company_name
   , d_year
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND ((d_year = 2000)
         OR ((d_year = (2000 - 1))
            AND (d_moy = 11))
         OR ((d_year = (2000 + 1))
            AND (d_moy = 2)))
   GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.s_store_name
   , v1.s_company_name
   , v1.d_year
   , v1.d_moy
   , v1.avg_monthly_sales
   , v1.sum_sales
   , v1_lag.sum_sales psum
   , v1_lead.sum_sales nsum
   FROM
     v1
   , v1 v1_lag
   , v1 v1_lead
   WHERE (v1.i_category = v1_lag.i_category)
      AND (v1.i_category = v1_lead.i_category)
      AND (v1.i_brand = v1_lag.i_brand)
      AND (v1.i_brand = v1_lead.i_brand)
      AND (v1.s_store_name = v1_lag.s_store_name)
      AND (v1.s_store_name = v1_lead.s_store_name)
      AND (v1.s_company_name = v1_lag.s_company_name)
      AND (v1.s_company_name = v1_lead.s_company_name)
      AND (v1.rn = (v1_lag.rn + 1))
      AND (v1.rn = (v1_lead.rn - 1))
)
SELECT *
FROM
  v2
WHERE (d_year = 2000)
   AND (avg_monthly_sales > 0)
   AND ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, 3 ASC
    LIMIT 100"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q47 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , s_store_name
   , s_company_name
   , d_year
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND ((d_year = 1999)
         OR ((d_year = (1999 - 1))
            AND (d_moy = 12))
         OR ((d_year = (1999 + 1))
            AND (d_moy = 1)))
   GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.s_store_name
   , v1.s_company_name
   , v1.d_year
   , v1.d_moy
   , v1.avg_monthly_sales
   , v1.sum_sales
   , v1_lag.sum_sales psum
   , v1_lead.sum_sales nsum
   FROM
     v1
   , v1 v1_lag
   , v1 v1_lead
   WHERE (v1.i_category = v1_lag.i_category)
      AND (v1.i_category = v1_lead.i_category)
      AND (v1.i_brand = v1_lag.i_brand)
      AND (v1.i_brand = v1_lead.i_brand)
      AND (v1.s_store_name = v1_lag.s_store_name)
      AND (v1.s_store_name = v1_lead.s_store_name)
      AND (v1.s_company_name = v1_lag.s_company_name)
      AND (v1.s_company_name = v1_lead.s_company_name)
      AND (v1.rn = (v1_lag.rn + 1))
      AND (v1.rn = (v1_lead.rn - 1))
)
SELECT *
FROM
  v2
WHERE (d_year = 1999)
   AND (avg_monthly_sales > 0)
   AND ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, 3 ASC
    LIMIT 100"""
    def explainSimilar = sql """EXPLAIN WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , s_store_name
   , s_company_name
   , d_year
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND ((d_year = 2000)
         OR ((d_year = (2000 - 1))
            AND (d_moy = 11))
         OR ((d_year = (2000 + 1))
            AND (d_moy = 2)))
   GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.s_store_name
   , v1.s_company_name
   , v1.d_year
   , v1.d_moy
   , v1.avg_monthly_sales
   , v1.sum_sales
   , v1_lag.sum_sales psum
   , v1_lead.sum_sales nsum
   FROM
     v1
   , v1 v1_lag
   , v1 v1_lead
   WHERE (v1.i_category = v1_lag.i_category)
      AND (v1.i_category = v1_lead.i_category)
      AND (v1.i_brand = v1_lag.i_brand)
      AND (v1.i_brand = v1_lead.i_brand)
      AND (v1.s_store_name = v1_lag.s_store_name)
      AND (v1.s_store_name = v1_lead.s_store_name)
      AND (v1.s_company_name = v1_lag.s_company_name)
      AND (v1.s_company_name = v1_lead.s_company_name)
      AND (v1.rn = (v1_lag.rn + 1))
      AND (v1.rn = (v1_lead.rn - 1))
)
SELECT *
FROM
  v2
WHERE (d_year = 2000)
   AND (avg_monthly_sales > 0)
   AND ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, 3 ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q47 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q47 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
