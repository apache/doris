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

suite("spm_tpcds_sf1_q31", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q31 query
    // (SQL unchanged from sql/q31.sql) against the REAL TPCDS sf1 data loaded
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
  ss AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ss_ext_sales_price) store_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
, ws AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ws_ext_sales_price) web_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (ws_bill_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
SELECT
  ss1.ca_county
, ss1.d_year
, (ws2.web_sales / ws1.web_sales) web_q1_q2_increase
, (ss2.store_sales / ss1.store_sales) store_q1_q2_increase
, (ws3.web_sales / ws2.web_sales) web_q2_q3_increase
, (ss3.store_sales / ss2.store_sales) store_q2_q3_increase
FROM
  ss ss1
, ss ss2
, ss ss3
, ws ws1
, ws ws2
, ws ws3
WHERE (ss1.d_qoy = 1)
   AND (ss1.d_year = 2000)
   AND (ss1.ca_county = ss2.ca_county)
   AND (ss2.d_qoy = 2)
   AND (ss2.d_year = 2000)
   AND (ss2.ca_county = ss3.ca_county)
   AND (ss3.d_qoy = 3)
   AND (ss3.d_year = 2000)
   AND (ss1.ca_county = ws1.ca_county)
   AND (ws1.d_qoy = 1)
   AND (ws1.d_year = 2000)
   AND (ws1.ca_county = ws2.ca_county)
   AND (ws2.d_qoy = 2)
   AND (ws2.d_year = 2000)
   AND (ws1.ca_county = ws3.ca_county)
   AND (ws3.d_qoy = 3)
   AND (ws3.d_year = 2000)
   AND ((CASE WHEN (ws1.web_sales > 0) THEN (CAST(ws2.web_sales AS DECIMAL(21,3)) / ws1.web_sales) ELSE null END) > (CASE WHEN (ss1.store_sales > 0) THEN (CAST(ss2.store_sales AS DECIMAL(21,3)) / ss1.store_sales) ELSE null END))
   AND ((CASE WHEN (ws2.web_sales > 0) THEN (CAST(ws3.web_sales AS DECIMAL(21,3)) / ws2.web_sales) ELSE null END) > (CASE WHEN (ss2.store_sales > 0) THEN (CAST(ss3.store_sales AS DECIMAL(21,3)) / ss2.store_sales) ELSE null END))
ORDER BY ss1.ca_county ASC"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("store_sales"),
                "q31 plan_sql should reference store_sales: ${own[0][4]}")
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
  ss AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ss_ext_sales_price) store_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
, ws AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ws_ext_sales_price) web_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (ws_bill_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
SELECT
  ss1.ca_county
, ss1.d_year
, (ws2.web_sales / ws1.web_sales) web_q1_q2_increase
, (ss2.store_sales / ss1.store_sales) store_q1_q2_increase
, (ws3.web_sales / ws2.web_sales) web_q2_q3_increase
, (ss3.store_sales / ss2.store_sales) store_q2_q3_increase
FROM
  ss ss1
, ss ss2
, ss ss3
, ws ws1
, ws ws2
, ws ws3
WHERE (ss1.d_qoy = 1)
   AND (ss1.d_year = 2000)
   AND (ss1.ca_county = ss2.ca_county)
   AND (ss2.d_qoy = 2)
   AND (ss2.d_year = 2000)
   AND (ss2.ca_county = ss3.ca_county)
   AND (ss3.d_qoy = 3)
   AND (ss3.d_year = 2000)
   AND (ss1.ca_county = ws1.ca_county)
   AND (ws1.d_qoy = 1)
   AND (ws1.d_year = 2000)
   AND (ws1.ca_county = ws2.ca_county)
   AND (ws2.d_qoy = 2)
   AND (ws2.d_year = 2000)
   AND (ws1.ca_county = ws3.ca_county)
   AND (ws3.d_qoy = 3)
   AND (ws3.d_year = 2000)
   AND ((CASE WHEN (ws1.web_sales > 0) THEN (CAST(ws2.web_sales AS DECIMAL(21,3)) / ws1.web_sales) ELSE null END) > (CASE WHEN (ss1.store_sales > 0) THEN (CAST(ss2.store_sales AS DECIMAL(21,3)) / ss1.store_sales) ELSE null END))
   AND ((CASE WHEN (ws2.web_sales > 0) THEN (CAST(ws3.web_sales AS DECIMAL(21,3)) / ws2.web_sales) ELSE null END) > (CASE WHEN (ss2.store_sales > 0) THEN (CAST(ss3.store_sales AS DECIMAL(21,3)) / ss2.store_sales) ELSE null END))
    ORDER BY ss1.ca_county ASC"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """WITH
  ss AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ss_ext_sales_price) store_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
, ws AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ws_ext_sales_price) web_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (ws_bill_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
SELECT
  ss1.ca_county
, ss1.d_year
, (ws2.web_sales / ws1.web_sales) web_q1_q2_increase
, (ss2.store_sales / ss1.store_sales) store_q1_q2_increase
, (ws3.web_sales / ws2.web_sales) web_q2_q3_increase
, (ss3.store_sales / ss2.store_sales) store_q2_q3_increase
FROM
  ss ss1
, ss ss2
, ss ss3
, ws ws1
, ws ws2
, ws ws3
WHERE (ss1.d_qoy = 1)
   AND (ss1.d_year = 2000)
   AND (ss1.ca_county = ss2.ca_county)
   AND (ss2.d_qoy = 2)
   AND (ss2.d_year = 2000)
   AND (ss2.ca_county = ss3.ca_county)
   AND (ss3.d_qoy = 3)
   AND (ss3.d_year = 2000)
   AND (ss1.ca_county = ws1.ca_county)
   AND (ws1.d_qoy = 1)
   AND (ws1.d_year = 2000)
   AND (ws1.ca_county = ws2.ca_county)
   AND (ws2.d_qoy = 2)
   AND (ws2.d_year = 2000)
   AND (ws1.ca_county = ws3.ca_county)
   AND (ws3.d_qoy = 3)
   AND (ws3.d_year = 2000)
   AND ((CASE WHEN (ws1.web_sales > 0) THEN (CAST(ws2.web_sales AS DECIMAL(21,3)) / ws1.web_sales) ELSE null END) > (CASE WHEN (ss1.store_sales > 0) THEN (CAST(ss2.store_sales AS DECIMAL(21,3)) / ss1.store_sales) ELSE null END))
   AND ((CASE WHEN (ws2.web_sales > 0) THEN (CAST(ws3.web_sales AS DECIMAL(21,3)) / ws2.web_sales) ELSE null END) > (CASE WHEN (ss2.store_sales > 0) THEN (CAST(ss3.store_sales AS DECIMAL(21,3)) / ss2.store_sales) ELSE null END))
    ORDER BY ss1.ca_county ASC"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q31 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """WITH
  ss AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ss_ext_sales_price) store_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
, ws AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ws_ext_sales_price) web_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (ws_bill_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
SELECT
  ss1.ca_county
, ss1.d_year
, (ws2.web_sales / ws1.web_sales) web_q1_q2_increase
, (ss2.store_sales / ss1.store_sales) store_q1_q2_increase
, (ws3.web_sales / ws2.web_sales) web_q2_q3_increase
, (ss3.store_sales / ss2.store_sales) store_q2_q3_increase
FROM
  ss ss1
, ss ss2
, ss ss3
, ws ws1
, ws ws2
, ws ws3
WHERE (ss1.d_qoy = 2)
   AND (ss1.d_year = 2001)
   AND (ss1.ca_county = ss2.ca_county)
   AND (ss2.d_qoy = 3)
   AND (ss2.d_year = 2001)
   AND (ss2.ca_county = ss3.ca_county)
   AND (ss3.d_qoy = 4)
   AND (ss3.d_year = 2001)
   AND (ss1.ca_county = ws1.ca_county)
   AND (ws1.d_qoy = 2)
   AND (ws1.d_year = 2001)
   AND (ws1.ca_county = ws2.ca_county)
   AND (ws2.d_qoy = 3)
   AND (ws2.d_year = 2001)
   AND (ws1.ca_county = ws3.ca_county)
   AND (ws3.d_qoy = 4)
   AND (ws3.d_year = 2001)
   AND ((CASE WHEN (ws1.web_sales > 0) THEN (CAST(ws2.web_sales AS DECIMAL(21,3)) / ws1.web_sales) ELSE null END) > (CASE WHEN (ss1.store_sales > 0) THEN (CAST(ss2.store_sales AS DECIMAL(21,3)) / ss1.store_sales) ELSE null END))
   AND ((CASE WHEN (ws2.web_sales > 0) THEN (CAST(ws3.web_sales AS DECIMAL(21,3)) / ws2.web_sales) ELSE null END) > (CASE WHEN (ss2.store_sales > 0) THEN (CAST(ss3.store_sales AS DECIMAL(21,3)) / ss2.store_sales) ELSE null END))
    ORDER BY ss1.ca_county ASC"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """WITH
  ss AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ss_ext_sales_price) store_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
, ws AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ws_ext_sales_price) web_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (ws_bill_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
SELECT
  ss1.ca_county
, ss1.d_year
, (ws2.web_sales / ws1.web_sales) web_q1_q2_increase
, (ss2.store_sales / ss1.store_sales) store_q1_q2_increase
, (ws3.web_sales / ws2.web_sales) web_q2_q3_increase
, (ss3.store_sales / ss2.store_sales) store_q2_q3_increase
FROM
  ss ss1
, ss ss2
, ss ss3
, ws ws1
, ws ws2
, ws ws3
WHERE (ss1.d_qoy = 2)
   AND (ss1.d_year = 2001)
   AND (ss1.ca_county = ss2.ca_county)
   AND (ss2.d_qoy = 3)
   AND (ss2.d_year = 2001)
   AND (ss2.ca_county = ss3.ca_county)
   AND (ss3.d_qoy = 4)
   AND (ss3.d_year = 2001)
   AND (ss1.ca_county = ws1.ca_county)
   AND (ws1.d_qoy = 2)
   AND (ws1.d_year = 2001)
   AND (ws1.ca_county = ws2.ca_county)
   AND (ws2.d_qoy = 3)
   AND (ws2.d_year = 2001)
   AND (ws1.ca_county = ws3.ca_county)
   AND (ws3.d_qoy = 4)
   AND (ws3.d_year = 2001)
   AND ((CASE WHEN (ws1.web_sales > 0) THEN (CAST(ws2.web_sales AS DECIMAL(21,3)) / ws1.web_sales) ELSE null END) > (CASE WHEN (ss1.store_sales > 0) THEN (CAST(ss2.store_sales AS DECIMAL(21,3)) / ss1.store_sales) ELSE null END))
   AND ((CASE WHEN (ws2.web_sales > 0) THEN (CAST(ws3.web_sales AS DECIMAL(21,3)) / ws2.web_sales) ELSE null END) > (CASE WHEN (ss2.store_sales > 0) THEN (CAST(ss3.store_sales AS DECIMAL(21,3)) / ss2.store_sales) ELSE null END))
    ORDER BY ss1.ca_county ASC"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q31 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN WITH
  ss AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ss_ext_sales_price) store_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
, ws AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ws_ext_sales_price) web_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (ws_bill_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
SELECT
  ss1.ca_county
, ss1.d_year
, (ws2.web_sales / ws1.web_sales) web_q1_q2_increase
, (ss2.store_sales / ss1.store_sales) store_q1_q2_increase
, (ws3.web_sales / ws2.web_sales) web_q2_q3_increase
, (ss3.store_sales / ss2.store_sales) store_q2_q3_increase
FROM
  ss ss1
, ss ss2
, ss ss3
, ws ws1
, ws ws2
, ws ws3
WHERE (ss1.d_qoy = 1)
   AND (ss1.d_year = 2000)
   AND (ss1.ca_county = ss2.ca_county)
   AND (ss2.d_qoy = 2)
   AND (ss2.d_year = 2000)
   AND (ss2.ca_county = ss3.ca_county)
   AND (ss3.d_qoy = 3)
   AND (ss3.d_year = 2000)
   AND (ss1.ca_county = ws1.ca_county)
   AND (ws1.d_qoy = 1)
   AND (ws1.d_year = 2000)
   AND (ws1.ca_county = ws2.ca_county)
   AND (ws2.d_qoy = 2)
   AND (ws2.d_year = 2000)
   AND (ws1.ca_county = ws3.ca_county)
   AND (ws3.d_qoy = 3)
   AND (ws3.d_year = 2000)
   AND ((CASE WHEN (ws1.web_sales > 0) THEN (CAST(ws2.web_sales AS DECIMAL(21,3)) / ws1.web_sales) ELSE null END) > (CASE WHEN (ss1.store_sales > 0) THEN (CAST(ss2.store_sales AS DECIMAL(21,3)) / ss1.store_sales) ELSE null END))
   AND ((CASE WHEN (ws2.web_sales > 0) THEN (CAST(ws3.web_sales AS DECIMAL(21,3)) / ws2.web_sales) ELSE null END) > (CASE WHEN (ss2.store_sales > 0) THEN (CAST(ss3.store_sales AS DECIMAL(21,3)) / ss2.store_sales) ELSE null END))
    ORDER BY ss1.ca_county ASC"""
    def explainSimilar = sql """EXPLAIN WITH
  ss AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ss_ext_sales_price) store_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
, ws AS (
   SELECT
     ca_county
   , d_qoy
   , d_year
   , sum(ws_ext_sales_price) web_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (ws_bill_addr_sk = ca_address_sk)
   GROUP BY ca_county, d_qoy, d_year
)
SELECT
  ss1.ca_county
, ss1.d_year
, (ws2.web_sales / ws1.web_sales) web_q1_q2_increase
, (ss2.store_sales / ss1.store_sales) store_q1_q2_increase
, (ws3.web_sales / ws2.web_sales) web_q2_q3_increase
, (ss3.store_sales / ss2.store_sales) store_q2_q3_increase
FROM
  ss ss1
, ss ss2
, ss ss3
, ws ws1
, ws ws2
, ws ws3
WHERE (ss1.d_qoy = 2)
   AND (ss1.d_year = 2001)
   AND (ss1.ca_county = ss2.ca_county)
   AND (ss2.d_qoy = 3)
   AND (ss2.d_year = 2001)
   AND (ss2.ca_county = ss3.ca_county)
   AND (ss3.d_qoy = 4)
   AND (ss3.d_year = 2001)
   AND (ss1.ca_county = ws1.ca_county)
   AND (ws1.d_qoy = 2)
   AND (ws1.d_year = 2001)
   AND (ws1.ca_county = ws2.ca_county)
   AND (ws2.d_qoy = 3)
   AND (ws2.d_year = 2001)
   AND (ws1.ca_county = ws3.ca_county)
   AND (ws3.d_qoy = 4)
   AND (ws3.d_year = 2001)
   AND ((CASE WHEN (ws1.web_sales > 0) THEN (CAST(ws2.web_sales AS DECIMAL(21,3)) / ws1.web_sales) ELSE null END) > (CASE WHEN (ss1.store_sales > 0) THEN (CAST(ss2.store_sales AS DECIMAL(21,3)) / ss1.store_sales) ELSE null END))
   AND ((CASE WHEN (ws2.web_sales > 0) THEN (CAST(ws3.web_sales AS DECIMAL(21,3)) / ws2.web_sales) ELSE null END) > (CASE WHEN (ss2.store_sales > 0) THEN (CAST(ss3.store_sales AS DECIMAL(21,3)) / ss2.store_sales) ELSE null END))
    ORDER BY ss1.ca_county ASC"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q31 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q31 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
