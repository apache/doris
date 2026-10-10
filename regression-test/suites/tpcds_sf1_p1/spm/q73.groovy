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

suite("spm_tpcds_sf1_q73", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q73 query
    // (SQL unchanged from sql/q73.sql) against the REAL TPCDS sf1 data loaded
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
  c_last_name
, c_first_name
, c_salutation
, c_preferred_cust_flag
, ss_ticket_number
, cnt
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (date_dim.d_dom BETWEEN 1 AND 2)
      AND ((household_demographics.hd_buy_potential = '>10000')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > 1)
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Franklin Parish'   , 'Bronx County'   , 'Orange County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dj
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 1 AND 5)
ORDER BY cnt DESC, c_last_name ASC"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("store_sales"),
                "q73 plan_sql should reference store_sales: ${own[0][4]}")
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
  c_last_name
, c_first_name
, c_salutation
, c_preferred_cust_flag
, ss_ticket_number
, cnt
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (date_dim.d_dom BETWEEN 1 AND 2)
      AND ((household_demographics.hd_buy_potential = '>10000')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > 1)
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Franklin Parish'   , 'Bronx County'   , 'Orange County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dj
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 1 AND 5)
    ORDER BY cnt DESC, c_last_name ASC"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """SELECT
  c_last_name
, c_first_name
, c_salutation
, c_preferred_cust_flag
, ss_ticket_number
, cnt
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (date_dim.d_dom BETWEEN 1 AND 2)
      AND ((household_demographics.hd_buy_potential = '>10000')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > 1)
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Franklin Parish'   , 'Bronx County'   , 'Orange County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dj
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 1 AND 5)
    ORDER BY cnt DESC, c_last_name ASC"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q73 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """SELECT
  c_last_name
, c_first_name
, c_salutation
, c_preferred_cust_flag
, ss_ticket_number
, cnt
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (date_dim.d_dom BETWEEN 3 AND 4)
      AND ((household_demographics.hd_buy_potential = '>10000~')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > 1)
      AND (date_dim.d_year IN (2000   , (2000 + 1)   , (2000 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Franklin Parish'   , 'Bronx County'   , 'Orange County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dj
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 2 AND 6)
    ORDER BY cnt DESC, c_last_name ASC"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """SELECT
  c_last_name
, c_first_name
, c_salutation
, c_preferred_cust_flag
, ss_ticket_number
, cnt
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (date_dim.d_dom BETWEEN 3 AND 4)
      AND ((household_demographics.hd_buy_potential = '>10000~')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > 1)
      AND (date_dim.d_year IN (2000   , (2000 + 1)   , (2000 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Franklin Parish'   , 'Bronx County'   , 'Orange County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dj
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 2 AND 6)
    ORDER BY cnt DESC, c_last_name ASC"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q73 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN SELECT
  c_last_name
, c_first_name
, c_salutation
, c_preferred_cust_flag
, ss_ticket_number
, cnt
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (date_dim.d_dom BETWEEN 1 AND 2)
      AND ((household_demographics.hd_buy_potential = '>10000')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > 1)
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Franklin Parish'   , 'Bronx County'   , 'Orange County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dj
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 1 AND 5)
    ORDER BY cnt DESC, c_last_name ASC"""
    def explainSimilar = sql """EXPLAIN SELECT
  c_last_name
, c_first_name
, c_salutation
, c_preferred_cust_flag
, ss_ticket_number
, cnt
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (date_dim.d_dom BETWEEN 3 AND 4)
      AND ((household_demographics.hd_buy_potential = '>10000~')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > 1)
      AND (date_dim.d_year IN (2000   , (2000 + 1)   , (2000 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Franklin Parish'   , 'Bronx County'   , 'Orange County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dj
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 2 AND 6)
    ORDER BY cnt DESC, c_last_name ASC"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q73 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q73 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
