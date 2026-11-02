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

suite("spm_tpcds_sf1_q85", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q85 query
    // (SQL unchanged from sql/q85.sql) against the REAL TPCDS sf1 data loaded
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
  substr(r_reason_desc, 1, 20)
, avg(ws_quantity)
, avg(wr_refunded_cash)
, avg(wr_fee)
FROM
  web_sales
, web_returns
, web_page
, customer_demographics cd1
, customer_demographics cd2
, customer_address
, date_dim
, reason
WHERE (ws_web_page_sk = wp_web_page_sk)
   AND (ws_item_sk = wr_item_sk)
   AND (ws_order_number = wr_order_number)
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_year = 2000)
   AND (cd1.cd_demo_sk = wr_refunded_cdemo_sk)
   AND (cd2.cd_demo_sk = wr_returning_cdemo_sk)
   AND (ca_address_sk = wr_refunded_addr_sk)
   AND (r_reason_sk = wr_reason_sk)
   AND (((cd1.cd_marital_status = 'M')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'Advanced Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'S')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'College')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'W')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = '2 yr Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))))
   AND (((ca_country = 'United States')
         AND (ca_state IN ('IN'      , 'OH'      , 'NJ'))
         AND (ws_net_profit BETWEEN 100 AND 200))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('WI'      , 'CT'      , 'KY'))
         AND (ws_net_profit BETWEEN 150 AND 300))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('LA'      , 'IA'      , 'AR'))
         AND (ws_net_profit BETWEEN 50 AND 250)))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 20) ASC, avg(ws_quantity) ASC, avg(wr_refunded_cash) ASC, avg(wr_fee) ASC
LIMIT 100"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("web_sales"),
                "q85 plan_sql should reference web_sales: ${own[0][4]}")
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
  substr(r_reason_desc, 1, 20)
, avg(ws_quantity)
, avg(wr_refunded_cash)
, avg(wr_fee)
FROM
  web_sales
, web_returns
, web_page
, customer_demographics cd1
, customer_demographics cd2
, customer_address
, date_dim
, reason
WHERE (ws_web_page_sk = wp_web_page_sk)
   AND (ws_item_sk = wr_item_sk)
   AND (ws_order_number = wr_order_number)
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_year = 2000)
   AND (cd1.cd_demo_sk = wr_refunded_cdemo_sk)
   AND (cd2.cd_demo_sk = wr_returning_cdemo_sk)
   AND (ca_address_sk = wr_refunded_addr_sk)
   AND (r_reason_sk = wr_reason_sk)
   AND (((cd1.cd_marital_status = 'M')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'Advanced Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'S')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'College')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'W')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = '2 yr Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))))
   AND (((ca_country = 'United States')
         AND (ca_state IN ('IN'      , 'OH'      , 'NJ'))
         AND (ws_net_profit BETWEEN 100 AND 200))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('WI'      , 'CT'      , 'KY'))
         AND (ws_net_profit BETWEEN 150 AND 300))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('LA'      , 'IA'      , 'AR'))
         AND (ws_net_profit BETWEEN 50 AND 250)))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 20) ASC, avg(ws_quantity) ASC, avg(wr_refunded_cash) ASC, avg(wr_fee) ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """SELECT
  substr(r_reason_desc, 1, 20)
, avg(ws_quantity)
, avg(wr_refunded_cash)
, avg(wr_fee)
FROM
  web_sales
, web_returns
, web_page
, customer_demographics cd1
, customer_demographics cd2
, customer_address
, date_dim
, reason
WHERE (ws_web_page_sk = wp_web_page_sk)
   AND (ws_item_sk = wr_item_sk)
   AND (ws_order_number = wr_order_number)
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_year = 2000)
   AND (cd1.cd_demo_sk = wr_refunded_cdemo_sk)
   AND (cd2.cd_demo_sk = wr_returning_cdemo_sk)
   AND (ca_address_sk = wr_refunded_addr_sk)
   AND (r_reason_sk = wr_reason_sk)
   AND (((cd1.cd_marital_status = 'M')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'Advanced Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'S')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'College')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'W')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = '2 yr Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))))
   AND (((ca_country = 'United States')
         AND (ca_state IN ('IN'      , 'OH'      , 'NJ'))
         AND (ws_net_profit BETWEEN 100 AND 200))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('WI'      , 'CT'      , 'KY'))
         AND (ws_net_profit BETWEEN 150 AND 300))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('LA'      , 'IA'      , 'AR'))
         AND (ws_net_profit BETWEEN 50 AND 250)))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 20) ASC, avg(ws_quantity) ASC, avg(wr_refunded_cash) ASC, avg(wr_fee) ASC
    LIMIT 100"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q85 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """SELECT
  substr(r_reason_desc, 1, 21)
, avg(ws_quantity)
, avg(wr_refunded_cash)
, avg(wr_fee)
FROM
  web_sales
, web_returns
, web_page
, customer_demographics cd1
, customer_demographics cd2
, customer_address
, date_dim
, reason
WHERE (ws_web_page_sk = wp_web_page_sk)
   AND (ws_item_sk = wr_item_sk)
   AND (ws_order_number = wr_order_number)
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_year = 2001)
   AND (cd1.cd_demo_sk = wr_refunded_cdemo_sk)
   AND (cd2.cd_demo_sk = wr_returning_cdemo_sk)
   AND (ca_address_sk = wr_refunded_addr_sk)
   AND (r_reason_sk = wr_reason_sk)
   AND (((cd1.cd_marital_status = 'U')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'Secondary')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('110.00' AS DECIMAL(5,2)) AND CAST('160.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'D')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = '4 yr Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('60.00' AS DECIMAL(5,2)) AND CAST('110.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'M')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'College')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('160.00' AS DECIMAL(5,2)) AND CAST('210.00' AS DECIMAL(5,2)))))
   AND (((ca_country = 'United States')
         AND (ca_state IN ('CA'      , 'WA'      , 'GA'))
         AND (ws_net_profit BETWEEN 200 AND 300))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('NV'      , 'AZ'      , 'MN'))
         AND (ws_net_profit BETWEEN 250 AND 350))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('FL'      , 'MI'      , 'NY'))
         AND (ws_net_profit BETWEEN 100 AND 300)))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 21) ASC, avg(ws_quantity) ASC, avg(wr_refunded_cash) ASC, avg(wr_fee) ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """SELECT
  substr(r_reason_desc, 1, 21)
, avg(ws_quantity)
, avg(wr_refunded_cash)
, avg(wr_fee)
FROM
  web_sales
, web_returns
, web_page
, customer_demographics cd1
, customer_demographics cd2
, customer_address
, date_dim
, reason
WHERE (ws_web_page_sk = wp_web_page_sk)
   AND (ws_item_sk = wr_item_sk)
   AND (ws_order_number = wr_order_number)
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_year = 2001)
   AND (cd1.cd_demo_sk = wr_refunded_cdemo_sk)
   AND (cd2.cd_demo_sk = wr_returning_cdemo_sk)
   AND (ca_address_sk = wr_refunded_addr_sk)
   AND (r_reason_sk = wr_reason_sk)
   AND (((cd1.cd_marital_status = 'U')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'Secondary')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('110.00' AS DECIMAL(5,2)) AND CAST('160.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'D')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = '4 yr Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('60.00' AS DECIMAL(5,2)) AND CAST('110.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'M')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'College')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('160.00' AS DECIMAL(5,2)) AND CAST('210.00' AS DECIMAL(5,2)))))
   AND (((ca_country = 'United States')
         AND (ca_state IN ('CA'      , 'WA'      , 'GA'))
         AND (ws_net_profit BETWEEN 200 AND 300))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('NV'      , 'AZ'      , 'MN'))
         AND (ws_net_profit BETWEEN 250 AND 350))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('FL'      , 'MI'      , 'NY'))
         AND (ws_net_profit BETWEEN 100 AND 300)))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 21) ASC, avg(ws_quantity) ASC, avg(wr_refunded_cash) ASC, avg(wr_fee) ASC
    LIMIT 100"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q85 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN SELECT
  substr(r_reason_desc, 1, 20)
, avg(ws_quantity)
, avg(wr_refunded_cash)
, avg(wr_fee)
FROM
  web_sales
, web_returns
, web_page
, customer_demographics cd1
, customer_demographics cd2
, customer_address
, date_dim
, reason
WHERE (ws_web_page_sk = wp_web_page_sk)
   AND (ws_item_sk = wr_item_sk)
   AND (ws_order_number = wr_order_number)
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_year = 2000)
   AND (cd1.cd_demo_sk = wr_refunded_cdemo_sk)
   AND (cd2.cd_demo_sk = wr_returning_cdemo_sk)
   AND (ca_address_sk = wr_refunded_addr_sk)
   AND (r_reason_sk = wr_reason_sk)
   AND (((cd1.cd_marital_status = 'M')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'Advanced Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'S')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'College')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'W')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = '2 yr Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))))
   AND (((ca_country = 'United States')
         AND (ca_state IN ('IN'      , 'OH'      , 'NJ'))
         AND (ws_net_profit BETWEEN 100 AND 200))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('WI'      , 'CT'      , 'KY'))
         AND (ws_net_profit BETWEEN 150 AND 300))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('LA'      , 'IA'      , 'AR'))
         AND (ws_net_profit BETWEEN 50 AND 250)))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 20) ASC, avg(ws_quantity) ASC, avg(wr_refunded_cash) ASC, avg(wr_fee) ASC
    LIMIT 100"""
    def explainSimilar = sql """EXPLAIN SELECT
  substr(r_reason_desc, 1, 21)
, avg(ws_quantity)
, avg(wr_refunded_cash)
, avg(wr_fee)
FROM
  web_sales
, web_returns
, web_page
, customer_demographics cd1
, customer_demographics cd2
, customer_address
, date_dim
, reason
WHERE (ws_web_page_sk = wp_web_page_sk)
   AND (ws_item_sk = wr_item_sk)
   AND (ws_order_number = wr_order_number)
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_year = 2001)
   AND (cd1.cd_demo_sk = wr_refunded_cdemo_sk)
   AND (cd2.cd_demo_sk = wr_returning_cdemo_sk)
   AND (ca_address_sk = wr_refunded_addr_sk)
   AND (r_reason_sk = wr_reason_sk)
   AND (((cd1.cd_marital_status = 'U')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'Secondary')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('110.00' AS DECIMAL(5,2)) AND CAST('160.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'D')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = '4 yr Degree')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('60.00' AS DECIMAL(5,2)) AND CAST('110.00' AS DECIMAL(5,2))))
      OR ((cd1.cd_marital_status = 'M')
         AND (cd1.cd_marital_status = cd2.cd_marital_status)
         AND (cd1.cd_education_status = 'College')
         AND (cd1.cd_education_status = cd2.cd_education_status)
         AND (ws_sales_price BETWEEN CAST('160.00' AS DECIMAL(5,2)) AND CAST('210.00' AS DECIMAL(5,2)))))
   AND (((ca_country = 'United States')
         AND (ca_state IN ('CA'      , 'WA'      , 'GA'))
         AND (ws_net_profit BETWEEN 200 AND 300))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('NV'      , 'AZ'      , 'MN'))
         AND (ws_net_profit BETWEEN 250 AND 350))
      OR ((ca_country = 'United States')
         AND (ca_state IN ('FL'      , 'MI'      , 'NY'))
         AND (ws_net_profit BETWEEN 100 AND 300)))
GROUP BY r_reason_desc
ORDER BY substr(r_reason_desc, 1, 21) ASC, avg(ws_quantity) ASC, avg(wr_refunded_cash) ASC, avg(wr_fee) ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q85 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q85 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
