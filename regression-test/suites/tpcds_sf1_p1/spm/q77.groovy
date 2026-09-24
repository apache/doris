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

suite("spm_tpcds_sf1_q77", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q77 query
    // (SQL unchanged from sql/q77.sql) against the REAL TPCDS sf1 data loaded
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
     s_store_sk
   , sum(ss_ext_sales_price) sales
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ss_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, sr AS (
   SELECT
     s_store_sk
   , sum(sr_return_amt) returns
   , sum(sr_net_loss) profit_loss
   FROM
     store_returns
   , date_dim
   , store
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (sr_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, cs AS (
   SELECT
     cs_call_center_sk
   , sum(cs_ext_sales_price) sales
   , sum(cs_net_profit) profit
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cs_call_center_sk
)
, cr AS (
   SELECT
     cr_call_center_sk
   , sum(cr_return_amount) returns
   , sum(cr_net_loss) profit_loss
   FROM
     catalog_returns
   , date_dim
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cr_call_center_sk
)
, ws AS (
   SELECT
     wp_web_page_sk
   , sum(ws_ext_sales_price) sales
   , sum(ws_net_profit) profit
   FROM
     web_sales
   , date_dim
   , web_page
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ws_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
, wr AS (
   SELECT
     wp_web_page_sk
   , sum(wr_return_amt) returns
   , sum(wr_net_loss) profit_loss
   FROM
     web_returns
   , date_dim
   , web_page
   WHERE (wr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (wr_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
SELECT
  channel
, id
, sum(sales) sales
, sum(returns) returns
, sum(profit) profit
FROM
  (
   SELECT
     'store channel' channel
   , ss.s_store_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ss
   LEFT JOIN sr ON (ss.s_store_sk = sr.s_store_sk)
UNION ALL    SELECT
     'catalog channel' channel
   , cs_call_center_sk id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     cs
   , cr
UNION ALL    SELECT
     'web channel' channel
   , ws.wp_web_page_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ws
   LEFT JOIN wr ON (ws.wp_web_page_sk = wr.wp_web_page_sk)
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC, sales ASC
LIMIT 100"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("store_sales"),
                "q77 plan_sql should reference store_sales: ${own[0][4]}")
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
     s_store_sk
   , sum(ss_ext_sales_price) sales
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ss_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, sr AS (
   SELECT
     s_store_sk
   , sum(sr_return_amt) returns
   , sum(sr_net_loss) profit_loss
   FROM
     store_returns
   , date_dim
   , store
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (sr_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, cs AS (
   SELECT
     cs_call_center_sk
   , sum(cs_ext_sales_price) sales
   , sum(cs_net_profit) profit
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cs_call_center_sk
)
, cr AS (
   SELECT
     cr_call_center_sk
   , sum(cr_return_amount) returns
   , sum(cr_net_loss) profit_loss
   FROM
     catalog_returns
   , date_dim
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cr_call_center_sk
)
, ws AS (
   SELECT
     wp_web_page_sk
   , sum(ws_ext_sales_price) sales
   , sum(ws_net_profit) profit
   FROM
     web_sales
   , date_dim
   , web_page
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ws_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
, wr AS (
   SELECT
     wp_web_page_sk
   , sum(wr_return_amt) returns
   , sum(wr_net_loss) profit_loss
   FROM
     web_returns
   , date_dim
   , web_page
   WHERE (wr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (wr_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
SELECT
  channel
, id
, sum(sales) sales
, sum(returns) returns
, sum(profit) profit
FROM
  (
   SELECT
     'store channel' channel
   , ss.s_store_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ss
   LEFT JOIN sr ON (ss.s_store_sk = sr.s_store_sk)
UNION ALL    SELECT
     'catalog channel' channel
   , cs_call_center_sk id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     cs
   , cr
UNION ALL    SELECT
     'web channel' channel
   , ws.wp_web_page_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ws
   LEFT JOIN wr ON (ws.wp_web_page_sk = wr.wp_web_page_sk)
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC, sales ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """WITH
  ss AS (
   SELECT
     s_store_sk
   , sum(ss_ext_sales_price) sales
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ss_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, sr AS (
   SELECT
     s_store_sk
   , sum(sr_return_amt) returns
   , sum(sr_net_loss) profit_loss
   FROM
     store_returns
   , date_dim
   , store
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (sr_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, cs AS (
   SELECT
     cs_call_center_sk
   , sum(cs_ext_sales_price) sales
   , sum(cs_net_profit) profit
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cs_call_center_sk
)
, cr AS (
   SELECT
     cr_call_center_sk
   , sum(cr_return_amount) returns
   , sum(cr_net_loss) profit_loss
   FROM
     catalog_returns
   , date_dim
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cr_call_center_sk
)
, ws AS (
   SELECT
     wp_web_page_sk
   , sum(ws_ext_sales_price) sales
   , sum(ws_net_profit) profit
   FROM
     web_sales
   , date_dim
   , web_page
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ws_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
, wr AS (
   SELECT
     wp_web_page_sk
   , sum(wr_return_amt) returns
   , sum(wr_net_loss) profit_loss
   FROM
     web_returns
   , date_dim
   , web_page
   WHERE (wr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (wr_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
SELECT
  channel
, id
, sum(sales) sales
, sum(returns) returns
, sum(profit) profit
FROM
  (
   SELECT
     'store channel' channel
   , ss.s_store_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ss
   LEFT JOIN sr ON (ss.s_store_sk = sr.s_store_sk)
UNION ALL    SELECT
     'catalog channel' channel
   , cs_call_center_sk id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     cs
   , cr
UNION ALL    SELECT
     'web channel' channel
   , ws.wp_web_page_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ws
   LEFT JOIN wr ON (ws.wp_web_page_sk = wr.wp_web_page_sk)
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC, sales ASC
    LIMIT 100"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q77 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """WITH
  ss AS (
   SELECT
     s_store_sk
   , sum(ss_ext_sales_price) sales
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ss_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, sr AS (
   SELECT
     s_store_sk
   , sum(sr_return_amt) returns
   , sum(sr_net_loss) profit_loss
   FROM
     store_returns
   , date_dim
   , store
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (sr_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, cs AS (
   SELECT
     cs_call_center_sk
   , sum(cs_ext_sales_price) sales
   , sum(cs_net_profit) profit
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cs_call_center_sk
)
, cr AS (
   SELECT
     cr_call_center_sk
   , sum(cr_return_amount) returns
   , sum(cr_net_loss) profit_loss
   FROM
     catalog_returns
   , date_dim
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cr_call_center_sk
)
, ws AS (
   SELECT
     wp_web_page_sk
   , sum(ws_ext_sales_price) sales
   , sum(ws_net_profit) profit
   FROM
     web_sales
   , date_dim
   , web_page
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ws_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
, wr AS (
   SELECT
     wp_web_page_sk
   , sum(wr_return_amt) returns
   , sum(wr_net_loss) profit_loss
   FROM
     web_returns
   , date_dim
   , web_page
   WHERE (wr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (wr_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
SELECT
  channel
, id
, sum(sales) sales
, sum(returns) returns
, sum(profit) profit
FROM
  (
   SELECT
     'store channel' channel
   , ss.s_store_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ss
   LEFT JOIN sr ON (ss.s_store_sk = sr.s_store_sk)
UNION ALL    SELECT
     'catalog channel' channel
   , cs_call_center_sk id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     cs
   , cr
UNION ALL    SELECT
     'web channel' channel
   , ws.wp_web_page_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ws
   LEFT JOIN wr ON (ws.wp_web_page_sk = wr.wp_web_page_sk)
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC, sales ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """WITH
  ss AS (
   SELECT
     s_store_sk
   , sum(ss_ext_sales_price) sales
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ss_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, sr AS (
   SELECT
     s_store_sk
   , sum(sr_return_amt) returns
   , sum(sr_net_loss) profit_loss
   FROM
     store_returns
   , date_dim
   , store
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (sr_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, cs AS (
   SELECT
     cs_call_center_sk
   , sum(cs_ext_sales_price) sales
   , sum(cs_net_profit) profit
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cs_call_center_sk
)
, cr AS (
   SELECT
     cr_call_center_sk
   , sum(cr_return_amount) returns
   , sum(cr_net_loss) profit_loss
   FROM
     catalog_returns
   , date_dim
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cr_call_center_sk
)
, ws AS (
   SELECT
     wp_web_page_sk
   , sum(ws_ext_sales_price) sales
   , sum(ws_net_profit) profit
   FROM
     web_sales
   , date_dim
   , web_page
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ws_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
, wr AS (
   SELECT
     wp_web_page_sk
   , sum(wr_return_amt) returns
   , sum(wr_net_loss) profit_loss
   FROM
     web_returns
   , date_dim
   , web_page
   WHERE (wr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (wr_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
SELECT
  channel
, id
, sum(sales) sales
, sum(returns) returns
, sum(profit) profit
FROM
  (
   SELECT
     'store channel' channel
   , ss.s_store_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ss
   LEFT JOIN sr ON (ss.s_store_sk = sr.s_store_sk)
UNION ALL    SELECT
     'catalog channel' channel
   , cs_call_center_sk id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     cs
   , cr
UNION ALL    SELECT
     'web channel' channel
   , ws.wp_web_page_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ws
   LEFT JOIN wr ON (ws.wp_web_page_sk = wr.wp_web_page_sk)
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC, sales ASC
    LIMIT 100"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q77 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN WITH
  ss AS (
   SELECT
     s_store_sk
   , sum(ss_ext_sales_price) sales
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ss_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, sr AS (
   SELECT
     s_store_sk
   , sum(sr_return_amt) returns
   , sum(sr_net_loss) profit_loss
   FROM
     store_returns
   , date_dim
   , store
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (sr_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, cs AS (
   SELECT
     cs_call_center_sk
   , sum(cs_ext_sales_price) sales
   , sum(cs_net_profit) profit
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cs_call_center_sk
)
, cr AS (
   SELECT
     cr_call_center_sk
   , sum(cr_return_amount) returns
   , sum(cr_net_loss) profit_loss
   FROM
     catalog_returns
   , date_dim
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cr_call_center_sk
)
, ws AS (
   SELECT
     wp_web_page_sk
   , sum(ws_ext_sales_price) sales
   , sum(ws_net_profit) profit
   FROM
     web_sales
   , date_dim
   , web_page
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ws_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
, wr AS (
   SELECT
     wp_web_page_sk
   , sum(wr_return_amt) returns
   , sum(wr_net_loss) profit_loss
   FROM
     web_returns
   , date_dim
   , web_page
   WHERE (wr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (wr_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
SELECT
  channel
, id
, sum(sales) sales
, sum(returns) returns
, sum(profit) profit
FROM
  (
   SELECT
     'store channel' channel
   , ss.s_store_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ss
   LEFT JOIN sr ON (ss.s_store_sk = sr.s_store_sk)
UNION ALL    SELECT
     'catalog channel' channel
   , cs_call_center_sk id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     cs
   , cr
UNION ALL    SELECT
     'web channel' channel
   , ws.wp_web_page_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ws
   LEFT JOIN wr ON (ws.wp_web_page_sk = wr.wp_web_page_sk)
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC, sales ASC
    LIMIT 100"""
    def explainSimilar = sql """EXPLAIN WITH
  ss AS (
   SELECT
     s_store_sk
   , sum(ss_ext_sales_price) sales
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ss_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, sr AS (
   SELECT
     s_store_sk
   , sum(sr_return_amt) returns
   , sum(sr_net_loss) profit_loss
   FROM
     store_returns
   , date_dim
   , store
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (sr_store_sk = s_store_sk)
   GROUP BY s_store_sk
)
, cs AS (
   SELECT
     cs_call_center_sk
   , sum(cs_ext_sales_price) sales
   , sum(cs_net_profit) profit
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cs_call_center_sk
)
, cr AS (
   SELECT
     cr_call_center_sk
   , sum(cr_return_amount) returns
   , sum(cr_net_loss) profit_loss
   FROM
     catalog_returns
   , date_dim
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY cr_call_center_sk
)
, ws AS (
   SELECT
     wp_web_page_sk
   , sum(ws_ext_sales_price) sales
   , sum(ws_net_profit) profit
   FROM
     web_sales
   , date_dim
   , web_page
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ws_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
, wr AS (
   SELECT
     wp_web_page_sk
   , sum(wr_return_amt) returns
   , sum(wr_net_loss) profit_loss
   FROM
     web_returns
   , date_dim
   , web_page
   WHERE (wr_returned_date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2001-08-23' AS DATE) AND (CAST('2001-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (wr_web_page_sk = wp_web_page_sk)
   GROUP BY wp_web_page_sk
)
SELECT
  channel
, id
, sum(sales) sales
, sum(returns) returns
, sum(profit) profit
FROM
  (
   SELECT
     'store channel' channel
   , ss.s_store_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ss
   LEFT JOIN sr ON (ss.s_store_sk = sr.s_store_sk)
UNION ALL    SELECT
     'catalog channel' channel
   , cs_call_center_sk id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     cs
   , cr
UNION ALL    SELECT
     'web channel' channel
   , ws.wp_web_page_sk id
   , sales
   , COALESCE(returns, 0) returns
   , (profit - COALESCE(profit_loss, 0)) profit
   FROM
     ws
   LEFT JOIN wr ON (ws.wp_web_page_sk = wr.wp_web_page_sk)
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC, sales ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q77 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q77 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
