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

suite("spm_tpcds_sf1_q49", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q49 query
    // (SQL unchanged from sql/q49.sql) against the REAL TPCDS sf1 data loaded
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
    def bindSql = """(SELECT
  'web' channel
, web.item
, web.return_ratio
, web.return_rank
, web.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        ws.ws_item_sk item
      , (CAST(sum(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        web_sales ws
      LEFT JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number)
         AND (ws.ws_item_sk = wr.wr_item_sk)
      , date_dim
      WHERE (wr.wr_return_amt > 10000)
         AND (ws.ws_net_profit > 1)
         AND (ws.ws_net_paid > 0)
         AND (ws.ws_quantity > 0)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY ws.ws_item_sk
   )  in_web
)  web
WHERE (web.return_rank <= 10)
   OR (web.currency_rank <= 10))
UNION (SELECT
  'catalog' channel
, catalog.item
, catalog.return_ratio
, catalog.return_rank
, catalog.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        cs.cs_item_sk item
      , (CAST(sum(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        catalog_sales cs
      LEFT JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number)
         AND (cs.cs_item_sk = cr.cr_item_sk)
      , date_dim
      WHERE (cr.cr_return_amount > 10000)
         AND (cs.cs_net_profit > 1)
         AND (cs.cs_net_paid > 0)
         AND (cs.cs_quantity > 0)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY cs.cs_item_sk
   )  in_cat
) catalog 
WHERE (catalog.return_rank <= 10)
   OR (catalog.currency_rank <= 10))
UNION (SELECT
  'store' channel
, store.item
, store.return_ratio
, store.return_rank
, store.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        sts.ss_item_sk item
      , (CAST(sum(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        store_sales sts
      LEFT JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number)
         AND (sts.ss_item_sk = sr.sr_item_sk)
      , date_dim
      WHERE (sr.sr_return_amt > 10000)
         AND (sts.ss_net_profit > 1)
         AND (sts.ss_net_paid > 0)
         AND (sts.ss_quantity > 0)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY sts.ss_item_sk
   )  in_store
)  store
WHERE (store.return_rank <= 10)
   OR (store.currency_rank <= 10))
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
LIMIT 100"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("web_sales"),
                "q49 plan_sql should reference web_sales: ${own[0][4]}")
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
    def origWithSpm = sql """(SELECT
  'web' channel
, web.item
, web.return_ratio
, web.return_rank
, web.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        ws.ws_item_sk item
      , (CAST(sum(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        web_sales ws
      LEFT JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number)
         AND (ws.ws_item_sk = wr.wr_item_sk)
      , date_dim
      WHERE (wr.wr_return_amt > 10000)
         AND (ws.ws_net_profit > 1)
         AND (ws.ws_net_paid > 0)
         AND (ws.ws_quantity > 0)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY ws.ws_item_sk
   )  in_web
)  web
WHERE (web.return_rank <= 10)
   OR (web.currency_rank <= 10))
UNION (SELECT
  'catalog' channel
, catalog.item
, catalog.return_ratio
, catalog.return_rank
, catalog.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        cs.cs_item_sk item
      , (CAST(sum(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        catalog_sales cs
      LEFT JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number)
         AND (cs.cs_item_sk = cr.cr_item_sk)
      , date_dim
      WHERE (cr.cr_return_amount > 10000)
         AND (cs.cs_net_profit > 1)
         AND (cs.cs_net_paid > 0)
         AND (cs.cs_quantity > 0)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY cs.cs_item_sk
   )  in_cat
) catalog 
WHERE (catalog.return_rank <= 10)
   OR (catalog.currency_rank <= 10))
UNION (SELECT
  'store' channel
, store.item
, store.return_ratio
, store.return_rank
, store.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        sts.ss_item_sk item
      , (CAST(sum(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        store_sales sts
      LEFT JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number)
         AND (sts.ss_item_sk = sr.sr_item_sk)
      , date_dim
      WHERE (sr.sr_return_amt > 10000)
         AND (sts.ss_net_profit > 1)
         AND (sts.ss_net_paid > 0)
         AND (sts.ss_quantity > 0)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY sts.ss_item_sk
   )  in_store
)  store
WHERE (store.return_rank <= 10)
   OR (store.currency_rank <= 10))
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """(SELECT
  'web' channel
, web.item
, web.return_ratio
, web.return_rank
, web.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        ws.ws_item_sk item
      , (CAST(sum(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        web_sales ws
      LEFT JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number)
         AND (ws.ws_item_sk = wr.wr_item_sk)
      , date_dim
      WHERE (wr.wr_return_amt > 10000)
         AND (ws.ws_net_profit > 1)
         AND (ws.ws_net_paid > 0)
         AND (ws.ws_quantity > 0)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY ws.ws_item_sk
   )  in_web
)  web
WHERE (web.return_rank <= 10)
   OR (web.currency_rank <= 10))
UNION (SELECT
  'catalog' channel
, catalog.item
, catalog.return_ratio
, catalog.return_rank
, catalog.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        cs.cs_item_sk item
      , (CAST(sum(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        catalog_sales cs
      LEFT JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number)
         AND (cs.cs_item_sk = cr.cr_item_sk)
      , date_dim
      WHERE (cr.cr_return_amount > 10000)
         AND (cs.cs_net_profit > 1)
         AND (cs.cs_net_paid > 0)
         AND (cs.cs_quantity > 0)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY cs.cs_item_sk
   )  in_cat
) catalog 
WHERE (catalog.return_rank <= 10)
   OR (catalog.currency_rank <= 10))
UNION (SELECT
  'store' channel
, store.item
, store.return_ratio
, store.return_rank
, store.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        sts.ss_item_sk item
      , (CAST(sum(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        store_sales sts
      LEFT JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number)
         AND (sts.ss_item_sk = sr.sr_item_sk)
      , date_dim
      WHERE (sr.sr_return_amt > 10000)
         AND (sts.ss_net_profit > 1)
         AND (sts.ss_net_paid > 0)
         AND (sts.ss_quantity > 0)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY sts.ss_item_sk
   )  in_store
)  store
WHERE (store.return_rank <= 10)
   OR (store.currency_rank <= 10))
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
    LIMIT 100"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q49 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """(SELECT
  'web' channel
, web.item
, web.return_ratio
, web.return_rank
, web.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        ws.ws_item_sk item
      , (CAST(sum(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        web_sales ws
      LEFT JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number)
         AND (ws.ws_item_sk = wr.wr_item_sk)
      , date_dim
      WHERE (wr.wr_return_amt > 10000)
         AND (ws.ws_net_profit > 1)
         AND (ws.ws_net_paid > 0)
         AND (ws.ws_quantity > 0)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2000)
         AND (d_moy = 11)
      GROUP BY ws.ws_item_sk
   )  in_web
)  web
WHERE (web.return_rank <= 8)
   OR (web.currency_rank <= 8))
UNION (SELECT
  'catalog' channel
, catalog.item
, catalog.return_ratio
, catalog.return_rank
, catalog.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        cs.cs_item_sk item
      , (CAST(sum(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        catalog_sales cs
      LEFT JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number)
         AND (cs.cs_item_sk = cr.cr_item_sk)
      , date_dim
      WHERE (cr.cr_return_amount > 10000)
         AND (cs.cs_net_profit > 1)
         AND (cs.cs_net_paid > 0)
         AND (cs.cs_quantity > 0)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2000)
         AND (d_moy = 11)
      GROUP BY cs.cs_item_sk
   )  in_cat
) catalog 
WHERE (catalog.return_rank <= 8)
   OR (catalog.currency_rank <= 8))
UNION (SELECT
  'store' channel
, store.item
, store.return_ratio
, store.return_rank
, store.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        sts.ss_item_sk item
      , (CAST(sum(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        store_sales sts
      LEFT JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number)
         AND (sts.ss_item_sk = sr.sr_item_sk)
      , date_dim
      WHERE (sr.sr_return_amt > 10000)
         AND (sts.ss_net_profit > 1)
         AND (sts.ss_net_paid > 0)
         AND (sts.ss_quantity > 0)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year = 2000)
         AND (d_moy = 11)
      GROUP BY sts.ss_item_sk
   )  in_store
)  store
WHERE (store.return_rank <= 8)
   OR (store.currency_rank <= 8))
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """(SELECT
  'web' channel
, web.item
, web.return_ratio
, web.return_rank
, web.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        ws.ws_item_sk item
      , (CAST(sum(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        web_sales ws
      LEFT JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number)
         AND (ws.ws_item_sk = wr.wr_item_sk)
      , date_dim
      WHERE (wr.wr_return_amt > 10000)
         AND (ws.ws_net_profit > 1)
         AND (ws.ws_net_paid > 0)
         AND (ws.ws_quantity > 0)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2000)
         AND (d_moy = 11)
      GROUP BY ws.ws_item_sk
   )  in_web
)  web
WHERE (web.return_rank <= 8)
   OR (web.currency_rank <= 8))
UNION (SELECT
  'catalog' channel
, catalog.item
, catalog.return_ratio
, catalog.return_rank
, catalog.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        cs.cs_item_sk item
      , (CAST(sum(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        catalog_sales cs
      LEFT JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number)
         AND (cs.cs_item_sk = cr.cr_item_sk)
      , date_dim
      WHERE (cr.cr_return_amount > 10000)
         AND (cs.cs_net_profit > 1)
         AND (cs.cs_net_paid > 0)
         AND (cs.cs_quantity > 0)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2000)
         AND (d_moy = 11)
      GROUP BY cs.cs_item_sk
   )  in_cat
) catalog 
WHERE (catalog.return_rank <= 8)
   OR (catalog.currency_rank <= 8))
UNION (SELECT
  'store' channel
, store.item
, store.return_ratio
, store.return_rank
, store.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        sts.ss_item_sk item
      , (CAST(sum(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        store_sales sts
      LEFT JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number)
         AND (sts.ss_item_sk = sr.sr_item_sk)
      , date_dim
      WHERE (sr.sr_return_amt > 10000)
         AND (sts.ss_net_profit > 1)
         AND (sts.ss_net_paid > 0)
         AND (sts.ss_quantity > 0)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year = 2000)
         AND (d_moy = 11)
      GROUP BY sts.ss_item_sk
   )  in_store
)  store
WHERE (store.return_rank <= 8)
   OR (store.currency_rank <= 8))
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
    LIMIT 100"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q49 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN (SELECT
  'web' channel
, web.item
, web.return_ratio
, web.return_rank
, web.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        ws.ws_item_sk item
      , (CAST(sum(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        web_sales ws
      LEFT JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number)
         AND (ws.ws_item_sk = wr.wr_item_sk)
      , date_dim
      WHERE (wr.wr_return_amt > 10000)
         AND (ws.ws_net_profit > 1)
         AND (ws.ws_net_paid > 0)
         AND (ws.ws_quantity > 0)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY ws.ws_item_sk
   )  in_web
)  web
WHERE (web.return_rank <= 10)
   OR (web.currency_rank <= 10))
UNION (SELECT
  'catalog' channel
, catalog.item
, catalog.return_ratio
, catalog.return_rank
, catalog.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        cs.cs_item_sk item
      , (CAST(sum(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        catalog_sales cs
      LEFT JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number)
         AND (cs.cs_item_sk = cr.cr_item_sk)
      , date_dim
      WHERE (cr.cr_return_amount > 10000)
         AND (cs.cs_net_profit > 1)
         AND (cs.cs_net_paid > 0)
         AND (cs.cs_quantity > 0)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY cs.cs_item_sk
   )  in_cat
) catalog 
WHERE (catalog.return_rank <= 10)
   OR (catalog.currency_rank <= 10))
UNION (SELECT
  'store' channel
, store.item
, store.return_ratio
, store.return_rank
, store.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        sts.ss_item_sk item
      , (CAST(sum(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        store_sales sts
      LEFT JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number)
         AND (sts.ss_item_sk = sr.sr_item_sk)
      , date_dim
      WHERE (sr.sr_return_amt > 10000)
         AND (sts.ss_net_profit > 1)
         AND (sts.ss_net_paid > 0)
         AND (sts.ss_quantity > 0)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year = 2001)
         AND (d_moy = 12)
      GROUP BY sts.ss_item_sk
   )  in_store
)  store
WHERE (store.return_rank <= 10)
   OR (store.currency_rank <= 10))
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
    LIMIT 100"""
    def explainSimilar = sql """EXPLAIN (SELECT
  'web' channel
, web.item
, web.return_ratio
, web.return_rank
, web.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        ws.ws_item_sk item
      , (CAST(sum(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        web_sales ws
      LEFT JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number)
         AND (ws.ws_item_sk = wr.wr_item_sk)
      , date_dim
      WHERE (wr.wr_return_amt > 10000)
         AND (ws.ws_net_profit > 1)
         AND (ws.ws_net_paid > 0)
         AND (ws.ws_quantity > 0)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2000)
         AND (d_moy = 11)
      GROUP BY ws.ws_item_sk
   )  in_web
)  web
WHERE (web.return_rank <= 8)
   OR (web.currency_rank <= 8))
UNION (SELECT
  'catalog' channel
, catalog.item
, catalog.return_ratio
, catalog.return_rank
, catalog.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        cs.cs_item_sk item
      , (CAST(sum(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        catalog_sales cs
      LEFT JOIN catalog_returns cr ON (cs.cs_order_number = cr.cr_order_number)
         AND (cs.cs_item_sk = cr.cr_item_sk)
      , date_dim
      WHERE (cr.cr_return_amount > 10000)
         AND (cs.cs_net_profit > 1)
         AND (cs.cs_net_paid > 0)
         AND (cs.cs_quantity > 0)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2000)
         AND (d_moy = 11)
      GROUP BY cs.cs_item_sk
   )  in_cat
) catalog 
WHERE (catalog.return_rank <= 8)
   OR (catalog.currency_rank <= 8))
UNION (SELECT
  'store' channel
, store.item
, store.return_ratio
, store.return_rank
, store.currency_rank
FROM
  (
   SELECT
     item
   , return_ratio
   , currency_ratio
   , rank() OVER (ORDER BY return_ratio ASC) return_rank
   , rank() OVER (ORDER BY currency_ratio ASC) currency_rank
   FROM
     (
      SELECT
        sts.ss_item_sk item
      , (CAST(sum(COALESCE(sr.sr_return_quantity, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_quantity, 0)) AS DECIMAL(15,4))) return_ratio
      , (CAST(sum(COALESCE(sr.sr_return_amt, 0)) AS DECIMAL(15,4)) / CAST(sum(COALESCE(sts.ss_net_paid, 0)) AS DECIMAL(15,4))) currency_ratio
      FROM
        store_sales sts
      LEFT JOIN store_returns sr ON (sts.ss_ticket_number = sr.sr_ticket_number)
         AND (sts.ss_item_sk = sr.sr_item_sk)
      , date_dim
      WHERE (sr.sr_return_amt > 10000)
         AND (sts.ss_net_profit > 1)
         AND (sts.ss_net_paid > 0)
         AND (sts.ss_quantity > 0)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year = 2000)
         AND (d_moy = 11)
      GROUP BY sts.ss_item_sk
   )  in_store
)  store
WHERE (store.return_rank <= 8)
   OR (store.currency_rank <= 8))
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q49 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q49 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
