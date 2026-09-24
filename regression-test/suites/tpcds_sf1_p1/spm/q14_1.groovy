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

suite("spm_tpcds_sf1_q14_1", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q14_1 query
    // (SQL unchanged from sql/q14_1.sql) against the REAL TPCDS sf1 data loaded
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
  cross_items AS (
   SELECT i_item_sk ss_item_sk
   FROM
     item
   , (
      SELECT
        iss.i_brand_id brand_id
      , iss.i_class_id class_id
      , iss.i_category_id category_id
      FROM
        store_sales
      , item iss
      , date_dim d1
      WHERE (ss_item_sk = iss.i_item_sk)
         AND (ss_sold_date_sk = d1.d_date_sk)
         AND (d1.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        ics.i_brand_id
      , ics.i_class_id
      , ics.i_category_id
      FROM
        catalog_sales
      , item ics
      , date_dim d2
      WHERE (cs_item_sk = ics.i_item_sk)
         AND (cs_sold_date_sk = d2.d_date_sk)
         AND (d2.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        iws.i_brand_id
      , iws.i_class_id
      , iws.i_category_id
      FROM
        web_sales
      , item iws
      , date_dim d3
      WHERE (ws_item_sk = iws.i_item_sk)
         AND (ws_sold_date_sk = d3.d_date_sk)
         AND (d3.d_year BETWEEN 1999 AND (1999 + 2))
   ) y 
   WHERE (i_brand_id = brand_id)
      AND (i_class_id = class_id)
      AND (i_category_id = category_id)
)
, avg_sales AS (
   SELECT avg((quantity * list_price)) average_sales
   FROM
     (
      SELECT
        ss_quantity quantity
      , ss_list_price list_price
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        cs_quantity quantity
      , cs_list_price list_price
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        ws_quantity quantity
      , ws_list_price list_price
      FROM
        web_sales
      , date_dim
      WHERE (ws_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
   )  x
)
SELECT
  channel
, i_brand_id
, i_class_id
, i_category_id
, sum(sales)
, sum(number_sales)
FROM
  (
   SELECT
     'store' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ss_quantity * ss_list_price)) sales
   , count(*) number_sales
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'catalog' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((cs_quantity * cs_list_price)) sales
   , count(*) number_sales
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((cs_quantity * cs_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'web' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ws_quantity * ws_list_price)) sales
   , count(*) number_sales
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ws_quantity * ws_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel ASC, i_brand_id ASC, i_class_id ASC, i_category_id ASC
LIMIT 100"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("store_sales"),
                "q14_1 plan_sql should reference store_sales: ${own[0][4]}")
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
  cross_items AS (
   SELECT i_item_sk ss_item_sk
   FROM
     item
   , (
      SELECT
        iss.i_brand_id brand_id
      , iss.i_class_id class_id
      , iss.i_category_id category_id
      FROM
        store_sales
      , item iss
      , date_dim d1
      WHERE (ss_item_sk = iss.i_item_sk)
         AND (ss_sold_date_sk = d1.d_date_sk)
         AND (d1.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        ics.i_brand_id
      , ics.i_class_id
      , ics.i_category_id
      FROM
        catalog_sales
      , item ics
      , date_dim d2
      WHERE (cs_item_sk = ics.i_item_sk)
         AND (cs_sold_date_sk = d2.d_date_sk)
         AND (d2.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        iws.i_brand_id
      , iws.i_class_id
      , iws.i_category_id
      FROM
        web_sales
      , item iws
      , date_dim d3
      WHERE (ws_item_sk = iws.i_item_sk)
         AND (ws_sold_date_sk = d3.d_date_sk)
         AND (d3.d_year BETWEEN 1999 AND (1999 + 2))
   ) y 
   WHERE (i_brand_id = brand_id)
      AND (i_class_id = class_id)
      AND (i_category_id = category_id)
)
, avg_sales AS (
   SELECT avg((quantity * list_price)) average_sales
   FROM
     (
      SELECT
        ss_quantity quantity
      , ss_list_price list_price
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        cs_quantity quantity
      , cs_list_price list_price
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        ws_quantity quantity
      , ws_list_price list_price
      FROM
        web_sales
      , date_dim
      WHERE (ws_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
   )  x
)
SELECT
  channel
, i_brand_id
, i_class_id
, i_category_id
, sum(sales)
, sum(number_sales)
FROM
  (
   SELECT
     'store' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ss_quantity * ss_list_price)) sales
   , count(*) number_sales
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'catalog' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((cs_quantity * cs_list_price)) sales
   , count(*) number_sales
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((cs_quantity * cs_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'web' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ws_quantity * ws_list_price)) sales
   , count(*) number_sales
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ws_quantity * ws_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel ASC, i_brand_id ASC, i_class_id ASC, i_category_id ASC
LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        def origWithoutSpm = sql """WITH
  cross_items AS (
   SELECT i_item_sk ss_item_sk
   FROM
     item
   , (
      SELECT
        iss.i_brand_id brand_id
      , iss.i_class_id class_id
      , iss.i_category_id category_id
      FROM
        store_sales
      , item iss
      , date_dim d1
      WHERE (ss_item_sk = iss.i_item_sk)
         AND (ss_sold_date_sk = d1.d_date_sk)
         AND (d1.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        ics.i_brand_id
      , ics.i_class_id
      , ics.i_category_id
      FROM
        catalog_sales
      , item ics
      , date_dim d2
      WHERE (cs_item_sk = ics.i_item_sk)
         AND (cs_sold_date_sk = d2.d_date_sk)
         AND (d2.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        iws.i_brand_id
      , iws.i_class_id
      , iws.i_category_id
      FROM
        web_sales
      , item iws
      , date_dim d3
      WHERE (ws_item_sk = iws.i_item_sk)
         AND (ws_sold_date_sk = d3.d_date_sk)
         AND (d3.d_year BETWEEN 1999 AND (1999 + 2))
   ) y 
   WHERE (i_brand_id = brand_id)
      AND (i_class_id = class_id)
      AND (i_category_id = category_id)
)
, avg_sales AS (
   SELECT avg((quantity * list_price)) average_sales
   FROM
     (
      SELECT
        ss_quantity quantity
      , ss_list_price list_price
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        cs_quantity quantity
      , cs_list_price list_price
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        ws_quantity quantity
      , ws_list_price list_price
      FROM
        web_sales
      , date_dim
      WHERE (ws_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
   )  x
)
SELECT
  channel
, i_brand_id
, i_class_id
, i_category_id
, sum(sales)
, sum(number_sales)
FROM
  (
   SELECT
     'store' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ss_quantity * ss_list_price)) sales
   , count(*) number_sales
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'catalog' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((cs_quantity * cs_list_price)) sales
   , count(*) number_sales
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((cs_quantity * cs_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'web' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ws_quantity * ws_list_price)) sales
   , count(*) number_sales
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ws_quantity * ws_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel ASC, i_brand_id ASC, i_class_id ASC, i_category_id ASC
LIMIT 100"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q14_1 query")

        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
        def similarWithSpm = sql """WITH
  cross_items AS (
   SELECT i_item_sk ss_item_sk
   FROM
     item
   , (
      SELECT
        iss.i_brand_id brand_id
      , iss.i_class_id class_id
      , iss.i_category_id category_id
      FROM
        store_sales
      , item iss
      , date_dim d1
      WHERE (ss_item_sk = iss.i_item_sk)
         AND (ss_sold_date_sk = d1.d_date_sk)
         AND (d1.d_year BETWEEN 1998 AND (1998 + 2))
INTERSECT       SELECT
        ics.i_brand_id
      , ics.i_class_id
      , ics.i_category_id
      FROM
        catalog_sales
      , item ics
      , date_dim d2
      WHERE (cs_item_sk = ics.i_item_sk)
         AND (cs_sold_date_sk = d2.d_date_sk)
         AND (d2.d_year BETWEEN 1998 AND (1998 + 2))
INTERSECT       SELECT
        iws.i_brand_id
      , iws.i_class_id
      , iws.i_category_id
      FROM
        web_sales
      , item iws
      , date_dim d3
      WHERE (ws_item_sk = iws.i_item_sk)
         AND (ws_sold_date_sk = d3.d_date_sk)
         AND (d3.d_year BETWEEN 1998 AND (1998 + 2))
   ) y 
   WHERE (i_brand_id = brand_id)
      AND (i_class_id = class_id)
      AND (i_category_id = category_id)
)
, avg_sales AS (
   SELECT avg((quantity * list_price)) average_sales
   FROM
     (
      SELECT
        ss_quantity quantity
      , ss_list_price list_price
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1998 AND (1998 + 2))
UNION ALL       SELECT
        cs_quantity quantity
      , cs_list_price list_price
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1998 AND (1998 + 2))
UNION ALL       SELECT
        ws_quantity quantity
      , ws_list_price list_price
      FROM
        web_sales
      , date_dim
      WHERE (ws_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1998 AND (1998 + 2))
   )  x
)
SELECT
  channel
, i_brand_id
, i_class_id
, i_category_id
, sum(sales)
, sum(number_sales)
FROM
  (
   SELECT
     'store' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ss_quantity * ss_list_price)) sales
   , count(*) number_sales
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = (1998 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'catalog' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((cs_quantity * cs_list_price)) sales
   , count(*) number_sales
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = (1998 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((cs_quantity * cs_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'web' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ws_quantity * ws_list_price)) sales
   , count(*) number_sales
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = (1998 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ws_quantity * ws_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel ASC, i_brand_id ASC, i_class_id ASC, i_category_id ASC
LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        def similarWithoutSpm = sql """WITH
  cross_items AS (
   SELECT i_item_sk ss_item_sk
   FROM
     item
   , (
      SELECT
        iss.i_brand_id brand_id
      , iss.i_class_id class_id
      , iss.i_category_id category_id
      FROM
        store_sales
      , item iss
      , date_dim d1
      WHERE (ss_item_sk = iss.i_item_sk)
         AND (ss_sold_date_sk = d1.d_date_sk)
         AND (d1.d_year BETWEEN 1998 AND (1998 + 2))
INTERSECT       SELECT
        ics.i_brand_id
      , ics.i_class_id
      , ics.i_category_id
      FROM
        catalog_sales
      , item ics
      , date_dim d2
      WHERE (cs_item_sk = ics.i_item_sk)
         AND (cs_sold_date_sk = d2.d_date_sk)
         AND (d2.d_year BETWEEN 1998 AND (1998 + 2))
INTERSECT       SELECT
        iws.i_brand_id
      , iws.i_class_id
      , iws.i_category_id
      FROM
        web_sales
      , item iws
      , date_dim d3
      WHERE (ws_item_sk = iws.i_item_sk)
         AND (ws_sold_date_sk = d3.d_date_sk)
         AND (d3.d_year BETWEEN 1998 AND (1998 + 2))
   ) y 
   WHERE (i_brand_id = brand_id)
      AND (i_class_id = class_id)
      AND (i_category_id = category_id)
)
, avg_sales AS (
   SELECT avg((quantity * list_price)) average_sales
   FROM
     (
      SELECT
        ss_quantity quantity
      , ss_list_price list_price
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1998 AND (1998 + 2))
UNION ALL       SELECT
        cs_quantity quantity
      , cs_list_price list_price
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1998 AND (1998 + 2))
UNION ALL       SELECT
        ws_quantity quantity
      , ws_list_price list_price
      FROM
        web_sales
      , date_dim
      WHERE (ws_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1998 AND (1998 + 2))
   )  x
)
SELECT
  channel
, i_brand_id
, i_class_id
, i_category_id
, sum(sales)
, sum(number_sales)
FROM
  (
   SELECT
     'store' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ss_quantity * ss_list_price)) sales
   , count(*) number_sales
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = (1998 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'catalog' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((cs_quantity * cs_list_price)) sales
   , count(*) number_sales
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = (1998 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((cs_quantity * cs_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'web' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ws_quantity * ws_list_price)) sales
   , count(*) number_sales
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = (1998 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ws_quantity * ws_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel ASC, i_brand_id ASC, i_class_id ASC, i_category_id ASC
LIMIT 100"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q14_1 query")

        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
        def explainOrig = sql """EXPLAIN WITH
  cross_items AS (
   SELECT i_item_sk ss_item_sk
   FROM
     item
   , (
      SELECT
        iss.i_brand_id brand_id
      , iss.i_class_id class_id
      , iss.i_category_id category_id
      FROM
        store_sales
      , item iss
      , date_dim d1
      WHERE (ss_item_sk = iss.i_item_sk)
         AND (ss_sold_date_sk = d1.d_date_sk)
         AND (d1.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        ics.i_brand_id
      , ics.i_class_id
      , ics.i_category_id
      FROM
        catalog_sales
      , item ics
      , date_dim d2
      WHERE (cs_item_sk = ics.i_item_sk)
         AND (cs_sold_date_sk = d2.d_date_sk)
         AND (d2.d_year BETWEEN 1999 AND (1999 + 2))
INTERSECT       SELECT
        iws.i_brand_id
      , iws.i_class_id
      , iws.i_category_id
      FROM
        web_sales
      , item iws
      , date_dim d3
      WHERE (ws_item_sk = iws.i_item_sk)
         AND (ws_sold_date_sk = d3.d_date_sk)
         AND (d3.d_year BETWEEN 1999 AND (1999 + 2))
   ) y 
   WHERE (i_brand_id = brand_id)
      AND (i_class_id = class_id)
      AND (i_category_id = category_id)
)
, avg_sales AS (
   SELECT avg((quantity * list_price)) average_sales
   FROM
     (
      SELECT
        ss_quantity quantity
      , ss_list_price list_price
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        cs_quantity quantity
      , cs_list_price list_price
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
UNION ALL       SELECT
        ws_quantity quantity
      , ws_list_price list_price
      FROM
        web_sales
      , date_dim
      WHERE (ws_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1999 AND (1999 + 2))
   )  x
)
SELECT
  channel
, i_brand_id
, i_class_id
, i_category_id
, sum(sales)
, sum(number_sales)
FROM
  (
   SELECT
     'store' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ss_quantity * ss_list_price)) sales
   , count(*) number_sales
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'catalog' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((cs_quantity * cs_list_price)) sales
   , count(*) number_sales
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((cs_quantity * cs_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'web' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ws_quantity * ws_list_price)) sales
   , count(*) number_sales
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = (1999 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ws_quantity * ws_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel ASC, i_brand_id ASC, i_class_id ASC, i_category_id ASC
LIMIT 100"""
        def explainSimilar = sql """EXPLAIN WITH
  cross_items AS (
   SELECT i_item_sk ss_item_sk
   FROM
     item
   , (
      SELECT
        iss.i_brand_id brand_id
      , iss.i_class_id class_id
      , iss.i_category_id category_id
      FROM
        store_sales
      , item iss
      , date_dim d1
      WHERE (ss_item_sk = iss.i_item_sk)
         AND (ss_sold_date_sk = d1.d_date_sk)
         AND (d1.d_year BETWEEN 1998 AND (1998 + 2))
INTERSECT       SELECT
        ics.i_brand_id
      , ics.i_class_id
      , ics.i_category_id
      FROM
        catalog_sales
      , item ics
      , date_dim d2
      WHERE (cs_item_sk = ics.i_item_sk)
         AND (cs_sold_date_sk = d2.d_date_sk)
         AND (d2.d_year BETWEEN 1998 AND (1998 + 2))
INTERSECT       SELECT
        iws.i_brand_id
      , iws.i_class_id
      , iws.i_category_id
      FROM
        web_sales
      , item iws
      , date_dim d3
      WHERE (ws_item_sk = iws.i_item_sk)
         AND (ws_sold_date_sk = d3.d_date_sk)
         AND (d3.d_year BETWEEN 1998 AND (1998 + 2))
   ) y 
   WHERE (i_brand_id = brand_id)
      AND (i_class_id = class_id)
      AND (i_category_id = category_id)
)
, avg_sales AS (
   SELECT avg((quantity * list_price)) average_sales
   FROM
     (
      SELECT
        ss_quantity quantity
      , ss_list_price list_price
      FROM
        store_sales
      , date_dim
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1998 AND (1998 + 2))
UNION ALL       SELECT
        cs_quantity quantity
      , cs_list_price list_price
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1998 AND (1998 + 2))
UNION ALL       SELECT
        ws_quantity quantity
      , ws_list_price list_price
      FROM
        web_sales
      , date_dim
      WHERE (ws_sold_date_sk = d_date_sk)
         AND (d_year BETWEEN 1998 AND (1998 + 2))
   )  x
)
SELECT
  channel
, i_brand_id
, i_class_id
, i_category_id
, sum(sales)
, sum(number_sales)
FROM
  (
   SELECT
     'store' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ss_quantity * ss_list_price)) sales
   , count(*) number_sales
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = (1998 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'catalog' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((cs_quantity * cs_list_price)) sales
   , count(*) number_sales
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = (1998 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((cs_quantity * cs_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
UNION ALL    SELECT
     'web' channel
   , i_brand_id
   , i_class_id
   , i_category_id
   , sum((ws_quantity * ws_list_price)) sales
   , count(*) number_sales
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk IN (
      SELECT ss_item_sk
      FROM
        cross_items
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = (1998 + 2))
      AND (d_moy = 11)
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ws_quantity * ws_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel ASC, i_brand_id ASC, i_class_id ASC, i_category_id ASC
LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q14_1 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q14_1 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
