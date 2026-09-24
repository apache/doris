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

suite("spm_tpcds_sf1_q41", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q41 query
    // (SQL unchanged from sql/q41.sql) against the REAL TPCDS sf1 data loaded
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
    def bindSql = """SELECT DISTINCT i_product_name
FROM
  item i1
WHERE (i_manufact_id BETWEEN 738 AND (738 + 40))
   AND ((
      SELECT count(*) item_cnt
      FROM
        item
      WHERE ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Women')
                  AND ((i_color = 'powder')
                     OR (i_color = 'khaki'))
                  AND ((i_units = 'Ounce')
                     OR (i_units = 'Oz'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Women')
                  AND ((i_color = 'brown')
                     OR (i_color = 'honeydew'))
                  AND ((i_units = 'Bunch')
                     OR (i_units = 'Ton'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'floral')
                     OR (i_color = 'deep'))
                  AND ((i_units = 'N/A')
                     OR (i_units = 'Dozen'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'light')
                     OR (i_color = 'cornflower'))
                  AND ((i_units = 'Box')
                     OR (i_units = 'Pound'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
         OR ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Women')
                  AND ((i_color = 'midnight')
                     OR (i_color = 'snow'))
                  AND ((i_units = 'Pallet')
                     OR (i_units = 'Gross'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Women')
                  AND ((i_color = 'cyan')
                     OR (i_color = 'papaya'))
                  AND ((i_units = 'Cup')
                     OR (i_units = 'Dram'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'orange')
                     OR (i_color = 'frosted'))
                  AND ((i_units = 'Each')
                     OR (i_units = 'Tbl'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'forest')
                     OR (i_color = 'ghost'))
                  AND ((i_units = 'Lb')
                     OR (i_units = 'Bundle'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
   ) > 0)
ORDER BY i_product_name ASC
LIMIT 100"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("item"),
                "q41 plan_sql should reference item: ${own[0][4]}")
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
    def origWithSpm = sql """SELECT DISTINCT i_product_name
FROM
  item i1
WHERE (i_manufact_id BETWEEN 738 AND (738 + 40))
   AND ((
      SELECT count(*) item_cnt
      FROM
        item
      WHERE ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Women')
                  AND ((i_color = 'powder')
                     OR (i_color = 'khaki'))
                  AND ((i_units = 'Ounce')
                     OR (i_units = 'Oz'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Women')
                  AND ((i_color = 'brown')
                     OR (i_color = 'honeydew'))
                  AND ((i_units = 'Bunch')
                     OR (i_units = 'Ton'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'floral')
                     OR (i_color = 'deep'))
                  AND ((i_units = 'N/A')
                     OR (i_units = 'Dozen'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'light')
                     OR (i_color = 'cornflower'))
                  AND ((i_units = 'Box')
                     OR (i_units = 'Pound'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
         OR ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Women')
                  AND ((i_color = 'midnight')
                     OR (i_color = 'snow'))
                  AND ((i_units = 'Pallet')
                     OR (i_units = 'Gross'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Women')
                  AND ((i_color = 'cyan')
                     OR (i_color = 'papaya'))
                  AND ((i_units = 'Cup')
                     OR (i_units = 'Dram'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'orange')
                     OR (i_color = 'frosted'))
                  AND ((i_units = 'Each')
                     OR (i_units = 'Tbl'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'forest')
                     OR (i_color = 'ghost'))
                  AND ((i_units = 'Lb')
                     OR (i_units = 'Bundle'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
   ) > 0)
ORDER BY i_product_name ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """SELECT DISTINCT i_product_name
FROM
  item i1
WHERE (i_manufact_id BETWEEN 738 AND (738 + 40))
   AND ((
      SELECT count(*) item_cnt
      FROM
        item
      WHERE ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Women')
                  AND ((i_color = 'powder')
                     OR (i_color = 'khaki'))
                  AND ((i_units = 'Ounce')
                     OR (i_units = 'Oz'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Women')
                  AND ((i_color = 'brown')
                     OR (i_color = 'honeydew'))
                  AND ((i_units = 'Bunch')
                     OR (i_units = 'Ton'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'floral')
                     OR (i_color = 'deep'))
                  AND ((i_units = 'N/A')
                     OR (i_units = 'Dozen'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'light')
                     OR (i_color = 'cornflower'))
                  AND ((i_units = 'Box')
                     OR (i_units = 'Pound'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
         OR ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Women')
                  AND ((i_color = 'midnight')
                     OR (i_color = 'snow'))
                  AND ((i_units = 'Pallet')
                     OR (i_units = 'Gross'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Women')
                  AND ((i_color = 'cyan')
                     OR (i_color = 'papaya'))
                  AND ((i_units = 'Cup')
                     OR (i_units = 'Dram'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'orange')
                     OR (i_color = 'frosted'))
                  AND ((i_units = 'Each')
                     OR (i_units = 'Tbl'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'forest')
                     OR (i_color = 'ghost'))
                  AND ((i_units = 'Lb')
                     OR (i_units = 'Bundle'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
   ) > 0)
ORDER BY i_product_name ASC
    LIMIT 100"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q41 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """SELECT DISTINCT i_product_name
FROM
  item i1
WHERE (i_manufact_id BETWEEN 739 AND (739 + 40))
   AND ((
      SELECT count(*) item_cnt
      FROM
        item
      WHERE ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Sports')
                  AND ((i_color = 'powder')
                     OR (i_color = 'khaki'))
                  AND ((i_units = 'Ounce')
                     OR (i_units = 'Oz'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Sports')
                  AND ((i_color = 'brown')
                     OR (i_color = 'honeydew'))
                  AND ((i_units = 'Bunch')
                     OR (i_units = 'Ton'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'floral')
                     OR (i_color = 'deep'))
                  AND ((i_units = 'N/A')
                     OR (i_units = 'Dozen'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'light')
                     OR (i_color = 'cornflower'))
                  AND ((i_units = 'Box')
                     OR (i_units = 'Pound'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
         OR ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Sports')
                  AND ((i_color = 'midnight')
                     OR (i_color = 'snow'))
                  AND ((i_units = 'Pallet')
                     OR (i_units = 'Gross'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Sports')
                  AND ((i_color = 'cyan')
                     OR (i_color = 'papaya'))
                  AND ((i_units = 'Cup')
                     OR (i_units = 'Dram'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'orange')
                     OR (i_color = 'frosted'))
                  AND ((i_units = 'Each')
                     OR (i_units = 'Tbl'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'forest')
                     OR (i_color = 'ghost'))
                  AND ((i_units = 'Lb')
                     OR (i_units = 'Bundle'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
   ) > 0)
ORDER BY i_product_name ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """SELECT DISTINCT i_product_name
FROM
  item i1
WHERE (i_manufact_id BETWEEN 739 AND (739 + 40))
   AND ((
      SELECT count(*) item_cnt
      FROM
        item
      WHERE ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Sports')
                  AND ((i_color = 'powder')
                     OR (i_color = 'khaki'))
                  AND ((i_units = 'Ounce')
                     OR (i_units = 'Oz'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Sports')
                  AND ((i_color = 'brown')
                     OR (i_color = 'honeydew'))
                  AND ((i_units = 'Bunch')
                     OR (i_units = 'Ton'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'floral')
                     OR (i_color = 'deep'))
                  AND ((i_units = 'N/A')
                     OR (i_units = 'Dozen'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'light')
                     OR (i_color = 'cornflower'))
                  AND ((i_units = 'Box')
                     OR (i_units = 'Pound'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
         OR ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Sports')
                  AND ((i_color = 'midnight')
                     OR (i_color = 'snow'))
                  AND ((i_units = 'Pallet')
                     OR (i_units = 'Gross'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Sports')
                  AND ((i_color = 'cyan')
                     OR (i_color = 'papaya'))
                  AND ((i_units = 'Cup')
                     OR (i_units = 'Dram'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'orange')
                     OR (i_color = 'frosted'))
                  AND ((i_units = 'Each')
                     OR (i_units = 'Tbl'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'forest')
                     OR (i_color = 'ghost'))
                  AND ((i_units = 'Lb')
                     OR (i_units = 'Bundle'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
   ) > 0)
ORDER BY i_product_name ASC
    LIMIT 100"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q41 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN SELECT DISTINCT i_product_name
FROM
  item i1
WHERE (i_manufact_id BETWEEN 738 AND (738 + 40))
   AND ((
      SELECT count(*) item_cnt
      FROM
        item
      WHERE ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Women')
                  AND ((i_color = 'powder')
                     OR (i_color = 'khaki'))
                  AND ((i_units = 'Ounce')
                     OR (i_units = 'Oz'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Women')
                  AND ((i_color = 'brown')
                     OR (i_color = 'honeydew'))
                  AND ((i_units = 'Bunch')
                     OR (i_units = 'Ton'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'floral')
                     OR (i_color = 'deep'))
                  AND ((i_units = 'N/A')
                     OR (i_units = 'Dozen'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'light')
                     OR (i_color = 'cornflower'))
                  AND ((i_units = 'Box')
                     OR (i_units = 'Pound'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
         OR ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Women')
                  AND ((i_color = 'midnight')
                     OR (i_color = 'snow'))
                  AND ((i_units = 'Pallet')
                     OR (i_units = 'Gross'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Women')
                  AND ((i_color = 'cyan')
                     OR (i_color = 'papaya'))
                  AND ((i_units = 'Cup')
                     OR (i_units = 'Dram'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'orange')
                     OR (i_color = 'frosted'))
                  AND ((i_units = 'Each')
                     OR (i_units = 'Tbl'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Men')
                  AND ((i_color = 'forest')
                     OR (i_color = 'ghost'))
                  AND ((i_units = 'Lb')
                     OR (i_units = 'Bundle'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
   ) > 0)
ORDER BY i_product_name ASC
    LIMIT 100"""
    def explainSimilar = sql """EXPLAIN SELECT DISTINCT i_product_name
FROM
  item i1
WHERE (i_manufact_id BETWEEN 739 AND (739 + 40))
   AND ((
      SELECT count(*) item_cnt
      FROM
        item
      WHERE ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Sports')
                  AND ((i_color = 'powder')
                     OR (i_color = 'khaki'))
                  AND ((i_units = 'Ounce')
                     OR (i_units = 'Oz'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Sports')
                  AND ((i_color = 'brown')
                     OR (i_color = 'honeydew'))
                  AND ((i_units = 'Bunch')
                     OR (i_units = 'Ton'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'floral')
                     OR (i_color = 'deep'))
                  AND ((i_units = 'N/A')
                     OR (i_units = 'Dozen'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'light')
                     OR (i_color = 'cornflower'))
                  AND ((i_units = 'Box')
                     OR (i_units = 'Pound'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
         OR ((i_manufact = i1.i_manufact)
            AND (((i_category = 'Sports')
                  AND ((i_color = 'midnight')
                     OR (i_color = 'snow'))
                  AND ((i_units = 'Pallet')
                     OR (i_units = 'Gross'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))
               OR ((i_category = 'Sports')
                  AND ((i_color = 'cyan')
                     OR (i_color = 'papaya'))
                  AND ((i_units = 'Cup')
                     OR (i_units = 'Dram'))
                  AND ((i_size = 'N/A')
                     OR (i_size = 'small')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'orange')
                     OR (i_color = 'frosted'))
                  AND ((i_units = 'Each')
                     OR (i_units = 'Tbl'))
                  AND ((i_size = 'petite')
                     OR (i_size = 'large')))
               OR ((i_category = 'Music')
                  AND ((i_color = 'forest')
                     OR (i_color = 'ghost'))
                  AND ((i_units = 'Lb')
                     OR (i_units = 'Bundle'))
                  AND ((i_size = 'medium')
                     OR (i_size = 'extra large')))))
   ) > 0)
ORDER BY i_product_name ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q41 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q41 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
