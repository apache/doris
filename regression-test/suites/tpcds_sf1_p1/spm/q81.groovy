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

suite("spm_tpcds_sf1_q81", "spm") {

    // SPM baseline DDL + match verification on the ORIGINAL TPCDS q81 query
    // (SQL unchanged from sql/q81.sql) against the REAL TPCDS sf1 data loaded
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
  customer_total_return AS (
   SELECT
     cr_returning_customer_sk ctr_customer_sk
   , ca_state ctr_state
   , sum(cr_return_amt_inc_tax) ctr_total_return
   FROM
     catalog_returns
   , date_dim
   , customer_address
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_year = 2000)
      AND (cr_returning_addr_sk = ca_address_sk)
   GROUP BY cr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id
, c_salutation
, c_first_name
, c_last_name
, ca_street_number
, ca_street_name
, ca_street_type
, ca_suite_number
, ca_city
, ca_county
, ca_state
, ca_zip
, ca_country
, ca_gmt_offset
, ca_location_type
, ctr_total_return
FROM
  customer_total_return ctr1
, customer_address
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_state = ctr2.ctr_state)
   ))
   AND (ca_address_sk = c_current_addr_sk)
   AND (ca_state = 'GA')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, ca_street_number ASC, ca_street_name ASC, ca_street_type ASC, ca_suite_number ASC, ca_city ASC, ca_county ASC, ca_state ASC, ca_zip ASC, ca_country ASC, ca_gmt_offset ASC, ca_location_type ASC, ctr_total_return ASC
LIMIT 100"""

    def createRes = sql ("CREATE GLOBAL BASELINE PLAN \"" + bindSql.replace('"', '\\"') + "\" WITH \"" + bindSql.replace('"', '\\"') + "\"")
    long id = Long.parseLong(createRes[0][0].toString())

    try {
        List<List<Object>> own = sql """SHOW BASELINE PLANS WHERE id = ${id}"""
        assertTrue(own.size() >= 1, "baseline should be visible by id ${id}, got: ${own}")
        assertEquals("USER", own[0][8])
        assertEquals("ENABLED", own[0][9])
        assertTrue(own[0][4].toString().contains("catalog_returns"),
                "q81 plan_sql should reference catalog_returns: ${own[0][4]}")
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
  customer_total_return AS (
   SELECT
     cr_returning_customer_sk ctr_customer_sk
   , ca_state ctr_state
   , sum(cr_return_amt_inc_tax) ctr_total_return
   FROM
     catalog_returns
   , date_dim
   , customer_address
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_year = 2000)
      AND (cr_returning_addr_sk = ca_address_sk)
   GROUP BY cr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id
, c_salutation
, c_first_name
, c_last_name
, ca_street_number
, ca_street_name
, ca_street_type
, ca_suite_number
, ca_city
, ca_county
, ca_state
, ca_zip
, ca_country
, ca_gmt_offset
, ca_location_type
, ctr_total_return
FROM
  customer_total_return ctr1
, customer_address
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_state = ctr2.ctr_state)
   ))
   AND (ca_address_sk = c_current_addr_sk)
   AND (ca_state = 'GA')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, ca_street_number ASC, ca_street_name ASC, ca_street_type ASC, ca_suite_number ASC, ca_city ASC, ca_county ASC, ca_state ASC, ca_zip ASC, ca_country ASC, ca_gmt_offset ASC, ca_location_type ASC, ctr_total_return ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def origWithoutSpm = sql """WITH
  customer_total_return AS (
   SELECT
     cr_returning_customer_sk ctr_customer_sk
   , ca_state ctr_state
   , sum(cr_return_amt_inc_tax) ctr_total_return
   FROM
     catalog_returns
   , date_dim
   , customer_address
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_year = 2000)
      AND (cr_returning_addr_sk = ca_address_sk)
   GROUP BY cr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id
, c_salutation
, c_first_name
, c_last_name
, ca_street_number
, ca_street_name
, ca_street_type
, ca_suite_number
, ca_city
, ca_county
, ca_state
, ca_zip
, ca_country
, ca_gmt_offset
, ca_location_type
, ctr_total_return
FROM
  customer_total_return ctr1
, customer_address
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_state = ctr2.ctr_state)
   ))
   AND (ca_address_sk = c_current_addr_sk)
   AND (ca_state = 'GA')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, ca_street_number ASC, ca_street_name ASC, ca_street_type ASC, ca_suite_number ASC, ca_city ASC, ca_county ASC, ca_state ASC, ca_zip ASC, ca_country ASC, ca_gmt_offset ASC, ca_location_type ASC, ctr_total_return ASC
    LIMIT 100"""
        assertEquals(origWithSpm, origWithoutSpm,
                "SPM rewrite must preserve the result of the q81 query")
    
        // ===== match verification: a similar query (same structure, different literals) =====
        sql 'set enable_spm_rewrite=true'
    def similarWithSpm = sql """WITH
  customer_total_return AS (
   SELECT
     cr_returning_customer_sk ctr_customer_sk
   , ca_state ctr_state
   , sum(cr_return_amt_inc_tax) ctr_total_return
   FROM
     catalog_returns
   , date_dim
   , customer_address
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (cr_returning_addr_sk = ca_address_sk)
   GROUP BY cr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id
, c_salutation
, c_first_name
, c_last_name
, ca_street_number
, ca_street_name
, ca_street_type
, ca_suite_number
, ca_city
, ca_county
, ca_state
, ca_zip
, ca_country
, ca_gmt_offset
, ca_location_type
, ctr_total_return
FROM
  customer_total_return ctr1
, customer_address
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.1' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_state = ctr2.ctr_state)
   ))
   AND (ca_address_sk = c_current_addr_sk)
   AND (ca_state = 'CA')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, ca_street_number ASC, ca_street_name ASC, ca_street_type ASC, ca_suite_number ASC, ca_city ASC, ca_county ASC, ca_state ASC, ca_zip ASC, ca_country ASC, ca_gmt_offset ASC, ca_location_type ASC, ctr_total_return ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
    def similarWithoutSpm = sql """WITH
  customer_total_return AS (
   SELECT
     cr_returning_customer_sk ctr_customer_sk
   , ca_state ctr_state
   , sum(cr_return_amt_inc_tax) ctr_total_return
   FROM
     catalog_returns
   , date_dim
   , customer_address
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (cr_returning_addr_sk = ca_address_sk)
   GROUP BY cr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id
, c_salutation
, c_first_name
, c_last_name
, ca_street_number
, ca_street_name
, ca_street_type
, ca_suite_number
, ca_city
, ca_county
, ca_state
, ca_zip
, ca_country
, ca_gmt_offset
, ca_location_type
, ctr_total_return
FROM
  customer_total_return ctr1
, customer_address
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.1' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_state = ctr2.ctr_state)
   ))
   AND (ca_address_sk = c_current_addr_sk)
   AND (ca_state = 'CA')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, ca_street_number ASC, ca_street_name ASC, ca_street_type ASC, ca_suite_number ASC, ca_city ASC, ca_county ASC, ca_state ASC, ca_zip ASC, ca_country ASC, ca_gmt_offset ASC, ca_location_type ASC, ctr_total_return ASC
    LIMIT 100"""
        assertEquals(similarWithSpm, similarWithoutSpm,
                "SPM rewrite must preserve the result of a similar q81 query")
    
        // ===== EXPLAIN check: original and similar queries must actually hit the baseline =====
        sql 'set enable_spm_rewrite=true'
    def explainOrig = sql """EXPLAIN WITH
  customer_total_return AS (
   SELECT
     cr_returning_customer_sk ctr_customer_sk
   , ca_state ctr_state
   , sum(cr_return_amt_inc_tax) ctr_total_return
   FROM
     catalog_returns
   , date_dim
   , customer_address
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_year = 2000)
      AND (cr_returning_addr_sk = ca_address_sk)
   GROUP BY cr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id
, c_salutation
, c_first_name
, c_last_name
, ca_street_number
, ca_street_name
, ca_street_type
, ca_suite_number
, ca_city
, ca_county
, ca_state
, ca_zip
, ca_country
, ca_gmt_offset
, ca_location_type
, ctr_total_return
FROM
  customer_total_return ctr1
, customer_address
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_state = ctr2.ctr_state)
   ))
   AND (ca_address_sk = c_current_addr_sk)
   AND (ca_state = 'GA')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, ca_street_number ASC, ca_street_name ASC, ca_street_type ASC, ca_suite_number ASC, ca_city ASC, ca_county ASC, ca_state ASC, ca_zip ASC, ca_country ASC, ca_gmt_offset ASC, ca_location_type ASC, ctr_total_return ASC
    LIMIT 100"""
    def explainSimilar = sql """EXPLAIN WITH
  customer_total_return AS (
   SELECT
     cr_returning_customer_sk ctr_customer_sk
   , ca_state ctr_state
   , sum(cr_return_amt_inc_tax) ctr_total_return
   FROM
     catalog_returns
   , date_dim
   , customer_address
   WHERE (cr_returned_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (cr_returning_addr_sk = ca_address_sk)
   GROUP BY cr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id
, c_salutation
, c_first_name
, c_last_name
, ca_street_number
, ca_street_name
, ca_street_type
, ca_suite_number
, ca_city
, ca_county
, ca_state
, ca_zip
, ca_country
, ca_gmt_offset
, ca_location_type
, ctr_total_return
FROM
  customer_total_return ctr1
, customer_address
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.1' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_state = ctr2.ctr_state)
   ))
   AND (ca_address_sk = c_current_addr_sk)
   AND (ca_state = 'CA')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, ca_street_number ASC, ca_street_name ASC, ca_street_type ASC, ca_suite_number ASC, ca_city ASC, ca_county ASC, ca_state ASC, ca_zip ASC, ca_country ASC, ca_gmt_offset ASC, ca_location_type ASC, ctr_total_return ASC
    LIMIT 100"""
        sql 'set enable_spm_rewrite=false'
        assertTrue(explainOrig.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the original q81 query should report SPM baseline hit id " + id + ", got: " + explainOrig)
        assertTrue(explainSimilar.toString().contains("SPM baseline hit: id=" + id),
                "EXPLAIN of the similar q81 query should report SPM baseline hit id " + id + ", got: " + explainSimilar)
    } finally {
        // ===== cleanup own baseline (also runs when an assertion fails above) =====
        sql """DROP BASELINE PLAN IF EXISTS ${id}"""
    }
}
