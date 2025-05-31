package mv.tpcds
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.
suite("mv_tpcds_test") {
    def tables = ["store", "store_returns", "customer", "date_dim", "web_sales",
                  "catalog_sales", "store_sales", "item", "web_returns", "catalog_returns",
                  "catalog_page", "web_site", "customer_address", "customer_demographics",
                  "ship_mode", "promotion", "inventory", "time_dim", "income_band",
                  "call_center", "reason", "household_demographics", "warehouse", "web_page"]
    def columnsMap = [
            "item"            : """tmp_item_sk, tmp_item_id, tmp_rec_start_date, tmp_rec_end_date, tmp_item_desc,
                tmp_current_price, tmp_wholesale_cost, tmp_brand_id, tmp_brand, tmp_class_id, tmp_class,
                tmp_category_id, tmp_category, tmp_manufact_id, tmp_manufact, tmp_size, tmp_formulation,
                tmp_color, tmp_units, tmp_container, tmp_manager_id, tmp_product_name,
                i_item_sk=tmp_item_sk, i_item_id=tmp_item_id, i_rec_start_date=tmp_rec_start_date,
                i_rec_end_date=tmp_rec_end_date, i_item_desc=tmp_item_desc, i_current_price=tmp_current_price,
                i_wholesale_cost=tmp_wholesale_cost, i_brand_id=tmp_brand_id, i_brand=tmp_brand,
                i_class_id=tmp_class_id, i_class=tmp_class, i_category_id=tmp_category_id,
                i_category=nullif(tmp_category, ''), i_manufact_id=tmp_manufact_id, i_manufact=tmp_manufact,
                i_size=tmp_size, i_formulation=tmp_formulation, i_color=tmp_color, i_units=tmp_units,
                i_container=tmp_container, i_manager_id=tmp_manager_id, i_product_name=tmp_product_name""",

            "customer_address": """tmp_address_sk, tmp_address_id, tmp_street_number, tmp_street_name, tmp_street_type, tmp_suite_number,
                            tmp_city, tmp_county, tmp_state, tmp_zip, tmp_country, tmp_gmt_offset, tmp_location_type,
                            ca_address_sk=tmp_address_sk, ca_address_id=tmp_address_id, ca_street_number=tmp_street_number,
                            ca_street_name=tmp_street_name, ca_street_type=tmp_street_type, ca_suite_number=tmp_suite_number, ca_city=tmp_city,
                            ca_county=nullif(tmp_county, ''), ca_state=tmp_state, ca_zip=tmp_zip, ca_country=tmp_country,
                            ca_gmt_offset=tmp_gmt_offset, ca_location_type=tmp_location_type""",
    ]

    def specialTables = ["item", "customer_address"]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    sql "set exec_mem_limit=8G;"

    for (String tableName in tables) {
        streamLoad {
            // you can skip db declaration, because a default db has already been
            // specified in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'

            if (specialTables.contains(tableName)) {
                set "columns", columnsMap[tableName]
            }


            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/regression/tpcds/sf1/${tableName}.dat.gz"""

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    Thread.sleep(70000) // wait for row count report of the tables just loaded
    for (String tableName in tables) {
        sql """ ANALYZE TABLE $tableName WITH SYNC """
    }
    sql """ sync """


    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_nereids_timeout=false"

    def mv1 = """
WITH
  customer_total_return AS (
   SELECT
     sr_customer_sk ctr_customer_sk
   , sr_store_sk ctr_store_sk
   , sum(sr_return_amt) ctr_total_return
   FROM
     store_returns
   , date_dim
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_year = 2000)
   GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM
  customer_total_return ctr1
, store
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
   ))
   AND (s_store_sk = ctr1.ctr_store_sk)
   AND (s_state = 'TN')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC
LIMIT 100;
"""
    def query1 = """
WITH
  customer_total_return AS (
   SELECT
     sr_customer_sk ctr_customer_sk
   , sr_store_sk ctr_store_sk
   , sum(sr_return_amt) ctr_total_return
   FROM
     store_returns
   , date_dim
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_year = 2000)
   GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM
  customer_total_return ctr1
, store
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
   ))
   AND (s_store_sk = ctr1.ctr_store_sk)
   AND (s_state = 'TN')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC
LIMIT 100
"""
    order_qt_query1_before "${query1}"
    async_mv_rewrite_fail(db, mv1, query1, "mv1")
    order_qt_query1_after "${query1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1"""

    def mv1_1 = """
WITH
  customer_total_return AS (
   SELECT
     sr_customer_sk ctr_customer_sk
   , sr_store_sk ctr_store_sk
   , sum(sr_return_amt) ctr_total_return
   FROM
     store_returns
   , date_dim
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_year = 2000)
   GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM
  customer_total_return ctr1
, store
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
   ))
   AND (s_store_sk = ctr1.ctr_store_sk)
   AND (s_state = 'TN')
   AND (ctr1.ctr_customer_sk = c_customer_sk);
"""
    def query1_1 = """
WITH
  customer_total_return AS (
   SELECT
     sr_customer_sk ctr_customer_sk
   , sr_store_sk ctr_store_sk
   , sum(sr_return_amt) ctr_total_return
   FROM
     store_returns
   , date_dim
   WHERE (sr_returned_date_sk = d_date_sk)
      AND (d_year = 2000)
   GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM
  customer_total_return ctr1
, store
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
   ))
   AND (s_store_sk = ctr1.ctr_store_sk)
   AND (s_state = 'TN')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC
LIMIT 100;
"""
    order_qt_query1_1_before "${query1_1}"
    async_mv_rewrite_fail(db, mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    def mv2 = """
WITH
  wscs AS (
   SELECT
     sold_date_sk
   , sales_price
   FROM
     (
      SELECT
        ws_sold_date_sk sold_date_sk
      , ws_ext_sales_price sales_price
      FROM
        web_sales
   ) x
UNION ALL (
      SELECT
        cs_sold_date_sk sold_date_sk
      , cs_ext_sales_price sales_price
      FROM
        catalog_sales
   ) )
, wswscs AS (
   SELECT
     d_week_seq
   , sum((CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE null END)) sun_sales
   , sum((CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE null END)) mon_sales
   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE null END)) tue_sales
   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE null END)) wed_sales
   , sum((CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE null END)) thu_sales
   , sum((CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE null END)) fri_sales
   , sum((CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE null END)) sat_sales
   FROM
     wscs
   , date_dim
   WHERE (d_date_sk = sold_date_sk)
   GROUP BY d_week_seq
)
SELECT
  d_week_seq1
, round((sun_sales1 / sun_sales2), 2)
, round((mon_sales1 / mon_sales2), 2)
, round((tue_sales1 / tue_sales2), 2)
, round((wed_sales1 / wed_sales2), 2)
, round((thu_sales1 / thu_sales2), 2)
, round((fri_sales1 / fri_sales2), 2)
, round((sat_sales1 / sat_sales2), 2)
FROM
  (
   SELECT
     wswscs.d_week_seq d_week_seq1
   , sun_sales sun_sales1
   , mon_sales mon_sales1
   , tue_sales tue_sales1
   , wed_sales wed_sales1
   , thu_sales thu_sales1
   , fri_sales fri_sales1
   , sat_sales sat_sales1
   FROM
     wswscs
   , date_dim
   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
      AND (d_year = 2001)
)  y
, (
   SELECT
     wswscs.d_week_seq d_week_seq2
   , sun_sales sun_sales2
   , mon_sales mon_sales2
   , tue_sales tue_sales2
   , wed_sales wed_sales2
   , thu_sales thu_sales2
   , fri_sales fri_sales2
   , sat_sales sat_sales2
   FROM
     wswscs
   , date_dim
   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
      AND (d_year = (2001 + 1))
)  z
WHERE (d_week_seq1 = (d_week_seq2 - 53))
ORDER BY d_week_seq1 ASC;
"""
    def query2 = """
WITH
  wscs AS (
   SELECT
     sold_date_sk
   , sales_price
   FROM
     (
      SELECT
        ws_sold_date_sk sold_date_sk
      , ws_ext_sales_price sales_price
      FROM
        web_sales
   ) x
UNION ALL (
      SELECT
        cs_sold_date_sk sold_date_sk
      , cs_ext_sales_price sales_price
      FROM
        catalog_sales
   ) )
, wswscs AS (
   SELECT
     d_week_seq
   , sum((CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE null END)) sun_sales
   , sum((CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE null END)) mon_sales
   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE null END)) tue_sales
   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE null END)) wed_sales
   , sum((CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE null END)) thu_sales
   , sum((CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE null END)) fri_sales
   , sum((CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE null END)) sat_sales
   FROM
     wscs
   , date_dim
   WHERE (d_date_sk = sold_date_sk)
   GROUP BY d_week_seq
)
SELECT
  d_week_seq1
, round((sun_sales1 / sun_sales2), 2)
, round((mon_sales1 / mon_sales2), 2)
, round((tue_sales1 / tue_sales2), 2)
, round((wed_sales1 / wed_sales2), 2)
, round((thu_sales1 / thu_sales2), 2)
, round((fri_sales1 / fri_sales2), 2)
, round((sat_sales1 / sat_sales2), 2)
FROM
  (
   SELECT
     wswscs.d_week_seq d_week_seq1
   , sun_sales sun_sales1
   , mon_sales mon_sales1
   , tue_sales tue_sales1
   , wed_sales wed_sales1
   , thu_sales thu_sales1
   , fri_sales fri_sales1
   , sat_sales sat_sales1
   FROM
     wswscs
   , date_dim
   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
      AND (d_year = 2001)
)  y
, (
   SELECT
     wswscs.d_week_seq d_week_seq2
   , sun_sales sun_sales2
   , mon_sales mon_sales2
   , tue_sales tue_sales2
   , wed_sales wed_sales2
   , thu_sales thu_sales2
   , fri_sales fri_sales2
   , sat_sales sat_sales2
   FROM
     wswscs
   , date_dim
   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
      AND (d_year = (2001 + 1))
)  z
WHERE (d_week_seq1 = (d_week_seq2 - 53))
ORDER BY d_week_seq1 ASC;
"""
    order_qt_query2_before "${query2}"
    async_mv_rewrite_fail(db, mv2, query2, "mv2")
    order_qt_query2_after "${query2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2"""


    def mv3 = """
SELECT
  dt.d_year
, item.i_brand_id brand_id
, item.i_brand brand
, sum(ss_ext_sales_price) sum_agg
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manufact_id = 128)
   AND (dt.d_moy = 11)
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year ASC, sum_agg DESC, brand_id ASC
LIMIT 100;
"""
    def query3 = """
SELECT
  dt.d_year
, item.i_brand_id brand_id
, item.i_brand brand
, sum(ss_ext_sales_price) sum_agg
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manufact_id = 128)
   AND (dt.d_moy = 11)
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year ASC, sum_agg DESC, brand_id ASC
LIMIT 100;
"""
    order_qt_query3_before "${query3}"
    async_mv_rewrite_fail(db, mv3, query3, "mv3")
    order_qt_query3_after "${query3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3"""


    def mv3_1 = """
SELECT
  dt.d_year
, item.i_brand_id brand_id
, item.i_brand brand
, sum(ss_ext_sales_price) sum_agg
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manufact_id = 128)
   AND (dt.d_moy = 11)
GROUP BY dt.d_year, item.i_brand, item.i_brand_id;
"""
    def query3_1 = """
SELECT
  dt.d_year
, item.i_brand_id brand_id
, item.i_brand brand
, sum(ss_ext_sales_price) sum_agg
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manufact_id = 128)
   AND (dt.d_moy = 11)
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year ASC, sum_agg DESC, brand_id ASC
LIMIT 100;
"""
    order_qt_query3_1_before "${query3_1}"
    async_mv_rewrite_success(db, mv3_1, query3_1, "mv3_1")
    order_qt_query3_1_after "${query3_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""


    def mv4 = """
WITH
  year_total AS (
   SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum(((((ss_ext_list_price - ss_ext_wholesale_cost) - ss_ext_discount_amt) + ss_ext_sales_price) / 2)) year_total
   , 's' sale_type
   FROM
     customer
   , store_sales
   , date_dim
   WHERE (c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
UNION ALL    SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum(((((cs_ext_list_price - cs_ext_wholesale_cost) - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) year_total
   , 'c' sale_type
   FROM
     customer
   , catalog_sales
   , date_dim
   WHERE (c_customer_sk = cs_bill_customer_sk)
      AND (cs_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
UNION ALL    SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum(((((ws_ext_list_price - ws_ext_wholesale_cost) - ws_ext_discount_amt) + ws_ext_sales_price) / 2)) year_total
   , 'w' sale_type
   FROM
     customer
   , web_sales
   , date_dim
   WHERE (c_customer_sk = ws_bill_customer_sk)
      AND (ws_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
)
SELECT
  t_s_secyear.customer_id
, t_s_secyear.customer_first_name
, t_s_secyear.customer_last_name
, t_s_secyear.customer_preferred_cust_flag
FROM
  year_total t_s_firstyear
, year_total t_s_secyear
, year_total t_c_firstyear
, year_total t_c_secyear
, year_total t_w_firstyear
, year_total t_w_secyear
WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_c_secyear.customer_id)
   AND (t_s_firstyear.customer_id = t_c_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
   AND (t_s_firstyear.sale_type = 's')
   AND (t_c_firstyear.sale_type = 'c')
   AND (t_w_firstyear.sale_type = 'w')
   AND (t_s_secyear.sale_type = 's')
   AND (t_c_secyear.sale_type = 'c')
   AND (t_w_secyear.sale_type = 'w')
   AND (t_s_firstyear.dyear = 2001)
   AND (t_s_secyear.dyear = (2001 + 1))
   AND (t_c_firstyear.dyear = 2001)
   AND (t_c_secyear.dyear = (2001 + 1))
   AND (t_w_firstyear.dyear = 2001)
   AND (t_w_secyear.dyear = (2001 + 1))
   AND (t_s_firstyear.year_total > 0)
   AND (t_c_firstyear.year_total > 0)
   AND (t_w_firstyear.year_total > 0)
   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE null END))
   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE null END))
ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
LIMIT 100;
"""
    def query4 = """
WITH
  year_total AS (
   SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum(((((ss_ext_list_price - ss_ext_wholesale_cost) - ss_ext_discount_amt) + ss_ext_sales_price) / 2)) year_total
   , 's' sale_type
   FROM
     customer
   , store_sales
   , date_dim
   WHERE (c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
UNION ALL    SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum(((((cs_ext_list_price - cs_ext_wholesale_cost) - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) year_total
   , 'c' sale_type
   FROM
     customer
   , catalog_sales
   , date_dim
   WHERE (c_customer_sk = cs_bill_customer_sk)
      AND (cs_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
UNION ALL    SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum(((((ws_ext_list_price - ws_ext_wholesale_cost) - ws_ext_discount_amt) + ws_ext_sales_price) / 2)) year_total
   , 'w' sale_type
   FROM
     customer
   , web_sales
   , date_dim
   WHERE (c_customer_sk = ws_bill_customer_sk)
      AND (ws_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
)
SELECT
  t_s_secyear.customer_id
, t_s_secyear.customer_first_name
, t_s_secyear.customer_last_name
, t_s_secyear.customer_preferred_cust_flag
FROM
  year_total t_s_firstyear
, year_total t_s_secyear
, year_total t_c_firstyear
, year_total t_c_secyear
, year_total t_w_firstyear
, year_total t_w_secyear
WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_c_secyear.customer_id)
   AND (t_s_firstyear.customer_id = t_c_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
   AND (t_s_firstyear.sale_type = 's')
   AND (t_c_firstyear.sale_type = 'c')
   AND (t_w_firstyear.sale_type = 'w')
   AND (t_s_secyear.sale_type = 's')
   AND (t_c_secyear.sale_type = 'c')
   AND (t_w_secyear.sale_type = 'w')
   AND (t_s_firstyear.dyear = 2001)
   AND (t_s_secyear.dyear = (2001 + 1))
   AND (t_c_firstyear.dyear = 2001)
   AND (t_c_secyear.dyear = (2001 + 1))
   AND (t_w_firstyear.dyear = 2001)
   AND (t_w_secyear.dyear = (2001 + 1))
   AND (t_s_firstyear.year_total > 0)
   AND (t_c_firstyear.year_total > 0)
   AND (t_w_firstyear.year_total > 0)
   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE null END))
   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE null END))
ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
LIMIT 100;
"""
    order_qt_query4_before "${query4}"
    async_mv_rewrite_fail(db, mv4, query4, "mv4")
    order_qt_query4_after "${query4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4"""

    def mv5 = """
WITH
  ssr AS (
   SELECT
     s_store_id
   , sum(sales_price) sales
   , sum(profit) profit
   , sum(return_amt) returns
   , sum(net_loss) profit_loss
   FROM
     (
      SELECT
        ss_store_sk store_sk
      , ss_sold_date_sk date_sk
      , ss_ext_sales_price sales_price
      , ss_net_profit profit
      , CAST(0 AS DECIMAL(7,2)) return_amt
      , CAST(0 AS DECIMAL(7,2)) net_loss
      FROM
        store_sales
UNION ALL       SELECT
        sr_store_sk store_sk
      , sr_returned_date_sk date_sk
      , CAST(0 AS DECIMAL(7,2)) sales_price
      , CAST(0 AS DECIMAL(7,2)) profit
      , sr_return_amt return_amt
      , sr_net_loss net_loss
      FROM
        store_returns
   )  salesreturns
   , date_dim
   , store
   WHERE (date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
      AND (store_sk = s_store_sk)
   GROUP BY s_store_id
)
, csr AS (
   SELECT
     cp_catalog_page_id
   , sum(sales_price) sales
   , sum(profit) profit
   , sum(return_amt) returns
   , sum(net_loss) profit_loss
   FROM
     (
      SELECT
        cs_catalog_page_sk page_sk
      , cs_sold_date_sk date_sk
      , cs_ext_sales_price sales_price
      , cs_net_profit profit
      , CAST(0 AS DECIMAL(7,2)) return_amt
      , CAST(0 AS DECIMAL(7,2)) net_loss
      FROM
        catalog_sales
UNION ALL       SELECT
        cr_catalog_page_sk page_sk
      , cr_returned_date_sk date_sk
      , CAST(0 AS DECIMAL(7,2)) sales_price
      , CAST(0 AS DECIMAL(7,2)) profit
      , cr_return_amount return_amt
      , cr_net_loss net_loss
      FROM
        catalog_returns
   )  salesreturns
   , date_dim
   , catalog_page
   WHERE (date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
      AND (page_sk = cp_catalog_page_sk)
   GROUP BY cp_catalog_page_id
)
, wsr AS (
   SELECT
     web_site_id
   , sum(sales_price) sales
   , sum(profit) profit
   , sum(return_amt) returns
   , sum(net_loss) profit_loss
   FROM
     (
      SELECT
        ws_web_site_sk wsr_web_site_sk
      , ws_sold_date_sk date_sk
      , ws_ext_sales_price sales_price
      , ws_net_profit profit
      , CAST(0 AS DECIMAL(7,2)) return_amt
      , CAST(0 AS DECIMAL(7,2)) net_loss
      FROM
        web_sales
UNION ALL       SELECT
        ws_web_site_sk wsr_web_site_sk
      , wr_returned_date_sk date_sk
      , CAST(0 AS DECIMAL(7,2)) sales_price
      , CAST(0 AS DECIMAL(7,2)) profit
      , wr_return_amt return_amt
      , wr_net_loss net_loss
      FROM
        web_returns
      LEFT JOIN web_sales ON (wr_item_sk = ws_item_sk)
         AND (wr_order_number = ws_order_number)
   )  salesreturns
   , date_dim
   , web_site
   WHERE (date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
      AND (wsr_web_site_sk = web_site_sk)
   GROUP BY web_site_id
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
   , concat('store', s_store_id) id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     ssr
UNION ALL    SELECT
     'catalog channel' channel
   , concat('catalog_page', cp_catalog_page_id) id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     csr
UNION ALL    SELECT
     'web channel' channel
   , concat('web_site', web_site_id) id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     wsr
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC
LIMIT 100;
"""
    def query5 = """
WITH
  ssr AS (
   SELECT
     s_store_id
   , sum(sales_price) sales
   , sum(profit) profit
   , sum(return_amt) returns
   , sum(net_loss) profit_loss
   FROM
     (
      SELECT
        ss_store_sk store_sk
      , ss_sold_date_sk date_sk
      , ss_ext_sales_price sales_price
      , ss_net_profit profit
      , CAST(0 AS DECIMAL(7,2)) return_amt
      , CAST(0 AS DECIMAL(7,2)) net_loss
      FROM
        store_sales
UNION ALL       SELECT
        sr_store_sk store_sk
      , sr_returned_date_sk date_sk
      , CAST(0 AS DECIMAL(7,2)) sales_price
      , CAST(0 AS DECIMAL(7,2)) profit
      , sr_return_amt return_amt
      , sr_net_loss net_loss
      FROM
        store_returns
   )  salesreturns
   , date_dim
   , store
   WHERE (date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
      AND (store_sk = s_store_sk)
   GROUP BY s_store_id
)
, csr AS (
   SELECT
     cp_catalog_page_id
   , sum(sales_price) sales
   , sum(profit) profit
   , sum(return_amt) returns
   , sum(net_loss) profit_loss
   FROM
     (
      SELECT
        cs_catalog_page_sk page_sk
      , cs_sold_date_sk date_sk
      , cs_ext_sales_price sales_price
      , cs_net_profit profit
      , CAST(0 AS DECIMAL(7,2)) return_amt
      , CAST(0 AS DECIMAL(7,2)) net_loss
      FROM
        catalog_sales
UNION ALL       SELECT
        cr_catalog_page_sk page_sk
      , cr_returned_date_sk date_sk
      , CAST(0 AS DECIMAL(7,2)) sales_price
      , CAST(0 AS DECIMAL(7,2)) profit
      , cr_return_amount return_amt
      , cr_net_loss net_loss
      FROM
        catalog_returns
   )  salesreturns
   , date_dim
   , catalog_page
   WHERE (date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
      AND (page_sk = cp_catalog_page_sk)
   GROUP BY cp_catalog_page_id
)
, wsr AS (
   SELECT
     web_site_id
   , sum(sales_price) sales
   , sum(profit) profit
   , sum(return_amt) returns
   , sum(net_loss) profit_loss
   FROM
     (
      SELECT
        ws_web_site_sk wsr_web_site_sk
      , ws_sold_date_sk date_sk
      , ws_ext_sales_price sales_price
      , ws_net_profit profit
      , CAST(0 AS DECIMAL(7,2)) return_amt
      , CAST(0 AS DECIMAL(7,2)) net_loss
      FROM
        web_sales
UNION ALL       SELECT
        ws_web_site_sk wsr_web_site_sk
      , wr_returned_date_sk date_sk
      , CAST(0 AS DECIMAL(7,2)) sales_price
      , CAST(0 AS DECIMAL(7,2)) profit
      , wr_return_amt return_amt
      , wr_net_loss net_loss
      FROM
        web_returns
      LEFT JOIN web_sales ON (wr_item_sk = ws_item_sk)
         AND (wr_order_number = ws_order_number)
   )  salesreturns
   , date_dim
   , web_site
   WHERE (date_sk = d_date_sk)
      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
      AND (wsr_web_site_sk = web_site_sk)
   GROUP BY web_site_id
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
   , concat('store', s_store_id) id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     ssr
UNION ALL    SELECT
     'catalog channel' channel
   , concat('catalog_page', cp_catalog_page_id) id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     csr
UNION ALL    SELECT
     'web channel' channel
   , concat('web_site', web_site_id) id
   , sales
   , returns
   , (profit - profit_loss) profit
   FROM
     wsr
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC
LIMIT 100;
"""
    order_qt_query5_before "${query5}"
    async_mv_rewrite_fail(db, mv5, query5, "mv5")
    order_qt_query5_after "${query5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5"""

    def mv6 = """
--- takes over 30 minutes on travis to complete
SELECT
  a.ca_state STATE
, count(*) cnt
FROM
  customer_address a
, customer c
, store_sales s
, date_dim d
, item i
WHERE (a.ca_address_sk = c.c_current_addr_sk)
   AND (c.c_customer_sk = s.ss_customer_sk)
   AND (s.ss_sold_date_sk = d.d_date_sk)
   AND (s.ss_item_sk = i.i_item_sk)
   AND (d.d_month_seq = (
      SELECT DISTINCT d_month_seq
      FROM
        date_dim
      WHERE (d_year = 2001)
         AND (d_moy = 1)
   ))
   AND (i.i_current_price > (CAST('1.2' AS DECIMAL(2,1)) * (
         SELECT avg(j.i_current_price)
         FROM
           item j
         WHERE (j.i_category = i.i_category)
      )))
GROUP BY a.ca_state
HAVING (count(*) >= 10)
ORDER BY cnt ASC, a.ca_state ASC
LIMIT 100;
"""
    def query6 = """
--- takes over 30 minutes on travis to complete
SELECT
  a.ca_state STATE
, count(*) cnt
FROM
  customer_address a
, customer c
, store_sales s
, date_dim d
, item i
WHERE (a.ca_address_sk = c.c_current_addr_sk)
   AND (c.c_customer_sk = s.ss_customer_sk)
   AND (s.ss_sold_date_sk = d.d_date_sk)
   AND (s.ss_item_sk = i.i_item_sk)
   AND (d.d_month_seq = (
      SELECT DISTINCT d_month_seq
      FROM
        date_dim
      WHERE (d_year = 2001)
         AND (d_moy = 1)
   ))
   AND (i.i_current_price > (CAST('1.2' AS DECIMAL(2,1)) * (
         SELECT avg(j.i_current_price)
         FROM
           item j
         WHERE (j.i_category = i.i_category)
      )))
GROUP BY a.ca_state
HAVING (count(*) >= 10)
ORDER BY cnt ASC, a.ca_state ASC
LIMIT 100;
"""
    order_qt_query6_before "${query6}"
    async_mv_rewrite_fail(db, mv6, query6, "mv6")
    order_qt_query6_after "${query6}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6"""


    def mv6_1 = """
--- takes over 30 minutes on travis to complete
SELECT
  a.ca_state STATE
, count(*) cnt
FROM
  customer_address a
, customer c
, store_sales s
, date_dim d
, item i
WHERE (a.ca_address_sk = c.c_current_addr_sk)
   AND (c.c_customer_sk = s.ss_customer_sk)
   AND (s.ss_sold_date_sk = d.d_date_sk)
   AND (s.ss_item_sk = i.i_item_sk)
   AND (d.d_month_seq = (
      SELECT DISTINCT d_month_seq
      FROM
        date_dim
      WHERE (d_year = 2001)
         AND (d_moy = 1)
   ))
   AND (i.i_current_price > (CAST('1.2' AS DECIMAL(2,1)) * (
         SELECT avg(j.i_current_price)
         FROM
           item j
         WHERE (j.i_category = i.i_category)
      )))
GROUP BY a.ca_state
HAVING (count(*) >= 10);
"""
    def query6_1 = """
--- takes over 30 minutes on travis to complete
SELECT
  a.ca_state STATE
, count(*) cnt
FROM
  customer_address a
, customer c
, store_sales s
, date_dim d
, item i
WHERE (a.ca_address_sk = c.c_current_addr_sk)
   AND (c.c_customer_sk = s.ss_customer_sk)
   AND (s.ss_sold_date_sk = d.d_date_sk)
   AND (s.ss_item_sk = i.i_item_sk)
   AND (d.d_month_seq = (
      SELECT DISTINCT d_month_seq
      FROM
        date_dim
      WHERE (d_year = 2001)
         AND (d_moy = 1)
   ))
   AND (i.i_current_price > (CAST('1.2' AS DECIMAL(2,1)) * (
         SELECT avg(j.i_current_price)
         FROM
           item j
         WHERE (j.i_category = i.i_category)
      )))
GROUP BY a.ca_state
HAVING (count(*) >= 10)
ORDER BY cnt ASC, a.ca_state ASC
LIMIT 100;
"""
    order_qt_query6_1_before "${query6_1}"
    async_mv_rewrite_fail(db, mv6_1, query6_1, "mv6_1")
    order_qt_query6_1_after "${query6_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_1"""

    def mv7 = """
SELECT
  i_item_id
, avg(ss_quantity) agg1
, avg(ss_list_price) agg2
, avg(ss_coupon_amt) agg3
, avg(ss_sales_price) agg4
FROM
  store_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE (ss_sold_date_sk = d_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (ss_cdemo_sk = cd_demo_sk)
   AND (ss_promo_sk = p_promo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND ((p_channel_email = 'N')
      OR (p_channel_event = 'N'))
   AND (d_year = 2000)
GROUP BY i_item_id
ORDER BY i_item_id ASC
LIMIT 100;
"""
    def query7 = """
SELECT
  i_item_id
, avg(ss_quantity) agg1
, avg(ss_list_price) agg2
, avg(ss_coupon_amt) agg3
, avg(ss_sales_price) agg4
FROM
  store_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE (ss_sold_date_sk = d_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (ss_cdemo_sk = cd_demo_sk)
   AND (ss_promo_sk = p_promo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND ((p_channel_email = 'N')
      OR (p_channel_event = 'N'))
   AND (d_year = 2000)
GROUP BY i_item_id
ORDER BY i_item_id ASC
LIMIT 100;
"""
    order_qt_query7_before "${query7}"
    async_mv_rewrite_fail(db, mv7, query7, "mv7")
    order_qt_query7_after "${query7}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7"""

    def mv7_1 = """
SELECT
  i_item_id
, avg(ss_quantity) agg1
, avg(ss_list_price) agg2
, avg(ss_coupon_amt) agg3
, avg(ss_sales_price) agg4
FROM
  store_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE (ss_sold_date_sk = d_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (ss_cdemo_sk = cd_demo_sk)
   AND (ss_promo_sk = p_promo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND ((p_channel_email = 'N')
      OR (p_channel_event = 'N'))
   AND (d_year = 2000)
GROUP BY i_item_id;
"""
    def query7_1 = """
SELECT
  i_item_id
, avg(ss_quantity) agg1
, avg(ss_list_price) agg2
, avg(ss_coupon_amt) agg3
, avg(ss_sales_price) agg4
FROM
  store_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE (ss_sold_date_sk = d_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (ss_cdemo_sk = cd_demo_sk)
   AND (ss_promo_sk = p_promo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND ((p_channel_email = 'N')
      OR (p_channel_event = 'N'))
   AND (d_year = 2000)
GROUP BY i_item_id
ORDER BY i_item_id ASC
LIMIT 100;
"""
    order_qt_query7_1_before "${query7_1}"
    async_mv_rewrite_success(db, mv7_1, query7_1, "mv7_1")
    order_qt_query7_1_after "${query7_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_1"""

    def mv8 = """
SELECT
  s_store_name
, sum(ss_net_profit)
FROM
  store_sales
, date_dim
, store
, (
   SELECT ca_zip
   FROM
     (
(
         SELECT substr(ca_zip, 1, 5) ca_zip
         FROM
           customer_address
         WHERE (substr(ca_zip, 1, 5) IN (
                '24128'
              , '57834'
              , '13354'
              , '15734'
              , '78668'
              , '76232'
              , '62878'
              , '45375'
              , '63435'
              , '22245'
              , '65084'
              , '49130'
              , '40558'
              , '25733'
              , '15798'
              , '87816'
              , '81096'
              , '56458'
              , '35474'
              , '27156'
              , '83926'
              , '18840'
              , '28286'
              , '24676'
              , '37930'
              , '77556'
              , '27700'
              , '45266'
              , '94627'
              , '62971'
              , '20548'
              , '23470'
              , '47305'
              , '53535'
              , '21337'
              , '26231'
              , '50412'
              , '69399'
              , '17879'
              , '51622'
              , '43848'
              , '21195'
              , '83921'
              , '15559'
              , '67853'
              , '15126'
              , '16021'
              , '26233'
              , '53268'
              , '10567'
              , '91137'
              , '76107'
              , '11101'
              , '59166'
              , '38415'
              , '61265'
              , '71954'
              , '15371'
              , '11928'
              , '15455'
              , '98294'
              , '68309'
              , '69913'
              , '59402'
              , '58263'
              , '25782'
              , '18119'
              , '35942'
              , '33282'
              , '42029'
              , '17920'
              , '98359'
              , '15882'
              , '45721'
              , '60279'
              , '18426'
              , '64544'
              , '25631'
              , '43933'
              , '37125'
              , '98235'
              , '10336'
              , '24610'
              , '68101'
              , '56240'
              , '40081'
              , '86379'
              , '44165'
              , '33515'
              , '88190'
              , '84093'
              , '27068'
              , '99076'
              , '36634'
              , '50308'
              , '28577'
              , '39736'
              , '33786'
              , '71286'
              , '26859'
              , '55565'
              , '98569'
              , '70738'
              , '19736'
              , '64457'
              , '17183'
              , '28915'
              , '26653'
              , '58058'
              , '89091'
              , '54601'
              , '24206'
              , '14328'
              , '55253'
              , '82136'
              , '67897'
              , '56529'
              , '72305'
              , '67473'
              , '62377'
              , '22752'
              , '57647'
              , '62496'
              , '41918'
              , '36233'
              , '86284'
              , '54917'
              , '22152'
              , '19515'
              , '63837'
              , '18376'
              , '42961'
              , '10144'
              , '36495'
              , '58078'
              , '38607'
              , '91110'
              , '64147'
              , '19430'
              , '17043'
              , '45200'
              , '63981'
              , '48425'
              , '22351'
              , '30010'
              , '21756'
              , '14922'
              , '14663'
              , '77191'
              , '60099'
              , '29741'
              , '36420'
              , '21076'
              , '91393'
              , '28810'
              , '96765'
              , '23006'
              , '18799'
              , '49156'
              , '98025'
              , '23932'
              , '67467'
              , '30450'
              , '50298'
              , '29178'
              , '89360'
              , '32754'
              , '63089'
              , '87501'
              , '87343'
              , '29839'
              , '30903'
              , '81019'
              , '18652'
              , '73273'
              , '25989'
              , '20260'
              , '68893'
              , '53179'
              , '30469'
              , '28898'
              , '31671'
              , '24996'
              , '18767'
              , '64034'
              , '91068'
              , '51798'
              , '51200'
              , '63193'
              , '39516'
              , '72550'
              , '72325'
              , '51211'
              , '23968'
              , '86057'
              , '10390'
              , '85816'
              , '45692'
              , '65164'
              , '21309'
              , '18845'
              , '68621'
              , '92712'
              , '68880'
              , '90257'
              , '47770'
              , '13955'
              , '70466'
              , '21286'
              , '67875'
              , '82636'
              , '36446'
              , '79994'
              , '72823'
              , '40162'
              , '41367'
              , '41766'
              , '22437'
              , '58470'
              , '11356'
              , '76638'
              , '68806'
              , '25280'
              , '67301'
              , '73650'
              , '86198'
              , '16725'
              , '38935'
              , '13394'
              , '61810'
              , '81312'
              , '15146'
              , '71791'
              , '31016'
              , '72013'
              , '37126'
              , '22744'
              , '73134'
              , '70372'
              , '30431'
              , '39192'
              , '35850'
              , '56571'
              , '67030'
              , '22461'
              , '88424'
              , '88086'
              , '14060'
              , '40604'
              , '19512'
              , '72175'
              , '51649'
              , '19505'
              , '24317'
              , '13375'
              , '81426'
              , '18270'
              , '72425'
              , '45748'
              , '55307'
              , '53672'
              , '52867'
              , '56575'
              , '39127'
              , '30625'
              , '10445'
              , '39972'
              , '74351'
              , '26065'
              , '83849'
              , '42666'
              , '96976'
              , '68786'
              , '77721'
              , '68908'
              , '66864'
              , '63792'
              , '51650'
              , '31029'
              , '26689'
              , '66708'
              , '11376'
              , '20004'
              , '31880'
              , '96451'
              , '41248'
              , '94898'
              , '18383'
              , '60576'
              , '38193'
              , '48583'
              , '13595'
              , '76614'
              , '24671'
              , '46820'
              , '82276'
              , '10516'
              , '11634'
              , '45549'
              , '88885'
              , '18842'
              , '90225'
              , '18906'
              , '13376'
              , '84935'
              , '78890'
              , '58943'
              , '15765'
              , '50016'
              , '69035'
              , '49448'
              , '39371'
              , '41368'
              , '33123'
              , '83144'
              , '14089'
              , '94945'
              , '73241'
              , '19769'
              , '47537'
              , '38122'
              , '28587'
              , '76698'
              , '22927'
              , '56616'
              , '34425'
              , '96576'
              , '78567'
              , '97789'
              , '94983'
              , '79077'
              , '57855'
              , '97189'
              , '46081'
              , '48033'
              , '19849'
              , '28488'
              , '28545'
              , '72151'
              , '69952'
              , '43285'
              , '26105'
              , '76231'
              , '15723'
              , '25486'
              , '39861'
              , '83933'
              , '75691'
              , '46136'
              , '61547'
              , '66162'
              , '25858'
              , '22246'
              , '51949'
              , '27385'
              , '77610'
              , '34322'
              , '51061'
              , '68100'
              , '61860'
              , '13695'
              , '44438'
              , '90578'
              , '96888'
              , '58048'
              , '99543'
              , '73171'
              , '56691'
              , '64528'
              , '56910'
              , '83444'
              , '30122'
              , '68014'
              , '14171'
              , '16807'
              , '83041'
              , '34102'
              , '51103'
              , '79777'
              , '17871'
              , '12305'
              , '22685'
              , '94167'
              , '28709'
              , '35258'
              , '57665'
              , '71256'
              , '57047'
              , '11489'
              , '31387'
              , '68341'
              , '78451'
              , '14867'
              , '25103'
              , '35458'
              , '25003'
              , '54364'
              , '73520'
              , '32213'
              , '35576'))
      )       INTERSECT (
         SELECT ca_zip
         FROM
           (
            SELECT
              substr(ca_zip, 1, 5) ca_zip
            , count(*) cnt
            FROM
              customer_address
            , customer
            WHERE (ca_address_sk = c_current_addr_sk)
               AND (c_preferred_cust_flag = 'Y')
            GROUP BY ca_zip
            HAVING (count(*) > 10)
         )  a1
      )    )  a2
)  v1
WHERE (ss_store_sk = s_store_sk)
   AND (ss_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 1998)
   AND (substr(s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
GROUP BY s_store_name
ORDER BY s_store_name ASC
LIMIT 100;
"""
    def query8 = """
SELECT
  s_store_name
, sum(ss_net_profit)
FROM
  store_sales
, date_dim
, store
, (
   SELECT ca_zip
   FROM
     (
(
         SELECT substr(ca_zip, 1, 5) ca_zip
         FROM
           customer_address
         WHERE (substr(ca_zip, 1, 5) IN (
                '24128'
              , '57834'
              , '13354'
              , '15734'
              , '78668'
              , '76232'
              , '62878'
              , '45375'
              , '63435'
              , '22245'
              , '65084'
              , '49130'
              , '40558'
              , '25733'
              , '15798'
              , '87816'
              , '81096'
              , '56458'
              , '35474'
              , '27156'
              , '83926'
              , '18840'
              , '28286'
              , '24676'
              , '37930'
              , '77556'
              , '27700'
              , '45266'
              , '94627'
              , '62971'
              , '20548'
              , '23470'
              , '47305'
              , '53535'
              , '21337'
              , '26231'
              , '50412'
              , '69399'
              , '17879'
              , '51622'
              , '43848'
              , '21195'
              , '83921'
              , '15559'
              , '67853'
              , '15126'
              , '16021'
              , '26233'
              , '53268'
              , '10567'
              , '91137'
              , '76107'
              , '11101'
              , '59166'
              , '38415'
              , '61265'
              , '71954'
              , '15371'
              , '11928'
              , '15455'
              , '98294'
              , '68309'
              , '69913'
              , '59402'
              , '58263'
              , '25782'
              , '18119'
              , '35942'
              , '33282'
              , '42029'
              , '17920'
              , '98359'
              , '15882'
              , '45721'
              , '60279'
              , '18426'
              , '64544'
              , '25631'
              , '43933'
              , '37125'
              , '98235'
              , '10336'
              , '24610'
              , '68101'
              , '56240'
              , '40081'
              , '86379'
              , '44165'
              , '33515'
              , '88190'
              , '84093'
              , '27068'
              , '99076'
              , '36634'
              , '50308'
              , '28577'
              , '39736'
              , '33786'
              , '71286'
              , '26859'
              , '55565'
              , '98569'
              , '70738'
              , '19736'
              , '64457'
              , '17183'
              , '28915'
              , '26653'
              , '58058'
              , '89091'
              , '54601'
              , '24206'
              , '14328'
              , '55253'
              , '82136'
              , '67897'
              , '56529'
              , '72305'
              , '67473'
              , '62377'
              , '22752'
              , '57647'
              , '62496'
              , '41918'
              , '36233'
              , '86284'
              , '54917'
              , '22152'
              , '19515'
              , '63837'
              , '18376'
              , '42961'
              , '10144'
              , '36495'
              , '58078'
              , '38607'
              , '91110'
              , '64147'
              , '19430'
              , '17043'
              , '45200'
              , '63981'
              , '48425'
              , '22351'
              , '30010'
              , '21756'
              , '14922'
              , '14663'
              , '77191'
              , '60099'
              , '29741'
              , '36420'
              , '21076'
              , '91393'
              , '28810'
              , '96765'
              , '23006'
              , '18799'
              , '49156'
              , '98025'
              , '23932'
              , '67467'
              , '30450'
              , '50298'
              , '29178'
              , '89360'
              , '32754'
              , '63089'
              , '87501'
              , '87343'
              , '29839'
              , '30903'
              , '81019'
              , '18652'
              , '73273'
              , '25989'
              , '20260'
              , '68893'
              , '53179'
              , '30469'
              , '28898'
              , '31671'
              , '24996'
              , '18767'
              , '64034'
              , '91068'
              , '51798'
              , '51200'
              , '63193'
              , '39516'
              , '72550'
              , '72325'
              , '51211'
              , '23968'
              , '86057'
              , '10390'
              , '85816'
              , '45692'
              , '65164'
              , '21309'
              , '18845'
              , '68621'
              , '92712'
              , '68880'
              , '90257'
              , '47770'
              , '13955'
              , '70466'
              , '21286'
              , '67875'
              , '82636'
              , '36446'
              , '79994'
              , '72823'
              , '40162'
              , '41367'
              , '41766'
              , '22437'
              , '58470'
              , '11356'
              , '76638'
              , '68806'
              , '25280'
              , '67301'
              , '73650'
              , '86198'
              , '16725'
              , '38935'
              , '13394'
              , '61810'
              , '81312'
              , '15146'
              , '71791'
              , '31016'
              , '72013'
              , '37126'
              , '22744'
              , '73134'
              , '70372'
              , '30431'
              , '39192'
              , '35850'
              , '56571'
              , '67030'
              , '22461'
              , '88424'
              , '88086'
              , '14060'
              , '40604'
              , '19512'
              , '72175'
              , '51649'
              , '19505'
              , '24317'
              , '13375'
              , '81426'
              , '18270'
              , '72425'
              , '45748'
              , '55307'
              , '53672'
              , '52867'
              , '56575'
              , '39127'
              , '30625'
              , '10445'
              , '39972'
              , '74351'
              , '26065'
              , '83849'
              , '42666'
              , '96976'
              , '68786'
              , '77721'
              , '68908'
              , '66864'
              , '63792'
              , '51650'
              , '31029'
              , '26689'
              , '66708'
              , '11376'
              , '20004'
              , '31880'
              , '96451'
              , '41248'
              , '94898'
              , '18383'
              , '60576'
              , '38193'
              , '48583'
              , '13595'
              , '76614'
              , '24671'
              , '46820'
              , '82276'
              , '10516'
              , '11634'
              , '45549'
              , '88885'
              , '18842'
              , '90225'
              , '18906'
              , '13376'
              , '84935'
              , '78890'
              , '58943'
              , '15765'
              , '50016'
              , '69035'
              , '49448'
              , '39371'
              , '41368'
              , '33123'
              , '83144'
              , '14089'
              , '94945'
              , '73241'
              , '19769'
              , '47537'
              , '38122'
              , '28587'
              , '76698'
              , '22927'
              , '56616'
              , '34425'
              , '96576'
              , '78567'
              , '97789'
              , '94983'
              , '79077'
              , '57855'
              , '97189'
              , '46081'
              , '48033'
              , '19849'
              , '28488'
              , '28545'
              , '72151'
              , '69952'
              , '43285'
              , '26105'
              , '76231'
              , '15723'
              , '25486'
              , '39861'
              , '83933'
              , '75691'
              , '46136'
              , '61547'
              , '66162'
              , '25858'
              , '22246'
              , '51949'
              , '27385'
              , '77610'
              , '34322'
              , '51061'
              , '68100'
              , '61860'
              , '13695'
              , '44438'
              , '90578'
              , '96888'
              , '58048'
              , '99543'
              , '73171'
              , '56691'
              , '64528'
              , '56910'
              , '83444'
              , '30122'
              , '68014'
              , '14171'
              , '16807'
              , '83041'
              , '34102'
              , '51103'
              , '79777'
              , '17871'
              , '12305'
              , '22685'
              , '94167'
              , '28709'
              , '35258'
              , '57665'
              , '71256'
              , '57047'
              , '11489'
              , '31387'
              , '68341'
              , '78451'
              , '14867'
              , '25103'
              , '35458'
              , '25003'
              , '54364'
              , '73520'
              , '32213'
              , '35576'))
      )       INTERSECT (
         SELECT ca_zip
         FROM
           (
            SELECT
              substr(ca_zip, 1, 5) ca_zip
            , count(*) cnt
            FROM
              customer_address
            , customer
            WHERE (ca_address_sk = c_current_addr_sk)
               AND (c_preferred_cust_flag = 'Y')
            GROUP BY ca_zip
            HAVING (count(*) > 10)
         )  a1
      )    )  a2
)  v1
WHERE (ss_store_sk = s_store_sk)
   AND (ss_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 1998)
   AND (substr(s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
GROUP BY s_store_name
ORDER BY s_store_name ASC
LIMIT 100;
"""
    order_qt_query8_before "${query8}"
    async_mv_rewrite_fail(db, mv8, query8, "mv8")
    order_qt_query8_after "${query8}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8"""

    def mv8_1 = """
SELECT
  s_store_name
, sum(ss_net_profit)
FROM
  store_sales
, date_dim
, store
, (
   SELECT ca_zip
   FROM
     (
(
         SELECT substr(ca_zip, 1, 5) ca_zip
         FROM
           customer_address
         WHERE (substr(ca_zip, 1, 5) IN (
                '24128'
              , '57834'
              , '13354'
              , '15734'
              , '78668'
              , '76232'
              , '62878'
              , '45375'
              , '63435'
              , '22245'
              , '65084'
              , '49130'
              , '40558'
              , '25733'
              , '15798'
              , '87816'
              , '81096'
              , '56458'
              , '35474'
              , '27156'
              , '83926'
              , '18840'
              , '28286'
              , '24676'
              , '37930'
              , '77556'
              , '27700'
              , '45266'
              , '94627'
              , '62971'
              , '20548'
              , '23470'
              , '47305'
              , '53535'
              , '21337'
              , '26231'
              , '50412'
              , '69399'
              , '17879'
              , '51622'
              , '43848'
              , '21195'
              , '83921'
              , '15559'
              , '67853'
              , '15126'
              , '16021'
              , '26233'
              , '53268'
              , '10567'
              , '91137'
              , '76107'
              , '11101'
              , '59166'
              , '38415'
              , '61265'
              , '71954'
              , '15371'
              , '11928'
              , '15455'
              , '98294'
              , '68309'
              , '69913'
              , '59402'
              , '58263'
              , '25782'
              , '18119'
              , '35942'
              , '33282'
              , '42029'
              , '17920'
              , '98359'
              , '15882'
              , '45721'
              , '60279'
              , '18426'
              , '64544'
              , '25631'
              , '43933'
              , '37125'
              , '98235'
              , '10336'
              , '24610'
              , '68101'
              , '56240'
              , '40081'
              , '86379'
              , '44165'
              , '33515'
              , '88190'
              , '84093'
              , '27068'
              , '99076'
              , '36634'
              , '50308'
              , '28577'
              , '39736'
              , '33786'
              , '71286'
              , '26859'
              , '55565'
              , '98569'
              , '70738'
              , '19736'
              , '64457'
              , '17183'
              , '28915'
              , '26653'
              , '58058'
              , '89091'
              , '54601'
              , '24206'
              , '14328'
              , '55253'
              , '82136'
              , '67897'
              , '56529'
              , '72305'
              , '67473'
              , '62377'
              , '22752'
              , '57647'
              , '62496'
              , '41918'
              , '36233'
              , '86284'
              , '54917'
              , '22152'
              , '19515'
              , '63837'
              , '18376'
              , '42961'
              , '10144'
              , '36495'
              , '58078'
              , '38607'
              , '91110'
              , '64147'
              , '19430'
              , '17043'
              , '45200'
              , '63981'
              , '48425'
              , '22351'
              , '30010'
              , '21756'
              , '14922'
              , '14663'
              , '77191'
              , '60099'
              , '29741'
              , '36420'
              , '21076'
              , '91393'
              , '28810'
              , '96765'
              , '23006'
              , '18799'
              , '49156'
              , '98025'
              , '23932'
              , '67467'
              , '30450'
              , '50298'
              , '29178'
              , '89360'
              , '32754'
              , '63089'
              , '87501'
              , '87343'
              , '29839'
              , '30903'
              , '81019'
              , '18652'
              , '73273'
              , '25989'
              , '20260'
              , '68893'
              , '53179'
              , '30469'
              , '28898'
              , '31671'
              , '24996'
              , '18767'
              , '64034'
              , '91068'
              , '51798'
              , '51200'
              , '63193'
              , '39516'
              , '72550'
              , '72325'
              , '51211'
              , '23968'
              , '86057'
              , '10390'
              , '85816'
              , '45692'
              , '65164'
              , '21309'
              , '18845'
              , '68621'
              , '92712'
              , '68880'
              , '90257'
              , '47770'
              , '13955'
              , '70466'
              , '21286'
              , '67875'
              , '82636'
              , '36446'
              , '79994'
              , '72823'
              , '40162'
              , '41367'
              , '41766'
              , '22437'
              , '58470'
              , '11356'
              , '76638'
              , '68806'
              , '25280'
              , '67301'
              , '73650'
              , '86198'
              , '16725'
              , '38935'
              , '13394'
              , '61810'
              , '81312'
              , '15146'
              , '71791'
              , '31016'
              , '72013'
              , '37126'
              , '22744'
              , '73134'
              , '70372'
              , '30431'
              , '39192'
              , '35850'
              , '56571'
              , '67030'
              , '22461'
              , '88424'
              , '88086'
              , '14060'
              , '40604'
              , '19512'
              , '72175'
              , '51649'
              , '19505'
              , '24317'
              , '13375'
              , '81426'
              , '18270'
              , '72425'
              , '45748'
              , '55307'
              , '53672'
              , '52867'
              , '56575'
              , '39127'
              , '30625'
              , '10445'
              , '39972'
              , '74351'
              , '26065'
              , '83849'
              , '42666'
              , '96976'
              , '68786'
              , '77721'
              , '68908'
              , '66864'
              , '63792'
              , '51650'
              , '31029'
              , '26689'
              , '66708'
              , '11376'
              , '20004'
              , '31880'
              , '96451'
              , '41248'
              , '94898'
              , '18383'
              , '60576'
              , '38193'
              , '48583'
              , '13595'
              , '76614'
              , '24671'
              , '46820'
              , '82276'
              , '10516'
              , '11634'
              , '45549'
              , '88885'
              , '18842'
              , '90225'
              , '18906'
              , '13376'
              , '84935'
              , '78890'
              , '58943'
              , '15765'
              , '50016'
              , '69035'
              , '49448'
              , '39371'
              , '41368'
              , '33123'
              , '83144'
              , '14089'
              , '94945'
              , '73241'
              , '19769'
              , '47537'
              , '38122'
              , '28587'
              , '76698'
              , '22927'
              , '56616'
              , '34425'
              , '96576'
              , '78567'
              , '97789'
              , '94983'
              , '79077'
              , '57855'
              , '97189'
              , '46081'
              , '48033'
              , '19849'
              , '28488'
              , '28545'
              , '72151'
              , '69952'
              , '43285'
              , '26105'
              , '76231'
              , '15723'
              , '25486'
              , '39861'
              , '83933'
              , '75691'
              , '46136'
              , '61547'
              , '66162'
              , '25858'
              , '22246'
              , '51949'
              , '27385'
              , '77610'
              , '34322'
              , '51061'
              , '68100'
              , '61860'
              , '13695'
              , '44438'
              , '90578'
              , '96888'
              , '58048'
              , '99543'
              , '73171'
              , '56691'
              , '64528'
              , '56910'
              , '83444'
              , '30122'
              , '68014'
              , '14171'
              , '16807'
              , '83041'
              , '34102'
              , '51103'
              , '79777'
              , '17871'
              , '12305'
              , '22685'
              , '94167'
              , '28709'
              , '35258'
              , '57665'
              , '71256'
              , '57047'
              , '11489'
              , '31387'
              , '68341'
              , '78451'
              , '14867'
              , '25103'
              , '35458'
              , '25003'
              , '54364'
              , '73520'
              , '32213'
              , '35576'))
      )       INTERSECT (
         SELECT ca_zip
         FROM
           (
            SELECT
              substr(ca_zip, 1, 5) ca_zip
            , count(*) cnt
            FROM
              customer_address
            , customer
            WHERE (ca_address_sk = c_current_addr_sk)
               AND (c_preferred_cust_flag = 'Y')
            GROUP BY ca_zip
            HAVING (count(*) > 10)
         )  a1
      )    )  a2
)  v1
WHERE (ss_store_sk = s_store_sk)
   AND (ss_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 1998)
   AND (substr(s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
GROUP BY s_store_name;
"""
    def query8_1 = """
SELECT
  s_store_name
, sum(ss_net_profit)
FROM
  store_sales
, date_dim
, store
, (
   SELECT ca_zip
   FROM
     (
(
         SELECT substr(ca_zip, 1, 5) ca_zip
         FROM
           customer_address
         WHERE (substr(ca_zip, 1, 5) IN (
                '24128'
              , '57834'
              , '13354'
              , '15734'
              , '78668'
              , '76232'
              , '62878'
              , '45375'
              , '63435'
              , '22245'
              , '65084'
              , '49130'
              , '40558'
              , '25733'
              , '15798'
              , '87816'
              , '81096'
              , '56458'
              , '35474'
              , '27156'
              , '83926'
              , '18840'
              , '28286'
              , '24676'
              , '37930'
              , '77556'
              , '27700'
              , '45266'
              , '94627'
              , '62971'
              , '20548'
              , '23470'
              , '47305'
              , '53535'
              , '21337'
              , '26231'
              , '50412'
              , '69399'
              , '17879'
              , '51622'
              , '43848'
              , '21195'
              , '83921'
              , '15559'
              , '67853'
              , '15126'
              , '16021'
              , '26233'
              , '53268'
              , '10567'
              , '91137'
              , '76107'
              , '11101'
              , '59166'
              , '38415'
              , '61265'
              , '71954'
              , '15371'
              , '11928'
              , '15455'
              , '98294'
              , '68309'
              , '69913'
              , '59402'
              , '58263'
              , '25782'
              , '18119'
              , '35942'
              , '33282'
              , '42029'
              , '17920'
              , '98359'
              , '15882'
              , '45721'
              , '60279'
              , '18426'
              , '64544'
              , '25631'
              , '43933'
              , '37125'
              , '98235'
              , '10336'
              , '24610'
              , '68101'
              , '56240'
              , '40081'
              , '86379'
              , '44165'
              , '33515'
              , '88190'
              , '84093'
              , '27068'
              , '99076'
              , '36634'
              , '50308'
              , '28577'
              , '39736'
              , '33786'
              , '71286'
              , '26859'
              , '55565'
              , '98569'
              , '70738'
              , '19736'
              , '64457'
              , '17183'
              , '28915'
              , '26653'
              , '58058'
              , '89091'
              , '54601'
              , '24206'
              , '14328'
              , '55253'
              , '82136'
              , '67897'
              , '56529'
              , '72305'
              , '67473'
              , '62377'
              , '22752'
              , '57647'
              , '62496'
              , '41918'
              , '36233'
              , '86284'
              , '54917'
              , '22152'
              , '19515'
              , '63837'
              , '18376'
              , '42961'
              , '10144'
              , '36495'
              , '58078'
              , '38607'
              , '91110'
              , '64147'
              , '19430'
              , '17043'
              , '45200'
              , '63981'
              , '48425'
              , '22351'
              , '30010'
              , '21756'
              , '14922'
              , '14663'
              , '77191'
              , '60099'
              , '29741'
              , '36420'
              , '21076'
              , '91393'
              , '28810'
              , '96765'
              , '23006'
              , '18799'
              , '49156'
              , '98025'
              , '23932'
              , '67467'
              , '30450'
              , '50298'
              , '29178'
              , '89360'
              , '32754'
              , '63089'
              , '87501'
              , '87343'
              , '29839'
              , '30903'
              , '81019'
              , '18652'
              , '73273'
              , '25989'
              , '20260'
              , '68893'
              , '53179'
              , '30469'
              , '28898'
              , '31671'
              , '24996'
              , '18767'
              , '64034'
              , '91068'
              , '51798'
              , '51200'
              , '63193'
              , '39516'
              , '72550'
              , '72325'
              , '51211'
              , '23968'
              , '86057'
              , '10390'
              , '85816'
              , '45692'
              , '65164'
              , '21309'
              , '18845'
              , '68621'
              , '92712'
              , '68880'
              , '90257'
              , '47770'
              , '13955'
              , '70466'
              , '21286'
              , '67875'
              , '82636'
              , '36446'
              , '79994'
              , '72823'
              , '40162'
              , '41367'
              , '41766'
              , '22437'
              , '58470'
              , '11356'
              , '76638'
              , '68806'
              , '25280'
              , '67301'
              , '73650'
              , '86198'
              , '16725'
              , '38935'
              , '13394'
              , '61810'
              , '81312'
              , '15146'
              , '71791'
              , '31016'
              , '72013'
              , '37126'
              , '22744'
              , '73134'
              , '70372'
              , '30431'
              , '39192'
              , '35850'
              , '56571'
              , '67030'
              , '22461'
              , '88424'
              , '88086'
              , '14060'
              , '40604'
              , '19512'
              , '72175'
              , '51649'
              , '19505'
              , '24317'
              , '13375'
              , '81426'
              , '18270'
              , '72425'
              , '45748'
              , '55307'
              , '53672'
              , '52867'
              , '56575'
              , '39127'
              , '30625'
              , '10445'
              , '39972'
              , '74351'
              , '26065'
              , '83849'
              , '42666'
              , '96976'
              , '68786'
              , '77721'
              , '68908'
              , '66864'
              , '63792'
              , '51650'
              , '31029'
              , '26689'
              , '66708'
              , '11376'
              , '20004'
              , '31880'
              , '96451'
              , '41248'
              , '94898'
              , '18383'
              , '60576'
              , '38193'
              , '48583'
              , '13595'
              , '76614'
              , '24671'
              , '46820'
              , '82276'
              , '10516'
              , '11634'
              , '45549'
              , '88885'
              , '18842'
              , '90225'
              , '18906'
              , '13376'
              , '84935'
              , '78890'
              , '58943'
              , '15765'
              , '50016'
              , '69035'
              , '49448'
              , '39371'
              , '41368'
              , '33123'
              , '83144'
              , '14089'
              , '94945'
              , '73241'
              , '19769'
              , '47537'
              , '38122'
              , '28587'
              , '76698'
              , '22927'
              , '56616'
              , '34425'
              , '96576'
              , '78567'
              , '97789'
              , '94983'
              , '79077'
              , '57855'
              , '97189'
              , '46081'
              , '48033'
              , '19849'
              , '28488'
              , '28545'
              , '72151'
              , '69952'
              , '43285'
              , '26105'
              , '76231'
              , '15723'
              , '25486'
              , '39861'
              , '83933'
              , '75691'
              , '46136'
              , '61547'
              , '66162'
              , '25858'
              , '22246'
              , '51949'
              , '27385'
              , '77610'
              , '34322'
              , '51061'
              , '68100'
              , '61860'
              , '13695'
              , '44438'
              , '90578'
              , '96888'
              , '58048'
              , '99543'
              , '73171'
              , '56691'
              , '64528'
              , '56910'
              , '83444'
              , '30122'
              , '68014'
              , '14171'
              , '16807'
              , '83041'
              , '34102'
              , '51103'
              , '79777'
              , '17871'
              , '12305'
              , '22685'
              , '94167'
              , '28709'
              , '35258'
              , '57665'
              , '71256'
              , '57047'
              , '11489'
              , '31387'
              , '68341'
              , '78451'
              , '14867'
              , '25103'
              , '35458'
              , '25003'
              , '54364'
              , '73520'
              , '32213'
              , '35576'))
      )       INTERSECT (
         SELECT ca_zip
         FROM
           (
            SELECT
              substr(ca_zip, 1, 5) ca_zip
            , count(*) cnt
            FROM
              customer_address
            , customer
            WHERE (ca_address_sk = c_current_addr_sk)
               AND (c_preferred_cust_flag = 'Y')
            GROUP BY ca_zip
            HAVING (count(*) > 10)
         )  a1
      )    )  a2
)  v1
WHERE (ss_store_sk = s_store_sk)
   AND (ss_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 1998)
   AND (substr(s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
GROUP BY s_store_name
ORDER BY s_store_name ASC
LIMIT 100;
"""
    order_qt_query8_1_before "${query8_1}"
    async_mv_rewrite_fail(db, mv8_1, query8_1, "mv8_1")
    order_qt_query8_1_after "${query8_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_1"""

    def mv9 = """
SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 20)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 21 AND 40)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 41 AND 60)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 61 AND 80)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 81 AND 100)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) END) bucket5
FROM
  reason
WHERE (r_reason_sk = 1);
"""
    def query9 = """
SELECT
  (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 1 AND 20)
   ) > 74129) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 1 AND 20)
) END) bucket1
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 21 AND 40)
   ) > 122840) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 40)
) END) bucket2
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 41 AND 60)
   ) > 56580) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 41 AND 60)
) END) bucket3
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 61 AND 80)
   ) > 10097) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 61 AND 80)
) END) bucket4
, (CASE WHEN ((
      SELECT count(*)
      FROM
        store_sales
      WHERE (ss_quantity BETWEEN 81 AND 100)
   ) > 165306) THEN (
   SELECT avg(ss_ext_discount_amt)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) ELSE (
   SELECT avg(ss_net_paid)
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 81 AND 100)
) END) bucket5
FROM
  reason
WHERE (r_reason_sk = 1);
"""
    order_qt_query9_before "${query9}"
    async_mv_rewrite_fail(db, mv9, query9, "mv9")
    order_qt_query9_after "${query9}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9"""


    def mv10 = """
SELECT
  cd_gender
, cd_marital_status
, cd_education_status
, count(*) cnt1
, cd_purchase_estimate
, count(*) cnt2
, cd_credit_rating
, count(*) cnt3
, cd_dep_count
, count(*) cnt4
, cd_dep_employed_count
, count(*) cnt5
, cd_dep_college_count
, count(*) cnt6
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'))
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (d_moy BETWEEN 1 AND (1 + 3))
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE (c.c_customer_sk = ws_bill_customer_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_moy BETWEEN 1 AND (1 + 3))
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE (c.c_customer_sk = cs_ship_customer_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_moy BETWEEN 1 AND (1 + 3))
   )))
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY cd_gender ASC, cd_marital_status ASC, cd_education_status ASC, cd_purchase_estimate ASC, cd_credit_rating ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
LIMIT 100;
"""
    def query10 = """
SELECT
  cd_gender
, cd_marital_status
, cd_education_status
, count(*) cnt1
, cd_purchase_estimate
, count(*) cnt2
, cd_credit_rating
, count(*) cnt3
, cd_dep_count
, count(*) cnt4
, cd_dep_employed_count
, count(*) cnt5
, cd_dep_college_count
, count(*) cnt6
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'))
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (d_moy BETWEEN 1 AND (1 + 3))
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE (c.c_customer_sk = ws_bill_customer_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_moy BETWEEN 1 AND (1 + 3))
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE (c.c_customer_sk = cs_ship_customer_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_moy BETWEEN 1 AND (1 + 3))
   )))
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY cd_gender ASC, cd_marital_status ASC, cd_education_status ASC, cd_purchase_estimate ASC, cd_credit_rating ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
LIMIT 100;
"""
    order_qt_query10_before "${query10}"
    async_mv_rewrite_fail(db, mv10, query10, "mv10")
    order_qt_query10_after "${query10}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10"""

    def mv10_1 = """
SELECT
  cd_gender
, cd_marital_status
, cd_education_status
, count(*) cnt1
, cd_purchase_estimate
, count(*) cnt2
, cd_credit_rating
, count(*) cnt3
, cd_dep_count
, count(*) cnt4
, cd_dep_employed_count
, count(*) cnt5
, cd_dep_college_count
, count(*) cnt6
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'))
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (d_moy BETWEEN 1 AND (1 + 3))
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE (c.c_customer_sk = ws_bill_customer_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_moy BETWEEN 1 AND (1 + 3))
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE (c.c_customer_sk = cs_ship_customer_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_moy BETWEEN 1 AND (1 + 3))
   )))
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;

"""
    def query10_1 = """
SELECT
  cd_gender
, cd_marital_status
, cd_education_status
, count(*) cnt1
, cd_purchase_estimate
, count(*) cnt2
, cd_credit_rating
, count(*) cnt3
, cd_dep_count
, count(*) cnt4
, cd_dep_employed_count
, count(*) cnt5
, cd_dep_college_count
, count(*) cnt6
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'))
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (d_moy BETWEEN 1 AND (1 + 3))
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE (c.c_customer_sk = ws_bill_customer_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_moy BETWEEN 1 AND (1 + 3))
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE (c.c_customer_sk = cs_ship_customer_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_moy BETWEEN 1 AND (1 + 3))
   )))
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY cd_gender ASC, cd_marital_status ASC, cd_education_status ASC, cd_purchase_estimate ASC, cd_credit_rating ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
LIMIT 100;
"""
    order_qt_query10_1_before "${query10_1}"
    async_mv_rewrite_fail(db, mv10_1, query10_1, "mv10_1")
    order_qt_query10_1_after "${query10_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10_1"""

    def mv11 = """
WITH
  year_total AS (
   SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum((ss_ext_list_price - ss_ext_discount_amt)) year_total
   , 's' sale_type
   FROM
     customer
   , store_sales
   , date_dim
   WHERE (c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
UNION ALL    SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum((ws_ext_list_price - ws_ext_discount_amt)) year_total
   , 'w' sale_type
   FROM
     customer
   , web_sales
   , date_dim
   WHERE (c_customer_sk = ws_bill_customer_sk)
      AND (ws_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
)
SELECT
  t_s_secyear.customer_id
, t_s_secyear.customer_first_name
, t_s_secyear.customer_last_name
, t_s_secyear.customer_preferred_cust_flag
, t_s_secyear.customer_birth_country
, t_s_secyear.customer_login
FROM
  year_total t_s_firstyear
, year_total t_s_secyear
, year_total t_w_firstyear
, year_total t_w_secyear
WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
   AND (t_s_firstyear.sale_type = 's')
   AND (t_w_firstyear.sale_type = 'w')
   AND (t_s_secyear.sale_type = 's')
   AND (t_w_secyear.sale_type = 'w')
   AND (t_s_firstyear.dyear = 2001)
   AND (t_s_secyear.dyear = (2001 + 1))
   AND (t_w_firstyear.dyear = 2001)
   AND (t_w_secyear.dyear = (2001 + 1))
   AND (t_s_firstyear.year_total > 0)
   AND (t_w_firstyear.year_total > 0)
   AND ((CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL(2,1)) END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL(2,1)) END))
ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
LIMIT 100;
"""
    def query11 = """
WITH
  year_total AS (
   SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum((ss_ext_list_price - ss_ext_discount_amt)) year_total
   , 's' sale_type
   FROM
     customer
   , store_sales
   , date_dim
   WHERE (c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
UNION ALL    SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , c_preferred_cust_flag customer_preferred_cust_flag
   , c_birth_country customer_birth_country
   , c_login customer_login
   , c_email_address customer_email_address
   , d_year dyear
   , sum((ws_ext_list_price - ws_ext_discount_amt)) year_total
   , 'w' sale_type
   FROM
     customer
   , web_sales
   , date_dim
   WHERE (c_customer_sk = ws_bill_customer_sk)
      AND (ws_sold_date_sk = d_date_sk)
   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
)
SELECT
  t_s_secyear.customer_id
, t_s_secyear.customer_first_name
, t_s_secyear.customer_last_name
, t_s_secyear.customer_preferred_cust_flag
, t_s_secyear.customer_birth_country
, t_s_secyear.customer_login
FROM
  year_total t_s_firstyear
, year_total t_s_secyear
, year_total t_w_firstyear
, year_total t_w_secyear
WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
   AND (t_s_firstyear.sale_type = 's')
   AND (t_w_firstyear.sale_type = 'w')
   AND (t_s_secyear.sale_type = 's')
   AND (t_w_secyear.sale_type = 'w')
   AND (t_s_firstyear.dyear = 2001)
   AND (t_s_secyear.dyear = (2001 + 1))
   AND (t_w_firstyear.dyear = 2001)
   AND (t_w_secyear.dyear = (2001 + 1))
   AND (t_s_firstyear.year_total > 0)
   AND (t_w_firstyear.year_total > 0)
   AND ((CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL(2,1)) END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL(2,1)) END))
ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
LIMIT 100;
"""
    order_qt_query11_before "${query11}"
    async_mv_rewrite_fail(db, mv11, query11, "mv11")
    order_qt_query11_after "${query11}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv11"""


    def mv12 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(ws_ext_sales_price) itemrevenue
, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  web_sales
, item
, date_dim
WHERE (ws_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (ws_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
LIMIT 100;
"""
    def query12 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(ws_ext_sales_price) itemrevenue
, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  web_sales
, item
, date_dim
WHERE (ws_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (ws_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
LIMIT 100;
"""
    order_qt_query12_before "${query12}"
    async_mv_rewrite_fail(db, mv12, query12, "mv12")
    order_qt_query12_after "${query12}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv12"""

    def mv12_1 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(ws_ext_sales_price) itemrevenue
, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  web_sales
, item
, date_dim
WHERE (ws_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (ws_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price;
"""
    def query12_1 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(ws_ext_sales_price) itemrevenue
, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  web_sales
, item
, date_dim
WHERE (ws_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (ws_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
LIMIT 100;
"""
    order_qt_query12_1_before "${query12_1}"
    async_mv_rewrite_fail(db, mv12_1, query12_1, "mv12_1")
    order_qt_query12_1_after "${query12_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv12_1"""

    def mv13 = """
SELECT
  avg(ss_quantity)
, avg(ss_ext_sales_price)
, avg(ss_ext_wholesale_cost)
, sum(ss_ext_wholesale_cost)
FROM
  store_sales
, store
, customer_demographics
, household_demographics
, customer_address
, date_dim
WHERE (s_store_sk = ss_store_sk)
   AND (ss_sold_date_sk = d_date_sk)
   AND (d_year = 2001)
   AND (((ss_hdemo_sk = hd_demo_sk)
         AND (cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'M')
         AND (cd_education_status = 'Advanced Degree')
         AND (ss_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2)))
         AND (hd_dep_count = 3))
      OR ((ss_hdemo_sk = hd_demo_sk)
         AND (cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'S')
         AND (cd_education_status = 'College')
         AND (ss_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2)))
         AND (hd_dep_count = 1))
      OR ((ss_hdemo_sk = hd_demo_sk)
         AND (cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'W')
         AND (cd_education_status = '2 yr Degree')
         AND (ss_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))
         AND (hd_dep_count = 1)))
   AND (((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('TX'      , 'OH'      , 'TX'))
         AND (ss_net_profit BETWEEN 100 AND 200))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('OR'      , 'NM'      , 'KY'))
         AND (ss_net_profit BETWEEN 150 AND 300))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('VA'      , 'TX'      , 'MS'))
         AND (ss_net_profit BETWEEN 50 AND 250)));
"""
    def query13 = """
SELECT
  avg(ss_quantity)
, avg(ss_ext_sales_price)
, avg(ss_ext_wholesale_cost)
, sum(ss_ext_wholesale_cost)
FROM
  store_sales
, store
, customer_demographics
, household_demographics
, customer_address
, date_dim
WHERE (s_store_sk = ss_store_sk)
   AND (ss_sold_date_sk = d_date_sk)
   AND (d_year = 2001)
   AND (((ss_hdemo_sk = hd_demo_sk)
         AND (cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'M')
         AND (cd_education_status = 'Advanced Degree')
         AND (ss_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2)))
         AND (hd_dep_count = 3))
      OR ((ss_hdemo_sk = hd_demo_sk)
         AND (cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'S')
         AND (cd_education_status = 'College')
         AND (ss_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2)))
         AND (hd_dep_count = 1))
      OR ((ss_hdemo_sk = hd_demo_sk)
         AND (cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'W')
         AND (cd_education_status = '2 yr Degree')
         AND (ss_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))
         AND (hd_dep_count = 1)))
   AND (((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('TX'      , 'OH'      , 'TX'))
         AND (ss_net_profit BETWEEN 100 AND 200))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('OR'      , 'NM'      , 'KY'))
         AND (ss_net_profit BETWEEN 150 AND 300))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('VA'      , 'TX'      , 'MS'))
         AND (ss_net_profit BETWEEN 50 AND 250)));
"""
    order_qt_query13_before "${query13}"
    // Should success
    async_mv_rewrite_fail(db, mv13, query13, "mv13")
    order_qt_query13_after "${query13}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13"""


    def mv14 = """
WITH
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
   )  x
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
   ) y
)
SELECT *
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
      AND (d_week_seq = (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_year = (1999 + 1))
            AND (d_moy = 12)
            AND (d_dom = 11)
      ))
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  this_year
, (
   SELECT
     'store' channel1
   , i_brand_id i_brand_id1
   , i_class_id i_class_id1
   , i_category_id i_category_id1
   , sum((ss_quantity * ss_list_price)) sales1
   , count(*) number_sales1
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
      AND (d_week_seq = (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_year = 1999)
            AND (d_moy = 12)
            AND (d_dom = 11)
      ))
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  last_year
WHERE (this_year.i_brand_id = last_year.i_brand_id1)
   AND (this_year.i_class_id = last_year.i_class_id1)
   AND (this_year.i_category_id = last_year.i_category_id1)
ORDER BY this_year.channel ASC, this_year.i_brand_id ASC, this_year.i_class_id ASC, this_year.i_category_id ASC
LIMIT 100;
"""
    def query14 = """
WITH
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
   )  x
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
   ) y
)
SELECT *
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
      AND (d_week_seq = (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_year = (1999 + 1))
            AND (d_moy = 12)
            AND (d_dom = 11)
      ))
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  this_year
, (
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
      AND (d_week_seq = (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_year = 1999)
            AND (d_moy = 12)
            AND (d_dom = 11)
      ))
   GROUP BY i_brand_id, i_class_id, i_category_id
   HAVING (sum((ss_quantity * ss_list_price)) > (
         SELECT average_sales
         FROM
           avg_sales
      ))
)  last_year
WHERE (this_year.i_brand_id = last_year.i_brand_id)
   AND (this_year.i_class_id = last_year.i_class_id)
   AND (this_year.i_category_id = last_year.i_category_id)
ORDER BY this_year.channel ASC, this_year.i_brand_id ASC, this_year.i_class_id ASC, this_year.i_category_id ASC
LIMIT 100;

"""
    order_qt_query14_before "${query14}"
    async_mv_rewrite_fail(db, mv14, query14, "mv14")
    order_qt_query14_after "${query14}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv14"""

    def mv15 = """
SELECT
  ca_zip
, sum(cs_sales_price)
FROM
  catalog_sales
, customer
, customer_address
, date_dim
WHERE (cs_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
      OR (cs_sales_price > 500))
   AND (cs_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip
ORDER BY ca_zip ASC
LIMIT 100;
"""
    def query15 = """
SELECT
  ca_zip
, sum(cs_sales_price)
FROM
  catalog_sales
, customer
, customer_address
, date_dim
WHERE (cs_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
      OR (cs_sales_price > 500))
   AND (cs_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip
ORDER BY ca_zip ASC
LIMIT 100;
"""
    order_qt_query15_before "${query15}"
    async_mv_rewrite_fail(db, mv15, query15, "mv15")
    order_qt_query15_after "${query15}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15"""

    def mv15_1 = """
SELECT
  ca_zip
, sum(cs_sales_price)
FROM
  catalog_sales
, customer
, customer_address
, date_dim
WHERE (cs_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
      OR (cs_sales_price > 500))
   AND (cs_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip;
"""
    def query15_1 = """
SELECT
  ca_zip
, sum(cs_sales_price)
FROM
  catalog_sales
, customer
, customer_address
, date_dim
WHERE (cs_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
      OR (cs_sales_price > 500))
   AND (cs_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip
ORDER BY ca_zip ASC
LIMIT 100;
"""
    order_qt_query15_1_before "${query15_1}"
    async_mv_rewrite_success(db, mv15_1, query15_1, "mv15_1")
    order_qt_query15_1_after "${query15_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15_1"""

    def mv16 = """
SELECT
  count(DISTINCT cs_order_number) 'order count'
, sum(cs_ext_ship_cost) 'total shipping cost'
, sum(cs_net_profit) 'total net profit'
FROM
  catalog_sales cs1
, date_dim
, customer_address
, call_center
WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (cs1.cs_ship_date_sk = d_date_sk)
   AND (cs1.cs_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'GA')
   AND (cs1.cs_call_center_sk = cc_call_center_sk)
   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
   AND (EXISTS (
   SELECT *
   FROM
     catalog_sales cs2
   WHERE (cs1.cs_order_number = cs2.cs_order_number)
      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     catalog_returns cr1
   WHERE (cs1.cs_order_number = cr1.cr_order_number)
)))
ORDER BY count(DISTINCT cs_order_number) ASC
LIMIT 100;
"""
    def query16 = """
SELECT
  count(DISTINCT cs_order_number) 'order count'
, sum(cs_ext_ship_cost) 'total shipping cost'
, sum(cs_net_profit) 'total net profit'
FROM
  catalog_sales cs1
, date_dim
, customer_address
, call_center
WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (cs1.cs_ship_date_sk = d_date_sk)
   AND (cs1.cs_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'GA')
   AND (cs1.cs_call_center_sk = cc_call_center_sk)
   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
   AND (EXISTS (
   SELECT *
   FROM
     catalog_sales cs2
   WHERE (cs1.cs_order_number = cs2.cs_order_number)
      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     catalog_returns cr1
   WHERE (cs1.cs_order_number = cr1.cr_order_number)
)))
ORDER BY count(DISTINCT cs_order_number) ASC
LIMIT 100;
"""
    order_qt_query16_before "${query16}"
    async_mv_rewrite_fail(db, mv16, query16, "mv16")
    order_qt_query16_after "${query16}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16"""

    def mv16_1 = """
SELECT
  count(DISTINCT cs_order_number) 'order count'
, sum(cs_ext_ship_cost) 'total shipping cost'
, sum(cs_net_profit) 'total net profit'
FROM
  catalog_sales cs1
, date_dim
, customer_address
, call_center
WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (cs1.cs_ship_date_sk = d_date_sk)
   AND (cs1.cs_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'GA')
   AND (cs1.cs_call_center_sk = cc_call_center_sk)
   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
   AND (EXISTS (
   SELECT *
   FROM
     catalog_sales cs2
   WHERE (cs1.cs_order_number = cs2.cs_order_number)
      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     catalog_returns cr1
   WHERE (cs1.cs_order_number = cr1.cr_order_number)
)));
"""
    def query16_1 = """
SELECT
  count(DISTINCT cs_order_number) 'order count'
, sum(cs_ext_ship_cost) 'total shipping cost'
, sum(cs_net_profit) 'total net profit'
FROM
  catalog_sales cs1
, date_dim
, customer_address
, call_center
WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (cs1.cs_ship_date_sk = d_date_sk)
   AND (cs1.cs_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'GA')
   AND (cs1.cs_call_center_sk = cc_call_center_sk)
   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
   AND (EXISTS (
   SELECT *
   FROM
     catalog_sales cs2
   WHERE (cs1.cs_order_number = cs2.cs_order_number)
      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     catalog_returns cr1
   WHERE (cs1.cs_order_number = cr1.cr_order_number)
)))
ORDER BY count(DISTINCT cs_order_number) ASC
LIMIT 100;
"""
    order_qt_query16_1_before "${query16_1}"
    async_mv_rewrite_success(db, mv16_1, query16_1, "mv16_1")
    order_qt_query16_1_after "${query16_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16_1"""

    def mv17 = """
SELECT
  i_item_id
, i_item_desc
, s_state
, count(ss_quantity) store_sales_quantitycount
, avg(ss_quantity) store_sales_quantityave
, stddev_samp(ss_quantity) store_sales_quantitystdev
, (stddev_samp(ss_quantity) / avg(ss_quantity)) store_sales_quantitycov
, count(sr_return_quantity) store_returns_quantitycount
, avg(sr_return_quantity) store_returns_quantityave
, stddev_samp(sr_return_quantity) store_returns_quantitystdev
, (stddev_samp(sr_return_quantity) / avg(sr_return_quantity)) store_returns_quantitycov
, count(cs_quantity) catalog_sales_quantitycount
, avg(cs_quantity) catalog_sales_quantityave
, stddev_samp(cs_quantity) catalog_sales_quantitystdev
, (stddev_samp(cs_quantity) / avg(cs_quantity)) catalog_sales_quantitycov
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_quarter_name = '2001Q1')
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
GROUP BY i_item_id, i_item_desc, s_state
ORDER BY i_item_id ASC, i_item_desc ASC, s_state ASC
LIMIT 100;
"""
    def query17 = """
SELECT
  i_item_id
, i_item_desc
, s_state
, count(ss_quantity) store_sales_quantitycount
, avg(ss_quantity) store_sales_quantityave
, stddev_samp(ss_quantity) store_sales_quantitystdev
, (stddev_samp(ss_quantity) / avg(ss_quantity)) store_sales_quantitycov
, count(sr_return_quantity) store_returns_quantitycount
, avg(sr_return_quantity) store_returns_quantityave
, stddev_samp(sr_return_quantity) store_returns_quantitystdev
, (stddev_samp(sr_return_quantity) / avg(sr_return_quantity)) store_returns_quantitycov
, count(cs_quantity) catalog_sales_quantitycount
, avg(cs_quantity) catalog_sales_quantityave
, stddev_samp(cs_quantity) catalog_sales_quantitystdev
, (stddev_samp(cs_quantity) / avg(cs_quantity)) catalog_sales_quantitycov
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_quarter_name = '2001Q1')
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
GROUP BY i_item_id, i_item_desc, s_state
ORDER BY i_item_id ASC, i_item_desc ASC, s_state ASC
LIMIT 100;
"""
    order_qt_query17_before "${query17}"
    async_mv_rewrite_fail(db, mv17, query17, "mv17")
    order_qt_query17_after "${query17}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17"""

    def mv17_1 = """
SELECT
  i_item_id
, i_item_desc
, s_state
, count(ss_quantity) store_sales_quantitycount
, avg(ss_quantity) store_sales_quantityave
, stddev_samp(ss_quantity) store_sales_quantitystdev
, (stddev_samp(ss_quantity) / avg(ss_quantity)) store_sales_quantitycov
, count(sr_return_quantity) store_returns_quantitycount
, avg(sr_return_quantity) store_returns_quantityave
, stddev_samp(sr_return_quantity) store_returns_quantitystdev
, (stddev_samp(sr_return_quantity) / avg(sr_return_quantity)) store_returns_quantitycov
, count(cs_quantity) catalog_sales_quantitycount
, avg(cs_quantity) catalog_sales_quantityave
, stddev_samp(cs_quantity) catalog_sales_quantitystdev
, (stddev_samp(cs_quantity) / avg(cs_quantity)) catalog_sales_quantitycov
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_quarter_name = '2001Q1')
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
GROUP BY i_item_id, i_item_desc, s_state;
"""
    def query17_1 = """
SELECT
  i_item_id
, i_item_desc
, s_state
, count(ss_quantity) store_sales_quantitycount
, avg(ss_quantity) store_sales_quantityave
, stddev_samp(ss_quantity) store_sales_quantitystdev
, (stddev_samp(ss_quantity) / avg(ss_quantity)) store_sales_quantitycov
, count(sr_return_quantity) store_returns_quantitycount
, avg(sr_return_quantity) store_returns_quantityave
, stddev_samp(sr_return_quantity) store_returns_quantitystdev
, (stddev_samp(sr_return_quantity) / avg(sr_return_quantity)) store_returns_quantitycov
, count(cs_quantity) catalog_sales_quantitycount
, avg(cs_quantity) catalog_sales_quantityave
, stddev_samp(cs_quantity) catalog_sales_quantitystdev
, (stddev_samp(cs_quantity) / avg(cs_quantity)) catalog_sales_quantitycov
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_quarter_name = '2001Q1')
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
GROUP BY i_item_id, i_item_desc, s_state
ORDER BY i_item_id ASC, i_item_desc ASC, s_state ASC
LIMIT 100;
"""
    order_qt_query17_1_before "${query17_1}"
    async_mv_rewrite_success(db, mv17_1, query17_1, "mv17_1")
    order_qt_query17_1_after "${query17_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17_1"""

    def mv18 = """
SELECT
  i_item_id
, ca_country
, ca_state
, ca_county
, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
FROM
  catalog_sales
, customer_demographics cd1
, customer_demographics cd2
, customer
, customer_address
, date_dim
, item
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
   AND (cs_bill_customer_sk = c_customer_sk)
   AND (cd1.cd_gender = 'F')
   AND (cd1.cd_education_status = 'Unknown')
   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
   AND (d_year = 1998)
   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country ASC, ca_state ASC, ca_county ASC, i_item_id ASC
LIMIT 100;
"""
    def query18 = """
SELECT
  i_item_id
, ca_country
, ca_state
, ca_county
, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
FROM
  catalog_sales
, customer_demographics cd1
, customer_demographics cd2
, customer
, customer_address
, date_dim
, item
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
   AND (cs_bill_customer_sk = c_customer_sk)
   AND (cd1.cd_gender = 'F')
   AND (cd1.cd_education_status = 'Unknown')
   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
   AND (d_year = 1998)
   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country ASC, ca_state ASC, ca_county ASC, i_item_id ASC
LIMIT 100;
"""
    order_qt_query18_before "${query18}"
    async_mv_rewrite_fail(db, mv18, query18, "mv18")
    order_qt_query18_after "${query18}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18"""

    def mv18_1 = """
SELECT
  i_item_id
, ca_country
, ca_state
, ca_county
, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
FROM
  catalog_sales
, customer_demographics cd1
, customer_demographics cd2
, customer
, customer_address
, date_dim
, item
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
   AND (cs_bill_customer_sk = c_customer_sk)
   AND (cd1.cd_gender = 'F')
   AND (cd1.cd_education_status = 'Unknown')
   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
   AND (d_year = 1998)
   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county);
"""
    def query18_1 = """
SELECT
  i_item_id
, ca_country
, ca_state
, ca_county
, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
FROM
  catalog_sales
, customer_demographics cd1
, customer_demographics cd2
, customer
, customer_address
, date_dim
, item
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
   AND (cs_bill_customer_sk = c_customer_sk)
   AND (cd1.cd_gender = 'F')
   AND (cd1.cd_education_status = 'Unknown')
   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
   AND (d_year = 1998)
   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country ASC, ca_state ASC, ca_county ASC, i_item_id ASC
LIMIT 100;
"""
    order_qt_query18_1_before "${query18_1}"
    // should success but not
    async_mv_rewrite_fail(db, mv18_1, query18_1, "mv18_1")
    order_qt_query18_1_after "${query18_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_1"""

    def mv19 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, i_manufact_id
, i_manufact
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
, customer
, customer_address
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 8)
   AND (d_moy = 11)
   AND (d_year = 1998)
   AND (ss_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
   AND (ss_store_sk = s_store_sk)
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand ASC, i_brand_id ASC, i_manufact_id ASC, i_manufact ASC
LIMIT 100;
"""
    def query19 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, i_manufact_id
, i_manufact
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
, customer
, customer_address
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 8)
   AND (d_moy = 11)
   AND (d_year = 1998)
   AND (ss_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
   AND (ss_store_sk = s_store_sk)
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand ASC, i_brand_id ASC, i_manufact_id ASC, i_manufact ASC
LIMIT 100;
"""
    order_qt_query19_before "${query19}"
    async_mv_rewrite_fail(db, mv19, query19, "mv19")
    order_qt_query19_after "${query19}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19"""

    def mv19_1 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, i_manufact_id
, i_manufact
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
, customer
, customer_address
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 8)
   AND (d_moy = 11)
   AND (d_year = 1998)
   AND (ss_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
   AND (ss_store_sk = s_store_sk)
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact;
"""
    def query19_1 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, i_manufact_id
, i_manufact
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
, customer
, customer_address
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 8)
   AND (d_moy = 11)
   AND (d_year = 1998)
   AND (ss_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
   AND (ss_store_sk = s_store_sk)
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand ASC, i_brand_id ASC, i_manufact_id ASC, i_manufact ASC
LIMIT 100;
"""
    order_qt_query19_1_before "${query19_1}"
    async_mv_rewrite_success(db, mv19_1, query19_1, "mv19_1")
    order_qt_query19_1_after "${query19_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19_1"""

    def mv20 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(cs_ext_sales_price) itemrevenue
, ((sum(cs_ext_sales_price) * 100) / sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  catalog_sales
, item
, date_dim
WHERE (cs_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (cs_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
LIMIT 100;
"""
    def query20 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(cs_ext_sales_price) itemrevenue
, ((sum(cs_ext_sales_price) * 100) / sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  catalog_sales
, item
, date_dim
WHERE (cs_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (cs_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
LIMIT 100;
"""
    order_qt_query20_before "${query20}"
    async_mv_rewrite_fail(db, mv20, query20, "mv20")
    order_qt_query20_after "${query20}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20"""

    def mv20_1 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(cs_ext_sales_price) itemrevenue
, ((sum(cs_ext_sales_price) * 100) / sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  catalog_sales
, item
, date_dim
WHERE (cs_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (cs_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price;
"""
    def query20_1 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(cs_ext_sales_price) itemrevenue
, ((sum(cs_ext_sales_price) * 100) / sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  catalog_sales
, item
, date_dim
WHERE (cs_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (cs_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
LIMIT 100;
"""
    order_qt_query20_1_before "${query20_1}"
    async_mv_rewrite_fail(db, mv20_1, query20_1, "mv20_1")
    order_qt_query20_1_after "${query20_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20_1"""

    def mv21 = """
SELECT *
FROM
  (
   SELECT
     w_warehouse_name
   , i_item_id
   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
   FROM
     inventory
   , warehouse
   , item
   , date_dim
   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
      AND (i_item_sk = inv_item_sk)
      AND (inv_warehouse_sk = w_warehouse_sk)
      AND (inv_date_sk = d_date_sk)
      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY w_warehouse_name, i_item_id
)  x
WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))))
ORDER BY w_warehouse_name ASC, i_item_id ASC
LIMIT 100;
"""
    def query21 = """
SELECT *
FROM
  (
   SELECT
     w_warehouse_name
   , i_item_id
   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
   FROM
     inventory
   , warehouse
   , item
   , date_dim
   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
      AND (i_item_sk = inv_item_sk)
      AND (inv_warehouse_sk = w_warehouse_sk)
      AND (inv_date_sk = d_date_sk)
      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY w_warehouse_name, i_item_id
)  x
WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))))
ORDER BY w_warehouse_name ASC, i_item_id ASC
LIMIT 100;
"""
    order_qt_query21_before "${query21}"
    async_mv_rewrite_fail(db, mv21, query21, "mv21")
    order_qt_query21_after "${query21}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv21"""

    def mv21_1 = """
SELECT *
FROM
  (
   SELECT
     w_warehouse_name
   , i_item_id
   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
   FROM
     inventory
   , warehouse
   , item
   , date_dim
   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
      AND (i_item_sk = inv_item_sk)
      AND (inv_warehouse_sk = w_warehouse_sk)
      AND (inv_date_sk = d_date_sk)
      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY w_warehouse_name, i_item_id
)  x
WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))));
"""
    def query21_1 = """
SELECT *
FROM
  (
   SELECT
     w_warehouse_name
   , i_item_id
   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
   FROM
     inventory
   , warehouse
   , item
   , date_dim
   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
      AND (i_item_sk = inv_item_sk)
      AND (inv_warehouse_sk = w_warehouse_sk)
      AND (inv_date_sk = d_date_sk)
      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
   GROUP BY w_warehouse_name, i_item_id
)  x
WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))))
ORDER BY w_warehouse_name ASC, i_item_id ASC
LIMIT 100;
"""
    order_qt_query21_1_before "${query21_1}"
    async_mv_rewrite_success(db, mv21_1, query21_1, "mv21_1")
    order_qt_query21_1_after "${query21_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv21_1"""

    def mv22 = """
SELECT
  i_product_name
, i_brand
, i_class
, i_category
, avg(inv_quantity_on_hand) qoh
FROM
  inventory
, date_dim
, item
WHERE (inv_date_sk = d_date_sk)
   AND (inv_item_sk = i_item_sk)
   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC
LIMIT 100;
"""
    def query22 = """
SELECT
  i_product_name
, i_brand
, i_class
, i_category
, avg(inv_quantity_on_hand) qoh
FROM
  inventory
, date_dim
, item
WHERE (inv_date_sk = d_date_sk)
   AND (inv_item_sk = i_item_sk)
   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC
LIMIT 100;
"""
    order_qt_query22_before "${query22}"
    async_mv_rewrite_fail(db, mv22, query22, "mv22")
    order_qt_query22_after "${query22}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv22"""

    def mv22_1 = """
SELECT
  i_product_name
, i_brand
, i_class
, i_category
, avg(inv_quantity_on_hand) qoh
FROM
  inventory
, date_dim
, item
WHERE (inv_date_sk = d_date_sk)
   AND (inv_item_sk = i_item_sk)
   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category);
"""
    def query22_1 = """
SELECT
  i_product_name
, i_brand
, i_class
, i_category
, avg(inv_quantity_on_hand) qoh
FROM
  inventory
, date_dim
, item
WHERE (inv_date_sk = d_date_sk)
   AND (inv_item_sk = i_item_sk)
   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC
LIMIT 100;
"""
    order_qt_query22_1_before "${query22_1}"
    // should success but fail
    async_mv_rewrite_fail(db, mv22_1, query22_1, "mv22_1")
    order_qt_query22_1_after "${query22_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv22_1"""

    def mv23 = """
WITH
  frequent_ss_items AS (
   SELECT
     substr(i_item_desc, 1, 30) itemdesc
   , i_item_sk item_sk
   , d_date solddate
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , item
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_item_sk = i_item_sk)
      AND (d_year IN (2000   , (2000 + 1)   , (2000 + 2)   , (2000 + 3)))
   GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date
   HAVING (count(*) > 4)
)
, max_store_sales AS (
   SELECT max(csales) tpcds_cmax
   FROM
     (
      SELECT
        c_customer_sk
      , sum((ss_quantity * ss_sales_price)) csales
      FROM
        store_sales
      , customer
      , date_dim
      WHERE (ss_customer_sk = c_customer_sk)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year IN (2000      , (2000 + 1)      , (2000 + 2)      , (2000 + 3)))
      GROUP BY c_customer_sk
   ) x
)
, best_ss_customer AS (
   SELECT
     c_customer_sk
   , sum((ss_quantity * ss_sales_price)) ssales
   FROM
     store_sales
   , customer
   WHERE (ss_customer_sk = c_customer_sk)
   GROUP BY c_customer_sk
   HAVING (sum((ss_quantity * ss_sales_price)) > ((50 / CAST('100.0' AS DECIMAL(5,2))) * (
            SELECT *
            FROM
              max_store_sales
         )))
)
SELECT sum(sales)
FROM
  (
   SELECT (cs_quantity * cs_list_price) sales
   FROM
     catalog_sales
   , date_dim
   WHERE (d_year = 2000)
      AND (d_moy = 2)
      AND (cs_sold_date_sk = d_date_sk)
      AND (cs_item_sk IN (
      SELECT item_sk
      FROM
        frequent_ss_items
   ))
      AND (cs_bill_customer_sk IN (
      SELECT c_customer_sk
      FROM
        best_ss_customer
   ))
UNION ALL    SELECT (ws_quantity * ws_list_price) sales
   FROM
     web_sales
   , date_dim
   WHERE (d_year = 2000)
      AND (d_moy = 2)
      AND (ws_sold_date_sk = d_date_sk)
      AND (ws_item_sk IN (
      SELECT item_sk
      FROM
        frequent_ss_items
   ))
      AND (ws_bill_customer_sk IN (
      SELECT c_customer_sk
      FROM
        best_ss_customer
   ))
) y
LIMIT 100;
"""
    def query23 = """
WITH
  frequent_ss_items AS (
   SELECT
     substr(i_item_desc, 1, 30) itemdesc
   , i_item_sk item_sk
   , d_date solddate
   , count(*) cnt
   FROM
     store_sales
   , date_dim
   , item
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_item_sk = i_item_sk)
      AND (d_year IN (2000   , (2000 + 1)   , (2000 + 2)   , (2000 + 3)))
   GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date
   HAVING (count(*) > 4)
)
, max_store_sales AS (
   SELECT max(csales) tpcds_cmax
   FROM
     (
      SELECT
        c_customer_sk
      , sum((ss_quantity * ss_sales_price)) csales
      FROM
        store_sales
      , customer
      , date_dim
      WHERE (ss_customer_sk = c_customer_sk)
         AND (ss_sold_date_sk = d_date_sk)
         AND (d_year IN (2000      , (2000 + 1)      , (2000 + 2)      , (2000 + 3)))
      GROUP BY c_customer_sk
   ) x
)
, best_ss_customer AS (
   SELECT
     c_customer_sk
   , sum((ss_quantity * ss_sales_price)) ssales
   FROM
     store_sales
   , customer
   WHERE (ss_customer_sk = c_customer_sk)
   GROUP BY c_customer_sk
   HAVING (sum((ss_quantity * ss_sales_price)) > ((50 / CAST('100.0' AS DECIMAL(5,2))) * (
            SELECT *
            FROM
              max_store_sales
         )))
)
SELECT sum(sales)
FROM
  (
   SELECT (cs_quantity * cs_list_price) sales
   FROM
     catalog_sales
   , date_dim
   WHERE (d_year = 2000)
      AND (d_moy = 2)
      AND (cs_sold_date_sk = d_date_sk)
      AND (cs_item_sk IN (
      SELECT item_sk
      FROM
        frequent_ss_items
   ))
      AND (cs_bill_customer_sk IN (
      SELECT c_customer_sk
      FROM
        best_ss_customer
   ))
UNION ALL    SELECT (ws_quantity * ws_list_price) sales
   FROM
     web_sales
   , date_dim
   WHERE (d_year = 2000)
      AND (d_moy = 2)
      AND (ws_sold_date_sk = d_date_sk)
      AND (ws_item_sk IN (
      SELECT item_sk
      FROM
        frequent_ss_items
   ))
      AND (ws_bill_customer_sk IN (
      SELECT c_customer_sk
      FROM
        best_ss_customer
   ))
) y
LIMIT 100;
"""
    order_qt_query23_before "${query23}"
    async_mv_rewrite_fail(db, mv23, query23, "mv23")
    order_qt_query23_after "${query23}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv23"""

    def mv24 = """
WITH
  ssales AS (
   SELECT
     c_last_name
   , c_first_name
   , s_store_name
   , ca_state
   , s_state
   , i_color
   , i_current_price
   , i_manager_id
   , i_units
   , i_size
   , sum(ss_net_paid) netpaid
   FROM
     store_sales
   , store_returns
   , store
   , item
   , customer
   , customer_address
   WHERE (ss_ticket_number = sr_ticket_number)
      AND (ss_item_sk = sr_item_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ss_store_sk = s_store_sk)
      AND (c_birth_country = upper(ca_country))
      AND (s_zip = ca_zip)
      AND (s_market_id = 8)
   GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size
)
SELECT
  c_last_name
, c_first_name
, s_store_name
, sum(netpaid) paid
FROM
  ssales
WHERE (i_color = 'pale')
GROUP BY c_last_name, c_first_name, s_store_name
HAVING (sum(netpaid) > (
      SELECT (CAST('0.05' AS DECIMAL(5,2)) * avg(netpaid))
      FROM
        ssales
   ))
ORDER BY c_last_name, c_first_name, s_store_name;
"""
    def query24 = """
WITH
  ssales AS (
   SELECT
     c_last_name
   , c_first_name
   , s_store_name
   , ca_state
   , s_state
   , i_color
   , i_current_price
   , i_manager_id
   , i_units
   , i_size
   , sum(ss_net_paid) netpaid
   FROM
     store_sales
   , store_returns
   , store
   , item
   , customer
   , customer_address
   WHERE (ss_ticket_number = sr_ticket_number)
      AND (ss_item_sk = sr_item_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ss_store_sk = s_store_sk)
      AND (c_birth_country = upper(ca_country))
      AND (s_zip = ca_zip)
      AND (s_market_id = 8)
   GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size
)
SELECT
  c_last_name
, c_first_name
, s_store_name
, sum(netpaid) paid
FROM
  ssales
WHERE (i_color = 'pale')
GROUP BY c_last_name, c_first_name, s_store_name
HAVING (sum(netpaid) > (
      SELECT (CAST('0.05' AS DECIMAL(5,2)) * avg(netpaid))
      FROM
        ssales
   ))
ORDER BY c_last_name, c_first_name, s_store_name;
"""
    order_qt_query24_before "${query24}"
    async_mv_rewrite_fail(db, mv24, query24, "mv24")
    order_qt_query24_after "${query24}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv24"""


    def mv25 = """
SELECT
  i_item_id
, i_item_desc
, s_store_id
, s_store_name
, sum(ss_net_profit) store_sales_profit
, sum(sr_net_loss) store_returns_loss
, sum(cs_net_profit) catalog_sales_profit
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_moy = 4)
   AND (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_moy BETWEEN 4 AND 10)
   AND (d2.d_year = 2001)
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_moy BETWEEN 4 AND 10)
   AND (d3.d_year = 2001)
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
LIMIT 100;
"""
    def query25 = """
SELECT
  i_item_id
, i_item_desc
, s_store_id
, s_store_name
, sum(ss_net_profit) store_sales_profit
, sum(sr_net_loss) store_returns_loss
, sum(cs_net_profit) catalog_sales_profit
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_moy = 4)
   AND (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_moy BETWEEN 4 AND 10)
   AND (d2.d_year = 2001)
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_moy BETWEEN 4 AND 10)
   AND (d3.d_year = 2001)
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
LIMIT 100;
"""
    order_qt_query25_before "${query25}"
    async_mv_rewrite_fail(db, mv25, query25, "mv25")
    order_qt_query25_after "${query25}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25"""

    def mv25_1 = """
SELECT
  i_item_id
, i_item_desc
, s_store_id
, s_store_name
, sum(ss_net_profit) store_sales_profit
, sum(sr_net_loss) store_returns_loss
, sum(cs_net_profit) catalog_sales_profit
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_moy = 4)
   AND (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_moy BETWEEN 4 AND 10)
   AND (d2.d_year = 2001)
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_moy BETWEEN 4 AND 10)
   AND (d3.d_year = 2001)
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name;
"""
    def query25_1 = """
SELECT
  i_item_id
, i_item_desc
, s_store_id
, s_store_name
, sum(ss_net_profit) store_sales_profit
, sum(sr_net_loss) store_returns_loss
, sum(cs_net_profit) catalog_sales_profit
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_moy = 4)
   AND (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_moy BETWEEN 4 AND 10)
   AND (d2.d_year = 2001)
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_moy BETWEEN 4 AND 10)
   AND (d3.d_year = 2001)
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
LIMIT 100;
"""
    order_qt_query25_1_before "${query25_1}"
    async_mv_rewrite_success(db, mv25_1, query25_1, "mv25_1")
    order_qt_query25_1_after "${query25_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_1"""

    def mv26 = """
SELECT
  i_item_id
, avg(cs_quantity) agg1
, avg(cs_list_price) agg2
, avg(cs_coupon_amt) agg3
, avg(cs_sales_price) agg4
FROM
  catalog_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd_demo_sk)
   AND (cs_promo_sk = p_promo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND ((p_channel_email = 'N')
      OR (p_channel_event = 'N'))
   AND (d_year = 2000)
GROUP BY i_item_id
ORDER BY i_item_id ASC
LIMIT 100;
"""
    def query26 = """
SELECT
  i_item_id
, avg(cs_quantity) agg1
, avg(cs_list_price) agg2
, avg(cs_coupon_amt) agg3
, avg(cs_sales_price) agg4
FROM
  catalog_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd_demo_sk)
   AND (cs_promo_sk = p_promo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND ((p_channel_email = 'N')
      OR (p_channel_event = 'N'))
   AND (d_year = 2000)
GROUP BY i_item_id
ORDER BY i_item_id ASC
LIMIT 100;
"""
    order_qt_query26_before "${query26}"
    async_mv_rewrite_fail(db, mv26, query26, "mv26")
    order_qt_query26_after "${query26}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv26"""

    def mv26_1 = """
SELECT
  i_item_id
, avg(cs_quantity) agg1
, avg(cs_list_price) agg2
, avg(cs_coupon_amt) agg3
, avg(cs_sales_price) agg4
FROM
  catalog_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd_demo_sk)
   AND (cs_promo_sk = p_promo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND ((p_channel_email = 'N')
      OR (p_channel_event = 'N'))
   AND (d_year = 2000)
GROUP BY i_item_id;
"""
    def query26_1 = """
SELECT
  i_item_id
, avg(cs_quantity) agg1
, avg(cs_list_price) agg2
, avg(cs_coupon_amt) agg3
, avg(cs_sales_price) agg4
FROM
  catalog_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE (cs_sold_date_sk = d_date_sk)
   AND (cs_item_sk = i_item_sk)
   AND (cs_bill_cdemo_sk = cd_demo_sk)
   AND (cs_promo_sk = p_promo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND ((p_channel_email = 'N')
      OR (p_channel_event = 'N'))
   AND (d_year = 2000)
GROUP BY i_item_id
ORDER BY i_item_id ASC
LIMIT 100;
"""
    order_qt_query26_1_before "${query26_1}"
    async_mv_rewrite_success(db, mv26_1, query26_1, "mv26_1")
    order_qt_query26_1_after "${query26_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv26_1"""

    def mv27 = """
SELECT
  i_item_id
, s_state
, GROUPING (s_state) g_state
, avg(ss_quantity) agg1
, avg(ss_list_price) agg2
, avg(ss_coupon_amt) agg3
, avg(ss_sales_price) agg4
FROM
  store_sales
, customer_demographics
, date_dim
, store
, item
WHERE (ss_sold_date_sk = d_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (ss_store_sk = s_store_sk)
   AND (ss_cdemo_sk = cd_demo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND (d_year = 2002)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_item_id, s_state)
ORDER BY i_item_id ASC, s_state ASC
LIMIT 100;
"""
    def query27 = """
SELECT
  i_item_id
, s_state
, GROUPING (s_state) g_state
, avg(ss_quantity) agg1
, avg(ss_list_price) agg2
, avg(ss_coupon_amt) agg3
, avg(ss_sales_price) agg4
FROM
  store_sales
, customer_demographics
, date_dim
, store
, item
WHERE (ss_sold_date_sk = d_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (ss_store_sk = s_store_sk)
   AND (ss_cdemo_sk = cd_demo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND (d_year = 2002)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_item_id, s_state)
ORDER BY i_item_id ASC, s_state ASC
LIMIT 100;
"""
    order_qt_query27_before "${query27}"
    async_mv_rewrite_fail(db, mv27, query27, "mv27")
    order_qt_query27_after "${query27}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv27"""

    def mv27_1 = """
SELECT
  i_item_id
, s_state
, GROUPING (s_state) g_state
, avg(ss_quantity) agg1
, avg(ss_list_price) agg2
, avg(ss_coupon_amt) agg3
, avg(ss_sales_price) agg4
FROM
  store_sales
, customer_demographics
, date_dim
, store
, item
WHERE (ss_sold_date_sk = d_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (ss_store_sk = s_store_sk)
   AND (ss_cdemo_sk = cd_demo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND (d_year = 2002)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_item_id, s_state);
"""
    def query27_1 = """
SELECT
  i_item_id
, s_state
, GROUPING (s_state) g_state
, avg(ss_quantity) agg1
, avg(ss_list_price) agg2
, avg(ss_coupon_amt) agg3
, avg(ss_sales_price) agg4
FROM
  store_sales
, customer_demographics
, date_dim
, store
, item
WHERE (ss_sold_date_sk = d_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (ss_store_sk = s_store_sk)
   AND (ss_cdemo_sk = cd_demo_sk)
   AND (cd_gender = 'M')
   AND (cd_marital_status = 'S')
   AND (cd_education_status = 'College')
   AND (d_year = 2002)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_item_id, s_state)
ORDER BY i_item_id ASC, s_state ASC
LIMIT 100;
"""
    order_qt_query27_1_before "${query27_1}"
    // should success but fail
    async_mv_rewrite_fail(db, mv27_1, query27_1, "mv27_1")
    order_qt_query27_1_after "${query27_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv27_1"""

    def mv28 = """
SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 0 AND 5)
      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 10)
      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 11 AND 15)
      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 16 AND 20)
      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 25)
      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 26 AND 30)
      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
LIMIT 100;
"""
    def query28 = """
SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 0 AND 5)
      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 10)
      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 11 AND 15)
      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 16 AND 20)
      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 25)
      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 26 AND 30)
      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
LIMIT 100;
"""
    order_qt_query28_before "${query28}"
    async_mv_rewrite_fail(db, mv28, query28, "mv28")
    order_qt_query28_after "${query28}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv28"""

    def mv28_1 = """
SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 0 AND 5)
      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 10)
      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 11 AND 15)
      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 16 AND 20)
      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 25)
      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 26 AND 30)
      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6;
"""
    def query28_1 = """
SELECT *
FROM
  (
   SELECT
     avg(ss_list_price) b1_lp
   , count(ss_list_price) b1_cnt
   , count(DISTINCT ss_list_price) b1_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 0 AND 5)
      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
)  b1
, (
   SELECT
     avg(ss_list_price) b2_lp
   , count(ss_list_price) b2_cnt
   , count(DISTINCT ss_list_price) b2_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 6 AND 10)
      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
)  b2
, (
   SELECT
     avg(ss_list_price) b3_lp
   , count(ss_list_price) b3_cnt
   , count(DISTINCT ss_list_price) b3_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 11 AND 15)
      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
)  b3
, (
   SELECT
     avg(ss_list_price) b4_lp
   , count(ss_list_price) b4_cnt
   , count(DISTINCT ss_list_price) b4_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 16 AND 20)
      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
)  b4
, (
   SELECT
     avg(ss_list_price) b5_lp
   , count(ss_list_price) b5_cnt
   , count(DISTINCT ss_list_price) b5_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 21 AND 25)
      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
)  b5
, (
   SELECT
     avg(ss_list_price) b6_lp
   , count(ss_list_price) b6_cnt
   , count(DISTINCT ss_list_price) b6_cntd
   FROM
     store_sales
   WHERE (ss_quantity BETWEEN 26 AND 30)
      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
)  b6
LIMIT 100;
"""
    order_qt_query28_1_before "${query28_1}"
    async_mv_rewrite_fail(db, mv28_1, query28_1, "mv28_1")
    order_qt_query28_1_after "${query28_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv28_1"""

    def mv29 = """
SELECT
  i_item_id
, i_item_desc
, s_store_id
, s_store_name
, sum(ss_quantity) store_sales_quantity
, sum(sr_return_quantity) store_returns_quantity
, sum(cs_quantity) catalog_sales_quantity
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_moy = 9)
   AND (d1.d_year = 1999)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_moy BETWEEN 9 AND (9 + 3))
   AND (d2.d_year = 1999)
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_year IN (1999, (1999 + 1), (1999 + 2)))
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
LIMIT 100;
"""
    def query29 = """
SELECT
  i_item_id
, i_item_desc
, s_store_id
, s_store_name
, sum(ss_quantity) store_sales_quantity
, sum(sr_return_quantity) store_returns_quantity
, sum(cs_quantity) catalog_sales_quantity
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_moy = 9)
   AND (d1.d_year = 1999)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_moy BETWEEN 9 AND (9 + 3))
   AND (d2.d_year = 1999)
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_year IN (1999, (1999 + 1), (1999 + 2)))
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
LIMIT 100;
"""
    order_qt_query29_before "${query29}"
    async_mv_rewrite_fail(db, mv29, query29, "mv29")
    order_qt_query29_after "${query29}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv29"""

    def mv29_1 = """
SELECT
  i_item_id
, i_item_desc
, s_store_id
, s_store_name
, sum(ss_quantity) store_sales_quantity
, sum(sr_return_quantity) store_returns_quantity
, sum(cs_quantity) catalog_sales_quantity
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_moy = 9)
   AND (d1.d_year = 1999)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_moy BETWEEN 9 AND (9 + 3))
   AND (d2.d_year = 1999)
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_year IN (1999, (1999 + 1), (1999 + 2)))
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name;
"""
    def query29_1 = """
SELECT
  i_item_id
, i_item_desc
, s_store_id
, s_store_name
, sum(ss_quantity) store_sales_quantity
, sum(sr_return_quantity) store_returns_quantity
, sum(cs_quantity) catalog_sales_quantity
FROM
  store_sales
, store_returns
, catalog_sales
, date_dim d1
, date_dim d2
, date_dim d3
, store
, item
WHERE (d1.d_moy = 9)
   AND (d1.d_year = 1999)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_ticket_number = sr_ticket_number)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (d2.d_moy BETWEEN 9 AND (9 + 3))
   AND (d2.d_year = 1999)
   AND (sr_customer_sk = cs_bill_customer_sk)
   AND (sr_item_sk = cs_item_sk)
   AND (cs_sold_date_sk = d3.d_date_sk)
   AND (d3.d_year IN (1999, (1999 + 1), (1999 + 2)))
GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
LIMIT 100;
"""
    order_qt_query29_1_before "${query29_1}"
    async_mv_rewrite_success(db, mv29_1, query29_1, "mv29_1")
    order_qt_query29_1_after "${query29_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv29_1"""

    def mv30 = """
WITH
  customer_total_return AS (
   SELECT
     wr_returning_customer_sk ctr_customer_sk
   , ca_state ctr_state
   , sum(wr_return_amt) ctr_total_return
   FROM
     web_returns
   , date_dim
   , customer_address
   WHERE (wr_returned_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (wr_returning_addr_sk = ca_address_sk)
   GROUP BY wr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id
, c_salutation
, c_first_name
, c_last_name
, c_preferred_cust_flag
, c_birth_day
, c_birth_month
, c_birth_year
, c_birth_country
, c_login
, c_email_address
, c_last_review_date_sk
, ctr_total_return
FROM
  customer_total_return ctr1
, customer_address
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_state = ctr2.ctr_state)
   ))
   AND (ca_address_sk = c_current_addr_sk)
   AND (ca_state = 'GA')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, c_preferred_cust_flag ASC, c_birth_day ASC, c_birth_month ASC, c_birth_year ASC, c_birth_country ASC, c_login ASC, c_email_address ASC, c_last_review_date_sk ASC, ctr_total_return ASC
LIMIT 100;
"""
    def query30 = """
WITH
  customer_total_return AS (
   SELECT
     wr_returning_customer_sk ctr_customer_sk
   , ca_state ctr_state
   , sum(wr_return_amt) ctr_total_return
   FROM
     web_returns
   , date_dim
   , customer_address
   WHERE (wr_returned_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (wr_returning_addr_sk = ca_address_sk)
   GROUP BY wr_returning_customer_sk, ca_state
)
SELECT
  c_customer_id
, c_salutation
, c_first_name
, c_last_name
, c_preferred_cust_flag
, c_birth_day
, c_birth_month
, c_birth_year
, c_birth_country
, c_login
, c_email_address
, c_last_review_date_sk
, ctr_total_return
FROM
  customer_total_return ctr1
, customer_address
, customer
WHERE (ctr1.ctr_total_return > (
      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL))
      FROM
        customer_total_return ctr2
      WHERE (ctr1.ctr_state = ctr2.ctr_state)
   ))
   AND (ca_address_sk = c_current_addr_sk)
   AND (ca_state = 'GA')
   AND (ctr1.ctr_customer_sk = c_customer_sk)
ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, c_preferred_cust_flag ASC, c_birth_day ASC, c_birth_month ASC, c_birth_year ASC, c_birth_country ASC, c_login ASC, c_email_address ASC, c_last_review_date_sk ASC, ctr_total_return ASC
LIMIT 100;
"""
    order_qt_query30_before "${query30}"
    async_mv_rewrite_fail(db, mv30, query30, "mv30")
    order_qt_query30_after "${query30}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv30"""


    def mv31 = """
WITH
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
ORDER BY ss1.ca_county ASC;
"""
    def query31 = """
WITH
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
ORDER BY ss1.ca_county ASC;
"""



    def mv32 = """
SELECT sum(cs_ext_discount_amt) 'excess discount amount'
FROM
  catalog_sales
, item
, date_dim
WHERE (i_manufact_id = 977)
   AND (i_item_sk = cs_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = cs_sold_date_sk)
   AND (cs_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(cs_ext_discount_amt))
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = cs_sold_date_sk)
   ))
LIMIT 100;
"""
    def query32 = """
SELECT sum(cs_ext_discount_amt) 'excess discount amount'
FROM
  catalog_sales
, item
, date_dim
WHERE (i_manufact_id = 977)
   AND (i_item_sk = cs_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = cs_sold_date_sk)
   AND (cs_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(cs_ext_discount_amt))
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = cs_sold_date_sk)
   ))
LIMIT 100;
"""
    order_qt_query32_before "${query32}"
    async_mv_rewrite_fail(db, mv32, query32, "mv32")
    order_qt_query32_after "${query32}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv32"""

    def mv32_1 = """
SELECT sum(cs_ext_discount_amt) 'excess discount amount'
FROM
  catalog_sales
, item
, date_dim
WHERE (i_manufact_id = 977)
   AND (i_item_sk = cs_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = cs_sold_date_sk)
   AND (cs_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(cs_ext_discount_amt))
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = cs_sold_date_sk)
   ));
"""
    def query32_1 = """
SELECT sum(cs_ext_discount_amt) 'excess discount amount'
FROM
  catalog_sales
, item
, date_dim
WHERE (i_manufact_id = 977)
   AND (i_item_sk = cs_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = cs_sold_date_sk)
   AND (cs_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(cs_ext_discount_amt))
      FROM
        catalog_sales
      , date_dim
      WHERE (cs_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = cs_sold_date_sk)
   ))
LIMIT 100;
"""
    order_qt_query32_1_before "${query32_1}"
    // Should success but fail
    async_mv_rewrite_fail(db, mv32_1, query32_1, "mv32_1")
    order_qt_query32_1_after "${query32_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv32_1"""

    def mv33 = """
WITH
  ss AS (
   SELECT
     i_manufact_id
   , sum(ss_ext_sales_price) total_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (ss_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
, cs AS (
   SELECT
     i_manufact_id
   , sum(cs_ext_sales_price) total_sales
   FROM
     catalog_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (cs_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
, ws AS (
   SELECT
     i_manufact_id
   , sum(ws_ext_sales_price) total_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (ws_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
SELECT
  i_manufact_id
, sum(total_sales) total_sales
FROM
  (
   SELECT *
   FROM
     ss
UNION ALL    SELECT *
   FROM
     cs
UNION ALL    SELECT *
   FROM
     ws
)  tmp1
GROUP BY i_manufact_id
ORDER BY total_sales ASC
LIMIT 100;
"""
    def query33 = """
WITH
  ss AS (
   SELECT
     i_manufact_id
   , sum(ss_ext_sales_price) total_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (ss_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
, cs AS (
   SELECT
     i_manufact_id
   , sum(cs_ext_sales_price) total_sales
   FROM
     catalog_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (cs_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
, ws AS (
   SELECT
     i_manufact_id
   , sum(ws_ext_sales_price) total_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (ws_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
SELECT
  i_manufact_id
, sum(total_sales) total_sales
FROM
  (
   SELECT *
   FROM
     ss
UNION ALL    SELECT *
   FROM
     cs
UNION ALL    SELECT *
   FROM
     ws
)  tmp1
GROUP BY i_manufact_id
ORDER BY total_sales ASC
LIMIT 100;
"""
    order_qt_query33_before "${query33}"
    async_mv_rewrite_fail(db, mv33, query33, "mv33")
    order_qt_query33_after "${query33}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv33"""

    def mv33_1 = """
WITH
  ss AS (
   SELECT
     i_manufact_id
   , sum(ss_ext_sales_price) total_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (ss_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
, cs AS (
   SELECT
     i_manufact_id
   , sum(cs_ext_sales_price) total_sales
   FROM
     catalog_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (cs_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
, ws AS (
   SELECT
     i_manufact_id
   , sum(ws_ext_sales_price) total_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (ws_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
SELECT
  i_manufact_id
, sum(total_sales) total_sales
FROM
  (
   SELECT *
   FROM
     ss
UNION ALL    SELECT *
   FROM
     cs
UNION ALL    SELECT *
   FROM
     ws
)  tmp1
GROUP BY i_manufact_id;
"""
    def query33_1 = """
WITH
  ss AS (
   SELECT
     i_manufact_id
   , sum(ss_ext_sales_price) total_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (ss_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
, cs AS (
   SELECT
     i_manufact_id
   , sum(cs_ext_sales_price) total_sales
   FROM
     catalog_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (cs_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
, ws AS (
   SELECT
     i_manufact_id
   , sum(ws_ext_sales_price) total_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_manufact_id IN (
      SELECT i_manufact_id
      FROM
        item
      WHERE (i_category IN ('Electronics'))
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 5)
      AND (ws_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_manufact_id
)
SELECT
  i_manufact_id
, sum(total_sales) total_sales
FROM
  (
   SELECT *
   FROM
     ss
UNION ALL    SELECT *
   FROM
     cs
UNION ALL    SELECT *
   FROM
     ws
)  tmp1
GROUP BY i_manufact_id
ORDER BY total_sales ASC
LIMIT 100;
"""
    order_qt_query33_1_before "${query33_1}"
    async_mv_rewrite_fail(db, mv33_1, query33_1, "mv33_1")
    order_qt_query33_1_after "${query33_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv33_1"""

    def mv34 = """
SELECT
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
      AND ((date_dim.d_dom BETWEEN 1 AND 3)
         OR (date_dim.d_dom BETWEEN 25 AND 28))
      AND ((household_demographics.hd_buy_potential = '>10000')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > CAST('1.2' AS DECIMAL(2,1)))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dn
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 15 AND 20)
ORDER BY c_last_name ASC, c_first_name ASC, c_salutation ASC, c_preferred_cust_flag DESC, ss_ticket_number ASC;
"""
    def query34 = """
SELECT
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
      AND ((date_dim.d_dom BETWEEN 1 AND 3)
         OR (date_dim.d_dom BETWEEN 25 AND 28))
      AND ((household_demographics.hd_buy_potential = '>10000')
         OR (household_demographics.hd_buy_potential = 'Unknown'))
      AND (household_demographics.hd_vehicle_count > 0)
      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > CAST('1.2' AS DECIMAL(2,1)))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_county IN ('Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'))
   GROUP BY ss_ticket_number, ss_customer_sk
)  dn
, customer
WHERE (ss_customer_sk = c_customer_sk)
   AND (cnt BETWEEN 15 AND 20)
ORDER BY c_last_name ASC, c_first_name ASC, c_salutation ASC, c_preferred_cust_flag DESC, ss_ticket_number ASC;
"""
    order_qt_query34_before "${query34}"
    // shoudl success but fail
    async_mv_rewrite_fail(db, mv34, query34, "mv34")
    order_qt_query34_after "${query34}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv34"""


    def mv35 = """
SELECT
  ca_state
, cd_gender
, cd_marital_status
, cd_dep_count
, count(*) cnt1
, min(cd_dep_count)
, max(cd_dep_count)
, avg(cd_dep_count)
, cd_dep_employed_count
, count(*) cnt2
, min(cd_dep_employed_count)
, max(cd_dep_employed_count)
, avg(cd_dep_employed_count)
, cd_dep_college_count
, count(*) cnt3
, min(cd_dep_college_count)
, max(cd_dep_college_count)
, avg(cd_dep_college_count)
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (d_qoy < 4)
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE (c.c_customer_sk = ws_bill_customer_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE (c.c_customer_sk = cs_ship_customer_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   )))
GROUP BY ca_state, cd_gender, cd_marital_status, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY ca_state ASC, cd_gender ASC, cd_marital_status ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
LIMIT 100;
"""
    def query35 = """
SELECT
  ca_state
, cd_gender
, cd_marital_status
, cd_dep_count
, count(*) cnt1
, min(cd_dep_count)
, max(cd_dep_count)
, avg(cd_dep_count)
, cd_dep_employed_count
, count(*) cnt2
, min(cd_dep_employed_count)
, max(cd_dep_employed_count)
, avg(cd_dep_employed_count)
, cd_dep_college_count
, count(*) cnt3
, min(cd_dep_college_count)
, max(cd_dep_college_count)
, avg(cd_dep_college_count)
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (d_qoy < 4)
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE (c.c_customer_sk = ws_bill_customer_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE (c.c_customer_sk = cs_ship_customer_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   )))
GROUP BY ca_state, cd_gender, cd_marital_status, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY ca_state ASC, cd_gender ASC, cd_marital_status ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
LIMIT 100;
"""
    order_qt_query35_before "${query35}"
    async_mv_rewrite_fail(db, mv35, query35, "mv35")
    order_qt_query35_after "${query35}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv35"""

    def mv35_1 = """
SELECT
  ca_state
, cd_gender
, cd_marital_status
, cd_dep_count
, count(*) cnt1
, min(cd_dep_count)
, max(cd_dep_count)
, avg(cd_dep_count)
, cd_dep_employed_count
, count(*) cnt2
, min(cd_dep_employed_count)
, max(cd_dep_employed_count)
, avg(cd_dep_employed_count)
, cd_dep_college_count
, count(*) cnt3
, min(cd_dep_college_count)
, max(cd_dep_college_count)
, avg(cd_dep_college_count)
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (d_qoy < 4)
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE (c.c_customer_sk = ws_bill_customer_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE (c.c_customer_sk = cs_ship_customer_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   )))
GROUP BY ca_state, cd_gender, cd_marital_status, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
"""
    def query35_1 = """
SELECT
  ca_state
, cd_gender
, cd_marital_status
, cd_dep_count
, count(*) cnt1
, min(cd_dep_count)
, max(cd_dep_count)
, avg(cd_dep_count)
, cd_dep_employed_count
, count(*) cnt2
, min(cd_dep_employed_count)
, max(cd_dep_employed_count)
, avg(cd_dep_employed_count)
, cd_dep_college_count
, count(*) cnt3
, min(cd_dep_college_count)
, max(cd_dep_college_count)
, avg(cd_dep_college_count)
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2002)
      AND (d_qoy < 4)
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE (c.c_customer_sk = ws_bill_customer_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE (c.c_customer_sk = cs_ship_customer_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (d_year = 2002)
         AND (d_qoy < 4)
   )))
GROUP BY ca_state, cd_gender, cd_marital_status, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
ORDER BY ca_state ASC, cd_gender ASC, cd_marital_status ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
LIMIT 100;
"""
    order_qt_query35_1_before "${query35_1}"
    // should success but fail
    async_mv_rewrite_fail(db, mv35_1, query35_1, "mv35_1")
    order_qt_query35_1_after "${query35_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv35_1"""

    def mv36 = """
SELECT
  (sum(ss_net_profit) / sum(ss_ext_sales_price)) gross_margin
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY (sum(ss_net_profit) / sum(ss_ext_sales_price)) ASC) rank_within_parent
FROM
  store_sales
, date_dim d1
, item
, store
WHERE (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_category, i_class)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC, i_category, i_class
LIMIT 100;
"""
    def query36 = """
SELECT
  (sum(ss_net_profit) / sum(ss_ext_sales_price)) gross_margin
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY (sum(ss_net_profit) / sum(ss_ext_sales_price)) ASC) rank_within_parent
FROM
  store_sales
, date_dim d1
, item
, store
WHERE (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_category, i_class)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC, i_category, i_class
LIMIT 100;
"""
    order_qt_query36_before "${query36}"
    async_mv_rewrite_fail(db, mv36, query36, "mv36")
    order_qt_query36_after "${query36}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv36"""

    def mv36_1 = """
SELECT
  (sum(ss_net_profit) / sum(ss_ext_sales_price)) gross_margin
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY (sum(ss_net_profit) / sum(ss_ext_sales_price)) ASC) rank_within_parent
FROM
  store_sales
, date_dim d1
, item
, store
WHERE (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_category, i_class);
"""
    def query36_1 = """
SELECT
  (sum(ss_net_profit) / sum(ss_ext_sales_price)) gross_margin
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY (sum(ss_net_profit) / sum(ss_ext_sales_price)) ASC) rank_within_parent
FROM
  store_sales
, date_dim d1
, item
, store
WHERE (d1.d_year = 2001)
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (i_item_sk = ss_item_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
     'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'
   , 'TN'))
GROUP BY ROLLUP (i_category, i_class)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC, i_category, i_class
LIMIT 100;
"""
    order_qt_query36_1_before "${query36_1}"
    // sould support window
    async_mv_rewrite_fail(db, mv36_1, query36_1, "mv36_1")
    order_qt_query36_1_after "${query36_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv36_1"""

    def mv37 = """
SELECT
  i_item_id
, i_item_desc
, i_current_price
FROM
  item
, inventory
, date_dim
, catalog_sales
WHERE (i_current_price BETWEEN 68 AND (68 + 30))
   AND (inv_item_sk = i_item_sk)
   AND (d_date_sk = inv_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-02-01' AS DATE) AND (CAST('2000-02-01' AS DATE) + INTERVAL  '60' DAY))
   AND (i_manufact_id IN (677, 940, 694, 808))
   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
   AND (cs_item_sk = i_item_sk)
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id ASC
LIMIT 100;
"""
    def query37 = """
SELECT
  i_item_id
, i_item_desc
, i_current_price
FROM
  item
, inventory
, date_dim
, catalog_sales
WHERE (i_current_price BETWEEN 68 AND (68 + 30))
   AND (inv_item_sk = i_item_sk)
   AND (d_date_sk = inv_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-02-01' AS DATE) AND (CAST('2000-02-01' AS DATE) + INTERVAL  '60' DAY))
   AND (i_manufact_id IN (677, 940, 694, 808))
   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
   AND (cs_item_sk = i_item_sk)
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id ASC
LIMIT 100;
"""
    order_qt_query37_before "${query37}"
    async_mv_rewrite_fail(db, mv37, query37, "mv37")
    order_qt_query37_after "${query37}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv37"""

    def mv37_1 = """
SELECT
  i_item_id
, i_item_desc
, i_current_price
FROM
  item
, inventory
, date_dim
, catalog_sales
WHERE (i_current_price BETWEEN 68 AND (68 + 30))
   AND (inv_item_sk = i_item_sk)
   AND (d_date_sk = inv_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-02-01' AS DATE) AND (CAST('2000-02-01' AS DATE) + INTERVAL  '60' DAY))
   AND (i_manufact_id IN (677, 940, 694, 808))
   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
   AND (cs_item_sk = i_item_sk)
GROUP BY i_item_id, i_item_desc, i_current_price;
"""
    def query37_1 = """
SELECT
  i_item_id
, i_item_desc
, i_current_price
FROM
  item
, inventory
, date_dim
, catalog_sales
WHERE (i_current_price BETWEEN 68 AND (68 + 30))
   AND (inv_item_sk = i_item_sk)
   AND (d_date_sk = inv_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-02-01' AS DATE) AND (CAST('2000-02-01' AS DATE) + INTERVAL  '60' DAY))
   AND (i_manufact_id IN (677, 940, 694, 808))
   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
   AND (cs_item_sk = i_item_sk)
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id ASC
LIMIT 100;
"""
    order_qt_query37_1_before "${query37_1}"
    async_mv_rewrite_success(db, mv37_1, query37_1, "mv37_1")
    order_qt_query37_1_after "${query37_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv37_1"""

    def mv38 = """
SELECT count(*)
FROM
  (
   SELECT DISTINCT
     c_last_name
   , c_first_name
   , d_date
   FROM
     store_sales
   , date_dim
   , customer
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_customer_sk = customer.c_customer_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
INTERSECT    SELECT DISTINCT
     c_last_name
   , c_first_name
   , d_date
   FROM
     catalog_sales
   , date_dim
   , customer
   WHERE (catalog_sales.cs_sold_date_sk = date_dim.d_date_sk)
      AND (catalog_sales.cs_bill_customer_sk = customer.c_customer_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
INTERSECT    SELECT DISTINCT
     c_last_name
   , c_first_name
   , d_date
   FROM
     web_sales
   , date_dim
   , customer
   WHERE (web_sales.ws_sold_date_sk = date_dim.d_date_sk)
      AND (web_sales.ws_bill_customer_sk = customer.c_customer_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
)  hot_cust
LIMIT 100;
"""
    def query38 = """
SELECT count(*)
FROM
  (
   SELECT DISTINCT
     c_last_name
   , c_first_name
   , d_date
   FROM
     store_sales
   , date_dim
   , customer
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_customer_sk = customer.c_customer_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
INTERSECT    SELECT DISTINCT
     c_last_name
   , c_first_name
   , d_date
   FROM
     catalog_sales
   , date_dim
   , customer
   WHERE (catalog_sales.cs_sold_date_sk = date_dim.d_date_sk)
      AND (catalog_sales.cs_bill_customer_sk = customer.c_customer_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
INTERSECT    SELECT DISTINCT
     c_last_name
   , c_first_name
   , d_date
   FROM
     web_sales
   , date_dim
   , customer
   WHERE (web_sales.ws_sold_date_sk = date_dim.d_date_sk)
      AND (web_sales.ws_bill_customer_sk = customer.c_customer_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
)  hot_cust
LIMIT 100;
"""
    order_qt_query38_before "${query38}"
    async_mv_rewrite_fail(db, mv38, query38, "mv38")
    order_qt_query38_after "${query38}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv38"""


    def mv39 = """
WITH
  inv AS (
   SELECT
     w_warehouse_name
   , w_warehouse_sk
   , i_item_sk
   , d_moy
   , stdev
   , mean
   , (CASE mean WHEN 0 THEN null ELSE (stdev / mean) END) cov
   FROM
     (
      SELECT
        w_warehouse_name
      , w_warehouse_sk
      , i_item_sk
      , d_moy
      , stddev_samp(inv_quantity_on_hand) stdev
      , avg(inv_quantity_on_hand) mean
      FROM
        inventory
      , item
      , warehouse
      , date_dim
      WHERE (inv_item_sk = i_item_sk)
         AND (inv_warehouse_sk = w_warehouse_sk)
         AND (inv_date_sk = d_date_sk)
         AND (d_year = 2001)
      GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy
   )  foo
   WHERE ((CASE mean WHEN 0 THEN 0 ELSE (stdev / mean) END) > 1)
)
SELECT
  inv1.w_warehouse_sk
, inv1.i_item_sk
, inv1.d_moy
, inv1.mean
, inv1.cov
, inv2.w_warehouse_sk as w_warehouse_sk2
, inv2.i_item_sk as i_item_sk2
, inv2.d_moy as d_moy2
, inv2.mean as mean2
, inv2.cov as cov2
FROM
  inv inv1
, inv inv2
WHERE (inv1.i_item_sk = inv2.i_item_sk)
   AND (inv1.w_warehouse_sk = inv2.w_warehouse_sk)
   AND (inv1.d_moy = 1)
   AND (inv2.d_moy = (1 + 1))
ORDER BY inv1.w_warehouse_sk ASC, inv1.i_item_sk ASC, inv1.d_moy ASC, inv1.mean ASC, inv1.cov ASC, inv2.d_moy ASC, inv2.mean ASC, inv2.cov ASC;
"""
    def query39 = """
WITH
  inv AS (
   SELECT
     w_warehouse_name
   , w_warehouse_sk
   , i_item_sk
   , d_moy
   , stdev
   , mean
   , (CASE mean WHEN 0 THEN null ELSE (stdev / mean) END) cov
   FROM
     (
      SELECT
        w_warehouse_name
      , w_warehouse_sk
      , i_item_sk
      , d_moy
      , stddev_samp(inv_quantity_on_hand) stdev
      , avg(inv_quantity_on_hand) mean
      FROM
        inventory
      , item
      , warehouse
      , date_dim
      WHERE (inv_item_sk = i_item_sk)
         AND (inv_warehouse_sk = w_warehouse_sk)
         AND (inv_date_sk = d_date_sk)
         AND (d_year = 2001)
      GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy
   )  foo
   WHERE ((CASE mean WHEN 0 THEN 0 ELSE (stdev / mean) END) > 1)
)
SELECT
  inv1.w_warehouse_sk
, inv1.i_item_sk
, inv1.d_moy
, inv1.mean
, inv1.cov
, inv2.w_warehouse_sk
, inv2.i_item_sk
, inv2.d_moy
, inv2.mean
, inv2.cov
FROM
  inv inv1
, inv inv2
WHERE (inv1.i_item_sk = inv2.i_item_sk)
   AND (inv1.w_warehouse_sk = inv2.w_warehouse_sk)
   AND (inv1.d_moy = 1)
   AND (inv2.d_moy = (1 + 1))
ORDER BY inv1.w_warehouse_sk ASC, inv1.i_item_sk ASC, inv1.d_moy ASC, inv1.mean ASC, inv1.cov ASC, inv2.d_moy ASC, inv2.mean ASC, inv2.cov ASC;
"""
    order_qt_query39_before "${query39}"
    async_mv_rewrite_fail(db, mv39, query39, "mv39")
    order_qt_query39_after "${query39}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv39"""


    def mv40 = """
SELECT
  w_state
, i_item_id
, sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_before
, sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_after
FROM
  catalog_sales
LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
   AND (cs_item_sk = cr_item_sk)
, warehouse
, item
, date_dim
WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
   AND (i_item_sk = cs_item_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
GROUP BY w_state, i_item_id
ORDER BY w_state ASC, i_item_id ASC
LIMIT 100;
"""
    def query40 = """
SELECT
  w_state
, i_item_id
, sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_before
, sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_after
FROM
  catalog_sales
LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
   AND (cs_item_sk = cr_item_sk)
, warehouse
, item
, date_dim
WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
   AND (i_item_sk = cs_item_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
GROUP BY w_state, i_item_id
ORDER BY w_state ASC, i_item_id ASC
LIMIT 100;
"""
    order_qt_query40_before "${query40}"
    async_mv_rewrite_fail(db, mv40, query40, "mv40")
    order_qt_query40_after "${query40}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv40"""

    def mv40_1 = """
SELECT
  w_state
, i_item_id
, sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_before
, sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_after
FROM
  catalog_sales
LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
   AND (cs_item_sk = cr_item_sk)
, warehouse
, item
, date_dim
WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
   AND (i_item_sk = cs_item_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
GROUP BY w_state, i_item_id;
"""
    def query40_1 = """
SELECT
  w_state
, i_item_id
, sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_before
, sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN (cs_sales_price - COALESCE(cr_refunded_cash, 0)) ELSE 0 END)) sales_after
FROM
  catalog_sales
LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
   AND (cs_item_sk = cr_item_sk)
, warehouse
, item
, date_dim
WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
   AND (i_item_sk = cs_item_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
GROUP BY w_state, i_item_id
ORDER BY w_state ASC, i_item_id ASC
LIMIT 100;
"""
    order_qt_query40_1_before "${query40_1}"
    async_mv_rewrite_success(db, mv40_1, query40_1, "mv40_1")
    order_qt_query40_1_after "${query40_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv40_1"""

    def mv41 = """
SELECT DISTINCT i_product_name
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
LIMIT 100;
"""
    def query41 = """
SELECT DISTINCT i_product_name
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
LIMIT 100;
"""
    order_qt_query41_before "${query41}"
    async_mv_rewrite_fail(db, mv41, query41, "mv41")
    order_qt_query41_after "${query41}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv41"""

    def mv41_1 = """
SELECT DISTINCT i_product_name
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
   ) > 0);
"""
    def query41_1 = """
SELECT DISTINCT i_product_name
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
LIMIT 100;
"""
    order_qt_query41_1_before "${query41_1}"
    async_mv_rewrite_success(db, mv41_1, query41_1, "mv41_1")
    order_qt_query41_1_after "${query41_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv41_1"""

    def mv42 = """
SELECT
  dt.d_year
, item.i_category_id
, item.i_category
, sum(ss_ext_sales_price)
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manager_id = 1)
   AND (dt.d_moy = 11)
   AND (dt.d_year = 2000)
GROUP BY dt.d_year, item.i_category_id, item.i_category
ORDER BY sum(ss_ext_sales_price) DESC, dt.d_year ASC, item.i_category_id ASC, item.i_category ASC
LIMIT 100;
"""
    def query42 = """
SELECT
  dt.d_year
, item.i_category_id
, item.i_category
, sum(ss_ext_sales_price)
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manager_id = 1)
   AND (dt.d_moy = 11)
   AND (dt.d_year = 2000)
GROUP BY dt.d_year, item.i_category_id, item.i_category
ORDER BY sum(ss_ext_sales_price) DESC, dt.d_year ASC, item.i_category_id ASC, item.i_category ASC
LIMIT 100;
"""
    order_qt_query42_before "${query42}"
    async_mv_rewrite_fail(db, mv42, query42, "mv42")
    order_qt_query42_after "${query42}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv42"""

    def mv42_1 = """
SELECT
  dt.d_year
, item.i_category_id
, item.i_category
, sum(ss_ext_sales_price)
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manager_id = 1)
   AND (dt.d_moy = 11)
   AND (dt.d_year = 2000)
GROUP BY dt.d_year, item.i_category_id, item.i_category;
"""
    def query42_1 = """
SELECT
  dt.d_year
, item.i_category_id
, item.i_category
, sum(ss_ext_sales_price)
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manager_id = 1)
   AND (dt.d_moy = 11)
   AND (dt.d_year = 2000)
GROUP BY dt.d_year, item.i_category_id, item.i_category
ORDER BY sum(ss_ext_sales_price) DESC, dt.d_year ASC, item.i_category_id ASC, item.i_category ASC
LIMIT 100;
"""
    order_qt_query42_1_before "${query42_1}"
    async_mv_rewrite_success(db, mv42_1, query42_1, "mv42_1")
    order_qt_query42_1_after "${query42_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv42_1"""

    def mv43 = """
SELECT
  s_store_name
, s_store_id
, sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
, sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
, sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
, sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
, sum((CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE null END)) thu_sales
, sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
, sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
FROM
  date_dim
, store_sales
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_gmt_offset = -5)
   AND (d_year = 2000)
GROUP BY s_store_name, s_store_id
ORDER BY s_store_name ASC, s_store_id ASC, sun_sales ASC, mon_sales ASC, tue_sales ASC, wed_sales ASC, thu_sales ASC, fri_sales ASC, sat_sales ASC
LIMIT 100;
"""
    def query43 = """
SELECT
  s_store_name
, s_store_id
, sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
, sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
, sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
, sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
, sum((CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE null END)) thu_sales
, sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
, sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
FROM
  date_dim
, store_sales
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_gmt_offset = -5)
   AND (d_year = 2000)
GROUP BY s_store_name, s_store_id
ORDER BY s_store_name ASC, s_store_id ASC, sun_sales ASC, mon_sales ASC, tue_sales ASC, wed_sales ASC, thu_sales ASC, fri_sales ASC, sat_sales ASC
LIMIT 100;
"""
    order_qt_query43_before "${query43}"
    async_mv_rewrite_fail(db, mv43, query43, "mv43")
    order_qt_query43_after "${query43}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv43"""

    def mv43_1 = """
SELECT
  s_store_name
, s_store_id
, sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
, sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
, sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
, sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
, sum((CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE null END)) thu_sales
, sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
, sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
FROM
  date_dim
, store_sales
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_gmt_offset = -5)
   AND (d_year = 2000)
GROUP BY s_store_name, s_store_id;
"""
    def query43_1 = """
SELECT
  s_store_name
, s_store_id
, sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
, sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
, sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
, sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
, sum((CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE null END)) thu_sales
, sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
, sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
FROM
  date_dim
, store_sales
, store
WHERE (d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_gmt_offset = -5)
   AND (d_year = 2000)
GROUP BY s_store_name, s_store_id
ORDER BY s_store_name ASC, s_store_id ASC, sun_sales ASC, mon_sales ASC, tue_sales ASC, wed_sales ASC, thu_sales ASC, fri_sales ASC, sat_sales ASC
LIMIT 100;
"""
    order_qt_query43_1_before "${query43_1}"
    async_mv_rewrite_success(db, mv43_1, query43_1, "mv43_1")
    order_qt_query43_1_after "${query43_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv43_1"""

    def mv44 = """
SELECT
  asceding.rnk
, i1.i_product_name best_performing
, i2.i_product_name worst_performing
FROM
  (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col ASC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v1
   )  v11
   WHERE (rnk < 11)
)  asceding
, (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col DESC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v2
   )  v21
   WHERE (rnk < 11)
)  descending
, item i1
, item i2
WHERE (asceding.rnk = descending.rnk)
   AND (i1.i_item_sk = asceding.item_sk)
   AND (i2.i_item_sk = descending.item_sk)
ORDER BY asceding.rnk ASC
LIMIT 100;
"""
    def query44 = """
SELECT
  asceding.rnk
, i1.i_product_name best_performing
, i2.i_product_name worst_performing
FROM
  (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col ASC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v1
   )  v11
   WHERE (rnk < 11)
)  asceding
, (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col DESC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v2
   )  v21
   WHERE (rnk < 11)
)  descending
, item i1
, item i2
WHERE (asceding.rnk = descending.rnk)
   AND (i1.i_item_sk = asceding.item_sk)
   AND (i2.i_item_sk = descending.item_sk)
ORDER BY asceding.rnk ASC
LIMIT 100;
"""
    order_qt_query44_before "${query44}"
    async_mv_rewrite_fail(db, mv44, query44, "mv44")
    order_qt_query44_after "${query44}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv44"""

    def mv44_1 = """
SELECT
  asceding.rnk
, i1.i_product_name best_performing
, i2.i_product_name worst_performing
FROM
  (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col ASC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v1
   )  v11
   WHERE (rnk < 11)
)  asceding
, (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col DESC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v2
   )  v21
   WHERE (rnk < 11)
)  descending
, item i1
, item i2
WHERE (asceding.rnk = descending.rnk)
   AND (i1.i_item_sk = asceding.item_sk)
   AND (i2.i_item_sk = descending.item_sk);
"""
    def query44_1 = """
SELECT
  asceding.rnk
, i1.i_product_name best_performing
, i2.i_product_name worst_performing
FROM
  (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col ASC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v1
   )  v11
   WHERE (rnk < 11)
)  asceding
, (
   SELECT *
   FROM
     (
      SELECT
        item_sk
      , rank() OVER (ORDER BY rank_col DESC) rnk
      FROM
        (
         SELECT
           ss_item_sk item_sk
         , avg(ss_net_profit) rank_col
         FROM
           store_sales ss1
         WHERE (ss_store_sk = 4)
         GROUP BY ss_item_sk
         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL(2,1)) * (
                  SELECT avg(ss_net_profit) rank_col
                  FROM
                    store_sales
                  WHERE (ss_store_sk = 4)
                     AND (ss_addr_sk IS NULL)
                  GROUP BY ss_store_sk
               )))
      )  v2
   )  v21
   WHERE (rnk < 11)
)  descending
, item i1
, item i2
WHERE (asceding.rnk = descending.rnk)
   AND (i1.i_item_sk = asceding.item_sk)
   AND (i2.i_item_sk = descending.item_sk)
ORDER BY asceding.rnk ASC
LIMIT 100;
"""
    order_qt_query44_1_before "${query44_1}"
    async_mv_rewrite_fail(db, mv44_1, query44_1, "mv44_1")
    order_qt_query44_1_after "${query44_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv44_1"""

    def mv45 = """
SELECT
  ca_zip
, ca_city
, sum(ws_sales_price)
FROM
  web_sales
, customer
, customer_address
, date_dim
, item
WHERE (ws_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (ws_item_sk = i_item_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_item_sk IN (2      , 3      , 5      , 7      , 11      , 13      , 17      , 19      , 23      , 29))
   )))
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip, ca_city
ORDER BY ca_zip ASC, ca_city ASC
LIMIT 100;
"""
    def query45 = """
SELECT
  ca_zip
, ca_city
, sum(ws_sales_price)
FROM
  web_sales
, customer
, customer_address
, date_dim
, item
WHERE (ws_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (ws_item_sk = i_item_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_item_sk IN (2      , 3      , 5      , 7      , 11      , 13      , 17      , 19      , 23      , 29))
   )))
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip, ca_city
ORDER BY ca_zip ASC, ca_city ASC
LIMIT 100;
"""
    order_qt_query45_before "${query45}"
    async_mv_rewrite_fail(db, mv45, query45, "mv45")
    order_qt_query45_after "${query45}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv45"""

    def mv45_1 = """
SELECT
  ca_zip
, ca_city
, sum(ws_sales_price)
FROM
  web_sales
, customer
, customer_address
, date_dim
, item
WHERE (ws_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (ws_item_sk = i_item_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_item_sk IN (2      , 3      , 5      , 7      , 11      , 13      , 17      , 19      , 23      , 29))
   )))
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip, ca_city;
"""
    def query45_1 = """
SELECT
  ca_zip
, ca_city
, sum(ws_sales_price)
FROM
  web_sales
, customer
, customer_address
, date_dim
, item
WHERE (ws_bill_customer_sk = c_customer_sk)
   AND (c_current_addr_sk = ca_address_sk)
   AND (ws_item_sk = i_item_sk)
   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_item_sk IN (2      , 3      , 5      , 7      , 11      , 13      , 17      , 19      , 23      , 29))
   )))
   AND (ws_sold_date_sk = d_date_sk)
   AND (d_qoy = 2)
   AND (d_year = 2001)
GROUP BY ca_zip, ca_city
ORDER BY ca_zip ASC, ca_city ASC
LIMIT 100;
"""
    order_qt_query45_1_before "${query45_1}"
    // should success but fail
    async_mv_rewrite_fail(db, mv45_1, query45_1, "mv45_1")
    order_qt_query45_1_after "${query45_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv45_1"""

    def mv46 = """
SELECT
  c_last_name
, c_first_name
, ca_city
, bought_city
, ss_ticket_number
, amt
, profit
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , ca_city bought_city
   , sum(ss_coupon_amt) amt
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      AND ((household_demographics.hd_dep_count = 4)
         OR (household_demographics.hd_vehicle_count = 3))
      AND (date_dim.d_dow IN (6   , 0))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_city IN ('Fairview'   , 'Midway'   , 'Fairview'   , 'Fairview'   , 'Fairview'))
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
)  dn
, customer
, customer_address current_addr
WHERE (ss_customer_sk = c_customer_sk)
   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
   AND (current_addr.ca_city <> bought_city)
ORDER BY c_last_name ASC, c_first_name ASC, ca_city ASC, bought_city ASC, ss_ticket_number ASC
LIMIT 100;
"""
    def query46 = """
SELECT
  c_last_name
, c_first_name
, ca_city
, bought_city
, ss_ticket_number
, amt
, profit
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , ca_city bought_city
   , sum(ss_coupon_amt) amt
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      AND ((household_demographics.hd_dep_count = 4)
         OR (household_demographics.hd_vehicle_count = 3))
      AND (date_dim.d_dow IN (6   , 0))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_city IN ('Fairview'   , 'Midway'   , 'Fairview'   , 'Fairview'   , 'Fairview'))
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
)  dn
, customer
, customer_address current_addr
WHERE (ss_customer_sk = c_customer_sk)
   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
   AND (current_addr.ca_city <> bought_city)
ORDER BY c_last_name ASC, c_first_name ASC, ca_city ASC, bought_city ASC, ss_ticket_number ASC
LIMIT 100;
"""
    order_qt_query46_before "${query46}"
    async_mv_rewrite_fail(db, mv46, query46, "mv46")
    order_qt_query46_after "${query46}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv46"""

    def mv46_1 = """
SELECT
  c_last_name
, c_first_name
, ca_city
, bought_city
, ss_ticket_number
, amt
, profit
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , ca_city bought_city
   , sum(ss_coupon_amt) amt
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      AND ((household_demographics.hd_dep_count = 4)
         OR (household_demographics.hd_vehicle_count = 3))
      AND (date_dim.d_dow IN (6   , 0))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_city IN ('Fairview'   , 'Midway'   , 'Fairview'   , 'Fairview'   , 'Fairview'))
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
)  dn
, customer
, customer_address current_addr
WHERE (ss_customer_sk = c_customer_sk)
   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
   AND (current_addr.ca_city <> bought_city);
"""
    def query46_1 = """
SELECT
  c_last_name
, c_first_name
, ca_city
, bought_city
, ss_ticket_number
, amt
, profit
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , ca_city bought_city
   , sum(ss_coupon_amt) amt
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      AND ((household_demographics.hd_dep_count = 4)
         OR (household_demographics.hd_vehicle_count = 3))
      AND (date_dim.d_dow IN (6   , 0))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_city IN ('Fairview'   , 'Midway'   , 'Fairview'   , 'Fairview'   , 'Fairview'))
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
)  dn
, customer
, customer_address current_addr
WHERE (ss_customer_sk = c_customer_sk)
   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
   AND (current_addr.ca_city <> bought_city)
ORDER BY c_last_name ASC, c_first_name ASC, ca_city ASC, bought_city ASC, ss_ticket_number ASC
LIMIT 100;
"""
    order_qt_query46_1_before "${query46_1}"
    async_mv_rewrite_fail(db, mv46_1, query46_1, "mv46_1")
    order_qt_query46_1_after "${query46_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv46_1"""

    def mv47 = """
WITH
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
LIMIT 100;
"""
    def query47 = """
WITH
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
LIMIT 100;
"""
    order_qt_query47_before "${query47}"
    async_mv_rewrite_fail(db, mv47, query47, "mv47")
    order_qt_query47_after "${query47}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv47"""


    def mv48 = """
SELECT sum(ss_quantity)
FROM
  store_sales
, store
, customer_demographics
, customer_address
, date_dim
WHERE (s_store_sk = ss_store_sk)
   AND (ss_sold_date_sk = d_date_sk)
   AND (d_year = 2000)
   AND (((cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'M')
         AND (cd_education_status = '4 yr Degree')
         AND (ss_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2))))
      OR ((cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'D')
         AND (cd_education_status = '2 yr Degree')
         AND (ss_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2))))
      OR ((cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'S')
         AND (cd_education_status = 'College')
         AND (ss_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))))
   AND (((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('CO'      , 'OH'      , 'TX'))
         AND (ss_net_profit BETWEEN 0 AND 2000))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('OR'      , 'MN'      , 'KY'))
         AND (ss_net_profit BETWEEN 150 AND 3000))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('VA'      , 'CA'      , 'MS'))
         AND (ss_net_profit BETWEEN 50 AND 25000)));
"""
    def query48 = """
SELECT sum(ss_quantity)
FROM
  store_sales
, store
, customer_demographics
, customer_address
, date_dim
WHERE (s_store_sk = ss_store_sk)
   AND (ss_sold_date_sk = d_date_sk)
   AND (d_year = 2000)
   AND (((cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'M')
         AND (cd_education_status = '4 yr Degree')
         AND (ss_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2))))
      OR ((cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'D')
         AND (cd_education_status = '2 yr Degree')
         AND (ss_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2))))
      OR ((cd_demo_sk = ss_cdemo_sk)
         AND (cd_marital_status = 'S')
         AND (cd_education_status = 'College')
         AND (ss_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))))
   AND (((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('CO'      , 'OH'      , 'TX'))
         AND (ss_net_profit BETWEEN 0 AND 2000))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('OR'      , 'MN'      , 'KY'))
         AND (ss_net_profit BETWEEN 150 AND 3000))
      OR ((ss_addr_sk = ca_address_sk)
         AND (ca_country = 'United States')
         AND (ca_state IN ('VA'      , 'CA'      , 'MS'))
         AND (ss_net_profit BETWEEN 50 AND 25000)));
"""
    order_qt_query48_before "${query48}"
    async_mv_rewrite_success(db, mv48, query48, "mv48")
    order_qt_query48_after "${query48}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv48"""

    def mv49 = """
-- 目前union后跟得order没有效果，需要在外面包一层select后再order

SELECT channel, item, return_ratio, return_rank, currency_rank
FROM
(SELECT
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
   OR (web.currency_rank <= 10)
UNION SELECT
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
   OR (catalog.currency_rank <= 10)
UNION SELECT
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
   OR (store.currency_rank <= 10)
) r
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
LIMIT 100;
"""
    def query49 = """
-- 目前union后跟得order没有效果，需要在外面包一层select后再order

SELECT channel, item, return_ratio, return_rank, currency_rank
FROM
(SELECT
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
   OR (web.currency_rank <= 10)
UNION SELECT
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
   OR (catalog.currency_rank <= 10)
UNION SELECT
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
   OR (store.currency_rank <= 10)
) r
ORDER BY 1 ASC, 4 ASC, 5 ASC, 2 ASC
LIMIT 100;
"""
    order_qt_query49_before "${query49}"
    async_mv_rewrite_fail(db, mv49, query49, "mv49")
    order_qt_query49_after "${query49}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv49"""


    def mv50 = """
SELECT
  s_store_name
, s_company_id
, s_street_number
, s_street_name
, s_street_type
, s_suite_number
, s_city
, s_county
, s_state
, s_zip
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 30)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 60)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 90)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '120 days'
FROM
  store_sales
, store_returns
, store
, date_dim d1
, date_dim d2
WHERE (d2.d_year = 2001)
   AND (d2.d_moy = 8)
   AND (ss_ticket_number = sr_ticket_number)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_sold_date_sk = d1.d_date_sk)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_store_sk = s_store_sk)
GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip
ORDER BY s_store_name ASC, s_company_id ASC, s_street_number ASC, s_street_name ASC, s_street_type ASC, s_suite_number ASC, s_city ASC, s_county ASC, s_state ASC, s_zip ASC
LIMIT 100;
"""
    def query50 = """
SELECT
  s_store_name
, s_company_id
, s_street_number
, s_street_name
, s_street_type
, s_suite_number
, s_city
, s_county
, s_state
, s_zip
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 30)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 60)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 90)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
FROM
  store_sales
, store_returns
, store
, date_dim d1
, date_dim d2
WHERE (d2.d_year = 2001)
   AND (d2.d_moy = 8)
   AND (ss_ticket_number = sr_ticket_number)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_sold_date_sk = d1.d_date_sk)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_store_sk = s_store_sk)
GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip
ORDER BY s_store_name ASC, s_company_id ASC, s_street_number ASC, s_street_name ASC, s_street_type ASC, s_suite_number ASC, s_city ASC, s_county ASC, s_state ASC, s_zip ASC
LIMIT 100;
"""
    order_qt_query50_before "${query50}"
    async_mv_rewrite_fail(db, mv50, query50, "mv50")
    order_qt_query50_after "${query50}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv50"""

    def mv50_1 = """
SELECT
  s_store_name
, s_company_id
, s_street_number
, s_street_name
, s_street_type
, s_suite_number
, s_city
, s_county
, s_state
, s_zip
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 30)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 60)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 90)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '120 days'
FROM
  store_sales
, store_returns
, store
, date_dim d1
, date_dim d2
WHERE (d2.d_year = 2001)
   AND (d2.d_moy = 8)
   AND (ss_ticket_number = sr_ticket_number)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_sold_date_sk = d1.d_date_sk)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_store_sk = s_store_sk)
GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip;
"""
    def query50_1 = """
SELECT
  s_store_name
, s_company_id
, s_street_number
, s_street_name
, s_street_type
, s_suite_number
, s_city
, s_county
, s_state
, s_zip
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 30)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 60)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 90)
   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
FROM
  store_sales
, store_returns
, store
, date_dim d1
, date_dim d2
WHERE (d2.d_year = 2001)
   AND (d2.d_moy = 8)
   AND (ss_ticket_number = sr_ticket_number)
   AND (ss_item_sk = sr_item_sk)
   AND (ss_sold_date_sk = d1.d_date_sk)
   AND (sr_returned_date_sk = d2.d_date_sk)
   AND (ss_customer_sk = sr_customer_sk)
   AND (ss_store_sk = s_store_sk)
GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip
ORDER BY s_store_name ASC, s_company_id ASC, s_street_number ASC, s_street_name ASC, s_street_type ASC, s_suite_number ASC, s_city ASC, s_county ASC, s_state ASC, s_zip ASC
LIMIT 100;
"""
    order_qt_query50_1_before "${query50_1}"
    async_mv_rewrite_success(db, mv50_1, query50_1, "mv50_1")
    order_qt_query50_1_after "${query50_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv50_1"""

    def mv51 = """
WITH
  web_v1 AS (
   SELECT
     ws_item_sk item_sk
   , d_date
   , sum(sum(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
   FROM
     web_sales
   , date_dim
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
      AND (ws_item_sk IS NOT NULL)
   GROUP BY ws_item_sk, d_date
)
, store_v1 AS (
   SELECT
     ss_item_sk item_sk
   , d_date
   , sum(sum(ss_sales_price)) OVER (PARTITION BY ss_item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
      AND (ss_item_sk IS NOT NULL)
   GROUP BY ss_item_sk, d_date
)
SELECT *
FROM
  (
   SELECT
     item_sk
   , d_date
   , web_sales
   , store_sales
   , max(web_sales) OVER (PARTITION BY item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) web_cumulative
   , max(store_sales) OVER (PARTITION BY item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) store_cumulative
   FROM
     (
      SELECT
        (CASE WHEN (web.item_sk IS NOT NULL) THEN web.item_sk ELSE store.item_sk END) item_sk
      , (CASE WHEN (web.d_date IS NOT NULL) THEN web.d_date ELSE store.d_date END) d_date
      , web.cume_sales web_sales
      , store.cume_sales store_sales
      FROM
        web_v1 web
      FULL JOIN store_v1 store ON (web.item_sk = store.item_sk)
         AND (web.d_date = store.d_date)
   )  x
)  y
WHERE (web_cumulative > store_cumulative)
ORDER BY item_sk ASC, d_date ASC
LIMIT 100;
"""
    def query51 = """
WITH
  web_v1 AS (
   SELECT
     ws_item_sk item_sk
   , d_date
   , sum(sum(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
   FROM
     web_sales
   , date_dim
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
      AND (ws_item_sk IS NOT NULL)
   GROUP BY ws_item_sk, d_date
)
, store_v1 AS (
   SELECT
     ss_item_sk item_sk
   , d_date
   , sum(sum(ss_sales_price)) OVER (PARTITION BY ss_item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cume_sales
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
      AND (ss_item_sk IS NOT NULL)
   GROUP BY ss_item_sk, d_date
)
SELECT *
FROM
  (
   SELECT
     item_sk
   , d_date
   , web_sales
   , store_sales
   , max(web_sales) OVER (PARTITION BY item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) web_cumulative
   , max(store_sales) OVER (PARTITION BY item_sk ORDER BY d_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) store_cumulative
   FROM
     (
      SELECT
        (CASE WHEN (web.item_sk IS NOT NULL) THEN web.item_sk ELSE store.item_sk END) item_sk
      , (CASE WHEN (web.d_date IS NOT NULL) THEN web.d_date ELSE store.d_date END) d_date
      , web.cume_sales web_sales
      , store.cume_sales store_sales
      FROM
        web_v1 web
      FULL JOIN store_v1 store ON (web.item_sk = store.item_sk)
         AND (web.d_date = store.d_date)
   )  x
)  y
WHERE (web_cumulative > store_cumulative)
ORDER BY item_sk ASC, d_date ASC
LIMIT 100;
"""
    order_qt_query51_before "${query51}"
    async_mv_rewrite_fail(db, mv51, query51, "mv51")
    order_qt_query51_after "${query51}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv51"""


    def mv52 = """
SELECT
  dt.d_year
, item.i_brand_id brand_id
, item.i_brand brand
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manager_id = 1)
   AND (dt.d_moy = 11)
   AND (dt.d_year = 2000)
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year ASC, ext_price DESC, brand_id ASC
LIMIT 100;
"""
    def query52 = """
SELECT
  dt.d_year
, item.i_brand_id brand_id
, item.i_brand brand
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manager_id = 1)
   AND (dt.d_moy = 11)
   AND (dt.d_year = 2000)
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year ASC, ext_price DESC, brand_id ASC
LIMIT 100;
"""
    order_qt_query52_before "${query52}"
    async_mv_rewrite_fail(db, mv52, query52, "mv52")
    order_qt_query52_after "${query52}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv52"""

    def mv52_1 = """
SELECT
  dt.d_year
, item.i_brand_id brand_id
, item.i_brand brand
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manager_id = 1)
   AND (dt.d_moy = 11)
   AND (dt.d_year = 2000)
GROUP BY dt.d_year, item.i_brand, item.i_brand_id;
"""
    def query52_1 = """
SELECT
  dt.d_year
, item.i_brand_id brand_id
, item.i_brand brand
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim dt
, store_sales
, item
WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
   AND (store_sales.ss_item_sk = item.i_item_sk)
   AND (item.i_manager_id = 1)
   AND (dt.d_moy = 11)
   AND (dt.d_year = 2000)
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year ASC, ext_price DESC, brand_id ASC
LIMIT 100;
"""
    order_qt_query52_1_before "${query52_1}"
    async_mv_rewrite_success(db, mv52_1, query52_1, "mv52_1")
    order_qt_query52_1_after "${query52_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv52_1"""

    def mv53 = """
SELECT *
FROM
  (
   SELECT
     i_manufact_id
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq IN (1200   , (1200 + 1)   , (1200 + 2)   , (1200 + 3)   , (1200 + 4)   , (1200 + 5)   , (1200 + 6)   , (1200 + 7)   , (1200 + 8)   , (1200 + 9)   , (1200 + 10)   , (1200 + 11)))
      AND (((i_category IN ('Books'         , 'Children'         , 'Electronics'))
            AND (i_class IN ('personal'         , 'portable'         , 'reference'         , 'self-help'))
            AND (i_brand IN ('scholaramalgamalg #14'         , 'scholaramalgamalg #7'         , 'exportiunivamalg #9'         , 'scholaramalgamalg #9')))
         OR ((i_category IN ('Women'         , 'Music'         , 'Men'))
            AND (i_class IN ('accessories'         , 'classical'         , 'fragrances'         , 'pants'))
            AND (i_brand IN ('amalgimporto #1'         , 'edu packscholar #1'         , 'exportiimporto #1'         , 'importoamalg #1'))))
   GROUP BY i_manufact_id, d_qoy
)  tmp1
WHERE ((CASE WHEN (avg_quarterly_sales > 0) THEN (abs((CAST(sum_sales AS DECIMAL(22,4)) - avg_quarterly_sales)) / avg_quarterly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY avg_quarterly_sales ASC, sum_sales ASC, i_manufact_id ASC
LIMIT 100;
"""
    def query53 = """
SELECT *
FROM
  (
   SELECT
     i_manufact_id
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq IN (1200   , (1200 + 1)   , (1200 + 2)   , (1200 + 3)   , (1200 + 4)   , (1200 + 5)   , (1200 + 6)   , (1200 + 7)   , (1200 + 8)   , (1200 + 9)   , (1200 + 10)   , (1200 + 11)))
      AND (((i_category IN ('Books'         , 'Children'         , 'Electronics'))
            AND (i_class IN ('personal'         , 'portable'         , 'reference'         , 'self-help'))
            AND (i_brand IN ('scholaramalgamalg #14'         , 'scholaramalgamalg #7'         , 'exportiunivamalg #9'         , 'scholaramalgamalg #9')))
         OR ((i_category IN ('Women'         , 'Music'         , 'Men'))
            AND (i_class IN ('accessories'         , 'classical'         , 'fragrances'         , 'pants'))
            AND (i_brand IN ('amalgimporto #1'         , 'edu packscholar #1'         , 'exportiimporto #1'         , 'importoamalg #1'))))
   GROUP BY i_manufact_id, d_qoy
)  tmp1
WHERE ((CASE WHEN (avg_quarterly_sales > 0) THEN (abs((CAST(sum_sales AS DECIMAL(22,4)) - avg_quarterly_sales)) / avg_quarterly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY avg_quarterly_sales ASC, sum_sales ASC, i_manufact_id ASC
LIMIT 100;
"""
    order_qt_query53_before "${query53}"
    async_mv_rewrite_fail(db, mv53, query53, "mv53")
    order_qt_query53_after "${query53}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv53"""


    def mv54 = """
WITH
  my_customers AS (
   SELECT DISTINCT
     c_customer_sk
   , c_current_addr_sk
   FROM
     (
      SELECT
        cs_sold_date_sk sold_date_sk
      , cs_bill_customer_sk customer_sk
      , cs_item_sk item_sk
      FROM
        catalog_sales
UNION ALL       SELECT
        ws_sold_date_sk sold_date_sk
      , ws_bill_customer_sk customer_sk
      , ws_item_sk item_sk
      FROM
        web_sales
   )  cs_or_ws_sales
   , item
   , date_dim
   , customer
   WHERE (sold_date_sk = d_date_sk)
      AND (item_sk = i_item_sk)
      AND (i_category = 'Women')
      AND (i_class = 'maternity')
      AND (c_customer_sk = cs_or_ws_sales.customer_sk)
      AND (d_moy = 12)
      AND (d_year = 1998)
)
, my_revenue AS (
   SELECT
     c_customer_sk
   , sum(ss_ext_sales_price) revenue
   FROM
     my_customers
   , store_sales
   , customer_address
   , store
   , date_dim
   WHERE (c_current_addr_sk = ca_address_sk)
      AND (ca_county = s_county)
      AND (ca_state = s_state)
      AND (ss_sold_date_sk = d_date_sk)
      AND (c_customer_sk = ss_customer_sk)
      AND (d_month_seq BETWEEN (
      SELECT DISTINCT (d_month_seq + 1)
      FROM
        date_dim
      WHERE (d_year = 1998)
         AND (d_moy = 12)
   ) AND (
      SELECT DISTINCT (d_month_seq + 3)
      FROM
        date_dim
      WHERE (d_year = 1998)
         AND (d_moy = 12)
   ))
   GROUP BY c_customer_sk
)
, segments AS (
   SELECT CAST((revenue / 50) AS INTEGER) segment
   FROM
     my_revenue
)
SELECT
  segment
, count(*) num_customers
, (segment * 50) segment_base
FROM
  segments
GROUP BY segment
ORDER BY segment ASC, num_customers ASC
LIMIT 100;
"""
    def query54 = """
WITH
  my_customers AS (
   SELECT DISTINCT
     c_customer_sk
   , c_current_addr_sk
   FROM
     (
      SELECT
        cs_sold_date_sk sold_date_sk
      , cs_bill_customer_sk customer_sk
      , cs_item_sk item_sk
      FROM
        catalog_sales
UNION ALL       SELECT
        ws_sold_date_sk sold_date_sk
      , ws_bill_customer_sk customer_sk
      , ws_item_sk item_sk
      FROM
        web_sales
   )  cs_or_ws_sales
   , item
   , date_dim
   , customer
   WHERE (sold_date_sk = d_date_sk)
      AND (item_sk = i_item_sk)
      AND (i_category = 'Women')
      AND (i_class = 'maternity')
      AND (c_customer_sk = cs_or_ws_sales.customer_sk)
      AND (d_moy = 12)
      AND (d_year = 1998)
)
, my_revenue AS (
   SELECT
     c_customer_sk
   , sum(ss_ext_sales_price) revenue
   FROM
     my_customers
   , store_sales
   , customer_address
   , store
   , date_dim
   WHERE (c_current_addr_sk = ca_address_sk)
      AND (ca_county = s_county)
      AND (ca_state = s_state)
      AND (ss_sold_date_sk = d_date_sk)
      AND (c_customer_sk = ss_customer_sk)
      AND (d_month_seq BETWEEN (
      SELECT DISTINCT (d_month_seq + 1)
      FROM
        date_dim
      WHERE (d_year = 1998)
         AND (d_moy = 12)
   ) AND (
      SELECT DISTINCT (d_month_seq + 3)
      FROM
        date_dim
      WHERE (d_year = 1998)
         AND (d_moy = 12)
   ))
   GROUP BY c_customer_sk
)
, segments AS (
   SELECT CAST((revenue / 50) AS INTEGER) segment
   FROM
     my_revenue
)
SELECT
  segment
, count(*) num_customers
, (segment * 50) segment_base
FROM
  segments
GROUP BY segment
ORDER BY segment ASC, num_customers ASC
LIMIT 100;
"""
    order_qt_query54_before "${query54}"
    async_mv_rewrite_fail(db, mv54, query54, "mv54")
    order_qt_query54_after "${query54}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv54"""


    def mv55 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 28)
   AND (d_moy = 11)
   AND (d_year = 1999)
GROUP BY i_brand, i_brand_id
ORDER BY ext_price DESC, i_brand_id ASC
LIMIT 100;
"""
    def query55 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 28)
   AND (d_moy = 11)
   AND (d_year = 1999)
GROUP BY i_brand, i_brand_id
ORDER BY ext_price DESC, i_brand_id ASC
LIMIT 100;
"""
    order_qt_query55_before "${query55}"
    async_mv_rewrite_fail(db, mv55, query55, "mv55")
    order_qt_query55_after "${query55}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv55"""

    def mv55_1 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 28)
   AND (d_moy = 11)
   AND (d_year = 1999)
GROUP BY i_brand, i_brand_id;
"""
    def query55_1 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, sum(ss_ext_sales_price) ext_price
FROM
  date_dim
, store_sales
, item
WHERE (d_date_sk = ss_sold_date_sk)
   AND (ss_item_sk = i_item_sk)
   AND (i_manager_id = 28)
   AND (d_moy = 11)
   AND (d_year = 1999)
GROUP BY i_brand, i_brand_id
ORDER BY ext_price DESC, i_brand_id ASC
LIMIT 100;
"""
    order_qt_query55_1_before "${query55_1}"
    async_mv_rewrite_success(db, mv55_1, query55_1, "mv55_1")
    order_qt_query55_1_after "${query55_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv55_1"""

    def mv56 = """
WITH
  ss AS (
   SELECT
     i_item_id
   , sum(ss_ext_sales_price) total_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_color IN ('slate'      , 'blanched'      , 'burnished'))
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy = 2)
      AND (ss_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
, cs AS (
   SELECT
     i_item_id
   , sum(cs_ext_sales_price) total_sales
   FROM
     catalog_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_color IN ('slate'      , 'blanched'      , 'burnished'))
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy = 2)
      AND (cs_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
, ws AS (
   SELECT
     i_item_id
   , sum(ws_ext_sales_price) total_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_color IN ('slate'      , 'blanched'      , 'burnished'))
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy = 2)
      AND (ws_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
SELECT
  i_item_id
, sum(total_sales) total_sales
FROM
  (
   SELECT *
   FROM
     ss
UNION ALL    SELECT *
   FROM
     cs
UNION ALL    SELECT *
   FROM
     ws
)  tmp1
GROUP BY i_item_id
ORDER BY total_sales ASC, i_item_id ASC
LIMIT 100;
"""
    def query56 = """
WITH
  ss AS (
   SELECT
     i_item_id
   , sum(ss_ext_sales_price) total_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_color IN ('slate'      , 'blanched'      , 'burnished'))
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy = 2)
      AND (ss_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
, cs AS (
   SELECT
     i_item_id
   , sum(cs_ext_sales_price) total_sales
   FROM
     catalog_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_color IN ('slate'      , 'blanched'      , 'burnished'))
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy = 2)
      AND (cs_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
, ws AS (
   SELECT
     i_item_id
   , sum(ws_ext_sales_price) total_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_color IN ('slate'      , 'blanched'      , 'burnished'))
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy = 2)
      AND (ws_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
SELECT
  i_item_id
, sum(total_sales) total_sales
FROM
  (
   SELECT *
   FROM
     ss
UNION ALL    SELECT *
   FROM
     cs
UNION ALL    SELECT *
   FROM
     ws
)  tmp1
GROUP BY i_item_id
ORDER BY total_sales ASC, i_item_id ASC
LIMIT 100;
"""
    order_qt_query56_before "${query56}"
    async_mv_rewrite_fail(db, mv56, query56, "mv56")
    order_qt_query56_after "${query56}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv56"""


    def mv57 = """
WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , cc_name
   , d_year
   , d_moy
   , sum(cs_sales_price) sum_sales
   , avg(sum(cs_sales_price)) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , catalog_sales
   , date_dim
   , call_center
   WHERE (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (cc_call_center_sk = cs_call_center_sk)
      AND ((d_year = 1999)
         OR ((d_year = (1999 - 1))
            AND (d_moy = 12))
         OR ((d_year = (1999 + 1))
            AND (d_moy = 1)))
   GROUP BY i_category, i_brand, cc_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.cc_name
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
      AND (v1.cc_name = v1_lag.cc_name)
      AND (v1.cc_name = v1_lead.cc_name)
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
LIMIT 100;
"""
    def query57 = """
WITH
  v1 AS (
   SELECT
     i_category
   , i_brand
   , cc_name
   , d_year
   , d_moy
   , sum(cs_sales_price) sum_sales
   , avg(sum(cs_sales_price)) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) avg_monthly_sales
   , rank() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year ASC, d_moy ASC) rn
   FROM
     item
   , catalog_sales
   , date_dim
   , call_center
   WHERE (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (cc_call_center_sk = cs_call_center_sk)
      AND ((d_year = 1999)
         OR ((d_year = (1999 - 1))
            AND (d_moy = 12))
         OR ((d_year = (1999 + 1))
            AND (d_moy = 1)))
   GROUP BY i_category, i_brand, cc_name, d_year, d_moy
)
, v2 AS (
   SELECT
     v1.i_category
   , v1.i_brand
   , v1.cc_name
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
      AND (v1.cc_name = v1_lag.cc_name)
      AND (v1.cc_name = v1_lead.cc_name)
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
LIMIT 100;
"""
    order_qt_query57_before "${query57}"
    async_mv_rewrite_fail(db, mv57, query57, "mv57")
    order_qt_query57_after "${query57}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv57"""


    def mv58 = """
WITH
  ss_items AS (
   SELECT
     i_item_id item_id
   , sum(ss_ext_sales_price) ss_item_rev
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq = (
            SELECT d_week_seq
            FROM
              date_dim
            WHERE (d_date = CAST('2000-01-03' AS DATE))
         ))
   ))
      AND (ss_sold_date_sk = d_date_sk)
   GROUP BY i_item_id
)
, cs_items AS (
   SELECT
     i_item_id item_id
   , sum(cs_ext_sales_price) cs_item_rev
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq = (
            SELECT d_week_seq
            FROM
              date_dim
            WHERE (d_date = CAST('2000-01-03' AS DATE))
         ))
   ))
      AND (cs_sold_date_sk = d_date_sk)
   GROUP BY i_item_id
)
, ws_items AS (
   SELECT
     i_item_id item_id
   , sum(ws_ext_sales_price) ws_item_rev
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq = (
            SELECT d_week_seq
            FROM
              date_dim
            WHERE (d_date = CAST('2000-01-03' AS DATE))
         ))
   ))
      AND (ws_sold_date_sk = d_date_sk)
   GROUP BY i_item_id
)
SELECT
  ss_items.item_id
, ss_item_rev
, CAST((((ss_item_rev / ((CAST(ss_item_rev AS DECIMAL(16,7)) + cs_item_rev) + ws_item_rev)) / 3) * 100) AS DECIMAL(7,2)) ss_dev
, cs_item_rev
, CAST((((cs_item_rev / ((CAST(ss_item_rev AS DECIMAL(16,7)) + cs_item_rev) + ws_item_rev)) / 3) * 100) AS DECIMAL(7,2)) cs_dev
, ws_item_rev
, CAST((((ws_item_rev / ((CAST(ss_item_rev AS DECIMAL(16,7)) + cs_item_rev) + ws_item_rev)) / 3) * 100) AS DECIMAL(7,2)) ws_dev
, (((ss_item_rev + cs_item_rev) + ws_item_rev) / 3) average
FROM
  ss_items
, cs_items
, ws_items
WHERE (ss_items.item_id = cs_items.item_id)
   AND (ss_items.item_id = ws_items.item_id)
   AND (ss_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * cs_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * cs_item_rev))
   AND (ss_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * ws_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * ws_item_rev))
   AND (cs_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * ss_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * ss_item_rev))
   AND (cs_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * ws_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * ws_item_rev))
   AND (ws_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * ss_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * ss_item_rev))
   AND (ws_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * cs_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * cs_item_rev))
ORDER BY ss_items.item_id ASC, ss_item_rev ASC
LIMIT 100;
"""
    def query58 = """
WITH
  ss_items AS (
   SELECT
     i_item_id item_id
   , sum(ss_ext_sales_price) ss_item_rev
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq = (
            SELECT d_week_seq
            FROM
              date_dim
            WHERE (d_date = CAST('2000-01-03' AS DATE))
         ))
   ))
      AND (ss_sold_date_sk = d_date_sk)
   GROUP BY i_item_id
)
, cs_items AS (
   SELECT
     i_item_id item_id
   , sum(cs_ext_sales_price) cs_item_rev
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq = (
            SELECT d_week_seq
            FROM
              date_dim
            WHERE (d_date = CAST('2000-01-03' AS DATE))
         ))
   ))
      AND (cs_sold_date_sk = d_date_sk)
   GROUP BY i_item_id
)
, ws_items AS (
   SELECT
     i_item_id item_id
   , sum(ws_ext_sales_price) ws_item_rev
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq = (
            SELECT d_week_seq
            FROM
              date_dim
            WHERE (d_date = CAST('2000-01-03' AS DATE))
         ))
   ))
      AND (ws_sold_date_sk = d_date_sk)
   GROUP BY i_item_id
)
SELECT
  ss_items.item_id
, ss_item_rev
, CAST((((ss_item_rev / ((CAST(ss_item_rev AS DECIMAL(16,7)) + cs_item_rev) + ws_item_rev)) / 3) * 100) AS DECIMAL(7,2)) ss_dev
, cs_item_rev
, CAST((((cs_item_rev / ((CAST(ss_item_rev AS DECIMAL(16,7)) + cs_item_rev) + ws_item_rev)) / 3) * 100) AS DECIMAL(7,2)) cs_dev
, ws_item_rev
, CAST((((ws_item_rev / ((CAST(ss_item_rev AS DECIMAL(16,7)) + cs_item_rev) + ws_item_rev)) / 3) * 100) AS DECIMAL(7,2)) ws_dev
, (((ss_item_rev + cs_item_rev) + ws_item_rev) / 3) average
FROM
  ss_items
, cs_items
, ws_items
WHERE (ss_items.item_id = cs_items.item_id)
   AND (ss_items.item_id = ws_items.item_id)
   AND (ss_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * cs_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * cs_item_rev))
   AND (ss_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * ws_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * ws_item_rev))
   AND (cs_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * ss_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * ss_item_rev))
   AND (cs_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * ws_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * ws_item_rev))
   AND (ws_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * ss_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * ss_item_rev))
   AND (ws_item_rev BETWEEN (CAST('0.9' AS DECIMAL(2,1)) * cs_item_rev) AND (CAST('1.1' AS DECIMAL(2,1)) * cs_item_rev))
ORDER BY ss_items.item_id ASC, ss_item_rev ASC
LIMIT 100;
"""
    order_qt_query58_before "${query58}"
    async_mv_rewrite_fail(db, mv58, query58, "mv58")
    order_qt_query58_after "${query58}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv58"""



    def mv59 = """
WITH
  wss AS (
   SELECT
     d_week_seq
   , ss_store_sk
   , sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
   , sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
   , sum((CASE WHEN (d_day_name = 'Thursday ') THEN ss_sales_price ELSE null END)) thu_sales
   , sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
   , sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
   FROM
     store_sales
   , date_dim
   WHERE (d_date_sk = ss_sold_date_sk)
   GROUP BY d_week_seq, ss_store_sk
)
SELECT
  s_store_name1
, s_store_id1
, d_week_seq1
, (sun_sales1 / sun_sales2)
, (mon_sales1 / mon_sales2)
, (tue_sales1 / tue_sales2)
, (wed_sales1 / wed_sales2)
, (thu_sales1 / thu_sales2)
, (fri_sales1 / fri_sales2)
, (sat_sales1 / sat_sales2)
FROM
  (
   SELECT
     s_store_name s_store_name1
   , wss.d_week_seq d_week_seq1
   , s_store_id s_store_id1
   , sun_sales sun_sales1
   , mon_sales mon_sales1
   , tue_sales tue_sales1
   , wed_sales wed_sales1
   , thu_sales thu_sales1
   , fri_sales fri_sales1
   , sat_sales sat_sales1
   FROM
     wss
   , store
   , date_dim d
   WHERE (d.d_week_seq = wss.d_week_seq)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq BETWEEN 1212 AND (1212 + 11))
)  y
, (
   SELECT
     s_store_name s_store_name2
   , wss.d_week_seq d_week_seq2
   , s_store_id s_store_id2
   , sun_sales sun_sales2
   , mon_sales mon_sales2
   , tue_sales tue_sales2
   , wed_sales wed_sales2
   , thu_sales thu_sales2
   , fri_sales fri_sales2
   , sat_sales sat_sales2
   FROM
     wss
   , store
   , date_dim d
   WHERE (d.d_week_seq = wss.d_week_seq)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq BETWEEN (1212 + 12) AND (1212 + 23))
)  x
WHERE (s_store_id1 = s_store_id2)
   AND (d_week_seq1 = (d_week_seq2 - 52))
ORDER BY s_store_name1 ASC, s_store_id1 ASC, d_week_seq1 ASC
LIMIT 100;
"""
    def query59 = """
WITH
  wss AS (
   SELECT
     d_week_seq
   , ss_store_sk
   , sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
   , sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
   , sum((CASE WHEN (d_day_name = 'Thursday ') THEN ss_sales_price ELSE null END)) thu_sales
   , sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
   , sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
   FROM
     store_sales
   , date_dim
   WHERE (d_date_sk = ss_sold_date_sk)
   GROUP BY d_week_seq, ss_store_sk
)
SELECT
  s_store_name1
, s_store_id1
, d_week_seq1
, (sun_sales1 / sun_sales2)
, (mon_sales1 / mon_sales2)
, (tue_sales1 / tue_sales2)
, (wed_sales1 / wed_sales2)
, (thu_sales1 / thu_sales2)
, (fri_sales1 / fri_sales2)
, (sat_sales1 / sat_sales2)
FROM
  (
   SELECT
     s_store_name s_store_name1
   , wss.d_week_seq d_week_seq1
   , s_store_id s_store_id1
   , sun_sales sun_sales1
   , mon_sales mon_sales1
   , tue_sales tue_sales1
   , wed_sales wed_sales1
   , thu_sales thu_sales1
   , fri_sales fri_sales1
   , sat_sales sat_sales1
   FROM
     wss
   , store
   , date_dim d
   WHERE (d.d_week_seq = wss.d_week_seq)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq BETWEEN 1212 AND (1212 + 11))
)  y
, (
   SELECT
     s_store_name s_store_name2
   , wss.d_week_seq d_week_seq2
   , s_store_id s_store_id2
   , sun_sales sun_sales2
   , mon_sales mon_sales2
   , tue_sales tue_sales2
   , wed_sales wed_sales2
   , thu_sales thu_sales2
   , fri_sales fri_sales2
   , sat_sales sat_sales2
   FROM
     wss
   , store
   , date_dim d
   WHERE (d.d_week_seq = wss.d_week_seq)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq BETWEEN (1212 + 12) AND (1212 + 23))
)  x
WHERE (s_store_id1 = s_store_id2)
   AND (d_week_seq1 = (d_week_seq2 - 52))
ORDER BY s_store_name1 ASC, s_store_id1 ASC, d_week_seq1 ASC
LIMIT 100;
"""
    order_qt_query59_before "${query59}"
    async_mv_rewrite_fail(db, mv59, query59, "mv59")
    order_qt_query59_after "${query59}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv59"""


    def mv60 = """
WITH
  ss AS (
   SELECT
     i_item_id
   , sum(ss_ext_sales_price) total_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_category IN ('Music'))
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 9)
      AND (ss_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
, cs AS (
   SELECT
     i_item_id
   , sum(cs_ext_sales_price) total_sales
   FROM
     catalog_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_category IN ('Music'))
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 9)
      AND (cs_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
, ws AS (
   SELECT
     i_item_id
   , sum(ws_ext_sales_price) total_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_category IN ('Music'))
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 9)
      AND (ws_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
SELECT
  i_item_id
, sum(total_sales) total_sales
FROM
  (
   SELECT *
   FROM
     ss
UNION ALL    SELECT *
   FROM
     cs
UNION ALL    SELECT *
   FROM
     ws
)  tmp1
GROUP BY i_item_id
ORDER BY i_item_id ASC, total_sales ASC
LIMIT 100;
"""
    def query60 = """
WITH
  ss AS (
   SELECT
     i_item_id
   , sum(ss_ext_sales_price) total_sales
   FROM
     store_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_category IN ('Music'))
   ))
      AND (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 9)
      AND (ss_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
, cs AS (
   SELECT
     i_item_id
   , sum(cs_ext_sales_price) total_sales
   FROM
     catalog_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_category IN ('Music'))
   ))
      AND (cs_item_sk = i_item_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 9)
      AND (cs_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
, ws AS (
   SELECT
     i_item_id
   , sum(ws_ext_sales_price) total_sales
   FROM
     web_sales
   , date_dim
   , customer_address
   , item
   WHERE (i_item_id IN (
      SELECT i_item_id
      FROM
        item
      WHERE (i_category IN ('Music'))
   ))
      AND (ws_item_sk = i_item_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 1998)
      AND (d_moy = 9)
      AND (ws_bill_addr_sk = ca_address_sk)
      AND (ca_gmt_offset = -5)
   GROUP BY i_item_id
)
SELECT
  i_item_id
, sum(total_sales) total_sales
FROM
  (
   SELECT *
   FROM
     ss
UNION ALL    SELECT *
   FROM
     cs
UNION ALL    SELECT *
   FROM
     ws
)  tmp1
GROUP BY i_item_id
ORDER BY i_item_id ASC, total_sales ASC
LIMIT 100;
"""
    order_qt_query60_before "${query60}"
    async_mv_rewrite_fail(db, mv60, query60, "mv60")
    order_qt_query60_after "${query60}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv60"""

    def mv61 = """
SELECT
  promotions
, total
, ((CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4))) * 100)
FROM
  (
   SELECT sum(ss_ext_sales_price) promotions
   FROM
     store_sales
   , store
   , promotion
   , date_dim
   , customer
   , customer_address
   , item
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (ss_promo_sk = p_promo_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ca_address_sk = c_current_addr_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ca_gmt_offset = -5)
      AND (i_category = 'Jewelry')
      AND ((p_channel_dmail = 'Y')
         OR (p_channel_email = 'Y')
         OR (p_channel_tv = 'Y'))
      AND (s_gmt_offset = -5)
      AND (d_year = 1998)
      AND (d_moy = 11)
)  promotional_sales
, (
   SELECT sum(ss_ext_sales_price) total
   FROM
     store_sales
   , store
   , date_dim
   , customer
   , customer_address
   , item
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ca_address_sk = c_current_addr_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ca_gmt_offset = -5)
      AND (i_category = 'Jewelry')
      AND (s_gmt_offset = -5)
      AND (d_year = 1998)
      AND (d_moy = 11)
)  all_sales
ORDER BY promotions ASC, total ASC
LIMIT 100;
"""
    def query61 = """
SELECT
  promotions
, total
, ((CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4))) * 100)
FROM
  (
   SELECT sum(ss_ext_sales_price) promotions
   FROM
     store_sales
   , store
   , promotion
   , date_dim
   , customer
   , customer_address
   , item
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (ss_promo_sk = p_promo_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ca_address_sk = c_current_addr_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ca_gmt_offset = -5)
      AND (i_category = 'Jewelry')
      AND ((p_channel_dmail = 'Y')
         OR (p_channel_email = 'Y')
         OR (p_channel_tv = 'Y'))
      AND (s_gmt_offset = -5)
      AND (d_year = 1998)
      AND (d_moy = 11)
)  promotional_sales
, (
   SELECT sum(ss_ext_sales_price) total
   FROM
     store_sales
   , store
   , date_dim
   , customer
   , customer_address
   , item
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ca_address_sk = c_current_addr_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ca_gmt_offset = -5)
      AND (i_category = 'Jewelry')
      AND (s_gmt_offset = -5)
      AND (d_year = 1998)
      AND (d_moy = 11)
)  all_sales
ORDER BY promotions ASC, total ASC
LIMIT 100;
"""
    order_qt_query61_before "${query61}"
    async_mv_rewrite_fail(db, mv61, query61, "mv61")
    order_qt_query61_after "${query61}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv61"""

    def mv62 = """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, web_name
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 30)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 60)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 90)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '120 days'
FROM
  web_sales
, warehouse
, ship_mode
, web_site
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (ws_ship_date_sk = d_date_sk)
   AND (ws_warehouse_sk = w_warehouse_sk)
   AND (ws_ship_mode_sk = sm_ship_mode_sk)
   AND (ws_web_site_sk = web_site_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, web_name
ORDER BY substr(w_warehouse_name, 1, 20) ASC, sm_type ASC, web_name ASC
LIMIT 100;
"""
    def query62 = """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, web_name
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 30)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 60)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 90)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
FROM
  web_sales
, warehouse
, ship_mode
, web_site
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (ws_ship_date_sk = d_date_sk)
   AND (ws_warehouse_sk = w_warehouse_sk)
   AND (ws_ship_mode_sk = sm_ship_mode_sk)
   AND (ws_web_site_sk = web_site_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, web_name
ORDER BY substr(w_warehouse_name, 1, 20) ASC, sm_type ASC, web_name ASC
LIMIT 100;
"""
    order_qt_query62_before "${query62}"
    async_mv_rewrite_fail(db, mv62, query62, "mv62")
    order_qt_query62_after "${query62}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv62"""

    def mv62_1 = """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, web_name
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 30)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 60)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 90)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '120 days'
FROM
  web_sales
, warehouse
, ship_mode
, web_site
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (ws_ship_date_sk = d_date_sk)
   AND (ws_warehouse_sk = w_warehouse_sk)
   AND (ws_ship_mode_sk = sm_ship_mode_sk)
   AND (ws_web_site_sk = web_site_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, web_name;
"""
    def query62_1 = """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, web_name
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 30)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 60)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 90)
   AND ((ws_ship_date_sk - ws_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((ws_ship_date_sk - ws_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
FROM
  web_sales
, warehouse
, ship_mode
, web_site
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (ws_ship_date_sk = d_date_sk)
   AND (ws_warehouse_sk = w_warehouse_sk)
   AND (ws_ship_mode_sk = sm_ship_mode_sk)
   AND (ws_web_site_sk = web_site_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, web_name
ORDER BY substr(w_warehouse_name, 1, 20) ASC, sm_type ASC, web_name ASC
LIMIT 100;
"""
    order_qt_query62_1_before "${query62_1}"
    async_mv_rewrite_success(db, mv62_1, query62_1, "mv62_1")
    order_qt_query62_1_after "${query62_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv62_1"""

    def mv63 = """
SELECT *
FROM
  (
   SELECT
     i_manager_id
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_manager_id) avg_monthly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq IN (1200   , (1200 + 1)   , (1200 + 2)   , (1200 + 3)   , (1200 + 4)   , (1200 + 5)   , (1200 + 6)   , (1200 + 7)   , (1200 + 8)   , (1200 + 9)   , (1200 + 10)   , (1200 + 11)))
      AND (((i_category IN ('Books'         , 'Children'         , 'Electronics'))
            AND (i_class IN ('personal'         , 'portable'         , 'refernece'         , 'self-help'))
            AND (i_brand IN ('scholaramalgamalg #14'         , 'scholaramalgamalg #7'         , 'exportiunivamalg #9'         , 'scholaramalgamalg #9')))
         OR ((i_category IN ('Women'         , 'Music'         , 'Men'))
            AND (i_class IN ('accessories'         , 'classical'         , 'fragrances'         , 'pants'))
            AND (i_brand IN ('amalgimporto #1'         , 'edu packscholar #1'         , 'exportiimporto #1'         , 'importoamalg #1'))))
   GROUP BY i_manager_id, d_moy
)  tmp1
WHERE ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY i_manager_id ASC, avg_monthly_sales ASC, sum_sales ASC
LIMIT 100;
"""
    def query63 = """
SELECT *
FROM
  (
   SELECT
     i_manager_id
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_manager_id) avg_monthly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq IN (1200   , (1200 + 1)   , (1200 + 2)   , (1200 + 3)   , (1200 + 4)   , (1200 + 5)   , (1200 + 6)   , (1200 + 7)   , (1200 + 8)   , (1200 + 9)   , (1200 + 10)   , (1200 + 11)))
      AND (((i_category IN ('Books'         , 'Children'         , 'Electronics'))
            AND (i_class IN ('personal'         , 'portable'         , 'refernece'         , 'self-help'))
            AND (i_brand IN ('scholaramalgamalg #14'         , 'scholaramalgamalg #7'         , 'exportiunivamalg #9'         , 'scholaramalgamalg #9')))
         OR ((i_category IN ('Women'         , 'Music'         , 'Men'))
            AND (i_class IN ('accessories'         , 'classical'         , 'fragrances'         , 'pants'))
            AND (i_brand IN ('amalgimporto #1'         , 'edu packscholar #1'         , 'exportiimporto #1'         , 'importoamalg #1'))))
   GROUP BY i_manager_id, d_moy
)  tmp1
WHERE ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY i_manager_id ASC, avg_monthly_sales ASC, sum_sales ASC
LIMIT 100;
"""
    order_qt_query63_before "${query63}"
    async_mv_rewrite_fail(db, mv63, query63, "mv63")
    order_qt_query63_after "${query63}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv63"""

    def mv63_1 = """
SELECT
     i_manager_id
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_manager_id) avg_monthly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq IN (1200   , (1200 + 1)   , (1200 + 2)   , (1200 + 3)   , (1200 + 4)   , (1200 + 5)   , (1200 + 6)   , (1200 + 7)   , (1200 + 8)   , (1200 + 9)   , (1200 + 10)   , (1200 + 11)))
      AND (((i_category IN ('Books'         , 'Children'         , 'Electronics'))
            AND (i_class IN ('personal'         , 'portable'         , 'refernece'         , 'self-help'))
            AND (i_brand IN ('scholaramalgamalg #14'         , 'scholaramalgamalg #7'         , 'exportiunivamalg #9'         , 'scholaramalgamalg #9')))
         OR ((i_category IN ('Women'         , 'Music'         , 'Men'))
            AND (i_class IN ('accessories'         , 'classical'         , 'fragrances'         , 'pants'))
            AND (i_brand IN ('amalgimporto #1'         , 'edu packscholar #1'         , 'exportiimporto #1'         , 'importoamalg #1'))))
   GROUP BY i_manager_id, d_moy;
"""
    def query63_1 = """
SELECT *
FROM
  (
   SELECT
     i_manager_id
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_manager_id) avg_monthly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_month_seq IN (1200   , (1200 + 1)   , (1200 + 2)   , (1200 + 3)   , (1200 + 4)   , (1200 + 5)   , (1200 + 6)   , (1200 + 7)   , (1200 + 8)   , (1200 + 9)   , (1200 + 10)   , (1200 + 11)))
      AND (((i_category IN ('Books'         , 'Children'         , 'Electronics'))
            AND (i_class IN ('personal'         , 'portable'         , 'refernece'         , 'self-help'))
            AND (i_brand IN ('scholaramalgamalg #14'         , 'scholaramalgamalg #7'         , 'exportiunivamalg #9'         , 'scholaramalgamalg #9')))
         OR ((i_category IN ('Women'         , 'Music'         , 'Men'))
            AND (i_class IN ('accessories'         , 'classical'         , 'fragrances'         , 'pants'))
            AND (i_brand IN ('amalgimporto #1'         , 'edu packscholar #1'         , 'exportiimporto #1'         , 'importoamalg #1'))))
   GROUP BY i_manager_id, d_moy
)  tmp1
WHERE ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY i_manager_id ASC, avg_monthly_sales ASC, sum_sales ASC
LIMIT 100;
"""
    order_qt_query63_1_before "${query63_1}"
    // should success but fail
    async_mv_rewrite_fail(db, mv63_1, query63_1, "mv63_1")
    order_qt_query63_1_after "${query63_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv63_1"""

    def mv64 = """
WITH
  cs_ui AS (
   SELECT
     cs_item_sk
   , sum(cs_ext_list_price) sale
   , sum(((cr_refunded_cash + cr_reversed_charge) + cr_store_credit)) refund
   FROM
     catalog_sales
   , catalog_returns
   WHERE (cs_item_sk = cr_item_sk)
      AND (cs_order_number = cr_order_number)
   GROUP BY cs_item_sk
   HAVING (sum(cs_ext_list_price) > (2 * sum(((cr_refunded_cash + cr_reversed_charge) + cr_store_credit))))
)
, cross_sales AS (
   SELECT
     i_product_name product_name
   , i_item_sk item_sk
   , s_store_name store_name
   , s_zip store_zip
   , ad1.ca_street_number b_street_number
   , ad1.ca_street_name b_street_name
   , ad1.ca_city b_city
   , ad1.ca_zip b_zip
   , ad2.ca_street_number c_street_number
   , ad2.ca_street_name c_street_name
   , ad2.ca_city c_city
   , ad2.ca_zip c_zip
   , d1.d_year syear
   , d2.d_year fsyear
   , d3.d_year s2year
   , count(*) cnt
   , sum(ss_wholesale_cost) s1
   , sum(ss_list_price) s2
   , sum(ss_coupon_amt) s3
   FROM
     store_sales
   , store_returns
   , cs_ui
   , date_dim d1
   , date_dim d2
   , date_dim d3
   , store
   , customer
   , customer_demographics cd1
   , customer_demographics cd2
   , promotion
   , household_demographics hd1
   , household_demographics hd2
   , customer_address ad1
   , customer_address ad2
   , income_band ib1
   , income_band ib2
   , item
   WHERE (ss_store_sk = s_store_sk)
      AND (ss_sold_date_sk = d1.d_date_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ss_cdemo_sk = cd1.cd_demo_sk)
      AND (ss_hdemo_sk = hd1.hd_demo_sk)
      AND (ss_addr_sk = ad1.ca_address_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ss_item_sk = sr_item_sk)
      AND (ss_ticket_number = sr_ticket_number)
      AND (ss_item_sk = cs_ui.cs_item_sk)
      AND (c_current_cdemo_sk = cd2.cd_demo_sk)
      AND (c_current_hdemo_sk = hd2.hd_demo_sk)
      AND (c_current_addr_sk = ad2.ca_address_sk)
      AND (c_first_sales_date_sk = d2.d_date_sk)
      AND (c_first_shipto_date_sk = d3.d_date_sk)
      AND (ss_promo_sk = p_promo_sk)
      AND (hd1.hd_income_band_sk = ib1.ib_income_band_sk)
      AND (hd2.hd_income_band_sk = ib2.ib_income_band_sk)
      AND (cd1.cd_marital_status <> cd2.cd_marital_status)
      AND (i_color IN ('purple'   , 'burlywood'   , 'indian'   , 'spring'   , 'floral'   , 'medium'))
      AND (i_current_price BETWEEN 64 AND (64 + 10))
      AND (i_current_price BETWEEN (64 + 1) AND (64 + 15))
   GROUP BY i_product_name, i_item_sk, s_store_name, s_zip, ad1.ca_street_number, ad1.ca_street_name, ad1.ca_city, ad1.ca_zip, ad2.ca_street_number, ad2.ca_street_name, ad2.ca_city, ad2.ca_zip, d1.d_year, d2.d_year, d3.d_year
)
SELECT
  cs1.product_name
, cs1.store_name
, cs1.store_zip
, cs1.b_street_number
, cs1.b_street_name
, cs1.b_city
, cs1.b_zip
, cs1.c_street_number
, cs1.c_street_name
, cs1.c_city
, cs1.c_zip
, cs1.syear
, cs1.cnt
, cs1.s1 s11
, cs1.s2 s21
, cs1.s3 s31
, cs2.s1 s12
, cs2.s2 s22
, cs2.s3 s32
, cs2.syear as syear_alias
, cs2.cnt as cnt_alias
FROM
  cross_sales cs1
, cross_sales cs2
WHERE (cs1.item_sk = cs2.item_sk)
   AND (cs1.syear = 1999)
   AND (cs2.syear = (1999 + 1))
   AND (cs2.cnt <= cs1.cnt)
   AND (cs1.store_name = cs2.store_name)
   AND (cs1.store_zip = cs2.store_zip)
ORDER BY cs1.product_name ASC, cs1.store_name ASC, cs2.cnt ASC, 14, 15, 16, 17, 18;
"""
    def query64 = """
WITH
  cs_ui AS (
   SELECT
     cs_item_sk
   , sum(cs_ext_list_price) sale
   , sum(((cr_refunded_cash + cr_reversed_charge) + cr_store_credit)) refund
   FROM
     catalog_sales
   , catalog_returns
   WHERE (cs_item_sk = cr_item_sk)
      AND (cs_order_number = cr_order_number)
   GROUP BY cs_item_sk
   HAVING (sum(cs_ext_list_price) > (2 * sum(((cr_refunded_cash + cr_reversed_charge) + cr_store_credit))))
)
, cross_sales AS (
   SELECT
     i_product_name product_name
   , i_item_sk item_sk
   , s_store_name store_name
   , s_zip store_zip
   , ad1.ca_street_number b_street_number
   , ad1.ca_street_name b_street_name
   , ad1.ca_city b_city
   , ad1.ca_zip b_zip
   , ad2.ca_street_number c_street_number
   , ad2.ca_street_name c_street_name
   , ad2.ca_city c_city
   , ad2.ca_zip c_zip
   , d1.d_year syear
   , d2.d_year fsyear
   , d3.d_year s2year
   , count(*) cnt
   , sum(ss_wholesale_cost) s1
   , sum(ss_list_price) s2
   , sum(ss_coupon_amt) s3
   FROM
     store_sales
   , store_returns
   , cs_ui
   , date_dim d1
   , date_dim d2
   , date_dim d3
   , store
   , customer
   , customer_demographics cd1
   , customer_demographics cd2
   , promotion
   , household_demographics hd1
   , household_demographics hd2
   , customer_address ad1
   , customer_address ad2
   , income_band ib1
   , income_band ib2
   , item
   WHERE (ss_store_sk = s_store_sk)
      AND (ss_sold_date_sk = d1.d_date_sk)
      AND (ss_customer_sk = c_customer_sk)
      AND (ss_cdemo_sk = cd1.cd_demo_sk)
      AND (ss_hdemo_sk = hd1.hd_demo_sk)
      AND (ss_addr_sk = ad1.ca_address_sk)
      AND (ss_item_sk = i_item_sk)
      AND (ss_item_sk = sr_item_sk)
      AND (ss_ticket_number = sr_ticket_number)
      AND (ss_item_sk = cs_ui.cs_item_sk)
      AND (c_current_cdemo_sk = cd2.cd_demo_sk)
      AND (c_current_hdemo_sk = hd2.hd_demo_sk)
      AND (c_current_addr_sk = ad2.ca_address_sk)
      AND (c_first_sales_date_sk = d2.d_date_sk)
      AND (c_first_shipto_date_sk = d3.d_date_sk)
      AND (ss_promo_sk = p_promo_sk)
      AND (hd1.hd_income_band_sk = ib1.ib_income_band_sk)
      AND (hd2.hd_income_band_sk = ib2.ib_income_band_sk)
      AND (cd1.cd_marital_status <> cd2.cd_marital_status)
      AND (i_color IN ('purple'   , 'burlywood'   , 'indian'   , 'spring'   , 'floral'   , 'medium'))
      AND (i_current_price BETWEEN 64 AND (64 + 10))
      AND (i_current_price BETWEEN (64 + 1) AND (64 + 15))
   GROUP BY i_product_name, i_item_sk, s_store_name, s_zip, ad1.ca_street_number, ad1.ca_street_name, ad1.ca_city, ad1.ca_zip, ad2.ca_street_number, ad2.ca_street_name, ad2.ca_city, ad2.ca_zip, d1.d_year, d2.d_year, d3.d_year
)
SELECT
  cs1.product_name
, cs1.store_name
, cs1.store_zip
, cs1.b_street_number
, cs1.b_street_name
, cs1.b_city
, cs1.b_zip
, cs1.c_street_number
, cs1.c_street_name
, cs1.c_city
, cs1.c_zip
, cs1.syear
, cs1.cnt
, cs1.s1 s11
, cs1.s2 s21
, cs1.s3 s31
, cs2.s1 s12
, cs2.s2 s22
, cs2.s3 s32
, cs2.syear
, cs2.cnt
FROM
  cross_sales cs1
, cross_sales cs2
WHERE (cs1.item_sk = cs2.item_sk)
   AND (cs1.syear = 1999)
   AND (cs2.syear = (1999 + 1))
   AND (cs2.cnt <= cs1.cnt)
   AND (cs1.store_name = cs2.store_name)
   AND (cs1.store_zip = cs2.store_zip)
ORDER BY cs1.product_name ASC, cs1.store_name ASC, cs2.cnt ASC, 14, 15, 16, 17, 18;
"""
    order_qt_query64_before "${query64}"
    async_mv_rewrite_fail(db, mv64, query64, "mv64")
    order_qt_query64_after "${query64}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv64"""


    def mv65 = """
SELECT
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
LIMIT 100;
"""
    def query65 = """
SELECT
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
LIMIT 100;
"""
    order_qt_query65_before "${query65}"
    async_mv_rewrite_fail(db, mv65, query65, "mv65")
    order_qt_query65_after "${query65}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv65"""


    def mv66 = """
SELECT
  w_warehouse_name
, w_warehouse_sq_ft
, w_city
, w_county
, w_state
, w_country
, ship_carriers
, year
, sum(jan_sales) jan_sales
, sum(feb_sales) feb_sales
, sum(mar_sales) mar_sales
, sum(apr_sales) apr_sales
, sum(may_sales) may_sales
, sum(jun_sales) jun_sales
, sum(jul_sales) jul_sales
, sum(aug_sales) aug_sales
, sum(sep_sales) sep_sales
, sum(oct_sales) oct_sales
, sum(nov_sales) nov_sales
, sum(dec_sales) dec_sales
, sum((jan_sales / w_warehouse_sq_ft)) jan_sales_per_sq_foot
, sum((feb_sales / w_warehouse_sq_ft)) feb_sales_per_sq_foot
, sum((mar_sales / w_warehouse_sq_ft)) mar_sales_per_sq_foot
, sum((apr_sales / w_warehouse_sq_ft)) apr_sales_per_sq_foot
, sum((may_sales / w_warehouse_sq_ft)) may_sales_per_sq_foot
, sum((jun_sales / w_warehouse_sq_ft)) jun_sales_per_sq_foot
, sum((jul_sales / w_warehouse_sq_ft)) jul_sales_per_sq_foot
, sum((aug_sales / w_warehouse_sq_ft)) aug_sales_per_sq_foot
, sum((sep_sales / w_warehouse_sq_ft)) sep_sales_per_sq_foot
, sum((oct_sales / w_warehouse_sq_ft)) oct_sales_per_sq_foot
, sum((nov_sales / w_warehouse_sq_ft)) nov_sales_per_sq_foot
, sum((dec_sales / w_warehouse_sq_ft)) dec_sales_per_sq_foot
, sum(jan_net) jan_net
, sum(feb_net) feb_net
, sum(mar_net) mar_net
, sum(apr_net) apr_net
, sum(may_net) may_net
, sum(jun_net) jun_net
, sum(jul_net) jul_net
, sum(aug_net) aug_net
, sum(sep_net) sep_net
, sum(oct_net) oct_net
, sum(nov_net) nov_net
, sum(dec_net) dec_net
FROM
(
      SELECT
        w_warehouse_name
      , w_warehouse_sq_ft
      , w_city
      , w_county
      , w_state
      , w_country
      , concat(concat('DHL', ','), 'BARIAN') ship_carriers
      , d_year YEAR
      , sum((CASE WHEN (d_moy = 1) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) jan_sales
      , sum((CASE WHEN (d_moy = 2) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) feb_sales
      , sum((CASE WHEN (d_moy = 3) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) mar_sales
      , sum((CASE WHEN (d_moy = 4) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) apr_sales
      , sum((CASE WHEN (d_moy = 5) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) may_sales
      , sum((CASE WHEN (d_moy = 6) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) jun_sales
      , sum((CASE WHEN (d_moy = 7) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) jul_sales
      , sum((CASE WHEN (d_moy = 8) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) aug_sales
      , sum((CASE WHEN (d_moy = 9) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) sep_sales
      , sum((CASE WHEN (d_moy = 10) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) oct_sales
      , sum((CASE WHEN (d_moy = 11) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) nov_sales
      , sum((CASE WHEN (d_moy = 12) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) dec_sales
      , sum((CASE WHEN (d_moy = 1) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) jan_net
      , sum((CASE WHEN (d_moy = 2) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) feb_net
      , sum((CASE WHEN (d_moy = 3) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) mar_net
      , sum((CASE WHEN (d_moy = 4) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) apr_net
      , sum((CASE WHEN (d_moy = 5) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) may_net
      , sum((CASE WHEN (d_moy = 6) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) jun_net
      , sum((CASE WHEN (d_moy = 7) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) jul_net
      , sum((CASE WHEN (d_moy = 8) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) aug_net
      , sum((CASE WHEN (d_moy = 9) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) sep_net
      , sum((CASE WHEN (d_moy = 10) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) oct_net
      , sum((CASE WHEN (d_moy = 11) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) nov_net
      , sum((CASE WHEN (d_moy = 12) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) dec_net
      FROM
        web_sales
      , warehouse
      , date_dim
      , time_dim
      , ship_mode
      WHERE (ws_warehouse_sk = w_warehouse_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (ws_sold_time_sk = t_time_sk)
         AND (ws_ship_mode_sk = sm_ship_mode_sk)
         AND (d_year = 2001)
         AND (t_time BETWEEN 30838 AND (30838 + 28800))
         AND (sm_carrier IN ('DHL'      , 'BARIAN'))
      GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
   UNION ALL
      SELECT
        w_warehouse_name
      , w_warehouse_sq_ft
      , w_city
      , w_county
      , w_state
      , w_country
      , concat(concat('DHL', ','), 'BARIAN') ship_carriers
      , d_year YEAR
      , sum((CASE WHEN (d_moy = 1) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) jan_sales
      , sum((CASE WHEN (d_moy = 2) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) feb_sales
      , sum((CASE WHEN (d_moy = 3) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) mar_sales
      , sum((CASE WHEN (d_moy = 4) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) apr_sales
      , sum((CASE WHEN (d_moy = 5) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) may_sales
      , sum((CASE WHEN (d_moy = 6) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) jun_sales
      , sum((CASE WHEN (d_moy = 7) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) jul_sales
      , sum((CASE WHEN (d_moy = 8) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) aug_sales
      , sum((CASE WHEN (d_moy = 9) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) sep_sales
      , sum((CASE WHEN (d_moy = 10) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) oct_sales
      , sum((CASE WHEN (d_moy = 11) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) nov_sales
      , sum((CASE WHEN (d_moy = 12) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) dec_sales
      , sum((CASE WHEN (d_moy = 1) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) jan_net
      , sum((CASE WHEN (d_moy = 2) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) feb_net
      , sum((CASE WHEN (d_moy = 3) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) mar_net
      , sum((CASE WHEN (d_moy = 4) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) apr_net
      , sum((CASE WHEN (d_moy = 5) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) may_net
      , sum((CASE WHEN (d_moy = 6) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) jun_net
      , sum((CASE WHEN (d_moy = 7) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) jul_net
      , sum((CASE WHEN (d_moy = 8) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) aug_net
      , sum((CASE WHEN (d_moy = 9) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) sep_net
      , sum((CASE WHEN (d_moy = 10) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) oct_net
      , sum((CASE WHEN (d_moy = 11) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) nov_net
      , sum((CASE WHEN (d_moy = 12) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) dec_net
      FROM
        catalog_sales
      , warehouse
      , date_dim
      , time_dim
      , ship_mode
      WHERE (cs_warehouse_sk = w_warehouse_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (cs_sold_time_sk = t_time_sk)
         AND (cs_ship_mode_sk = sm_ship_mode_sk)
         AND (d_year = 2001)
         AND (t_time BETWEEN 30838 AND (30838 + 28800))
         AND (sm_carrier IN ('DHL'      , 'BARIAN'))
      GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
   )  x
GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, ship_carriers, year
ORDER BY w_warehouse_name ASC
LIMIT 100;
"""
    def query66 = """
SELECT
  w_warehouse_name
, w_warehouse_sq_ft
, w_city
, w_county
, w_state
, w_country
, ship_carriers
, year
, sum(jan_sales) jan_sales
, sum(feb_sales) feb_sales
, sum(mar_sales) mar_sales
, sum(apr_sales) apr_sales
, sum(may_sales) may_sales
, sum(jun_sales) jun_sales
, sum(jul_sales) jul_sales
, sum(aug_sales) aug_sales
, sum(sep_sales) sep_sales
, sum(oct_sales) oct_sales
, sum(nov_sales) nov_sales
, sum(dec_sales) dec_sales
, sum((jan_sales / w_warehouse_sq_ft)) jan_sales_per_sq_foot
, sum((feb_sales / w_warehouse_sq_ft)) feb_sales_per_sq_foot
, sum((mar_sales / w_warehouse_sq_ft)) mar_sales_per_sq_foot
, sum((apr_sales / w_warehouse_sq_ft)) apr_sales_per_sq_foot
, sum((may_sales / w_warehouse_sq_ft)) may_sales_per_sq_foot
, sum((jun_sales / w_warehouse_sq_ft)) jun_sales_per_sq_foot
, sum((jul_sales / w_warehouse_sq_ft)) jul_sales_per_sq_foot
, sum((aug_sales / w_warehouse_sq_ft)) aug_sales_per_sq_foot
, sum((sep_sales / w_warehouse_sq_ft)) sep_sales_per_sq_foot
, sum((oct_sales / w_warehouse_sq_ft)) oct_sales_per_sq_foot
, sum((nov_sales / w_warehouse_sq_ft)) nov_sales_per_sq_foot
, sum((dec_sales / w_warehouse_sq_ft)) dec_sales_per_sq_foot
, sum(jan_net) jan_net
, sum(feb_net) feb_net
, sum(mar_net) mar_net
, sum(apr_net) apr_net
, sum(may_net) may_net
, sum(jun_net) jun_net
, sum(jul_net) jul_net
, sum(aug_net) aug_net
, sum(sep_net) sep_net
, sum(oct_net) oct_net
, sum(nov_net) nov_net
, sum(dec_net) dec_net
FROM
(
      SELECT
        w_warehouse_name
      , w_warehouse_sq_ft
      , w_city
      , w_county
      , w_state
      , w_country
      , concat(concat('DHL', ','), 'BARIAN') ship_carriers
      , d_year YEAR
      , sum((CASE WHEN (d_moy = 1) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) jan_sales
      , sum((CASE WHEN (d_moy = 2) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) feb_sales
      , sum((CASE WHEN (d_moy = 3) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) mar_sales
      , sum((CASE WHEN (d_moy = 4) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) apr_sales
      , sum((CASE WHEN (d_moy = 5) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) may_sales
      , sum((CASE WHEN (d_moy = 6) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) jun_sales
      , sum((CASE WHEN (d_moy = 7) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) jul_sales
      , sum((CASE WHEN (d_moy = 8) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) aug_sales
      , sum((CASE WHEN (d_moy = 9) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) sep_sales
      , sum((CASE WHEN (d_moy = 10) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) oct_sales
      , sum((CASE WHEN (d_moy = 11) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) nov_sales
      , sum((CASE WHEN (d_moy = 12) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) dec_sales
      , sum((CASE WHEN (d_moy = 1) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) jan_net
      , sum((CASE WHEN (d_moy = 2) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) feb_net
      , sum((CASE WHEN (d_moy = 3) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) mar_net
      , sum((CASE WHEN (d_moy = 4) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) apr_net
      , sum((CASE WHEN (d_moy = 5) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) may_net
      , sum((CASE WHEN (d_moy = 6) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) jun_net
      , sum((CASE WHEN (d_moy = 7) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) jul_net
      , sum((CASE WHEN (d_moy = 8) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) aug_net
      , sum((CASE WHEN (d_moy = 9) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) sep_net
      , sum((CASE WHEN (d_moy = 10) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) oct_net
      , sum((CASE WHEN (d_moy = 11) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) nov_net
      , sum((CASE WHEN (d_moy = 12) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) dec_net
      FROM
        web_sales
      , warehouse
      , date_dim
      , time_dim
      , ship_mode
      WHERE (ws_warehouse_sk = w_warehouse_sk)
         AND (ws_sold_date_sk = d_date_sk)
         AND (ws_sold_time_sk = t_time_sk)
         AND (ws_ship_mode_sk = sm_ship_mode_sk)
         AND (d_year = 2001)
         AND (t_time BETWEEN 30838 AND (30838 + 28800))
         AND (sm_carrier IN ('DHL'      , 'BARIAN'))
      GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
   UNION ALL
      SELECT
        w_warehouse_name
      , w_warehouse_sq_ft
      , w_city
      , w_county
      , w_state
      , w_country
      , concat(concat('DHL', ','), 'BARIAN') ship_carriers
      , d_year YEAR
      , sum((CASE WHEN (d_moy = 1) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) jan_sales
      , sum((CASE WHEN (d_moy = 2) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) feb_sales
      , sum((CASE WHEN (d_moy = 3) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) mar_sales
      , sum((CASE WHEN (d_moy = 4) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) apr_sales
      , sum((CASE WHEN (d_moy = 5) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) may_sales
      , sum((CASE WHEN (d_moy = 6) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) jun_sales
      , sum((CASE WHEN (d_moy = 7) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) jul_sales
      , sum((CASE WHEN (d_moy = 8) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) aug_sales
      , sum((CASE WHEN (d_moy = 9) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) sep_sales
      , sum((CASE WHEN (d_moy = 10) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) oct_sales
      , sum((CASE WHEN (d_moy = 11) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) nov_sales
      , sum((CASE WHEN (d_moy = 12) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) dec_sales
      , sum((CASE WHEN (d_moy = 1) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) jan_net
      , sum((CASE WHEN (d_moy = 2) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) feb_net
      , sum((CASE WHEN (d_moy = 3) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) mar_net
      , sum((CASE WHEN (d_moy = 4) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) apr_net
      , sum((CASE WHEN (d_moy = 5) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) may_net
      , sum((CASE WHEN (d_moy = 6) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) jun_net
      , sum((CASE WHEN (d_moy = 7) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) jul_net
      , sum((CASE WHEN (d_moy = 8) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) aug_net
      , sum((CASE WHEN (d_moy = 9) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) sep_net
      , sum((CASE WHEN (d_moy = 10) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) oct_net
      , sum((CASE WHEN (d_moy = 11) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) nov_net
      , sum((CASE WHEN (d_moy = 12) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) dec_net
      FROM
        catalog_sales
      , warehouse
      , date_dim
      , time_dim
      , ship_mode
      WHERE (cs_warehouse_sk = w_warehouse_sk)
         AND (cs_sold_date_sk = d_date_sk)
         AND (cs_sold_time_sk = t_time_sk)
         AND (cs_ship_mode_sk = sm_ship_mode_sk)
         AND (d_year = 2001)
         AND (t_time BETWEEN 30838 AND (30838 + 28800))
         AND (sm_carrier IN ('DHL'      , 'BARIAN'))
      GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
   )  x
GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, ship_carriers, year
ORDER BY w_warehouse_name ASC
LIMIT 100;
"""
    order_qt_query66_before "${query66}"
    async_mv_rewrite_fail(db, mv66, query66, "mv66")
    order_qt_query66_after "${query66}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv66"""


    def mv67 = """
SELECT *
FROM
  (
   SELECT
     i_category
   , i_class
   , i_brand
   , i_product_name
   , d_year
   , d_qoy
   , d_moy
   , s_store_id
   , sumsales
   , rank() OVER (PARTITION BY i_category ORDER BY sumsales DESC) rk
   FROM
     (
      SELECT
        i_category
      , i_class
      , i_brand
      , i_product_name
      , d_year
      , d_qoy
      , d_moy
      , s_store_id
      , sum(COALESCE((ss_sales_price * ss_quantity), 0)) sumsales
      FROM
        store_sales
      , date_dim
      , store
      , item
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (ss_item_sk = i_item_sk)
         AND (ss_store_sk = s_store_sk)
         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
      GROUP BY ROLLUP (i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)
   )  dw1
)  dw2
WHERE (rk <= 100)
ORDER BY i_category ASC, i_class ASC, i_brand ASC, i_product_name ASC, d_year ASC, d_qoy ASC, d_moy ASC, s_store_id ASC, sumsales ASC, rk ASC
LIMIT 100;
"""
    def query67 = """
SELECT *
FROM
  (
   SELECT
     i_category
   , i_class
   , i_brand
   , i_product_name
   , d_year
   , d_qoy
   , d_moy
   , s_store_id
   , sumsales
   , rank() OVER (PARTITION BY i_category ORDER BY sumsales DESC) rk
   FROM
     (
      SELECT
        i_category
      , i_class
      , i_brand
      , i_product_name
      , d_year
      , d_qoy
      , d_moy
      , s_store_id
      , sum(COALESCE((ss_sales_price * ss_quantity), 0)) sumsales
      FROM
        store_sales
      , date_dim
      , store
      , item
      WHERE (ss_sold_date_sk = d_date_sk)
         AND (ss_item_sk = i_item_sk)
         AND (ss_store_sk = s_store_sk)
         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
      GROUP BY ROLLUP (i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)
   )  dw1
)  dw2
WHERE (rk <= 100)
ORDER BY i_category ASC, i_class ASC, i_brand ASC, i_product_name ASC, d_year ASC, d_qoy ASC, d_moy ASC, s_store_id ASC, sumsales ASC, rk ASC
LIMIT 100;
"""
    order_qt_query67_before "${query67}"
    async_mv_rewrite_fail(db, mv67, query67, "mv67")
    order_qt_query67_after "${query67}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv67"""


    def mv68 = """
SELECT
  c_last_name
, c_first_name
, ca_city
, bought_city
, ss_ticket_number
, extended_price
, extended_tax
, list_price
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , ca_city bought_city
   , sum(ss_ext_sales_price) extended_price
   , sum(ss_ext_list_price) list_price
   , sum(ss_ext_tax) extended_tax
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      AND (date_dim.d_dom BETWEEN 1 AND 2)
      AND ((household_demographics.hd_dep_count = 4)
         OR (household_demographics.hd_vehicle_count = 3))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_city IN ('Midway'   , 'Fairview'))
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
)  dn
, customer
, customer_address current_addr
WHERE (ss_customer_sk = c_customer_sk)
   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
   AND (current_addr.ca_city <> bought_city)
ORDER BY c_last_name ASC, ss_ticket_number ASC
LIMIT 100;
"""
    def query68 = """
SELECT
  c_last_name
, c_first_name
, ca_city
, bought_city
, ss_ticket_number
, extended_price
, extended_tax
, list_price
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , ca_city bought_city
   , sum(ss_ext_sales_price) extended_price
   , sum(ss_ext_list_price) list_price
   , sum(ss_ext_tax) extended_tax
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      AND (date_dim.d_dom BETWEEN 1 AND 2)
      AND ((household_demographics.hd_dep_count = 4)
         OR (household_demographics.hd_vehicle_count = 3))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_city IN ('Midway'   , 'Fairview'))
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
)  dn
, customer
, customer_address current_addr
WHERE (ss_customer_sk = c_customer_sk)
   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
   AND (current_addr.ca_city <> bought_city)
ORDER BY c_last_name ASC, ss_ticket_number ASC
LIMIT 100;
"""
    order_qt_query68_before "${query68}"
    async_mv_rewrite_fail(db, mv68, query68, "mv68")
    order_qt_query68_after "${query68}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv68"""

    def mv68_1 = """
SELECT
     ss_ticket_number
   , ss_customer_sk
   , ca_city bought_city
   , sum(ss_ext_sales_price) extended_price
   , sum(ss_ext_list_price) list_price
   , sum(ss_ext_tax) extended_tax
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      AND (date_dim.d_dom BETWEEN 1 AND 2)
      AND ((household_demographics.hd_dep_count = 4)
         OR (household_demographics.hd_vehicle_count = 3))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_city IN ('Midway'   , 'Fairview'))
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city;
"""
    def query68_1 = """
SELECT
  c_last_name
, c_first_name
, ca_city
, bought_city
, ss_ticket_number
, extended_price
, extended_tax
, list_price
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , ca_city bought_city
   , sum(ss_ext_sales_price) extended_price
   , sum(ss_ext_list_price) list_price
   , sum(ss_ext_tax) extended_tax
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
      AND (date_dim.d_dom BETWEEN 1 AND 2)
      AND ((household_demographics.hd_dep_count = 4)
         OR (household_demographics.hd_vehicle_count = 3))
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_city IN ('Midway'   , 'Fairview'))
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
)  dn
, customer
, customer_address current_addr
WHERE (ss_customer_sk = c_customer_sk)
   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
   AND (current_addr.ca_city <> bought_city)
ORDER BY c_last_name ASC, ss_ticket_number ASC
LIMIT 100;
"""
    order_qt_query68_1_before "${query68_1}"
    async_mv_rewrite_success(db, mv68_1, query68_1, "mv68_1")
    order_qt_query68_1_after "${query68_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv68_1"""

    def mv69 = """
SELECT
  cd_gender
, cd_marital_status
, cd_education_status
, count(*) cnt1
, cd_purchase_estimate
, count(*) cnt2
, cd_credit_rating
, count(*) cnt3
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (ca_state IN ('KY', 'GA', 'NM'))
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy BETWEEN 4 AND (4 + 2))
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     web_sales
   , date_dim
   WHERE (c.c_customer_sk = ws_bill_customer_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy BETWEEN 4 AND (4 + 2))
)))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     catalog_sales
   , date_dim
   WHERE (c.c_customer_sk = cs_ship_customer_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy BETWEEN 4 AND (4 + 2))
)))
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating
ORDER BY cd_gender ASC, cd_marital_status ASC, cd_education_status ASC, cd_purchase_estimate ASC, cd_credit_rating ASC
LIMIT 100;
"""
    def query69 = """
SELECT
  cd_gender
, cd_marital_status
, cd_education_status
, count(*) cnt1
, cd_purchase_estimate
, count(*) cnt2
, cd_credit_rating
, count(*) cnt3
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE (c.c_current_addr_sk = ca.ca_address_sk)
   AND (ca_state IN ('KY', 'GA', 'NM'))
   AND (cd_demo_sk = c.c_current_cdemo_sk)
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE (c.c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy BETWEEN 4 AND (4 + 2))
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     web_sales
   , date_dim
   WHERE (c.c_customer_sk = ws_bill_customer_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy BETWEEN 4 AND (4 + 2))
)))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     catalog_sales
   , date_dim
   WHERE (c.c_customer_sk = cs_ship_customer_sk)
      AND (cs_sold_date_sk = d_date_sk)
      AND (d_year = 2001)
      AND (d_moy BETWEEN 4 AND (4 + 2))
)))
GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating
ORDER BY cd_gender ASC, cd_marital_status ASC, cd_education_status ASC, cd_purchase_estimate ASC, cd_credit_rating ASC
LIMIT 100;
"""
    order_qt_query69_before "${query69}"
    async_mv_rewrite_fail(db, mv69, query69, "mv69")
    order_qt_query69_after "${query69}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv69"""

    def mv70 = """
SELECT
  sum(ss_net_profit) total_sum
, s_state
, s_county
, (GROUPING (s_state) + GROUPING (s_county)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (s_state) + GROUPING (s_county)), (CASE WHEN (GROUPING (s_county) = 0) THEN s_state END) ORDER BY sum(ss_net_profit) DESC) rank_within_parent
FROM
  store_sales
, date_dim d1
, store
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
   SELECT s_state
   FROM
     (
      SELECT
        s_state s_state
      , rank() OVER (PARTITION BY s_state ORDER BY sum(ss_net_profit) DESC) ranking
      FROM
        store_sales
      , store
      , date_dim
      WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
         AND (d_date_sk = ss_sold_date_sk)
         AND (s_store_sk = ss_store_sk)
      GROUP BY s_state
   )  tmp1
   WHERE (ranking <= 5)
))
GROUP BY ROLLUP (s_state, s_county)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN s_state END) ASC, rank_within_parent ASC
LIMIT 100;
"""
    def query70 = """
SELECT
  sum(ss_net_profit) total_sum
, s_state
, s_county
, (GROUPING (s_state) + GROUPING (s_county)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (s_state) + GROUPING (s_county)), (CASE WHEN (GROUPING (s_county) = 0) THEN s_state END) ORDER BY sum(ss_net_profit) DESC) rank_within_parent
FROM
  store_sales
, date_dim d1
, store
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
   SELECT s_state
   FROM
     (
      SELECT
        s_state s_state
      , rank() OVER (PARTITION BY s_state ORDER BY sum(ss_net_profit) DESC) ranking
      FROM
        store_sales
      , store
      , date_dim
      WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
         AND (d_date_sk = ss_sold_date_sk)
         AND (s_store_sk = ss_store_sk)
      GROUP BY s_state
   )  tmp1
   WHERE (ranking <= 5)
))
GROUP BY ROLLUP (s_state, s_county)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN s_state END) ASC, rank_within_parent ASC
LIMIT 100;
"""
    order_qt_query70_before "${query70}"
    async_mv_rewrite_fail(db, mv70, query70, "mv70")
    order_qt_query70_after "${query70}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv70"""

    def mv70_1 = """
SELECT
  sum(ss_net_profit) total_sum
, s_state
, s_county
, (GROUPING (s_state) + GROUPING (s_county)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (s_state) + GROUPING (s_county)), (CASE WHEN (GROUPING (s_county) = 0) THEN s_state END) ORDER BY sum(ss_net_profit) DESC) rank_within_parent
FROM
  store_sales
, date_dim d1
, store
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
   SELECT s_state
   FROM
     (
      SELECT
        s_state s_state
      , rank() OVER (PARTITION BY s_state ORDER BY sum(ss_net_profit) DESC) ranking
      FROM
        store_sales
      , store
      , date_dim
      WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
         AND (d_date_sk = ss_sold_date_sk)
         AND (s_store_sk = ss_store_sk)
      GROUP BY s_state
   )  tmp1
   WHERE (ranking <= 5)
))
GROUP BY ROLLUP (s_state, s_county);
"""
    def query70_1 = """
SELECT
  sum(ss_net_profit) total_sum
, s_state
, s_county
, (GROUPING (s_state) + GROUPING (s_county)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (s_state) + GROUPING (s_county)), (CASE WHEN (GROUPING (s_county) = 0) THEN s_state END) ORDER BY sum(ss_net_profit) DESC) rank_within_parent
FROM
  store_sales
, date_dim d1
, store
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ss_sold_date_sk)
   AND (s_store_sk = ss_store_sk)
   AND (s_state IN (
   SELECT s_state
   FROM
     (
      SELECT
        s_state s_state
      , rank() OVER (PARTITION BY s_state ORDER BY sum(ss_net_profit) DESC) ranking
      FROM
        store_sales
      , store
      , date_dim
      WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
         AND (d_date_sk = ss_sold_date_sk)
         AND (s_store_sk = ss_store_sk)
      GROUP BY s_state
   )  tmp1
   WHERE (ranking <= 5)
))
GROUP BY ROLLUP (s_state, s_county)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN s_state END) ASC, rank_within_parent ASC
LIMIT 100;
"""
    order_qt_query70_1_before "${query70_1}"
    async_mv_rewrite_fail(db, mv70_1, query70_1, "mv70_1")
    order_qt_query70_1_after "${query70_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv70_1"""


    def mv71 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, t_hour
, t_minute
, sum(ext_price) ext_price
FROM
  item
, (
   SELECT
     ws_ext_sales_price ext_price
   , ws_sold_date_sk sold_date_sk
   , ws_item_sk sold_item_sk
   , ws_sold_time_sk time_sk
   FROM
     web_sales
   , date_dim
   WHERE (d_date_sk = ws_sold_date_sk)
      AND (d_moy = 11)
      AND (d_year = 1999)
UNION ALL    SELECT
     cs_ext_sales_price ext_price
   , cs_sold_date_sk sold_date_sk
   , cs_item_sk sold_item_sk
   , cs_sold_time_sk time_sk
   FROM
     catalog_sales
   , date_dim
   WHERE (d_date_sk = cs_sold_date_sk)
      AND (d_moy = 11)
      AND (d_year = 1999)
UNION ALL    SELECT
     ss_ext_sales_price ext_price
   , ss_sold_date_sk sold_date_sk
   , ss_item_sk sold_item_sk
   , ss_sold_time_sk time_sk
   FROM
     store_sales
   , date_dim
   WHERE (d_date_sk = ss_sold_date_sk)
      AND (d_moy = 11)
      AND (d_year = 1999)
)  tmp
, time_dim
WHERE (sold_item_sk = i_item_sk)
   AND (i_manager_id = 1)
   AND (time_sk = t_time_sk)
   AND ((t_meal_time = 'breakfast')
      OR (t_meal_time = 'dinner'))
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id ASC;
"""
    def query71 = """
SELECT
  i_brand_id brand_id
, i_brand brand
, t_hour
, t_minute
, sum(ext_price) ext_price
FROM
  item
, (
   SELECT
     ws_ext_sales_price ext_price
   , ws_sold_date_sk sold_date_sk
   , ws_item_sk sold_item_sk
   , ws_sold_time_sk time_sk
   FROM
     web_sales
   , date_dim
   WHERE (d_date_sk = ws_sold_date_sk)
      AND (d_moy = 11)
      AND (d_year = 1999)
UNION ALL    SELECT
     cs_ext_sales_price ext_price
   , cs_sold_date_sk sold_date_sk
   , cs_item_sk sold_item_sk
   , cs_sold_time_sk time_sk
   FROM
     catalog_sales
   , date_dim
   WHERE (d_date_sk = cs_sold_date_sk)
      AND (d_moy = 11)
      AND (d_year = 1999)
UNION ALL    SELECT
     ss_ext_sales_price ext_price
   , ss_sold_date_sk sold_date_sk
   , ss_item_sk sold_item_sk
   , ss_sold_time_sk time_sk
   FROM
     store_sales
   , date_dim
   WHERE (d_date_sk = ss_sold_date_sk)
      AND (d_moy = 11)
      AND (d_year = 1999)
)  tmp
, time_dim
WHERE (sold_item_sk = i_item_sk)
   AND (i_manager_id = 1)
   AND (time_sk = t_time_sk)
   AND ((t_meal_time = 'breakfast')
      OR (t_meal_time = 'dinner'))
GROUP BY i_brand, i_brand_id, t_hour, t_minute
ORDER BY ext_price DESC, i_brand_id ASC;
"""
    order_qt_query71_before "${query71}"
    async_mv_rewrite_fail(db, mv71, query71, "mv71")
    order_qt_query71_after "${query71}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv71"""

    def mv72 = """
SELECT
  i_item_desc
, w_warehouse_name
, d1.d_week_seq
, sum((CASE WHEN (p_promo_sk IS NULL) THEN 1 ELSE 0 END)) no_promo
, sum((CASE WHEN (p_promo_sk IS NOT NULL) THEN 1 ELSE 0 END)) promo
, count(*) total_cnt
FROM
  catalog_sales
INNER JOIN inventory ON (cs_item_sk = inv_item_sk)
INNER JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk)
INNER JOIN item ON (i_item_sk = cs_item_sk)
INNER JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
INNER JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
INNER JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
INNER JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
INNER JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
LEFT JOIN promotion ON (cs_promo_sk = p_promo_sk)
LEFT JOIN catalog_returns ON (cr_item_sk = cs_item_sk)
   AND (cr_order_number = cs_order_number)
WHERE (d1.d_week_seq = d2.d_week_seq)
   AND (inv_quantity_on_hand < cs_quantity)
   AND (d3.d_date > (d1.d_date + INTERVAL  '5' DAY))
   AND (hd_buy_potential = '>10000')
   AND (d1.d_year = 1999)
   AND (cd_marital_status = 'D')
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i_item_desc ASC, w_warehouse_name ASC, d1.d_week_seq ASC
LIMIT 100;
"""
    def query72 = """
SELECT
  i_item_desc
, w_warehouse_name
, d1.d_week_seq
, sum((CASE WHEN (p_promo_sk IS NULL) THEN 1 ELSE 0 END)) no_promo
, sum((CASE WHEN (p_promo_sk IS NOT NULL) THEN 1 ELSE 0 END)) promo
, count(*) total_cnt
FROM
  catalog_sales
INNER JOIN inventory ON (cs_item_sk = inv_item_sk)
INNER JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk)
INNER JOIN item ON (i_item_sk = cs_item_sk)
INNER JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
INNER JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
INNER JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
INNER JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
INNER JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
LEFT JOIN promotion ON (cs_promo_sk = p_promo_sk)
LEFT JOIN catalog_returns ON (cr_item_sk = cs_item_sk)
   AND (cr_order_number = cs_order_number)
WHERE (d1.d_week_seq = d2.d_week_seq)
   AND (inv_quantity_on_hand < cs_quantity)
   AND (d3.d_date > (d1.d_date + INTERVAL  '5' DAY))
   AND (hd_buy_potential = '>10000')
   AND (d1.d_year = 1999)
   AND (cd_marital_status = 'D')
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i_item_desc ASC, w_warehouse_name ASC, d1.d_week_seq ASC
LIMIT 100;
"""
    order_qt_query72_before "${query72}"
    async_mv_rewrite_fail(db, mv72, query72, "mv72")
    order_qt_query72_after "${query72}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv72"""

    def mv72_1 = """
SELECT
  i_item_desc
, w_warehouse_name
, d1.d_week_seq
, sum((CASE WHEN (p_promo_sk IS NULL) THEN 1 ELSE 0 END)) no_promo
, sum((CASE WHEN (p_promo_sk IS NOT NULL) THEN 1 ELSE 0 END)) promo
, count(*) total_cnt
FROM
  catalog_sales
INNER JOIN inventory ON (cs_item_sk = inv_item_sk)
INNER JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk)
INNER JOIN item ON (i_item_sk = cs_item_sk)
INNER JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
INNER JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
INNER JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
INNER JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
INNER JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
LEFT JOIN promotion ON (cs_promo_sk = p_promo_sk)
LEFT JOIN catalog_returns ON (cr_item_sk = cs_item_sk)
   AND (cr_order_number = cs_order_number)
WHERE (d1.d_week_seq = d2.d_week_seq)
   AND (inv_quantity_on_hand < cs_quantity)
   AND (d3.d_date > (d1.d_date + INTERVAL  '5' DAY))
   AND (hd_buy_potential = '>10000')
   AND (d1.d_year = 1999)
   AND (cd_marital_status = 'D')
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq;
"""
    def query72_1 = """
SELECT
  i_item_desc
, w_warehouse_name
, d1.d_week_seq
, sum((CASE WHEN (p_promo_sk IS NULL) THEN 1 ELSE 0 END)) no_promo
, sum((CASE WHEN (p_promo_sk IS NOT NULL) THEN 1 ELSE 0 END)) promo
, count(*) total_cnt
FROM
  catalog_sales
INNER JOIN inventory ON (cs_item_sk = inv_item_sk)
INNER JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk)
INNER JOIN item ON (i_item_sk = cs_item_sk)
INNER JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
INNER JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
INNER JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
INNER JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
INNER JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
LEFT JOIN promotion ON (cs_promo_sk = p_promo_sk)
LEFT JOIN catalog_returns ON (cr_item_sk = cs_item_sk)
   AND (cr_order_number = cs_order_number)
WHERE (d1.d_week_seq = d2.d_week_seq)
   AND (inv_quantity_on_hand < cs_quantity)
   AND (d3.d_date > (d1.d_date + INTERVAL  '5' DAY))
   AND (hd_buy_potential = '>10000')
   AND (d1.d_year = 1999)
   AND (cd_marital_status = 'D')
GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
ORDER BY total_cnt DESC, i_item_desc ASC, w_warehouse_name ASC, d1.d_week_seq ASC
LIMIT 100;
"""
    order_qt_query72_1_before "${query72_1}"
    async_mv_rewrite_success(db, mv72_1, query72_1, "mv72_1")
    order_qt_query72_1_after "${query72_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv72_1"""

    def mv73 = """
SELECT
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
ORDER BY cnt DESC, c_last_name ASC;
"""
    def query73 = """
SELECT
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
ORDER BY cnt DESC, c_last_name ASC;
"""
    order_qt_query73_before "${query73}"
    async_mv_rewrite_fail(db, mv73, query73, "mv73")
    order_qt_query73_after "${query73}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv73"""

    def mv73_1 = """
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
   GROUP BY ss_ticket_number, ss_customer_sk;
"""
    def query73_1 = """
SELECT
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
ORDER BY cnt DESC, c_last_name ASC;
"""
    order_qt_query73_1_before "${query73_1}"
    async_mv_rewrite_success(db, mv73_1, query73_1, "mv73_1")
    order_qt_query73_1_after "${query73_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv73_1"""

    def mv74 = """
WITH
  year_total AS (
   SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , d_year YEAR
   , sum(ss_net_paid) year_total
   , 's' sale_type
   FROM
     customer
   , store_sales
   , date_dim
   WHERE (c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year IN (2001   , (2001 + 1)))
   GROUP BY c_customer_id, c_first_name, c_last_name, d_year
UNION ALL    SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , d_year YEAR
   , sum(ws_net_paid) year_total
   , 'w' sale_type
   FROM
     customer
   , web_sales
   , date_dim
   WHERE (c_customer_sk = ws_bill_customer_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year IN (2001   , (2001 + 1)))
   GROUP BY c_customer_id, c_first_name, c_last_name, d_year
)
SELECT
  t_s_secyear.customer_id
, t_s_secyear.customer_first_name
, t_s_secyear.customer_last_name
FROM
  year_total t_s_firstyear
, year_total t_s_secyear
, year_total t_w_firstyear
, year_total t_w_secyear
WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
   AND (t_s_firstyear.sale_type = 's')
   AND (t_w_firstyear.sale_type = 'w')
   AND (t_s_secyear.sale_type = 's')
   AND (t_w_secyear.sale_type = 'w')
   AND (t_s_firstyear.year = 2001)
   AND (t_s_secyear.year = (2001 + 1))
   AND (t_w_firstyear.year = 2001)
   AND (t_w_secyear.year = (2001 + 1))
   AND (t_s_firstyear.year_total > 0)
   AND (t_w_firstyear.year_total > 0)
   AND ((CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE null END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE null END))
ORDER BY 1 ASC, 1 ASC, 1 ASC
LIMIT 100;
"""
    def query74 = """
WITH
  year_total AS (
   SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , d_year YEAR
   , sum(ss_net_paid) year_total
   , 's' sale_type
   FROM
     customer
   , store_sales
   , date_dim
   WHERE (c_customer_sk = ss_customer_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (d_year IN (2001   , (2001 + 1)))
   GROUP BY c_customer_id, c_first_name, c_last_name, d_year
UNION ALL    SELECT
     c_customer_id customer_id
   , c_first_name customer_first_name
   , c_last_name customer_last_name
   , d_year YEAR
   , sum(ws_net_paid) year_total
   , 'w' sale_type
   FROM
     customer
   , web_sales
   , date_dim
   WHERE (c_customer_sk = ws_bill_customer_sk)
      AND (ws_sold_date_sk = d_date_sk)
      AND (d_year IN (2001   , (2001 + 1)))
   GROUP BY c_customer_id, c_first_name, c_last_name, d_year
)
SELECT
  t_s_secyear.customer_id
, t_s_secyear.customer_first_name
, t_s_secyear.customer_last_name
FROM
  year_total t_s_firstyear
, year_total t_s_secyear
, year_total t_w_firstyear
, year_total t_w_secyear
WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
   AND (t_s_firstyear.sale_type = 's')
   AND (t_w_firstyear.sale_type = 'w')
   AND (t_s_secyear.sale_type = 's')
   AND (t_w_secyear.sale_type = 'w')
   AND (t_s_firstyear.year = 2001)
   AND (t_s_secyear.year = (2001 + 1))
   AND (t_w_firstyear.year = 2001)
   AND (t_w_secyear.year = (2001 + 1))
   AND (t_s_firstyear.year_total > 0)
   AND (t_w_firstyear.year_total > 0)
   AND ((CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE null END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE null END))
ORDER BY 1 ASC, 1 ASC, 1 ASC
LIMIT 100;
"""
    order_qt_query74_before "${query74}"
    async_mv_rewrite_fail(db, mv74, query74, "mv74")
    order_qt_query74_after "${query74}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv74"""


    def mv75 = """
WITH
  all_sales AS (
   SELECT
     d_year
   , i_brand_id
   , i_class_id
   , i_category_id
   , i_manufact_id
   , sum(sales_cnt) sales_cnt
   , sum(sales_amt) sales_amt
   FROM
     (
      SELECT
        d_year
      , i_brand_id
      , i_class_id
      , i_category_id
      , i_manufact_id
      , (cs_quantity - COALESCE(cr_return_quantity, 0)) sales_cnt
      , (cs_ext_sales_price - COALESCE(cr_return_amount, CAST('0.0' AS DECIMAL(2,1)))) sales_amt
      FROM
        catalog_sales
      INNER JOIN item ON (i_item_sk = cs_item_sk)
      INNER JOIN date_dim ON (d_date_sk = cs_sold_date_sk)
      LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
         AND (cs_item_sk = cr_item_sk)
      WHERE (i_category = 'Books')
UNION       SELECT
        d_year
      , i_brand_id
      , i_class_id
      , i_category_id
      , i_manufact_id
      , (ss_quantity - COALESCE(sr_return_quantity, 0)) sales_cnt
      , (ss_ext_sales_price - COALESCE(sr_return_amt, CAST('0.0' AS DECIMAL(2,1)))) sales_amt
      FROM
        store_sales
      INNER JOIN item ON (i_item_sk = ss_item_sk)
      INNER JOIN date_dim ON (d_date_sk = ss_sold_date_sk)
      LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number)
         AND (ss_item_sk = sr_item_sk)
      WHERE (i_category = 'Books')
UNION       SELECT
        d_year
      , i_brand_id
      , i_class_id
      , i_category_id
      , i_manufact_id
      , (ws_quantity - COALESCE(wr_return_quantity, 0)) sales_cnt
      , (ws_ext_sales_price - COALESCE(wr_return_amt, CAST('0.0' AS DECIMAL(2,1)))) sales_amt
      FROM
        web_sales
      INNER JOIN item ON (i_item_sk = ws_item_sk)
      INNER JOIN date_dim ON (d_date_sk = ws_sold_date_sk)
      LEFT JOIN web_returns ON (ws_order_number = wr_order_number)
         AND (ws_item_sk = wr_item_sk)
      WHERE (i_category = 'Books')
   )  sales_detail
   GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id
)
SELECT
  prev_yr.d_year prev_year
, curr_yr.d_year year
, curr_yr.i_brand_id
, curr_yr.i_class_id
, curr_yr.i_category_id
, curr_yr.i_manufact_id
, prev_yr.sales_cnt prev_yr_cnt
, curr_yr.sales_cnt curr_yr_cnt
, (curr_yr.sales_cnt - prev_yr.sales_cnt) sales_cnt_diff
, (curr_yr.sales_amt - prev_yr.sales_amt) sales_amt_diff
FROM
  all_sales curr_yr
, all_sales prev_yr
WHERE (curr_yr.i_brand_id = prev_yr.i_brand_id)
   AND (curr_yr.i_class_id = prev_yr.i_class_id)
   AND (curr_yr.i_category_id = prev_yr.i_category_id)
   AND (curr_yr.i_manufact_id = prev_yr.i_manufact_id)
   AND (curr_yr.d_year = 2002)
   AND (prev_yr.d_year = (2002 - 1))
   AND ((CAST(curr_yr.sales_cnt AS DECIMAL(17,2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17,2))) < CAST('0.9' AS DECIMAL(2,1)))
ORDER BY sales_cnt_diff ASC, sales_amt_diff ASC
LIMIT 100;
"""
    def query75 = """
WITH
  all_sales AS (
   SELECT
     d_year
   , i_brand_id
   , i_class_id
   , i_category_id
   , i_manufact_id
   , sum(sales_cnt) sales_cnt
   , sum(sales_amt) sales_amt
   FROM
     (
      SELECT
        d_year
      , i_brand_id
      , i_class_id
      , i_category_id
      , i_manufact_id
      , (cs_quantity - COALESCE(cr_return_quantity, 0)) sales_cnt
      , (cs_ext_sales_price - COALESCE(cr_return_amount, CAST('0.0' AS DECIMAL(2,1)))) sales_amt
      FROM
        catalog_sales
      INNER JOIN item ON (i_item_sk = cs_item_sk)
      INNER JOIN date_dim ON (d_date_sk = cs_sold_date_sk)
      LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
         AND (cs_item_sk = cr_item_sk)
      WHERE (i_category = 'Books')
UNION       SELECT
        d_year
      , i_brand_id
      , i_class_id
      , i_category_id
      , i_manufact_id
      , (ss_quantity - COALESCE(sr_return_quantity, 0)) sales_cnt
      , (ss_ext_sales_price - COALESCE(sr_return_amt, CAST('0.0' AS DECIMAL(2,1)))) sales_amt
      FROM
        store_sales
      INNER JOIN item ON (i_item_sk = ss_item_sk)
      INNER JOIN date_dim ON (d_date_sk = ss_sold_date_sk)
      LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number)
         AND (ss_item_sk = sr_item_sk)
      WHERE (i_category = 'Books')
UNION       SELECT
        d_year
      , i_brand_id
      , i_class_id
      , i_category_id
      , i_manufact_id
      , (ws_quantity - COALESCE(wr_return_quantity, 0)) sales_cnt
      , (ws_ext_sales_price - COALESCE(wr_return_amt, CAST('0.0' AS DECIMAL(2,1)))) sales_amt
      FROM
        web_sales
      INNER JOIN item ON (i_item_sk = ws_item_sk)
      INNER JOIN date_dim ON (d_date_sk = ws_sold_date_sk)
      LEFT JOIN web_returns ON (ws_order_number = wr_order_number)
         AND (ws_item_sk = wr_item_sk)
      WHERE (i_category = 'Books')
   )  sales_detail
   GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id
)
SELECT
  prev_yr.d_year prev_year
, curr_yr.d_year year
, curr_yr.i_brand_id
, curr_yr.i_class_id
, curr_yr.i_category_id
, curr_yr.i_manufact_id
, prev_yr.sales_cnt prev_yr_cnt
, curr_yr.sales_cnt curr_yr_cnt
, (curr_yr.sales_cnt - prev_yr.sales_cnt) sales_cnt_diff
, (curr_yr.sales_amt - prev_yr.sales_amt) sales_amt_diff
FROM
  all_sales curr_yr
, all_sales prev_yr
WHERE (curr_yr.i_brand_id = prev_yr.i_brand_id)
   AND (curr_yr.i_class_id = prev_yr.i_class_id)
   AND (curr_yr.i_category_id = prev_yr.i_category_id)
   AND (curr_yr.i_manufact_id = prev_yr.i_manufact_id)
   AND (curr_yr.d_year = 2002)
   AND (prev_yr.d_year = (2002 - 1))
   AND ((CAST(curr_yr.sales_cnt AS DECIMAL(17,2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17,2))) < CAST('0.9' AS DECIMAL(2,1)))
ORDER BY sales_cnt_diff ASC, sales_amt_diff ASC
LIMIT 100;
"""
    order_qt_query75_before "${query75}"
    async_mv_rewrite_fail(db, mv75, query75, "mv75")
    order_qt_query75_after "${query75}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv75"""



    def mv76 = """
SELECT
  channel
, col_name
, d_year
, d_qoy
, i_category
, count(*) sales_cnt
, sum(ext_sales_price) sales_amt
FROM
  (
   SELECT
     'store' channel
   , 'ss_store_sk' col_name
   , d_year
   , d_qoy
   , i_category
   , ss_ext_sales_price ext_sales_price
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_store_sk IS NULL)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_item_sk = i_item_sk)
UNION ALL    SELECT
     'web' channel
   , 'ws_ship_customer_sk' col_name
   , d_year
   , d_qoy
   , i_category
   , ws_ext_sales_price ext_sales_price
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_ship_customer_sk IS NULL)
      AND (ws_sold_date_sk = d_date_sk)
      AND (ws_item_sk = i_item_sk)
UNION ALL    SELECT
     'catalog' channel
   , 'cs_ship_addr_sk' col_name
   , d_year
   , d_qoy
   , i_category
   , cs_ext_sales_price ext_sales_price
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_ship_addr_sk IS NULL)
      AND (cs_sold_date_sk = d_date_sk)
      AND (cs_item_sk = i_item_sk)
)  foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel ASC, col_name ASC, d_year ASC, d_qoy ASC, i_category ASC
LIMIT 100;
"""
    def query76 = """
SELECT
  channel
, col_name
, d_year
, d_qoy
, i_category
, count(*) sales_cnt
, sum(ext_sales_price) sales_amt
FROM
  (
   SELECT
     'store' channel
   , 'ss_store_sk' col_name
   , d_year
   , d_qoy
   , i_category
   , ss_ext_sales_price ext_sales_price
   FROM
     store_sales
   , item
   , date_dim
   WHERE (ss_store_sk IS NULL)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_item_sk = i_item_sk)
UNION ALL    SELECT
     'web' channel
   , 'ws_ship_customer_sk' col_name
   , d_year
   , d_qoy
   , i_category
   , ws_ext_sales_price ext_sales_price
   FROM
     web_sales
   , item
   , date_dim
   WHERE (ws_ship_customer_sk IS NULL)
      AND (ws_sold_date_sk = d_date_sk)
      AND (ws_item_sk = i_item_sk)
UNION ALL    SELECT
     'catalog' channel
   , 'cs_ship_addr_sk' col_name
   , d_year
   , d_qoy
   , i_category
   , cs_ext_sales_price ext_sales_price
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE (cs_ship_addr_sk IS NULL)
      AND (cs_sold_date_sk = d_date_sk)
      AND (cs_item_sk = i_item_sk)
)  foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel ASC, col_name ASC, d_year ASC, d_qoy ASC, i_category ASC
LIMIT 100;
"""
    order_qt_query76_before "${query76}"
    async_mv_rewrite_fail(db, mv76, query76, "mv76")
    order_qt_query76_after "${query76}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv76"""


    def mv77 = """
WITH
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
LIMIT 100;
"""
    def query77 = """
WITH
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
LIMIT 100;
"""
    order_qt_query77_before "${query77}"
    async_mv_rewrite_fail(db, mv77, query77, "mv77")
    order_qt_query77_after "${query77}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv77"""



    def mv78 = """
WITH
  ws AS (
   SELECT
     d_year ws_sold_year
   , ws_item_sk
   , ws_bill_customer_sk ws_customer_sk
   , sum(ws_quantity) ws_qty
   , sum(ws_wholesale_cost) ws_wc
   , sum(ws_sales_price) ws_sp
   FROM
     web_sales
   LEFT JOIN web_returns ON (wr_order_number = ws_order_number)
      AND (ws_item_sk = wr_item_sk)
   INNER JOIN date_dim ON (ws_sold_date_sk = d_date_sk)
   WHERE (wr_order_number IS NULL)
   GROUP BY d_year, ws_item_sk, ws_bill_customer_sk
)
, cs AS (
   SELECT
     d_year cs_sold_year
   , cs_item_sk
   , cs_bill_customer_sk cs_customer_sk
   , sum(cs_quantity) cs_qty
   , sum(cs_wholesale_cost) cs_wc
   , sum(cs_sales_price) cs_sp
   FROM
     catalog_sales
   LEFT JOIN catalog_returns ON (cr_order_number = cs_order_number)
      AND (cs_item_sk = cr_item_sk)
   INNER JOIN date_dim ON (cs_sold_date_sk = d_date_sk)
   WHERE (cr_order_number IS NULL)
   GROUP BY d_year, cs_item_sk, cs_bill_customer_sk
)
, ss AS (
   SELECT
     d_year ss_sold_year
   , ss_item_sk
   , ss_customer_sk
   , sum(ss_quantity) ss_qty
   , sum(ss_wholesale_cost) ss_wc
   , sum(ss_sales_price) ss_sp
   FROM
     store_sales
   LEFT JOIN store_returns ON (sr_ticket_number = ss_ticket_number)
      AND (ss_item_sk = sr_item_sk)
   INNER JOIN date_dim ON (ss_sold_date_sk = d_date_sk)
   WHERE (sr_ticket_number IS NULL)
   GROUP BY d_year, ss_item_sk, ss_customer_sk
)
SELECT
  ss_sold_year
, ss_item_sk
, ss_customer_sk
, round((CAST(ss_qty AS DECIMAL(10,2)) / COALESCE((ws_qty + cs_qty), 1)), 2) ratio
, ss_qty store_qty
, ss_wc store_wholesale_cost
, ss_sp store_sales_price
, (COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0)) other_chan_qty
, (COALESCE(ws_wc, 0) + COALESCE(cs_wc, 0)) other_chan_wholesale_cost
, (COALESCE(ws_sp, 0) + COALESCE(cs_sp, 0)) other_chan_sales_price
FROM
  ss
LEFT JOIN ws ON (ws_sold_year = ss_sold_year)
   AND (ws_item_sk = ss_item_sk)
   AND (ws_customer_sk = ss_customer_sk)
LEFT JOIN cs ON (cs_sold_year = ss_sold_year)
   AND (cs_item_sk = cs_item_sk)
   AND (cs_customer_sk = ss_customer_sk)
WHERE (COALESCE(ws_qty, 0) > 0)
   AND (COALESCE(cs_qty, 0) > 0)
   AND (ss_sold_year = 2000)
ORDER BY ss_sold_year ASC, ss_item_sk ASC, ss_customer_sk ASC, ss_qty DESC, ss_wc DESC, ss_sp DESC, other_chan_qty ASC, other_chan_wholesale_cost ASC, other_chan_sales_price ASC, round((CAST(ss_qty AS DECIMAL(10,2)) / COALESCE((ws_qty + cs_qty), 1)), 2) ASC
LIMIT 100;
"""
    def query78 = """
WITH
  ws AS (
   SELECT
     d_year ws_sold_year
   , ws_item_sk
   , ws_bill_customer_sk ws_customer_sk
   , sum(ws_quantity) ws_qty
   , sum(ws_wholesale_cost) ws_wc
   , sum(ws_sales_price) ws_sp
   FROM
     web_sales
   LEFT JOIN web_returns ON (wr_order_number = ws_order_number)
      AND (ws_item_sk = wr_item_sk)
   INNER JOIN date_dim ON (ws_sold_date_sk = d_date_sk)
   WHERE (wr_order_number IS NULL)
   GROUP BY d_year, ws_item_sk, ws_bill_customer_sk
)
, cs AS (
   SELECT
     d_year cs_sold_year
   , cs_item_sk
   , cs_bill_customer_sk cs_customer_sk
   , sum(cs_quantity) cs_qty
   , sum(cs_wholesale_cost) cs_wc
   , sum(cs_sales_price) cs_sp
   FROM
     catalog_sales
   LEFT JOIN catalog_returns ON (cr_order_number = cs_order_number)
      AND (cs_item_sk = cr_item_sk)
   INNER JOIN date_dim ON (cs_sold_date_sk = d_date_sk)
   WHERE (cr_order_number IS NULL)
   GROUP BY d_year, cs_item_sk, cs_bill_customer_sk
)
, ss AS (
   SELECT
     d_year ss_sold_year
   , ss_item_sk
   , ss_customer_sk
   , sum(ss_quantity) ss_qty
   , sum(ss_wholesale_cost) ss_wc
   , sum(ss_sales_price) ss_sp
   FROM
     store_sales
   LEFT JOIN store_returns ON (sr_ticket_number = ss_ticket_number)
      AND (ss_item_sk = sr_item_sk)
   INNER JOIN date_dim ON (ss_sold_date_sk = d_date_sk)
   WHERE (sr_ticket_number IS NULL)
   GROUP BY d_year, ss_item_sk, ss_customer_sk
)
SELECT
  ss_sold_year
, ss_item_sk
, ss_customer_sk
, round((CAST(ss_qty AS DECIMAL(10,2)) / COALESCE((ws_qty + cs_qty), 1)), 2) ratio
, ss_qty store_qty
, ss_wc store_wholesale_cost
, ss_sp store_sales_price
, (COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0)) other_chan_qty
, (COALESCE(ws_wc, 0) + COALESCE(cs_wc, 0)) other_chan_wholesale_cost
, (COALESCE(ws_sp, 0) + COALESCE(cs_sp, 0)) other_chan_sales_price
FROM
  ss
LEFT JOIN ws ON (ws_sold_year = ss_sold_year)
   AND (ws_item_sk = ss_item_sk)
   AND (ws_customer_sk = ss_customer_sk)
LEFT JOIN cs ON (cs_sold_year = ss_sold_year)
   AND (cs_item_sk = cs_item_sk)
   AND (cs_customer_sk = ss_customer_sk)
WHERE (COALESCE(ws_qty, 0) > 0)
   AND (COALESCE(cs_qty, 0) > 0)
   AND (ss_sold_year = 2000)
ORDER BY ss_sold_year ASC, ss_item_sk ASC, ss_customer_sk ASC, ss_qty DESC, ss_wc DESC, ss_sp DESC, other_chan_qty ASC, other_chan_wholesale_cost ASC, other_chan_sales_price ASC, round((CAST(ss_qty AS DECIMAL(10,2)) / COALESCE((ws_qty + cs_qty), 1)), 2) ASC
LIMIT 100;
"""
    order_qt_query78_before "${query78}"
    async_mv_rewrite_fail(db, mv78, query78, "mv78")
    order_qt_query78_after "${query78}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv78"""


    def mv79 = """
SELECT
  c_last_name
, c_first_name
, substr(s_city, 1, 30)
, ss_ticket_number
, amt
, profit
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , store.s_city
   , sum(ss_coupon_amt) amt
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND ((household_demographics.hd_dep_count = 6)
         OR (household_demographics.hd_vehicle_count > 2))
      AND (date_dim.d_dow = 1)
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_number_employees BETWEEN 200 AND 295)
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city
)  ms
, customer
WHERE (ss_customer_sk = c_customer_sk)
ORDER BY c_last_name ASC, c_first_name ASC, substr(s_city, 1, 30) ASC, profit ASC, ss_ticket_number, amt
LIMIT 100;
"""
    def query79 = """
SELECT
  c_last_name
, c_first_name
, substr(s_city, 1, 30)
, ss_ticket_number
, amt
, profit
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , store.s_city
   , sum(ss_coupon_amt) amt
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND ((household_demographics.hd_dep_count = 6)
         OR (household_demographics.hd_vehicle_count > 2))
      AND (date_dim.d_dow = 1)
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_number_employees BETWEEN 200 AND 295)
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city
)  ms
, customer
WHERE (ss_customer_sk = c_customer_sk)
ORDER BY c_last_name ASC, c_first_name ASC, substr(s_city, 1, 30) ASC, profit ASC, ss_ticket_number, amt
LIMIT 100;
"""
    order_qt_query79_before "${query79}"
    async_mv_rewrite_fail(db, mv79, query79, "mv79")
    order_qt_query79_after "${query79}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv79"""

    def mv79_1 = """
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , store.s_city
   , sum(ss_coupon_amt) amt
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND ((household_demographics.hd_dep_count = 6)
         OR (household_demographics.hd_vehicle_count > 2))
      AND (date_dim.d_dow = 1)
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_number_employees BETWEEN 200 AND 295)
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city;
"""
    def query79_1 = """
SELECT
  c_last_name
, c_first_name
, substr(s_city, 1, 30)
, ss_ticket_number
, amt
, profit
FROM
  (
   SELECT
     ss_ticket_number
   , ss_customer_sk
   , store.s_city
   , sum(ss_coupon_amt) amt
   , sum(ss_net_profit) profit
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
      AND (store_sales.ss_store_sk = store.s_store_sk)
      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND ((household_demographics.hd_dep_count = 6)
         OR (household_demographics.hd_vehicle_count > 2))
      AND (date_dim.d_dow = 1)
      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
      AND (store.s_number_employees BETWEEN 200 AND 295)
   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city
)  ms
, customer
WHERE (ss_customer_sk = c_customer_sk)
ORDER BY c_last_name ASC, c_first_name ASC, substr(s_city, 1, 30) ASC, profit ASC, ss_ticket_number, amt
LIMIT 100;
"""
    order_qt_query79_1_before "${query79_1}"
    async_mv_rewrite_success(db, mv79_1, query79_1, "mv79_1")
    order_qt_query79_1_after "${query79_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv79_1"""

    def mv80 = """
WITH
  ssr AS (
   SELECT
     s_store_id store_id
   , sum(ss_ext_sales_price) sales
   , sum(COALESCE(sr_return_amt, 0)) returns
   , sum((ss_net_profit - COALESCE(sr_net_loss, 0))) profit
   FROM
     store_sales
   LEFT JOIN store_returns ON (ss_item_sk = sr_item_sk)
      AND (ss_ticket_number = sr_ticket_number)
   , date_dim
   , store
   , item
   , promotion
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (CAST(d_date AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ss_store_sk = s_store_sk)
      AND (ss_item_sk = i_item_sk)
      AND (i_current_price > 50)
      AND (ss_promo_sk = p_promo_sk)
      AND (p_channel_tv = 'N')
   GROUP BY s_store_id
)
, csr AS (
   SELECT
     cp_catalog_page_id catalog_page_id
   , sum(cs_ext_sales_price) sales
   , sum(COALESCE(cr_return_amount, 0)) returns
   , sum((cs_net_profit - COALESCE(cr_net_loss, 0))) profit
   FROM
     catalog_sales
   LEFT JOIN catalog_returns ON (cs_item_sk = cr_item_sk)
      AND (cs_order_number = cr_order_number)
   , date_dim
   , catalog_page
   , item
   , promotion
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (CAST(d_date AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (cs_catalog_page_sk = cp_catalog_page_sk)
      AND (cs_item_sk = i_item_sk)
      AND (i_current_price > 50)
      AND (cs_promo_sk = p_promo_sk)
      AND (p_channel_tv = 'N')
   GROUP BY cp_catalog_page_id
)
, wsr AS (
   SELECT
     web_site_id
   , sum(ws_ext_sales_price) sales
   , sum(COALESCE(wr_return_amt, 0)) returns
   , sum((ws_net_profit - COALESCE(wr_net_loss, 0))) profit
   FROM
     web_sales
   LEFT JOIN web_returns ON (ws_item_sk = wr_item_sk)
      AND (ws_order_number = wr_order_number)
   , date_dim
   , web_site
   , item
   , promotion
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (CAST(d_date AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ws_web_site_sk = web_site_sk)
      AND (ws_item_sk = i_item_sk)
      AND (i_current_price > 50)
      AND (ws_promo_sk = p_promo_sk)
      AND (p_channel_tv = 'N')
   GROUP BY web_site_id
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
   , concat('store', store_id) id
   , sales
   , returns
   , profit
   FROM
     ssr
UNION ALL    SELECT
     'catalog channel' channel
   , concat('catalog_page', catalog_page_id) id
   , sales
   , returns
   , profit
   FROM
     csr
UNION ALL    SELECT
     'web channel' channel
   , concat('web_site', web_site_id) id
   , sales
   , returns
   , profit
   FROM
     wsr
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC
LIMIT 100;
"""
    def query80 = """
WITH
  ssr AS (
   SELECT
     s_store_id store_id
   , sum(ss_ext_sales_price) sales
   , sum(COALESCE(sr_return_amt, 0)) returns
   , sum((ss_net_profit - COALESCE(sr_net_loss, 0))) profit
   FROM
     store_sales
   LEFT JOIN store_returns ON (ss_item_sk = sr_item_sk)
      AND (ss_ticket_number = sr_ticket_number)
   , date_dim
   , store
   , item
   , promotion
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (CAST(d_date AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ss_store_sk = s_store_sk)
      AND (ss_item_sk = i_item_sk)
      AND (i_current_price > 50)
      AND (ss_promo_sk = p_promo_sk)
      AND (p_channel_tv = 'N')
   GROUP BY s_store_id
)
, csr AS (
   SELECT
     cp_catalog_page_id catalog_page_id
   , sum(cs_ext_sales_price) sales
   , sum(COALESCE(cr_return_amount, 0)) returns
   , sum((cs_net_profit - COALESCE(cr_net_loss, 0))) profit
   FROM
     catalog_sales
   LEFT JOIN catalog_returns ON (cs_item_sk = cr_item_sk)
      AND (cs_order_number = cr_order_number)
   , date_dim
   , catalog_page
   , item
   , promotion
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (CAST(d_date AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (cs_catalog_page_sk = cp_catalog_page_sk)
      AND (cs_item_sk = i_item_sk)
      AND (i_current_price > 50)
      AND (cs_promo_sk = p_promo_sk)
      AND (p_channel_tv = 'N')
   GROUP BY cp_catalog_page_id
)
, wsr AS (
   SELECT
     web_site_id
   , sum(ws_ext_sales_price) sales
   , sum(COALESCE(wr_return_amt, 0)) returns
   , sum((ws_net_profit - COALESCE(wr_net_loss, 0))) profit
   FROM
     web_sales
   LEFT JOIN web_returns ON (ws_item_sk = wr_item_sk)
      AND (ws_order_number = wr_order_number)
   , date_dim
   , web_site
   , item
   , promotion
   WHERE (ws_sold_date_sk = d_date_sk)
      AND (CAST(d_date AS DATE) BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '30' DAY))
      AND (ws_web_site_sk = web_site_sk)
      AND (ws_item_sk = i_item_sk)
      AND (i_current_price > 50)
      AND (ws_promo_sk = p_promo_sk)
      AND (p_channel_tv = 'N')
   GROUP BY web_site_id
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
   , concat('store', store_id) id
   , sales
   , returns
   , profit
   FROM
     ssr
UNION ALL    SELECT
     'catalog channel' channel
   , concat('catalog_page', catalog_page_id) id
   , sales
   , returns
   , profit
   FROM
     csr
UNION ALL    SELECT
     'web channel' channel
   , concat('web_site', web_site_id) id
   , sales
   , returns
   , profit
   FROM
     wsr
)  x
GROUP BY ROLLUP (channel, id)
ORDER BY channel ASC, id ASC
LIMIT 100;
"""
    order_qt_query80_before "${query80}"
    async_mv_rewrite_fail(db, mv80, query80, "mv80")
    order_qt_query80_after "${query80}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv80"""


    def mv81 = """
WITH
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
LIMIT 100;
"""
    def query81 = """
WITH
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
LIMIT 100;
"""
    order_qt_query81_before "${query81}"
    async_mv_rewrite_fail(db, mv81, query81, "mv81")
    order_qt_query81_after "${query81}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv81"""

    def mv81_1 = """
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
   GROUP BY cr_returning_customer_sk, ca_state;
"""
    def query81_1 = """
WITH
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
LIMIT 100;
"""
    order_qt_query81_1_before "${query81_1}"
    async_mv_rewrite_success(db, mv81_1, query81_1, "mv81_1")
    order_qt_query81_1_after "${query81_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv81_1"""

    def mv82 = """
SELECT
  i_item_id
, i_item_desc
, i_current_price
FROM
  item
, inventory
, date_dim
, store_sales
WHERE (i_current_price BETWEEN 62 AND (62 + 30))
   AND (inv_item_sk = i_item_sk)
   AND (d_date_sk = inv_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-05-25' AS DATE) AND (CAST('2000-05-25' AS DATE) + INTERVAL  '60' DAY))
   AND (i_manufact_id IN (129, 270, 821, 423))
   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
   AND (ss_item_sk = i_item_sk)
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id ASC
LIMIT 100;
"""
    def query82 = """
SELECT
  i_item_id
, i_item_desc
, i_current_price
FROM
  item
, inventory
, date_dim
, store_sales
WHERE (i_current_price BETWEEN 62 AND (62 + 30))
   AND (inv_item_sk = i_item_sk)
   AND (d_date_sk = inv_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-05-25' AS DATE) AND (CAST('2000-05-25' AS DATE) + INTERVAL  '60' DAY))
   AND (i_manufact_id IN (129, 270, 821, 423))
   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
   AND (ss_item_sk = i_item_sk)
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id ASC
LIMIT 100;
"""
    order_qt_query82_before "${query82}"
    async_mv_rewrite_fail(db, mv82, query82, "mv82")
    order_qt_query82_after "${query82}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv82"""

    def mv82_1 = """
SELECT
  i_item_id
, i_item_desc
, i_current_price
FROM
  item
, inventory
, date_dim
, store_sales
WHERE (i_current_price BETWEEN 62 AND (62 + 30))
   AND (inv_item_sk = i_item_sk)
   AND (d_date_sk = inv_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-05-25' AS DATE) AND (CAST('2000-05-25' AS DATE) + INTERVAL  '60' DAY))
   AND (i_manufact_id IN (129, 270, 821, 423))
   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
   AND (ss_item_sk = i_item_sk)
GROUP BY i_item_id, i_item_desc, i_current_price;
"""
    def query82_1 = """
SELECT
  i_item_id
, i_item_desc
, i_current_price
FROM
  item
, inventory
, date_dim
, store_sales
WHERE (i_current_price BETWEEN 62 AND (62 + 30))
   AND (inv_item_sk = i_item_sk)
   AND (d_date_sk = inv_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('2000-05-25' AS DATE) AND (CAST('2000-05-25' AS DATE) + INTERVAL  '60' DAY))
   AND (i_manufact_id IN (129, 270, 821, 423))
   AND (inv_quantity_on_hand BETWEEN 100 AND 500)
   AND (ss_item_sk = i_item_sk)
GROUP BY i_item_id, i_item_desc, i_current_price
ORDER BY i_item_id ASC
LIMIT 100;
"""
    order_qt_query82_1_before "${query82_1}"
    async_mv_rewrite_success(db, mv82_1, query82_1, "mv82_1")
    order_qt_query82_1_after "${query82_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv82_1"""

    def mv83 = """
WITH
  sr_items AS (
   SELECT
     i_item_id item_id
   , sum(sr_return_quantity) sr_item_qty
   FROM
     store_returns
   , item
   , date_dim
   WHERE (sr_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq IN (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_date IN (CAST('2000-06-30' AS DATE)         , CAST('2000-09-27' AS DATE)         , CAST('2000-11-17' AS DATE)))
      ))
   ))
      AND (sr_returned_date_sk = d_date_sk)
   GROUP BY i_item_id
)
, cr_items AS (
   SELECT
     i_item_id item_id
   , sum(cr_return_quantity) cr_item_qty
   FROM
     catalog_returns
   , item
   , date_dim
   WHERE (cr_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq IN (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_date IN (CAST('2000-06-30' AS DATE)         , CAST('2000-09-27' AS DATE)         , CAST('2000-11-17' AS DATE)))
      ))
   ))
      AND (cr_returned_date_sk = d_date_sk)
   GROUP BY i_item_id
)
, wr_items AS (
   SELECT
     i_item_id item_id
   , sum(wr_return_quantity) wr_item_qty
   FROM
     web_returns
   , item
   , date_dim
   WHERE (wr_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq IN (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_date IN (CAST('2000-06-30' AS DATE)         , CAST('2000-09-27' AS DATE)         , CAST('2000-11-17' AS DATE)))
      ))
   ))
      AND (wr_returned_date_sk = d_date_sk)
   GROUP BY i_item_id
)
SELECT
  sr_items.item_id
, sr_item_qty
, CAST((((sr_item_qty / ((CAST(sr_item_qty AS DECIMAL(9,4)) + cr_item_qty) + wr_item_qty)) / CAST('3.0' AS DECIMAL(2,1))) * 100) AS DECIMAL(7,2)) sr_dev
, cr_item_qty
, CAST((((cr_item_qty / ((CAST(sr_item_qty AS DECIMAL(9,4)) + cr_item_qty) + wr_item_qty)) / CAST('3.0' AS DECIMAL(2,1))) * 100) AS DECIMAL(7,2)) cr_dev
, wr_item_qty
, CAST((((wr_item_qty / ((CAST(sr_item_qty AS DECIMAL(9,4)) + cr_item_qty) + wr_item_qty)) / CAST('3.0' AS DECIMAL(2,1))) * 100) AS DECIMAL(7,2)) wr_dev
, (((sr_item_qty + cr_item_qty) + wr_item_qty) / CAST('3.00' AS DECIMAL(5,2))) average
FROM
  sr_items
, cr_items
, wr_items
WHERE (sr_items.item_id = cr_items.item_id)
   AND (sr_items.item_id = wr_items.item_id)
ORDER BY sr_items.item_id ASC, sr_item_qty ASC
LIMIT 100;
"""
    def query83 = """
WITH
  sr_items AS (
   SELECT
     i_item_id item_id
   , sum(sr_return_quantity) sr_item_qty
   FROM
     store_returns
   , item
   , date_dim
   WHERE (sr_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq IN (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_date IN (CAST('2000-06-30' AS DATE)         , CAST('2000-09-27' AS DATE)         , CAST('2000-11-17' AS DATE)))
      ))
   ))
      AND (sr_returned_date_sk = d_date_sk)
   GROUP BY i_item_id
)
, cr_items AS (
   SELECT
     i_item_id item_id
   , sum(cr_return_quantity) cr_item_qty
   FROM
     catalog_returns
   , item
   , date_dim
   WHERE (cr_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq IN (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_date IN (CAST('2000-06-30' AS DATE)         , CAST('2000-09-27' AS DATE)         , CAST('2000-11-17' AS DATE)))
      ))
   ))
      AND (cr_returned_date_sk = d_date_sk)
   GROUP BY i_item_id
)
, wr_items AS (
   SELECT
     i_item_id item_id
   , sum(wr_return_quantity) wr_item_qty
   FROM
     web_returns
   , item
   , date_dim
   WHERE (wr_item_sk = i_item_sk)
      AND (d_date IN (
      SELECT d_date
      FROM
        date_dim
      WHERE (d_week_seq IN (
         SELECT d_week_seq
         FROM
           date_dim
         WHERE (d_date IN (CAST('2000-06-30' AS DATE)         , CAST('2000-09-27' AS DATE)         , CAST('2000-11-17' AS DATE)))
      ))
   ))
      AND (wr_returned_date_sk = d_date_sk)
   GROUP BY i_item_id
)
SELECT
  sr_items.item_id
, sr_item_qty
, CAST((((sr_item_qty / ((CAST(sr_item_qty AS DECIMAL(9,4)) + cr_item_qty) + wr_item_qty)) / CAST('3.0' AS DECIMAL(2,1))) * 100) AS DECIMAL(7,2)) sr_dev
, cr_item_qty
, CAST((((cr_item_qty / ((CAST(sr_item_qty AS DECIMAL(9,4)) + cr_item_qty) + wr_item_qty)) / CAST('3.0' AS DECIMAL(2,1))) * 100) AS DECIMAL(7,2)) cr_dev
, wr_item_qty
, CAST((((wr_item_qty / ((CAST(sr_item_qty AS DECIMAL(9,4)) + cr_item_qty) + wr_item_qty)) / CAST('3.0' AS DECIMAL(2,1))) * 100) AS DECIMAL(7,2)) wr_dev
, (((sr_item_qty + cr_item_qty) + wr_item_qty) / CAST('3.00' AS DECIMAL(5,2))) average
FROM
  sr_items
, cr_items
, wr_items
WHERE (sr_items.item_id = cr_items.item_id)
   AND (sr_items.item_id = wr_items.item_id)
ORDER BY sr_items.item_id ASC, sr_item_qty ASC
LIMIT 100;
"""
    order_qt_query83_before "${query83}"
    async_mv_rewrite_fail(db, mv83, query83, "mv83")
    order_qt_query83_after "${query83}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv83"""


    def mv84 = """
SELECT
  c_customer_id customer_id
, concat(concat(c_last_name, ', '), c_first_name) customername
FROM
  customer
, customer_address
, customer_demographics
, household_demographics
, income_band
, store_returns
WHERE (ca_city = 'Edgewood')
   AND (c_current_addr_sk = ca_address_sk)
   AND (ib_lower_bound >= 38128)
   AND (ib_upper_bound <= (38128 + 50000))
   AND (ib_income_band_sk = hd_income_band_sk)
   AND (cd_demo_sk = c_current_cdemo_sk)
   AND (hd_demo_sk = c_current_hdemo_sk)
   AND (sr_cdemo_sk = cd_demo_sk)
ORDER BY c_customer_id ASC
LIMIT 100;
"""
    def query84 = """
SELECT
  c_customer_id customer_id
, concat(concat(c_last_name, ', '), c_first_name) customername
FROM
  customer
, customer_address
, customer_demographics
, household_demographics
, income_band
, store_returns
WHERE (ca_city = 'Edgewood')
   AND (c_current_addr_sk = ca_address_sk)
   AND (ib_lower_bound >= 38128)
   AND (ib_upper_bound <= (38128 + 50000))
   AND (ib_income_band_sk = hd_income_band_sk)
   AND (cd_demo_sk = c_current_cdemo_sk)
   AND (hd_demo_sk = c_current_hdemo_sk)
   AND (sr_cdemo_sk = cd_demo_sk)
ORDER BY c_customer_id ASC
LIMIT 100;
"""
    order_qt_query84_before "${query84}"
    async_mv_rewrite_fail(db, mv84, query84, "mv84")
    order_qt_query84_after "${query84}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv84"""

    def mv84_1 = """
SELECT
  c_customer_id customer_id
, concat(concat(c_last_name, ', '), c_first_name) customername
FROM
  customer
, customer_address
, customer_demographics
, household_demographics
, income_band
, store_returns
WHERE (ca_city = 'Edgewood')
   AND (c_current_addr_sk = ca_address_sk)
   AND (ib_lower_bound >= 38128)
   AND (ib_upper_bound <= (38128 + 50000))
   AND (ib_income_band_sk = hd_income_band_sk)
   AND (cd_demo_sk = c_current_cdemo_sk)
   AND (hd_demo_sk = c_current_hdemo_sk)
   AND (sr_cdemo_sk = cd_demo_sk);
"""
    def query84_1 = """
SELECT
  c_customer_id customer_id
, concat(concat(c_last_name, ', '), c_first_name) customername
FROM
  customer
, customer_address
, customer_demographics
, household_demographics
, income_band
, store_returns
WHERE (ca_city = 'Edgewood')
   AND (c_current_addr_sk = ca_address_sk)
   AND (ib_lower_bound >= 38128)
   AND (ib_upper_bound <= (38128 + 50000))
   AND (ib_income_band_sk = hd_income_band_sk)
   AND (cd_demo_sk = c_current_cdemo_sk)
   AND (hd_demo_sk = c_current_hdemo_sk)
   AND (sr_cdemo_sk = cd_demo_sk)
ORDER BY c_customer_id ASC
LIMIT 100;
"""
    order_qt_query84_1_before "${query84_1}"
    async_mv_rewrite_success(db, mv84_1, query84_1, "mv84_1")
    order_qt_query84_1_after "${query84_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv84_1"""

    def mv85 = """
SELECT
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
LIMIT 100;
"""
    def query85 = """
SELECT
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
LIMIT 100;
"""
    order_qt_query85_before "${query85}"
    async_mv_rewrite_fail(db, mv85, query85, "mv85")
    order_qt_query85_after "${query85}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv85"""

    def mv85_1 = """
SELECT
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
GROUP BY r_reason_desc;
"""
    def query85_1 = """
SELECT
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
LIMIT 100;
"""
    order_qt_query85_1_before "${query85_1}"
    async_mv_rewrite_success(db, mv85_1, query85_1, "mv85_1")
    order_qt_query85_1_after "${query85_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv85_1"""

    def mv86 = """
SELECT
  sum(ws_net_paid) total_sum
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY sum(ws_net_paid) DESC) rank_within_parent
FROM
  web_sales
, date_dim d1
, item
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ws_sold_date_sk)
   AND (i_item_sk = ws_item_sk)
GROUP BY ROLLUP (i_category, i_class)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC
LIMIT 100;
"""
    def query86 = """
SELECT
  sum(ws_net_paid) total_sum
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY sum(ws_net_paid) DESC) rank_within_parent
FROM
  web_sales
, date_dim d1
, item
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ws_sold_date_sk)
   AND (i_item_sk = ws_item_sk)
GROUP BY ROLLUP (i_category, i_class)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC
LIMIT 100;
"""
    order_qt_query86_before "${query86}"
    async_mv_rewrite_fail(db, mv86, query86, "mv86")
    order_qt_query86_after "${query86}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv86"""

    def mv86_1 = """
SELECT
  sum(ws_net_paid) total_sum
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY sum(ws_net_paid) DESC) rank_within_parent
FROM
  web_sales
, date_dim d1
, item
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ws_sold_date_sk)
   AND (i_item_sk = ws_item_sk)
GROUP BY ROLLUP (i_category, i_class);
"""
    def query86_1 = """
SELECT
  sum(ws_net_paid) total_sum
, i_category
, i_class
, (GROUPING (i_category) + GROUPING (i_class)) lochierarchy
, rank() OVER (PARTITION BY (GROUPING (i_category) + GROUPING (i_class)), (CASE WHEN (GROUPING (i_class) = 0) THEN i_category END) ORDER BY sum(ws_net_paid) DESC) rank_within_parent
FROM
  web_sales
, date_dim d1
, item
WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (d1.d_date_sk = ws_sold_date_sk)
   AND (i_item_sk = ws_item_sk)
GROUP BY ROLLUP (i_category, i_class)
ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN i_category END) ASC, rank_within_parent ASC
LIMIT 100;
"""
    order_qt_query86_1_before "${query86_1}"
    async_mv_rewrite_fail(db, mv86_1, query86_1, "mv86_1")
    order_qt_query86_1_after "${query86_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv86_1"""

    def mv87 = """
SELECT count(*)
FROM
  (
(
      SELECT DISTINCT
        c_last_name
      , c_first_name
      , d_date
      FROM
        store_sales
      , date_dim
      , customer
      WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
         AND (store_sales.ss_customer_sk = customer.c_customer_sk)
         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   ) EXCEPT (
      SELECT DISTINCT
        c_last_name
      , c_first_name
      , d_date
      FROM
        catalog_sales
      , date_dim
      , customer
      WHERE (catalog_sales.cs_sold_date_sk = date_dim.d_date_sk)
         AND (catalog_sales.cs_bill_customer_sk = customer.c_customer_sk)
         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   ) EXCEPT (
      SELECT DISTINCT
        c_last_name
      , c_first_name
      , d_date
      FROM
        web_sales
      , date_dim
      , customer
      WHERE (web_sales.ws_sold_date_sk = date_dim.d_date_sk)
         AND (web_sales.ws_bill_customer_sk = customer.c_customer_sk)
         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   ) )  cool_cust

"""
    def query87 = """
SELECT count(*)
FROM
  (
(
      SELECT DISTINCT
        c_last_name
      , c_first_name
      , d_date
      FROM
        store_sales
      , date_dim
      , customer
      WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
         AND (store_sales.ss_customer_sk = customer.c_customer_sk)
         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   ) EXCEPT (
      SELECT DISTINCT
        c_last_name
      , c_first_name
      , d_date
      FROM
        catalog_sales
      , date_dim
      , customer
      WHERE (catalog_sales.cs_sold_date_sk = date_dim.d_date_sk)
         AND (catalog_sales.cs_bill_customer_sk = customer.c_customer_sk)
         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   ) EXCEPT (
      SELECT DISTINCT
        c_last_name
      , c_first_name
      , d_date
      FROM
        web_sales
      , date_dim
      , customer
      WHERE (web_sales.ws_sold_date_sk = date_dim.d_date_sk)
         AND (web_sales.ws_bill_customer_sk = customer.c_customer_sk)
         AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   ) )  cool_cust;
"""
    order_qt_query87_before "${query87}"
    async_mv_rewrite_fail(db, mv87, query87, "mv87")
    order_qt_query87_after "${query87}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv87"""


    def mv88 = """
SELECT *
FROM
  (
   SELECT count(*) h8_30_to_9
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 8)
      AND (time_dim.t_minute >= 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s1
, (
   SELECT count(*) h9_to_9_30
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 9)
      AND (time_dim.t_minute < 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s2
, (
   SELECT count(*) h9_30_to_10
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 9)
      AND (time_dim.t_minute >= 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s3
, (
   SELECT count(*) h10_to_10_30
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 10)
      AND (time_dim.t_minute < 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s4
, (
   SELECT count(*) h10_30_to_11
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 10)
      AND (time_dim.t_minute >= 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s5
, (
   SELECT count(*) h11_to_11_30
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 11)
      AND (time_dim.t_minute < 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s6
, (
   SELECT count(*) h11_30_to_12
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 11)
      AND (time_dim.t_minute >= 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s7
, (
   SELECT count(*) h12_to_12_30
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 12)
      AND (time_dim.t_minute < 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s8;
"""
    def query88 = """
SELECT *
FROM
  (
   SELECT count(*) h8_30_to_9
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 8)
      AND (time_dim.t_minute >= 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s1
, (
   SELECT count(*) h9_to_9_30
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 9)
      AND (time_dim.t_minute < 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s2
, (
   SELECT count(*) h9_30_to_10
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 9)
      AND (time_dim.t_minute >= 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s3
, (
   SELECT count(*) h10_to_10_30
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 10)
      AND (time_dim.t_minute < 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s4
, (
   SELECT count(*) h10_30_to_11
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 10)
      AND (time_dim.t_minute >= 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s5
, (
   SELECT count(*) h11_to_11_30
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 11)
      AND (time_dim.t_minute < 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s6
, (
   SELECT count(*) h11_30_to_12
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 11)
      AND (time_dim.t_minute >= 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s7
, (
   SELECT count(*) h12_to_12_30
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ss_store_sk = s_store_sk)
      AND (time_dim.t_hour = 12)
      AND (time_dim.t_minute < 30)
      AND (((household_demographics.hd_dep_count = 4)
            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
         OR ((household_demographics.hd_dep_count = 2)
            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
         OR ((household_demographics.hd_dep_count = 0)
            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
      AND (store.s_store_name = 'ese')
)  s8;
"""
    order_qt_query88_before "${query88}"
    async_mv_rewrite_fail(db, mv88, query88, "mv88")
    order_qt_query88_after "${query88}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv88"""


    def mv89 = """
SELECT *
FROM
  (
   SELECT
     i_category
   , i_class
   , i_brand
   , s_store_name
   , s_company_name
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_year IN (1999))
      AND (((i_category IN ('Books'         , 'Electronics'         , 'Sports'))
            AND (i_class IN ('computers'         , 'stereo'         , 'football')))
         OR ((i_category IN ('Men'         , 'Jewelry'         , 'Women'))
            AND (i_class IN ('shirts'         , 'birdal'         , 'dresses'))))
   GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
)  tmp1
WHERE ((CASE WHEN (avg_monthly_sales <> 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, s_store_name ASC
LIMIT 100;
"""
    def query89 = """
SELECT *
FROM
  (
   SELECT
     i_category
   , i_class
   , i_brand
   , s_store_name
   , s_company_name
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_year IN (1999))
      AND (((i_category IN ('Books'         , 'Electronics'         , 'Sports'))
            AND (i_class IN ('computers'         , 'stereo'         , 'football')))
         OR ((i_category IN ('Men'         , 'Jewelry'         , 'Women'))
            AND (i_class IN ('shirts'         , 'birdal'         , 'dresses'))))
   GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
)  tmp1
WHERE ((CASE WHEN (avg_monthly_sales <> 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, s_store_name ASC
LIMIT 100;
"""
    order_qt_query89_before "${query89}"
    async_mv_rewrite_fail(db, mv89, query89, "mv89")
    order_qt_query89_after "${query89}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv89"""

    def mv89_1 = """
   SELECT
     i_category
   , i_class
   , i_brand
   , s_store_name
   , s_company_name
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_year IN (1999))
      AND (((i_category IN ('Books'         , 'Electronics'         , 'Sports'))
            AND (i_class IN ('computers'         , 'stereo'         , 'football')))
         OR ((i_category IN ('Men'         , 'Jewelry'         , 'Women'))
            AND (i_class IN ('shirts'         , 'birdal'         , 'dresses'))))
   GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy;
"""
    def query89_1 = """
SELECT *
FROM
  (
   SELECT
     i_category
   , i_class
   , i_brand
   , s_store_name
   , s_company_name
   , d_moy
   , sum(ss_sales_price) sum_sales
   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE (ss_item_sk = i_item_sk)
      AND (ss_sold_date_sk = d_date_sk)
      AND (ss_store_sk = s_store_sk)
      AND (d_year IN (1999))
      AND (((i_category IN ('Books'         , 'Electronics'         , 'Sports'))
            AND (i_class IN ('computers'         , 'stereo'         , 'football')))
         OR ((i_category IN ('Men'         , 'Jewelry'         , 'Women'))
            AND (i_class IN ('shirts'         , 'birdal'         , 'dresses'))))
   GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy
)  tmp1
WHERE ((CASE WHEN (avg_monthly_sales <> 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL(2,1)))
ORDER BY (sum_sales - avg_monthly_sales) ASC, s_store_name ASC
LIMIT 100;
"""
    order_qt_query89_1_before "${query89_1}"
    // should success but fail
    async_mv_rewrite_fail(db, mv89_1, query89_1, "mv89_1")
    order_qt_query89_1_after "${query89_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv89_1"""

    def mv90 = """
SELECT (CAST(amc AS DECIMAL(15,4)) / CAST(pmc AS DECIMAL(15,4))) am_pm_ratio
FROM
  (
   SELECT count(*) amc
   FROM
     web_sales
   , household_demographics
   , time_dim
   , web_page
   WHERE (ws_sold_time_sk = time_dim.t_time_sk)
      AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ws_web_page_sk = web_page.wp_web_page_sk)
      AND (time_dim.t_hour BETWEEN 8 AND (8 + 1))
      AND (household_demographics.hd_dep_count = 6)
      AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
)  at
, (
   SELECT count(*) pmc
   FROM
     web_sales
   , household_demographics
   , time_dim
   , web_page
   WHERE (ws_sold_time_sk = time_dim.t_time_sk)
      AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ws_web_page_sk = web_page.wp_web_page_sk)
      AND (time_dim.t_hour BETWEEN 19 AND (19 + 1))
      AND (household_demographics.hd_dep_count = 6)
      AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
)  pt
ORDER BY am_pm_ratio ASC
LIMIT 100;
"""
    def query90 = """
SELECT (CAST(amc AS DECIMAL(15,4)) / CAST(pmc AS DECIMAL(15,4))) am_pm_ratio
FROM
  (
   SELECT count(*) amc
   FROM
     web_sales
   , household_demographics
   , time_dim
   , web_page
   WHERE (ws_sold_time_sk = time_dim.t_time_sk)
      AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ws_web_page_sk = web_page.wp_web_page_sk)
      AND (time_dim.t_hour BETWEEN 8 AND (8 + 1))
      AND (household_demographics.hd_dep_count = 6)
      AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
)  at
, (
   SELECT count(*) pmc
   FROM
     web_sales
   , household_demographics
   , time_dim
   , web_page
   WHERE (ws_sold_time_sk = time_dim.t_time_sk)
      AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
      AND (ws_web_page_sk = web_page.wp_web_page_sk)
      AND (time_dim.t_hour BETWEEN 19 AND (19 + 1))
      AND (household_demographics.hd_dep_count = 6)
      AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
)  pt
ORDER BY am_pm_ratio ASC
LIMIT 100;
"""
    order_qt_query90_before "${query90}"
    async_mv_rewrite_fail(db, mv90, query90, "mv90")
    order_qt_query90_after "${query90}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv90"""


    def mv91 = """
SELECT
  cc_call_center_id Call_Center
, cc_name Call_Center_Name
, cc_manager Manager
, sum(cr_net_loss) Returns_Loss
FROM
  call_center
, catalog_returns
, date_dim
, customer
, customer_address
, customer_demographics
, household_demographics
WHERE (cr_call_center_sk = cc_call_center_sk)
   AND (cr_returned_date_sk = d_date_sk)
   AND (cr_returning_customer_sk = c_customer_sk)
   AND (cd_demo_sk = c_current_cdemo_sk)
   AND (hd_demo_sk = c_current_hdemo_sk)
   AND (ca_address_sk = c_current_addr_sk)
   AND (d_year = 1998)
   AND (d_moy = 11)
   AND (((cd_marital_status = 'M')
         AND (cd_education_status = 'Unknown'))
      OR ((cd_marital_status = 'W')
         AND (cd_education_status = 'Advanced Degree')))
   AND (hd_buy_potential LIKE 'Unknown%')
   AND (ca_gmt_offset = -7)
GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status
ORDER BY sum(cr_net_loss) DESC;
"""
    def query91 = """
SELECT
  cc_call_center_id Call_Center
, cc_name Call_Center_Name
, cc_manager Manager
, sum(cr_net_loss) Returns_Loss
FROM
  call_center
, catalog_returns
, date_dim
, customer
, customer_address
, customer_demographics
, household_demographics
WHERE (cr_call_center_sk = cc_call_center_sk)
   AND (cr_returned_date_sk = d_date_sk)
   AND (cr_returning_customer_sk = c_customer_sk)
   AND (cd_demo_sk = c_current_cdemo_sk)
   AND (hd_demo_sk = c_current_hdemo_sk)
   AND (ca_address_sk = c_current_addr_sk)
   AND (d_year = 1998)
   AND (d_moy = 11)
   AND (((cd_marital_status = 'M')
         AND (cd_education_status = 'Unknown'))
      OR ((cd_marital_status = 'W')
         AND (cd_education_status = 'Advanced Degree')))
   AND (hd_buy_potential LIKE 'Unknown%')
   AND (ca_gmt_offset = -7)
GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status
ORDER BY sum(cr_net_loss) DESC;
"""
    order_qt_query91_before "${query91}"
    async_mv_rewrite_success(db, mv91, query91, "mv91")
    order_qt_query91_after "${query91}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv91"""

    def mv92 = """
SELECT sum(ws_ext_discount_amt) 'Excess Discount Amount'
FROM
  web_sales
, item
, date_dim
WHERE (i_manufact_id = 350)
   AND (i_item_sk = ws_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = ws_sold_date_sk)
   AND (ws_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(ws_ext_discount_amt))
      FROM
        web_sales
      , date_dim
      WHERE (ws_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = ws_sold_date_sk)
   ))
ORDER BY sum(ws_ext_discount_amt) ASC
LIMIT 100;
"""
    def query92 = """
SELECT sum(ws_ext_discount_amt) 'Excess Discount Amount'
FROM
  web_sales
, item
, date_dim
WHERE (i_manufact_id = 350)
   AND (i_item_sk = ws_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = ws_sold_date_sk)
   AND (ws_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(ws_ext_discount_amt))
      FROM
        web_sales
      , date_dim
      WHERE (ws_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = ws_sold_date_sk)
   ))
ORDER BY sum(ws_ext_discount_amt) ASC
LIMIT 100;
"""
    order_qt_query92_before "${query92}"
    async_mv_rewrite_fail(db, mv92, query92, "mv92")
    order_qt_query92_after "${query92}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv92"""

    def mv92_1 = """
SELECT sum(ws_ext_discount_amt) 'Excess Discount Amount'
FROM
  web_sales
, item
, date_dim
WHERE (i_manufact_id = 350)
   AND (i_item_sk = ws_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = ws_sold_date_sk)
   AND (ws_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(ws_ext_discount_amt))
      FROM
        web_sales
      , date_dim
      WHERE (ws_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = ws_sold_date_sk)
   ));
"""
    def query92_1 = """
SELECT sum(ws_ext_discount_amt) 'Excess Discount Amount'
FROM
  web_sales
, item
, date_dim
WHERE (i_manufact_id = 350)
   AND (i_item_sk = ws_item_sk)
   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
   AND (d_date_sk = ws_sold_date_sk)
   AND (ws_ext_discount_amt > (
      SELECT (CAST('1.3' AS DECIMAL(2,1)) * avg(ws_ext_discount_amt))
      FROM
        web_sales
      , date_dim
      WHERE (ws_item_sk = i_item_sk)
         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND (d_date_sk = ws_sold_date_sk)
   ))
ORDER BY sum(ws_ext_discount_amt) ASC
LIMIT 100;
"""
    order_qt_query92_1_before "${query92_1}"
    // should success but fail
    async_mv_rewrite_fail(db, mv92_1, query92_1, "mv92_1")
    order_qt_query92_1_after "${query92_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv92_1"""

    def mv93 = """
SELECT
  ss_customer_sk
, sum(act_sales) sumsales
FROM
  (
   SELECT
     ss_item_sk
   , ss_ticket_number
   , ss_customer_sk
   , (CASE WHEN (sr_return_quantity IS NOT NULL) THEN ((ss_quantity - sr_return_quantity) * ss_sales_price) ELSE (ss_quantity * ss_sales_price) END) act_sales
   FROM
     store_sales
   LEFT JOIN store_returns ON (sr_item_sk = ss_item_sk)
      AND (sr_ticket_number = ss_ticket_number)
   , reason
   WHERE (sr_reason_sk = r_reason_sk)
      AND (r_reason_desc = 'reason 28')
)  t
GROUP BY ss_customer_sk
ORDER BY sumsales ASC, ss_customer_sk ASC
LIMIT 100;
"""
    def query93 = """
SELECT
  ss_customer_sk
, sum(act_sales) sumsales
FROM
  (
   SELECT
     ss_item_sk
   , ss_ticket_number
   , ss_customer_sk
   , (CASE WHEN (sr_return_quantity IS NOT NULL) THEN ((ss_quantity - sr_return_quantity) * ss_sales_price) ELSE (ss_quantity * ss_sales_price) END) act_sales
   FROM
     store_sales
   LEFT JOIN store_returns ON (sr_item_sk = ss_item_sk)
      AND (sr_ticket_number = ss_ticket_number)
   , reason
   WHERE (sr_reason_sk = r_reason_sk)
      AND (r_reason_desc = 'reason 28')
)  t
GROUP BY ss_customer_sk
ORDER BY sumsales ASC, ss_customer_sk ASC
LIMIT 100;
"""
    order_qt_query93_before "${query93}"
    async_mv_rewrite_fail(db, mv93, query93, "mv93")
    order_qt_query93_after "${query93}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv93"""

    def mv93_1 = """
SELECT
  ss_customer_sk
, sum(act_sales) sumsales
FROM
  (
   SELECT
     ss_item_sk
   , ss_ticket_number
   , ss_customer_sk
   , (CASE WHEN (sr_return_quantity IS NOT NULL) THEN ((ss_quantity - sr_return_quantity) * ss_sales_price) ELSE (ss_quantity * ss_sales_price) END) act_sales
   FROM
     store_sales
   LEFT JOIN store_returns ON (sr_item_sk = ss_item_sk)
      AND (sr_ticket_number = ss_ticket_number)
   , reason
   WHERE (sr_reason_sk = r_reason_sk)
      AND (r_reason_desc = 'reason 28')
)  t
GROUP BY ss_customer_sk;
"""
    def query93_1 = """
SELECT
  ss_customer_sk
, sum(act_sales) sumsales
FROM
  (
   SELECT
     ss_item_sk
   , ss_ticket_number
   , ss_customer_sk
   , (CASE WHEN (sr_return_quantity IS NOT NULL) THEN ((ss_quantity - sr_return_quantity) * ss_sales_price) ELSE (ss_quantity * ss_sales_price) END) act_sales
   FROM
     store_sales
   LEFT JOIN store_returns ON (sr_item_sk = ss_item_sk)
      AND (sr_ticket_number = ss_ticket_number)
   , reason
   WHERE (sr_reason_sk = r_reason_sk)
      AND (r_reason_desc = 'reason 28')
)  t
GROUP BY ss_customer_sk
ORDER BY sumsales ASC, ss_customer_sk ASC
LIMIT 100;
"""
    order_qt_query93_1_before "${query93_1}"
    async_mv_rewrite_success(db, mv93_1, query93_1, "mv93_1")
    order_qt_query93_1_after "${query93_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv93_1"""

    def mv94 = """
SELECT
  count(DISTINCT ws_order_number) 'order count'
, sum(ws_ext_ship_cost) 'total shipping cost'
, sum(ws_net_profit) 'total net profit'
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE (d_date BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (ws1.ws_ship_date_sk = d_date_sk)
   AND (ws1.ws_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'IL')
   AND (ws1.ws_web_site_sk = web_site_sk)
   AND (web_company_name = 'pri')
   AND (EXISTS (
   SELECT *
   FROM
     web_sales ws2
   WHERE (ws1.ws_order_number = ws2.ws_order_number)
      AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     web_returns wr1
   WHERE (ws1.ws_order_number = wr1.wr_order_number)
)))
ORDER BY count(DISTINCT ws_order_number) ASC
LIMIT 100;
"""
    def query94 = """
SELECT
  count(DISTINCT ws_order_number) 'order count'
, sum(ws_ext_ship_cost) 'total shipping cost'
, sum(ws_net_profit) 'total net profit'
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE (d_date BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (ws1.ws_ship_date_sk = d_date_sk)
   AND (ws1.ws_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'IL')
   AND (ws1.ws_web_site_sk = web_site_sk)
   AND (web_company_name = 'pri')
   AND (EXISTS (
   SELECT *
   FROM
     web_sales ws2
   WHERE (ws1.ws_order_number = ws2.ws_order_number)
      AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     web_returns wr1
   WHERE (ws1.ws_order_number = wr1.wr_order_number)
)))
ORDER BY count(DISTINCT ws_order_number) ASC
LIMIT 100;
"""
    order_qt_query94_before "${query94}"
    async_mv_rewrite_fail(db, mv94, query94, "mv94")
    order_qt_query94_after "${query94}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv94"""

    def mv94_1 = """
SELECT
  count(DISTINCT ws_order_number) 'order count'
, sum(ws_ext_ship_cost) 'total shipping cost'
, sum(ws_net_profit) 'total net profit'
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE (d_date BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (ws1.ws_ship_date_sk = d_date_sk)
   AND (ws1.ws_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'IL')
   AND (ws1.ws_web_site_sk = web_site_sk)
   AND (web_company_name = 'pri')
   AND (EXISTS (
   SELECT *
   FROM
     web_sales ws2
   WHERE (ws1.ws_order_number = ws2.ws_order_number)
      AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     web_returns wr1
   WHERE (ws1.ws_order_number = wr1.wr_order_number)
)));
"""
    def query94_1 = """
SELECT
  count(DISTINCT ws_order_number) 'order count'
, sum(ws_ext_ship_cost) 'total shipping cost'
, sum(ws_net_profit) 'total net profit'
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE (d_date BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (ws1.ws_ship_date_sk = d_date_sk)
   AND (ws1.ws_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'IL')
   AND (ws1.ws_web_site_sk = web_site_sk)
   AND (web_company_name = 'pri')
   AND (EXISTS (
   SELECT *
   FROM
     web_sales ws2
   WHERE (ws1.ws_order_number = ws2.ws_order_number)
      AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     web_returns wr1
   WHERE (ws1.ws_order_number = wr1.wr_order_number)
)))
ORDER BY count(DISTINCT ws_order_number) ASC
LIMIT 100;
"""
    order_qt_query94_1_before "${query94_1}"
    async_mv_rewrite_success(db, mv94_1, query94_1, "mv94_1")
    order_qt_query94_1_after "${query94_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv94_1"""

    def mv95 = """
WITH
  ws_wh AS (
   SELECT
     ws1.ws_order_number
   , ws1.ws_warehouse_sk wh1
   , ws2.ws_warehouse_sk wh2
   FROM
     web_sales ws1
   , web_sales ws2
   WHERE (ws1.ws_order_number = ws2.ws_order_number)
      AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
)
SELECT
  count(DISTINCT ws_order_number) 'order count'
, sum(ws_ext_ship_cost) 'total shipping cost'
, sum(ws_net_profit) 'total net profit'
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE (CAST(d_date AS DATE) BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (ws1.ws_ship_date_sk = d_date_sk)
   AND (ws1.ws_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'IL')
   AND (ws1.ws_web_site_sk = web_site_sk)
   AND (web_company_name = 'pri')
   AND (ws1.ws_order_number IN (
   SELECT ws_order_number
   FROM
     ws_wh
))
   AND (ws1.ws_order_number IN (
   SELECT wr_order_number
   FROM
     web_returns
   , ws_wh
   WHERE (wr_order_number = ws_wh.ws_order_number)
))
ORDER BY count(DISTINCT ws_order_number) ASC
LIMIT 100;
"""
    def query95 = """
WITH
  ws_wh AS (
   SELECT
     ws1.ws_order_number
   , ws1.ws_warehouse_sk wh1
   , ws2.ws_warehouse_sk wh2
   FROM
     web_sales ws1
   , web_sales ws2
   WHERE (ws1.ws_order_number = ws2.ws_order_number)
      AND (ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
)
SELECT
  count(DISTINCT ws_order_number) 'order count'
, sum(ws_ext_ship_cost) 'total shipping cost'
, sum(ws_net_profit) 'total net profit'
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE (CAST(d_date AS DATE) BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND (ws1.ws_ship_date_sk = d_date_sk)
   AND (ws1.ws_ship_addr_sk = ca_address_sk)
   AND (ca_state = 'IL')
   AND (ws1.ws_web_site_sk = web_site_sk)
   AND (web_company_name = 'pri')
   AND (ws1.ws_order_number IN (
   SELECT ws_order_number
   FROM
     ws_wh
))
   AND (ws1.ws_order_number IN (
   SELECT wr_order_number
   FROM
     web_returns
   , ws_wh
   WHERE (wr_order_number = ws_wh.ws_order_number)
))
ORDER BY count(DISTINCT ws_order_number) ASC
LIMIT 100;
"""
    order_qt_query95_before "${query95}"
    async_mv_rewrite_fail(db, mv95, query95, "mv95")
    order_qt_query95_after "${query95}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv95"""



    def mv96 = """
SELECT count(*)
FROM
  store_sales
, household_demographics
, time_dim
, store
WHERE (ss_sold_time_sk = time_dim.t_time_sk)
   AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
   AND (ss_store_sk = s_store_sk)
   AND (time_dim.t_hour = 20)
   AND (time_dim.t_minute >= 30)
   AND (household_demographics.hd_dep_count = 7)
   AND (store.s_store_name = 'ese')
ORDER BY count(*) ASC
LIMIT 100;
"""
    def query96 = """
SELECT count(*)
FROM
  store_sales
, household_demographics
, time_dim
, store
WHERE (ss_sold_time_sk = time_dim.t_time_sk)
   AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
   AND (ss_store_sk = s_store_sk)
   AND (time_dim.t_hour = 20)
   AND (time_dim.t_minute >= 30)
   AND (household_demographics.hd_dep_count = 7)
   AND (store.s_store_name = 'ese')
ORDER BY count(*) ASC
LIMIT 100;
"""
    order_qt_query96_before "${query96}"
    async_mv_rewrite_fail(db, mv96, query96, "mv96")
    order_qt_query96_after "${query96}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv96"""

    def mv96_1 = """
SELECT count(*)
FROM
  store_sales
, household_demographics
, time_dim
, store
WHERE (ss_sold_time_sk = time_dim.t_time_sk)
   AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
   AND (ss_store_sk = s_store_sk)
   AND (time_dim.t_hour = 20)
   AND (time_dim.t_minute >= 30)
   AND (household_demographics.hd_dep_count = 7)
   AND (store.s_store_name = 'ese');
"""
    def query96_1 = """
SELECT count(*)
FROM
  store_sales
, household_demographics
, time_dim
, store
WHERE (ss_sold_time_sk = time_dim.t_time_sk)
   AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
   AND (ss_store_sk = s_store_sk)
   AND (time_dim.t_hour = 20)
   AND (time_dim.t_minute >= 30)
   AND (household_demographics.hd_dep_count = 7)
   AND (store.s_store_name = 'ese')
ORDER BY count(*) ASC
LIMIT 100;
"""
    order_qt_query96_1_before "${query96_1}"
    async_mv_rewrite_success(db, mv96_1, query96_1, "mv96_1")
    order_qt_query96_1_after "${query96_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv96_1"""

    def mv97 = """
WITH
  ssci AS (
   SELECT
     ss_customer_sk customer_sk
   , ss_item_sk item_sk
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   GROUP BY ss_customer_sk, ss_item_sk
)
, csci AS (
   SELECT
     cs_bill_customer_sk customer_sk
   , cs_item_sk item_sk
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   GROUP BY cs_bill_customer_sk, cs_item_sk
)
SELECT
  sum((CASE WHEN (ssci.customer_sk IS NOT NULL)
   AND (csci.customer_sk IS NULL) THEN 1 ELSE 0 END)) store_only
, sum((CASE WHEN (ssci.customer_sk IS NULL)
   AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END)) catalog_only
, sum((CASE WHEN (ssci.customer_sk IS NOT NULL)
   AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END)) store_and_catalog
FROM
  ssci
FULL JOIN csci ON (ssci.customer_sk = csci.customer_sk)
   AND (ssci.item_sk = csci.item_sk)
LIMIT 100;
"""
    def query97 = """
WITH
  ssci AS (
   SELECT
     ss_customer_sk customer_sk
   , ss_item_sk item_sk
   FROM
     store_sales
   , date_dim
   WHERE (ss_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   GROUP BY ss_customer_sk, ss_item_sk
)
, csci AS (
   SELECT
     cs_bill_customer_sk customer_sk
   , cs_item_sk item_sk
   FROM
     catalog_sales
   , date_dim
   WHERE (cs_sold_date_sk = d_date_sk)
      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
   GROUP BY cs_bill_customer_sk, cs_item_sk
)
SELECT
  sum((CASE WHEN (ssci.customer_sk IS NOT NULL)
   AND (csci.customer_sk IS NULL) THEN 1 ELSE 0 END)) store_only
, sum((CASE WHEN (ssci.customer_sk IS NULL)
   AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END)) catalog_only
, sum((CASE WHEN (ssci.customer_sk IS NOT NULL)
   AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END)) store_and_catalog
FROM
  ssci
FULL JOIN csci ON (ssci.customer_sk = csci.customer_sk)
   AND (ssci.item_sk = csci.item_sk)
LIMIT 100;
"""
    order_qt_query97_before "${query97}"
    async_mv_rewrite_fail(db, mv97, query97, "mv97")
    order_qt_query97_after "${query97}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv97"""


    def mv98 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(ss_ext_sales_price) itemrevenue
, ((sum(ss_ext_sales_price) * 100) / sum(sum(ss_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  store_sales
, item
, date_dim
WHERE (ss_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (ss_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC;
"""
    def query98 = """
SELECT
  i_item_id
, i_item_desc
, i_category
, i_class
, i_current_price
, sum(ss_ext_sales_price) itemrevenue
, ((sum(ss_ext_sales_price) * 100) / sum(sum(ss_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
FROM
  store_sales
, item
, date_dim
WHERE (ss_item_sk = i_item_sk)
   AND (i_category IN ('Sports', 'Books', 'Home'))
   AND (ss_sold_date_sk = d_date_sk)
   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC;
"""
    order_qt_query98_before "${query98}"
    async_mv_rewrite_fail(db, mv98, query98, "mv98")
    order_qt_query98_after "${query98}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv98"""


    def mv99 = """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, cc_name
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 30)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 60)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 90)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '120 days'
FROM
  catalog_sales
, warehouse
, ship_mode
, call_center
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (cs_ship_date_sk = d_date_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_ship_mode_sk = sm_ship_mode_sk)
   AND (cs_call_center_sk = cc_call_center_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, cc_name
ORDER BY substr(w_warehouse_name, 1, 20) ASC, sm_type ASC, cc_name ASC
LIMIT 100;
"""
    def query99 = """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, cc_name
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 30)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 60)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 90)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
FROM
  catalog_sales
, warehouse
, ship_mode
, call_center
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (cs_ship_date_sk = d_date_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_ship_mode_sk = sm_ship_mode_sk)
   AND (cs_call_center_sk = cc_call_center_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, cc_name
ORDER BY substr(w_warehouse_name, 1, 20) ASC, sm_type ASC, cc_name ASC
LIMIT 100;
"""
    order_qt_query99_before "${query99}"
    async_mv_rewrite_fail(db, mv99, query99, "mv99")
    order_qt_query99_after "${query99}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv99"""

    def mv99_1 = """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, cc_name
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 30)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 60)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 90)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '120 days'
FROM
  catalog_sales
, warehouse
, ship_mode
, call_center
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (cs_ship_date_sk = d_date_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_ship_mode_sk = sm_ship_mode_sk)
   AND (cs_call_center_sk = cc_call_center_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, cc_name;
"""
    def query99_1 = """
SELECT
  substr(w_warehouse_name, 1, 20)
, sm_type
, cc_name
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 30)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 60)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 90)
   AND ((cs_ship_date_sk - cs_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
, sum((CASE WHEN ((cs_ship_date_sk - cs_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
FROM
  catalog_sales
, warehouse
, ship_mode
, call_center
, date_dim
WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
   AND (cs_ship_date_sk = d_date_sk)
   AND (cs_warehouse_sk = w_warehouse_sk)
   AND (cs_ship_mode_sk = sm_ship_mode_sk)
   AND (cs_call_center_sk = cc_call_center_sk)
GROUP BY substr(w_warehouse_name, 1, 20), sm_type, cc_name
ORDER BY substr(w_warehouse_name, 1, 20) ASC, sm_type ASC, cc_name ASC
LIMIT 100;
"""
    order_qt_query99_1_before "${query99_1}"
    async_mv_rewrite_success(db, mv99_1, query99_1, "mv99_1")
    order_qt_query99_1_after "${query99_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv99_1"""
}