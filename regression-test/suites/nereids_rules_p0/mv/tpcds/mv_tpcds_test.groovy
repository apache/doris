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

//    for (String table in tables) {
//        sql """ DROP TABLE IF EXISTS $table """
//    }
//
//    for (String table in tables) {
//        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
//    }
//
//    sql "set exec_mem_limit=8G;"
//
//    for (String tableName in tables) {
//        streamLoad {
//            // you can skip db declaration, because a default db has already been
//            // specified in ${DORIS_HOME}/conf/regression-conf.groovy
//            // db 'regression_test'
//            table tableName
//
//            // default label is UUID:
//            // set 'label' UUID.randomUUID().toString()
//
//            // default column_separator is specify in doris fe config, usually is '\t'.
//            // this line change to ','
//            set 'column_separator', '|'
//            set 'compress_type', 'GZ'
//
//            if (specialTables.contains(tableName)) {
//                set "columns", columnsMap[tableName]
//            }
//
//
//            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
//            // also, you can stream load a http stream, e.g. http://xxx/some.csv
//            file """${getS3Url()}/regression/tpcds/sf1/${tableName}.dat.gz"""
//
//            time 10000 // limit inflight 10s
//
//            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
//
//            // if declared a check callback, the default check condition will ignore.
//            // So you must check all condition
//            check { result, exception, startTime, endTime ->
//                if (exception != null) {
//                    throw exception
//                }
//                log.info("Stream load result: ${result}".toString())
//                def json = parseJson(result)
//                assertEquals("success", json.Status.toLowerCase())
//                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
//                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
//            }
//        }
//    }
//
//    Thread.sleep(70000) // wait for row count report of the tables just loaded
//    for (String tableName in tables) {
//        sql """ ANALYZE TABLE $tableName WITH SYNC """
//    }
//    sql """ sync """


    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_nereids_timeout=false"

//    def mv1 = """
//WITH
//  customer_total_return AS (
//   SELECT
//     sr_customer_sk ctr_customer_sk
//   , sr_store_sk ctr_store_sk
//   , sum(sr_return_amt) ctr_total_return
//   FROM
//     store_returns
//   , date_dim
//   WHERE (sr_returned_date_sk = d_date_sk)
//      AND (d_year = 2000)
//   GROUP BY sr_customer_sk, sr_store_sk
//)
//SELECT c_customer_id
//FROM
//  customer_total_return ctr1
//, store
//, customer
//WHERE (ctr1.ctr_total_return > (
//      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
//      FROM
//        customer_total_return ctr2
//      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
//   ))
//   AND (s_store_sk = ctr1.ctr_store_sk)
//   AND (s_state = 'TN')
//   AND (ctr1.ctr_customer_sk = c_customer_sk)
//ORDER BY c_customer_id ASC
//LIMIT 100;
//"""
//    def query1 = """
//WITH
//  customer_total_return AS (
//   SELECT
//     sr_customer_sk ctr_customer_sk
//   , sr_store_sk ctr_store_sk
//   , sum(sr_return_amt) ctr_total_return
//   FROM
//     store_returns
//   , date_dim
//   WHERE (sr_returned_date_sk = d_date_sk)
//      AND (d_year = 2000)
//   GROUP BY sr_customer_sk, sr_store_sk
//)
//SELECT c_customer_id
//FROM
//  customer_total_return ctr1
//, store
//, customer
//WHERE (ctr1.ctr_total_return > (
//      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
//      FROM
//        customer_total_return ctr2
//      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
//   ))
//   AND (s_store_sk = ctr1.ctr_store_sk)
//   AND (s_state = 'TN')
//   AND (ctr1.ctr_customer_sk = c_customer_sk)
//ORDER BY c_customer_id ASC
//LIMIT 100
//"""
//    order_qt_query1_before "${query1}"
//    async_mv_rewrite_fail(db, mv1, query1, "mv1")
//    order_qt_query1_after "${query1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1"""
//
//    def mv1_1 = """
//WITH
//  customer_total_return AS (
//   SELECT
//     sr_customer_sk ctr_customer_sk
//   , sr_store_sk ctr_store_sk
//   , sum(sr_return_amt) ctr_total_return
//   FROM
//     store_returns
//   , date_dim
//   WHERE (sr_returned_date_sk = d_date_sk)
//      AND (d_year = 2000)
//   GROUP BY sr_customer_sk, sr_store_sk
//)
//SELECT c_customer_id
//FROM
//  customer_total_return ctr1
//, store
//, customer
//WHERE (ctr1.ctr_total_return > (
//      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
//      FROM
//        customer_total_return ctr2
//      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
//   ))
//   AND (s_store_sk = ctr1.ctr_store_sk)
//   AND (s_state = 'TN')
//   AND (ctr1.ctr_customer_sk = c_customer_sk);
//"""
//    def query1_1 = """
//WITH
//  customer_total_return AS (
//   SELECT
//     sr_customer_sk ctr_customer_sk
//   , sr_store_sk ctr_store_sk
//   , sum(sr_return_amt) ctr_total_return
//   FROM
//     store_returns
//   , date_dim
//   WHERE (sr_returned_date_sk = d_date_sk)
//      AND (d_year = 2000)
//   GROUP BY sr_customer_sk, sr_store_sk
//)
//SELECT c_customer_id
//FROM
//  customer_total_return ctr1
//, store
//, customer
//WHERE (ctr1.ctr_total_return > (
//      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL(2,1)))
//      FROM
//        customer_total_return ctr2
//      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
//   ))
//   AND (s_store_sk = ctr1.ctr_store_sk)
//   AND (s_state = 'TN')
//   AND (ctr1.ctr_customer_sk = c_customer_sk)
//ORDER BY c_customer_id ASC
//LIMIT 100;
//"""
//    order_qt_query1_1_before "${query1_1}"
//    async_mv_rewrite_fail(db, mv1_1, query1_1, "mv1_1")
//    order_qt_query1_1_after "${query1_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""
//
//
//    def mv2 = """
//WITH
//  wscs AS (
//   SELECT
//     sold_date_sk
//   , sales_price
//   FROM
//     (
//      SELECT
//        ws_sold_date_sk sold_date_sk
//      , ws_ext_sales_price sales_price
//      FROM
//        web_sales
//   ) x
//UNION ALL (
//      SELECT
//        cs_sold_date_sk sold_date_sk
//      , cs_ext_sales_price sales_price
//      FROM
//        catalog_sales
//   ) )
//, wswscs AS (
//   SELECT
//     d_week_seq
//   , sum((CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE null END)) sun_sales
//   , sum((CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE null END)) mon_sales
//   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE null END)) tue_sales
//   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE null END)) wed_sales
//   , sum((CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE null END)) thu_sales
//   , sum((CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE null END)) fri_sales
//   , sum((CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE null END)) sat_sales
//   FROM
//     wscs
//   , date_dim
//   WHERE (d_date_sk = sold_date_sk)
//   GROUP BY d_week_seq
//)
//SELECT
//  d_week_seq1
//, round((sun_sales1 / sun_sales2), 2)
//, round((mon_sales1 / mon_sales2), 2)
//, round((tue_sales1 / tue_sales2), 2)
//, round((wed_sales1 / wed_sales2), 2)
//, round((thu_sales1 / thu_sales2), 2)
//, round((fri_sales1 / fri_sales2), 2)
//, round((sat_sales1 / sat_sales2), 2)
//FROM
//  (
//   SELECT
//     wswscs.d_week_seq d_week_seq1
//   , sun_sales sun_sales1
//   , mon_sales mon_sales1
//   , tue_sales tue_sales1
//   , wed_sales wed_sales1
//   , thu_sales thu_sales1
//   , fri_sales fri_sales1
//   , sat_sales sat_sales1
//   FROM
//     wswscs
//   , date_dim
//   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
//      AND (d_year = 2001)
//)  y
//, (
//   SELECT
//     wswscs.d_week_seq d_week_seq2
//   , sun_sales sun_sales2
//   , mon_sales mon_sales2
//   , tue_sales tue_sales2
//   , wed_sales wed_sales2
//   , thu_sales thu_sales2
//   , fri_sales fri_sales2
//   , sat_sales sat_sales2
//   FROM
//     wswscs
//   , date_dim
//   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
//      AND (d_year = (2001 + 1))
//)  z
//WHERE (d_week_seq1 = (d_week_seq2 - 53))
//ORDER BY d_week_seq1 ASC;
//"""
//    def query2 = """
//WITH
//  wscs AS (
//   SELECT
//     sold_date_sk
//   , sales_price
//   FROM
//     (
//      SELECT
//        ws_sold_date_sk sold_date_sk
//      , ws_ext_sales_price sales_price
//      FROM
//        web_sales
//   ) x
//UNION ALL (
//      SELECT
//        cs_sold_date_sk sold_date_sk
//      , cs_ext_sales_price sales_price
//      FROM
//        catalog_sales
//   ) )
//, wswscs AS (
//   SELECT
//     d_week_seq
//   , sum((CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE null END)) sun_sales
//   , sum((CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE null END)) mon_sales
//   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE null END)) tue_sales
//   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE null END)) wed_sales
//   , sum((CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE null END)) thu_sales
//   , sum((CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE null END)) fri_sales
//   , sum((CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE null END)) sat_sales
//   FROM
//     wscs
//   , date_dim
//   WHERE (d_date_sk = sold_date_sk)
//   GROUP BY d_week_seq
//)
//SELECT
//  d_week_seq1
//, round((sun_sales1 / sun_sales2), 2)
//, round((mon_sales1 / mon_sales2), 2)
//, round((tue_sales1 / tue_sales2), 2)
//, round((wed_sales1 / wed_sales2), 2)
//, round((thu_sales1 / thu_sales2), 2)
//, round((fri_sales1 / fri_sales2), 2)
//, round((sat_sales1 / sat_sales2), 2)
//FROM
//  (
//   SELECT
//     wswscs.d_week_seq d_week_seq1
//   , sun_sales sun_sales1
//   , mon_sales mon_sales1
//   , tue_sales tue_sales1
//   , wed_sales wed_sales1
//   , thu_sales thu_sales1
//   , fri_sales fri_sales1
//   , sat_sales sat_sales1
//   FROM
//     wswscs
//   , date_dim
//   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
//      AND (d_year = 2001)
//)  y
//, (
//   SELECT
//     wswscs.d_week_seq d_week_seq2
//   , sun_sales sun_sales2
//   , mon_sales mon_sales2
//   , tue_sales tue_sales2
//   , wed_sales wed_sales2
//   , thu_sales thu_sales2
//   , fri_sales fri_sales2
//   , sat_sales sat_sales2
//   FROM
//     wswscs
//   , date_dim
//   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
//      AND (d_year = (2001 + 1))
//)  z
//WHERE (d_week_seq1 = (d_week_seq2 - 53))
//ORDER BY d_week_seq1 ASC;
//"""
//    order_qt_query2_before "${query2}"
//    async_mv_rewrite_fail(db, mv2, query2, "mv2")
//    order_qt_query2_after "${query2}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2"""
//
//
//    def mv3 = """
//SELECT
//  dt.d_year
//, item.i_brand_id brand_id
//, item.i_brand brand
//, sum(ss_ext_sales_price) sum_agg
//FROM
//  date_dim dt
//, store_sales
//, item
//WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
//   AND (store_sales.ss_item_sk = item.i_item_sk)
//   AND (item.i_manufact_id = 128)
//   AND (dt.d_moy = 11)
//GROUP BY dt.d_year, item.i_brand, item.i_brand_id
//ORDER BY dt.d_year ASC, sum_agg DESC, brand_id ASC
//LIMIT 100;
//"""
//    def query3 = """
//SELECT
//  dt.d_year
//, item.i_brand_id brand_id
//, item.i_brand brand
//, sum(ss_ext_sales_price) sum_agg
//FROM
//  date_dim dt
//, store_sales
//, item
//WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
//   AND (store_sales.ss_item_sk = item.i_item_sk)
//   AND (item.i_manufact_id = 128)
//   AND (dt.d_moy = 11)
//GROUP BY dt.d_year, item.i_brand, item.i_brand_id
//ORDER BY dt.d_year ASC, sum_agg DESC, brand_id ASC
//LIMIT 100;
//"""
//    order_qt_query3_before "${query3}"
//    async_mv_rewrite_fail(db, mv3, query3, "mv3")
//    order_qt_query3_after "${query3}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3"""
//
//
//    def mv3_1 = """
//SELECT
//  dt.d_year
//, item.i_brand_id brand_id
//, item.i_brand brand
//, sum(ss_ext_sales_price) sum_agg
//FROM
//  date_dim dt
//, store_sales
//, item
//WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
//   AND (store_sales.ss_item_sk = item.i_item_sk)
//   AND (item.i_manufact_id = 128)
//   AND (dt.d_moy = 11)
//GROUP BY dt.d_year, item.i_brand, item.i_brand_id;
//"""
//    def query3_1 = """
//SELECT
//  dt.d_year
//, item.i_brand_id brand_id
//, item.i_brand brand
//, sum(ss_ext_sales_price) sum_agg
//FROM
//  date_dim dt
//, store_sales
//, item
//WHERE (dt.d_date_sk = store_sales.ss_sold_date_sk)
//   AND (store_sales.ss_item_sk = item.i_item_sk)
//   AND (item.i_manufact_id = 128)
//   AND (dt.d_moy = 11)
//GROUP BY dt.d_year, item.i_brand, item.i_brand_id
//ORDER BY dt.d_year ASC, sum_agg DESC, brand_id ASC
//LIMIT 100;
//"""
//    order_qt_query3_1_before "${query3_1}"
//    async_mv_rewrite_success(db, mv3_1, query3_1, "mv3_1")
//    order_qt_query3_1_after "${query3_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv3_1"""
//
//
//    def mv4 = """
//WITH
//  year_total AS (
//   SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum(((((ss_ext_list_price - ss_ext_wholesale_cost) - ss_ext_discount_amt) + ss_ext_sales_price) / 2)) year_total
//   , 's' sale_type
//   FROM
//     customer
//   , store_sales
//   , date_dim
//   WHERE (c_customer_sk = ss_customer_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//UNION ALL    SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum(((((cs_ext_list_price - cs_ext_wholesale_cost) - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) year_total
//   , 'c' sale_type
//   FROM
//     customer
//   , catalog_sales
//   , date_dim
//   WHERE (c_customer_sk = cs_bill_customer_sk)
//      AND (cs_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//UNION ALL    SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum(((((ws_ext_list_price - ws_ext_wholesale_cost) - ws_ext_discount_amt) + ws_ext_sales_price) / 2)) year_total
//   , 'w' sale_type
//   FROM
//     customer
//   , web_sales
//   , date_dim
//   WHERE (c_customer_sk = ws_bill_customer_sk)
//      AND (ws_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//)
//SELECT
//  t_s_secyear.customer_id
//, t_s_secyear.customer_first_name
//, t_s_secyear.customer_last_name
//, t_s_secyear.customer_preferred_cust_flag
//FROM
//  year_total t_s_firstyear
//, year_total t_s_secyear
//, year_total t_c_firstyear
//, year_total t_c_secyear
//, year_total t_w_firstyear
//, year_total t_w_secyear
//WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_c_secyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_c_firstyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
//   AND (t_s_firstyear.sale_type = 's')
//   AND (t_c_firstyear.sale_type = 'c')
//   AND (t_w_firstyear.sale_type = 'w')
//   AND (t_s_secyear.sale_type = 's')
//   AND (t_c_secyear.sale_type = 'c')
//   AND (t_w_secyear.sale_type = 'w')
//   AND (t_s_firstyear.dyear = 2001)
//   AND (t_s_secyear.dyear = (2001 + 1))
//   AND (t_c_firstyear.dyear = 2001)
//   AND (t_c_secyear.dyear = (2001 + 1))
//   AND (t_w_firstyear.dyear = 2001)
//   AND (t_w_secyear.dyear = (2001 + 1))
//   AND (t_s_firstyear.year_total > 0)
//   AND (t_c_firstyear.year_total > 0)
//   AND (t_w_firstyear.year_total > 0)
//   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE null END))
//   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE null END))
//ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
//LIMIT 100;
//"""
//    def query4 = """
//WITH
//  year_total AS (
//   SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum(((((ss_ext_list_price - ss_ext_wholesale_cost) - ss_ext_discount_amt) + ss_ext_sales_price) / 2)) year_total
//   , 's' sale_type
//   FROM
//     customer
//   , store_sales
//   , date_dim
//   WHERE (c_customer_sk = ss_customer_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//UNION ALL    SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum(((((cs_ext_list_price - cs_ext_wholesale_cost) - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) year_total
//   , 'c' sale_type
//   FROM
//     customer
//   , catalog_sales
//   , date_dim
//   WHERE (c_customer_sk = cs_bill_customer_sk)
//      AND (cs_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//UNION ALL    SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum(((((ws_ext_list_price - ws_ext_wholesale_cost) - ws_ext_discount_amt) + ws_ext_sales_price) / 2)) year_total
//   , 'w' sale_type
//   FROM
//     customer
//   , web_sales
//   , date_dim
//   WHERE (c_customer_sk = ws_bill_customer_sk)
//      AND (ws_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//)
//SELECT
//  t_s_secyear.customer_id
//, t_s_secyear.customer_first_name
//, t_s_secyear.customer_last_name
//, t_s_secyear.customer_preferred_cust_flag
//FROM
//  year_total t_s_firstyear
//, year_total t_s_secyear
//, year_total t_c_firstyear
//, year_total t_c_secyear
//, year_total t_w_firstyear
//, year_total t_w_secyear
//WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_c_secyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_c_firstyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
//   AND (t_s_firstyear.sale_type = 's')
//   AND (t_c_firstyear.sale_type = 'c')
//   AND (t_w_firstyear.sale_type = 'w')
//   AND (t_s_secyear.sale_type = 's')
//   AND (t_c_secyear.sale_type = 'c')
//   AND (t_w_secyear.sale_type = 'w')
//   AND (t_s_firstyear.dyear = 2001)
//   AND (t_s_secyear.dyear = (2001 + 1))
//   AND (t_c_firstyear.dyear = 2001)
//   AND (t_c_secyear.dyear = (2001 + 1))
//   AND (t_w_firstyear.dyear = 2001)
//   AND (t_w_secyear.dyear = (2001 + 1))
//   AND (t_s_firstyear.year_total > 0)
//   AND (t_c_firstyear.year_total > 0)
//   AND (t_w_firstyear.year_total > 0)
//   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE null END))
//   AND ((CASE WHEN (t_c_firstyear.year_total > 0) THEN (t_c_secyear.year_total / t_c_firstyear.year_total) ELSE null END) > (CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE null END))
//ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
//LIMIT 100;
//"""
//    order_qt_query4_before "${query4}"
//    async_mv_rewrite_fail(db, mv4, query4, "mv4")
//    order_qt_query4_after "${query4}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv4"""
//
//    def mv5 = """
//WITH
//  ssr AS (
//   SELECT
//     s_store_id
//   , sum(sales_price) sales
//   , sum(profit) profit
//   , sum(return_amt) returns
//   , sum(net_loss) profit_loss
//   FROM
//     (
//      SELECT
//        ss_store_sk store_sk
//      , ss_sold_date_sk date_sk
//      , ss_ext_sales_price sales_price
//      , ss_net_profit profit
//      , CAST(0 AS DECIMAL(7,2)) return_amt
//      , CAST(0 AS DECIMAL(7,2)) net_loss
//      FROM
//        store_sales
//UNION ALL       SELECT
//        sr_store_sk store_sk
//      , sr_returned_date_sk date_sk
//      , CAST(0 AS DECIMAL(7,2)) sales_price
//      , CAST(0 AS DECIMAL(7,2)) profit
//      , sr_return_amt return_amt
//      , sr_net_loss net_loss
//      FROM
//        store_returns
//   )  salesreturns
//   , date_dim
//   , store
//   WHERE (date_sk = d_date_sk)
//      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
//      AND (store_sk = s_store_sk)
//   GROUP BY s_store_id
//)
//, csr AS (
//   SELECT
//     cp_catalog_page_id
//   , sum(sales_price) sales
//   , sum(profit) profit
//   , sum(return_amt) returns
//   , sum(net_loss) profit_loss
//   FROM
//     (
//      SELECT
//        cs_catalog_page_sk page_sk
//      , cs_sold_date_sk date_sk
//      , cs_ext_sales_price sales_price
//      , cs_net_profit profit
//      , CAST(0 AS DECIMAL(7,2)) return_amt
//      , CAST(0 AS DECIMAL(7,2)) net_loss
//      FROM
//        catalog_sales
//UNION ALL       SELECT
//        cr_catalog_page_sk page_sk
//      , cr_returned_date_sk date_sk
//      , CAST(0 AS DECIMAL(7,2)) sales_price
//      , CAST(0 AS DECIMAL(7,2)) profit
//      , cr_return_amount return_amt
//      , cr_net_loss net_loss
//      FROM
//        catalog_returns
//   )  salesreturns
//   , date_dim
//   , catalog_page
//   WHERE (date_sk = d_date_sk)
//      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
//      AND (page_sk = cp_catalog_page_sk)
//   GROUP BY cp_catalog_page_id
//)
//, wsr AS (
//   SELECT
//     web_site_id
//   , sum(sales_price) sales
//   , sum(profit) profit
//   , sum(return_amt) returns
//   , sum(net_loss) profit_loss
//   FROM
//     (
//      SELECT
//        ws_web_site_sk wsr_web_site_sk
//      , ws_sold_date_sk date_sk
//      , ws_ext_sales_price sales_price
//      , ws_net_profit profit
//      , CAST(0 AS DECIMAL(7,2)) return_amt
//      , CAST(0 AS DECIMAL(7,2)) net_loss
//      FROM
//        web_sales
//UNION ALL       SELECT
//        ws_web_site_sk wsr_web_site_sk
//      , wr_returned_date_sk date_sk
//      , CAST(0 AS DECIMAL(7,2)) sales_price
//      , CAST(0 AS DECIMAL(7,2)) profit
//      , wr_return_amt return_amt
//      , wr_net_loss net_loss
//      FROM
//        web_returns
//      LEFT JOIN web_sales ON (wr_item_sk = ws_item_sk)
//         AND (wr_order_number = ws_order_number)
//   )  salesreturns
//   , date_dim
//   , web_site
//   WHERE (date_sk = d_date_sk)
//      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
//      AND (wsr_web_site_sk = web_site_sk)
//   GROUP BY web_site_id
//)
//SELECT
//  channel
//, id
//, sum(sales) sales
//, sum(returns) returns
//, sum(profit) profit
//FROM
//  (
//   SELECT
//     'store channel' channel
//   , concat('store', s_store_id) id
//   , sales
//   , returns
//   , (profit - profit_loss) profit
//   FROM
//     ssr
//UNION ALL    SELECT
//     'catalog channel' channel
//   , concat('catalog_page', cp_catalog_page_id) id
//   , sales
//   , returns
//   , (profit - profit_loss) profit
//   FROM
//     csr
//UNION ALL    SELECT
//     'web channel' channel
//   , concat('web_site', web_site_id) id
//   , sales
//   , returns
//   , (profit - profit_loss) profit
//   FROM
//     wsr
//)  x
//GROUP BY ROLLUP (channel, id)
//ORDER BY channel ASC, id ASC
//LIMIT 100;
//"""
//    def query5 = """
//WITH
//  ssr AS (
//   SELECT
//     s_store_id
//   , sum(sales_price) sales
//   , sum(profit) profit
//   , sum(return_amt) returns
//   , sum(net_loss) profit_loss
//   FROM
//     (
//      SELECT
//        ss_store_sk store_sk
//      , ss_sold_date_sk date_sk
//      , ss_ext_sales_price sales_price
//      , ss_net_profit profit
//      , CAST(0 AS DECIMAL(7,2)) return_amt
//      , CAST(0 AS DECIMAL(7,2)) net_loss
//      FROM
//        store_sales
//UNION ALL       SELECT
//        sr_store_sk store_sk
//      , sr_returned_date_sk date_sk
//      , CAST(0 AS DECIMAL(7,2)) sales_price
//      , CAST(0 AS DECIMAL(7,2)) profit
//      , sr_return_amt return_amt
//      , sr_net_loss net_loss
//      FROM
//        store_returns
//   )  salesreturns
//   , date_dim
//   , store
//   WHERE (date_sk = d_date_sk)
//      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
//      AND (store_sk = s_store_sk)
//   GROUP BY s_store_id
//)
//, csr AS (
//   SELECT
//     cp_catalog_page_id
//   , sum(sales_price) sales
//   , sum(profit) profit
//   , sum(return_amt) returns
//   , sum(net_loss) profit_loss
//   FROM
//     (
//      SELECT
//        cs_catalog_page_sk page_sk
//      , cs_sold_date_sk date_sk
//      , cs_ext_sales_price sales_price
//      , cs_net_profit profit
//      , CAST(0 AS DECIMAL(7,2)) return_amt
//      , CAST(0 AS DECIMAL(7,2)) net_loss
//      FROM
//        catalog_sales
//UNION ALL       SELECT
//        cr_catalog_page_sk page_sk
//      , cr_returned_date_sk date_sk
//      , CAST(0 AS DECIMAL(7,2)) sales_price
//      , CAST(0 AS DECIMAL(7,2)) profit
//      , cr_return_amount return_amt
//      , cr_net_loss net_loss
//      FROM
//        catalog_returns
//   )  salesreturns
//   , date_dim
//   , catalog_page
//   WHERE (date_sk = d_date_sk)
//      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
//      AND (page_sk = cp_catalog_page_sk)
//   GROUP BY cp_catalog_page_id
//)
//, wsr AS (
//   SELECT
//     web_site_id
//   , sum(sales_price) sales
//   , sum(profit) profit
//   , sum(return_amt) returns
//   , sum(net_loss) profit_loss
//   FROM
//     (
//      SELECT
//        ws_web_site_sk wsr_web_site_sk
//      , ws_sold_date_sk date_sk
//      , ws_ext_sales_price sales_price
//      , ws_net_profit profit
//      , CAST(0 AS DECIMAL(7,2)) return_amt
//      , CAST(0 AS DECIMAL(7,2)) net_loss
//      FROM
//        web_sales
//UNION ALL       SELECT
//        ws_web_site_sk wsr_web_site_sk
//      , wr_returned_date_sk date_sk
//      , CAST(0 AS DECIMAL(7,2)) sales_price
//      , CAST(0 AS DECIMAL(7,2)) profit
//      , wr_return_amt return_amt
//      , wr_net_loss net_loss
//      FROM
//        web_returns
//      LEFT JOIN web_sales ON (wr_item_sk = ws_item_sk)
//         AND (wr_order_number = ws_order_number)
//   )  salesreturns
//   , date_dim
//   , web_site
//   WHERE (date_sk = d_date_sk)
//      AND (d_date BETWEEN CAST('2000-08-23' AS DATE) AND (CAST('2000-08-23' AS DATE) + INTERVAL  '14' DAY))
//      AND (wsr_web_site_sk = web_site_sk)
//   GROUP BY web_site_id
//)
//SELECT
//  channel
//, id
//, sum(sales) sales
//, sum(returns) returns
//, sum(profit) profit
//FROM
//  (
//   SELECT
//     'store channel' channel
//   , concat('store', s_store_id) id
//   , sales
//   , returns
//   , (profit - profit_loss) profit
//   FROM
//     ssr
//UNION ALL    SELECT
//     'catalog channel' channel
//   , concat('catalog_page', cp_catalog_page_id) id
//   , sales
//   , returns
//   , (profit - profit_loss) profit
//   FROM
//     csr
//UNION ALL    SELECT
//     'web channel' channel
//   , concat('web_site', web_site_id) id
//   , sales
//   , returns
//   , (profit - profit_loss) profit
//   FROM
//     wsr
//)  x
//GROUP BY ROLLUP (channel, id)
//ORDER BY channel ASC, id ASC
//LIMIT 100;
//"""
//    order_qt_query5_before "${query5}"
//    async_mv_rewrite_fail(db, mv5, query5, "mv5")
//    order_qt_query5_after "${query5}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv5"""
//
//    def mv6 = """
//--- takes over 30 minutes on travis to complete
//SELECT
//  a.ca_state STATE
//, count(*) cnt
//FROM
//  customer_address a
//, customer c
//, store_sales s
//, date_dim d
//, item i
//WHERE (a.ca_address_sk = c.c_current_addr_sk)
//   AND (c.c_customer_sk = s.ss_customer_sk)
//   AND (s.ss_sold_date_sk = d.d_date_sk)
//   AND (s.ss_item_sk = i.i_item_sk)
//   AND (d.d_month_seq = (
//      SELECT DISTINCT d_month_seq
//      FROM
//        date_dim
//      WHERE (d_year = 2001)
//         AND (d_moy = 1)
//   ))
//   AND (i.i_current_price > (CAST('1.2' AS DECIMAL(2,1)) * (
//         SELECT avg(j.i_current_price)
//         FROM
//           item j
//         WHERE (j.i_category = i.i_category)
//      )))
//GROUP BY a.ca_state
//HAVING (count(*) >= 10)
//ORDER BY cnt ASC, a.ca_state ASC
//LIMIT 100;
//"""
//    def query6 = """
//--- takes over 30 minutes on travis to complete
//SELECT
//  a.ca_state STATE
//, count(*) cnt
//FROM
//  customer_address a
//, customer c
//, store_sales s
//, date_dim d
//, item i
//WHERE (a.ca_address_sk = c.c_current_addr_sk)
//   AND (c.c_customer_sk = s.ss_customer_sk)
//   AND (s.ss_sold_date_sk = d.d_date_sk)
//   AND (s.ss_item_sk = i.i_item_sk)
//   AND (d.d_month_seq = (
//      SELECT DISTINCT d_month_seq
//      FROM
//        date_dim
//      WHERE (d_year = 2001)
//         AND (d_moy = 1)
//   ))
//   AND (i.i_current_price > (CAST('1.2' AS DECIMAL(2,1)) * (
//         SELECT avg(j.i_current_price)
//         FROM
//           item j
//         WHERE (j.i_category = i.i_category)
//      )))
//GROUP BY a.ca_state
//HAVING (count(*) >= 10)
//ORDER BY cnt ASC, a.ca_state ASC
//LIMIT 100;
//"""
//    order_qt_query6_before "${query6}"
//    async_mv_rewrite_fail(db, mv6, query6, "mv6")
//    order_qt_query6_after "${query6}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6"""
//
//
//    def mv6_1 = """
//--- takes over 30 minutes on travis to complete
//SELECT
//  a.ca_state STATE
//, count(*) cnt
//FROM
//  customer_address a
//, customer c
//, store_sales s
//, date_dim d
//, item i
//WHERE (a.ca_address_sk = c.c_current_addr_sk)
//   AND (c.c_customer_sk = s.ss_customer_sk)
//   AND (s.ss_sold_date_sk = d.d_date_sk)
//   AND (s.ss_item_sk = i.i_item_sk)
//   AND (d.d_month_seq = (
//      SELECT DISTINCT d_month_seq
//      FROM
//        date_dim
//      WHERE (d_year = 2001)
//         AND (d_moy = 1)
//   ))
//   AND (i.i_current_price > (CAST('1.2' AS DECIMAL(2,1)) * (
//         SELECT avg(j.i_current_price)
//         FROM
//           item j
//         WHERE (j.i_category = i.i_category)
//      )))
//GROUP BY a.ca_state
//HAVING (count(*) >= 10);
//"""
//    def query6_1 = """
//--- takes over 30 minutes on travis to complete
//SELECT
//  a.ca_state STATE
//, count(*) cnt
//FROM
//  customer_address a
//, customer c
//, store_sales s
//, date_dim d
//, item i
//WHERE (a.ca_address_sk = c.c_current_addr_sk)
//   AND (c.c_customer_sk = s.ss_customer_sk)
//   AND (s.ss_sold_date_sk = d.d_date_sk)
//   AND (s.ss_item_sk = i.i_item_sk)
//   AND (d.d_month_seq = (
//      SELECT DISTINCT d_month_seq
//      FROM
//        date_dim
//      WHERE (d_year = 2001)
//         AND (d_moy = 1)
//   ))
//   AND (i.i_current_price > (CAST('1.2' AS DECIMAL(2,1)) * (
//         SELECT avg(j.i_current_price)
//         FROM
//           item j
//         WHERE (j.i_category = i.i_category)
//      )))
//GROUP BY a.ca_state
//HAVING (count(*) >= 10)
//ORDER BY cnt ASC, a.ca_state ASC
//LIMIT 100;
//"""
//    order_qt_query6_1_before "${query6_1}"
//    async_mv_rewrite_fail(db, mv6_1, query6_1, "mv6_1")
//    order_qt_query6_1_after "${query6_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv6_1"""
//
//    def mv7 = """
//SELECT
//  i_item_id
//, avg(ss_quantity) agg1
//, avg(ss_list_price) agg2
//, avg(ss_coupon_amt) agg3
//, avg(ss_sales_price) agg4
//FROM
//  store_sales
//, customer_demographics
//, date_dim
//, item
//, promotion
//WHERE (ss_sold_date_sk = d_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (ss_cdemo_sk = cd_demo_sk)
//   AND (ss_promo_sk = p_promo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND ((p_channel_email = 'N')
//      OR (p_channel_event = 'N'))
//   AND (d_year = 2000)
//GROUP BY i_item_id
//ORDER BY i_item_id ASC
//LIMIT 100;
//"""
//    def query7 = """
//SELECT
//  i_item_id
//, avg(ss_quantity) agg1
//, avg(ss_list_price) agg2
//, avg(ss_coupon_amt) agg3
//, avg(ss_sales_price) agg4
//FROM
//  store_sales
//, customer_demographics
//, date_dim
//, item
//, promotion
//WHERE (ss_sold_date_sk = d_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (ss_cdemo_sk = cd_demo_sk)
//   AND (ss_promo_sk = p_promo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND ((p_channel_email = 'N')
//      OR (p_channel_event = 'N'))
//   AND (d_year = 2000)
//GROUP BY i_item_id
//ORDER BY i_item_id ASC
//LIMIT 100;
//"""
//    order_qt_query7_before "${query7}"
//    async_mv_rewrite_fail(db, mv7, query7, "mv7")
//    order_qt_query7_after "${query7}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7"""
//
//    def mv7_1 = """
//SELECT
//  i_item_id
//, avg(ss_quantity) agg1
//, avg(ss_list_price) agg2
//, avg(ss_coupon_amt) agg3
//, avg(ss_sales_price) agg4
//FROM
//  store_sales
//, customer_demographics
//, date_dim
//, item
//, promotion
//WHERE (ss_sold_date_sk = d_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (ss_cdemo_sk = cd_demo_sk)
//   AND (ss_promo_sk = p_promo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND ((p_channel_email = 'N')
//      OR (p_channel_event = 'N'))
//   AND (d_year = 2000)
//GROUP BY i_item_id;
//"""
//    def query7_1 = """
//SELECT
//  i_item_id
//, avg(ss_quantity) agg1
//, avg(ss_list_price) agg2
//, avg(ss_coupon_amt) agg3
//, avg(ss_sales_price) agg4
//FROM
//  store_sales
//, customer_demographics
//, date_dim
//, item
//, promotion
//WHERE (ss_sold_date_sk = d_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (ss_cdemo_sk = cd_demo_sk)
//   AND (ss_promo_sk = p_promo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND ((p_channel_email = 'N')
//      OR (p_channel_event = 'N'))
//   AND (d_year = 2000)
//GROUP BY i_item_id
//ORDER BY i_item_id ASC
//LIMIT 100;
//"""
//    order_qt_query7_1_before "${query7_1}"
//    async_mv_rewrite_success(db, mv7_1, query7_1, "mv7_1")
//    order_qt_query7_1_after "${query7_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv7_1"""
//
//    def mv8 = """
//SELECT
//  s_store_name
//, sum(ss_net_profit)
//FROM
//  store_sales
//, date_dim
//, store
//, (
//   SELECT ca_zip
//   FROM
//     (
//(
//         SELECT substr(ca_zip, 1, 5) ca_zip
//         FROM
//           customer_address
//         WHERE (substr(ca_zip, 1, 5) IN (
//                '24128'
//              , '57834'
//              , '13354'
//              , '15734'
//              , '78668'
//              , '76232'
//              , '62878'
//              , '45375'
//              , '63435'
//              , '22245'
//              , '65084'
//              , '49130'
//              , '40558'
//              , '25733'
//              , '15798'
//              , '87816'
//              , '81096'
//              , '56458'
//              , '35474'
//              , '27156'
//              , '83926'
//              , '18840'
//              , '28286'
//              , '24676'
//              , '37930'
//              , '77556'
//              , '27700'
//              , '45266'
//              , '94627'
//              , '62971'
//              , '20548'
//              , '23470'
//              , '47305'
//              , '53535'
//              , '21337'
//              , '26231'
//              , '50412'
//              , '69399'
//              , '17879'
//              , '51622'
//              , '43848'
//              , '21195'
//              , '83921'
//              , '15559'
//              , '67853'
//              , '15126'
//              , '16021'
//              , '26233'
//              , '53268'
//              , '10567'
//              , '91137'
//              , '76107'
//              , '11101'
//              , '59166'
//              , '38415'
//              , '61265'
//              , '71954'
//              , '15371'
//              , '11928'
//              , '15455'
//              , '98294'
//              , '68309'
//              , '69913'
//              , '59402'
//              , '58263'
//              , '25782'
//              , '18119'
//              , '35942'
//              , '33282'
//              , '42029'
//              , '17920'
//              , '98359'
//              , '15882'
//              , '45721'
//              , '60279'
//              , '18426'
//              , '64544'
//              , '25631'
//              , '43933'
//              , '37125'
//              , '98235'
//              , '10336'
//              , '24610'
//              , '68101'
//              , '56240'
//              , '40081'
//              , '86379'
//              , '44165'
//              , '33515'
//              , '88190'
//              , '84093'
//              , '27068'
//              , '99076'
//              , '36634'
//              , '50308'
//              , '28577'
//              , '39736'
//              , '33786'
//              , '71286'
//              , '26859'
//              , '55565'
//              , '98569'
//              , '70738'
//              , '19736'
//              , '64457'
//              , '17183'
//              , '28915'
//              , '26653'
//              , '58058'
//              , '89091'
//              , '54601'
//              , '24206'
//              , '14328'
//              , '55253'
//              , '82136'
//              , '67897'
//              , '56529'
//              , '72305'
//              , '67473'
//              , '62377'
//              , '22752'
//              , '57647'
//              , '62496'
//              , '41918'
//              , '36233'
//              , '86284'
//              , '54917'
//              , '22152'
//              , '19515'
//              , '63837'
//              , '18376'
//              , '42961'
//              , '10144'
//              , '36495'
//              , '58078'
//              , '38607'
//              , '91110'
//              , '64147'
//              , '19430'
//              , '17043'
//              , '45200'
//              , '63981'
//              , '48425'
//              , '22351'
//              , '30010'
//              , '21756'
//              , '14922'
//              , '14663'
//              , '77191'
//              , '60099'
//              , '29741'
//              , '36420'
//              , '21076'
//              , '91393'
//              , '28810'
//              , '96765'
//              , '23006'
//              , '18799'
//              , '49156'
//              , '98025'
//              , '23932'
//              , '67467'
//              , '30450'
//              , '50298'
//              , '29178'
//              , '89360'
//              , '32754'
//              , '63089'
//              , '87501'
//              , '87343'
//              , '29839'
//              , '30903'
//              , '81019'
//              , '18652'
//              , '73273'
//              , '25989'
//              , '20260'
//              , '68893'
//              , '53179'
//              , '30469'
//              , '28898'
//              , '31671'
//              , '24996'
//              , '18767'
//              , '64034'
//              , '91068'
//              , '51798'
//              , '51200'
//              , '63193'
//              , '39516'
//              , '72550'
//              , '72325'
//              , '51211'
//              , '23968'
//              , '86057'
//              , '10390'
//              , '85816'
//              , '45692'
//              , '65164'
//              , '21309'
//              , '18845'
//              , '68621'
//              , '92712'
//              , '68880'
//              , '90257'
//              , '47770'
//              , '13955'
//              , '70466'
//              , '21286'
//              , '67875'
//              , '82636'
//              , '36446'
//              , '79994'
//              , '72823'
//              , '40162'
//              , '41367'
//              , '41766'
//              , '22437'
//              , '58470'
//              , '11356'
//              , '76638'
//              , '68806'
//              , '25280'
//              , '67301'
//              , '73650'
//              , '86198'
//              , '16725'
//              , '38935'
//              , '13394'
//              , '61810'
//              , '81312'
//              , '15146'
//              , '71791'
//              , '31016'
//              , '72013'
//              , '37126'
//              , '22744'
//              , '73134'
//              , '70372'
//              , '30431'
//              , '39192'
//              , '35850'
//              , '56571'
//              , '67030'
//              , '22461'
//              , '88424'
//              , '88086'
//              , '14060'
//              , '40604'
//              , '19512'
//              , '72175'
//              , '51649'
//              , '19505'
//              , '24317'
//              , '13375'
//              , '81426'
//              , '18270'
//              , '72425'
//              , '45748'
//              , '55307'
//              , '53672'
//              , '52867'
//              , '56575'
//              , '39127'
//              , '30625'
//              , '10445'
//              , '39972'
//              , '74351'
//              , '26065'
//              , '83849'
//              , '42666'
//              , '96976'
//              , '68786'
//              , '77721'
//              , '68908'
//              , '66864'
//              , '63792'
//              , '51650'
//              , '31029'
//              , '26689'
//              , '66708'
//              , '11376'
//              , '20004'
//              , '31880'
//              , '96451'
//              , '41248'
//              , '94898'
//              , '18383'
//              , '60576'
//              , '38193'
//              , '48583'
//              , '13595'
//              , '76614'
//              , '24671'
//              , '46820'
//              , '82276'
//              , '10516'
//              , '11634'
//              , '45549'
//              , '88885'
//              , '18842'
//              , '90225'
//              , '18906'
//              , '13376'
//              , '84935'
//              , '78890'
//              , '58943'
//              , '15765'
//              , '50016'
//              , '69035'
//              , '49448'
//              , '39371'
//              , '41368'
//              , '33123'
//              , '83144'
//              , '14089'
//              , '94945'
//              , '73241'
//              , '19769'
//              , '47537'
//              , '38122'
//              , '28587'
//              , '76698'
//              , '22927'
//              , '56616'
//              , '34425'
//              , '96576'
//              , '78567'
//              , '97789'
//              , '94983'
//              , '79077'
//              , '57855'
//              , '97189'
//              , '46081'
//              , '48033'
//              , '19849'
//              , '28488'
//              , '28545'
//              , '72151'
//              , '69952'
//              , '43285'
//              , '26105'
//              , '76231'
//              , '15723'
//              , '25486'
//              , '39861'
//              , '83933'
//              , '75691'
//              , '46136'
//              , '61547'
//              , '66162'
//              , '25858'
//              , '22246'
//              , '51949'
//              , '27385'
//              , '77610'
//              , '34322'
//              , '51061'
//              , '68100'
//              , '61860'
//              , '13695'
//              , '44438'
//              , '90578'
//              , '96888'
//              , '58048'
//              , '99543'
//              , '73171'
//              , '56691'
//              , '64528'
//              , '56910'
//              , '83444'
//              , '30122'
//              , '68014'
//              , '14171'
//              , '16807'
//              , '83041'
//              , '34102'
//              , '51103'
//              , '79777'
//              , '17871'
//              , '12305'
//              , '22685'
//              , '94167'
//              , '28709'
//              , '35258'
//              , '57665'
//              , '71256'
//              , '57047'
//              , '11489'
//              , '31387'
//              , '68341'
//              , '78451'
//              , '14867'
//              , '25103'
//              , '35458'
//              , '25003'
//              , '54364'
//              , '73520'
//              , '32213'
//              , '35576'))
//      )       INTERSECT (
//         SELECT ca_zip
//         FROM
//           (
//            SELECT
//              substr(ca_zip, 1, 5) ca_zip
//            , count(*) cnt
//            FROM
//              customer_address
//            , customer
//            WHERE (ca_address_sk = c_current_addr_sk)
//               AND (c_preferred_cust_flag = 'Y')
//            GROUP BY ca_zip
//            HAVING (count(*) > 10)
//         )  a1
//      )    )  a2
//)  v1
//WHERE (ss_store_sk = s_store_sk)
//   AND (ss_sold_date_sk = d_date_sk)
//   AND (d_qoy = 2)
//   AND (d_year = 1998)
//   AND (substr(s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
//GROUP BY s_store_name
//ORDER BY s_store_name ASC
//LIMIT 100;
//"""
//    def query8 = """
//SELECT
//  s_store_name
//, sum(ss_net_profit)
//FROM
//  store_sales
//, date_dim
//, store
//, (
//   SELECT ca_zip
//   FROM
//     (
//(
//         SELECT substr(ca_zip, 1, 5) ca_zip
//         FROM
//           customer_address
//         WHERE (substr(ca_zip, 1, 5) IN (
//                '24128'
//              , '57834'
//              , '13354'
//              , '15734'
//              , '78668'
//              , '76232'
//              , '62878'
//              , '45375'
//              , '63435'
//              , '22245'
//              , '65084'
//              , '49130'
//              , '40558'
//              , '25733'
//              , '15798'
//              , '87816'
//              , '81096'
//              , '56458'
//              , '35474'
//              , '27156'
//              , '83926'
//              , '18840'
//              , '28286'
//              , '24676'
//              , '37930'
//              , '77556'
//              , '27700'
//              , '45266'
//              , '94627'
//              , '62971'
//              , '20548'
//              , '23470'
//              , '47305'
//              , '53535'
//              , '21337'
//              , '26231'
//              , '50412'
//              , '69399'
//              , '17879'
//              , '51622'
//              , '43848'
//              , '21195'
//              , '83921'
//              , '15559'
//              , '67853'
//              , '15126'
//              , '16021'
//              , '26233'
//              , '53268'
//              , '10567'
//              , '91137'
//              , '76107'
//              , '11101'
//              , '59166'
//              , '38415'
//              , '61265'
//              , '71954'
//              , '15371'
//              , '11928'
//              , '15455'
//              , '98294'
//              , '68309'
//              , '69913'
//              , '59402'
//              , '58263'
//              , '25782'
//              , '18119'
//              , '35942'
//              , '33282'
//              , '42029'
//              , '17920'
//              , '98359'
//              , '15882'
//              , '45721'
//              , '60279'
//              , '18426'
//              , '64544'
//              , '25631'
//              , '43933'
//              , '37125'
//              , '98235'
//              , '10336'
//              , '24610'
//              , '68101'
//              , '56240'
//              , '40081'
//              , '86379'
//              , '44165'
//              , '33515'
//              , '88190'
//              , '84093'
//              , '27068'
//              , '99076'
//              , '36634'
//              , '50308'
//              , '28577'
//              , '39736'
//              , '33786'
//              , '71286'
//              , '26859'
//              , '55565'
//              , '98569'
//              , '70738'
//              , '19736'
//              , '64457'
//              , '17183'
//              , '28915'
//              , '26653'
//              , '58058'
//              , '89091'
//              , '54601'
//              , '24206'
//              , '14328'
//              , '55253'
//              , '82136'
//              , '67897'
//              , '56529'
//              , '72305'
//              , '67473'
//              , '62377'
//              , '22752'
//              , '57647'
//              , '62496'
//              , '41918'
//              , '36233'
//              , '86284'
//              , '54917'
//              , '22152'
//              , '19515'
//              , '63837'
//              , '18376'
//              , '42961'
//              , '10144'
//              , '36495'
//              , '58078'
//              , '38607'
//              , '91110'
//              , '64147'
//              , '19430'
//              , '17043'
//              , '45200'
//              , '63981'
//              , '48425'
//              , '22351'
//              , '30010'
//              , '21756'
//              , '14922'
//              , '14663'
//              , '77191'
//              , '60099'
//              , '29741'
//              , '36420'
//              , '21076'
//              , '91393'
//              , '28810'
//              , '96765'
//              , '23006'
//              , '18799'
//              , '49156'
//              , '98025'
//              , '23932'
//              , '67467'
//              , '30450'
//              , '50298'
//              , '29178'
//              , '89360'
//              , '32754'
//              , '63089'
//              , '87501'
//              , '87343'
//              , '29839'
//              , '30903'
//              , '81019'
//              , '18652'
//              , '73273'
//              , '25989'
//              , '20260'
//              , '68893'
//              , '53179'
//              , '30469'
//              , '28898'
//              , '31671'
//              , '24996'
//              , '18767'
//              , '64034'
//              , '91068'
//              , '51798'
//              , '51200'
//              , '63193'
//              , '39516'
//              , '72550'
//              , '72325'
//              , '51211'
//              , '23968'
//              , '86057'
//              , '10390'
//              , '85816'
//              , '45692'
//              , '65164'
//              , '21309'
//              , '18845'
//              , '68621'
//              , '92712'
//              , '68880'
//              , '90257'
//              , '47770'
//              , '13955'
//              , '70466'
//              , '21286'
//              , '67875'
//              , '82636'
//              , '36446'
//              , '79994'
//              , '72823'
//              , '40162'
//              , '41367'
//              , '41766'
//              , '22437'
//              , '58470'
//              , '11356'
//              , '76638'
//              , '68806'
//              , '25280'
//              , '67301'
//              , '73650'
//              , '86198'
//              , '16725'
//              , '38935'
//              , '13394'
//              , '61810'
//              , '81312'
//              , '15146'
//              , '71791'
//              , '31016'
//              , '72013'
//              , '37126'
//              , '22744'
//              , '73134'
//              , '70372'
//              , '30431'
//              , '39192'
//              , '35850'
//              , '56571'
//              , '67030'
//              , '22461'
//              , '88424'
//              , '88086'
//              , '14060'
//              , '40604'
//              , '19512'
//              , '72175'
//              , '51649'
//              , '19505'
//              , '24317'
//              , '13375'
//              , '81426'
//              , '18270'
//              , '72425'
//              , '45748'
//              , '55307'
//              , '53672'
//              , '52867'
//              , '56575'
//              , '39127'
//              , '30625'
//              , '10445'
//              , '39972'
//              , '74351'
//              , '26065'
//              , '83849'
//              , '42666'
//              , '96976'
//              , '68786'
//              , '77721'
//              , '68908'
//              , '66864'
//              , '63792'
//              , '51650'
//              , '31029'
//              , '26689'
//              , '66708'
//              , '11376'
//              , '20004'
//              , '31880'
//              , '96451'
//              , '41248'
//              , '94898'
//              , '18383'
//              , '60576'
//              , '38193'
//              , '48583'
//              , '13595'
//              , '76614'
//              , '24671'
//              , '46820'
//              , '82276'
//              , '10516'
//              , '11634'
//              , '45549'
//              , '88885'
//              , '18842'
//              , '90225'
//              , '18906'
//              , '13376'
//              , '84935'
//              , '78890'
//              , '58943'
//              , '15765'
//              , '50016'
//              , '69035'
//              , '49448'
//              , '39371'
//              , '41368'
//              , '33123'
//              , '83144'
//              , '14089'
//              , '94945'
//              , '73241'
//              , '19769'
//              , '47537'
//              , '38122'
//              , '28587'
//              , '76698'
//              , '22927'
//              , '56616'
//              , '34425'
//              , '96576'
//              , '78567'
//              , '97789'
//              , '94983'
//              , '79077'
//              , '57855'
//              , '97189'
//              , '46081'
//              , '48033'
//              , '19849'
//              , '28488'
//              , '28545'
//              , '72151'
//              , '69952'
//              , '43285'
//              , '26105'
//              , '76231'
//              , '15723'
//              , '25486'
//              , '39861'
//              , '83933'
//              , '75691'
//              , '46136'
//              , '61547'
//              , '66162'
//              , '25858'
//              , '22246'
//              , '51949'
//              , '27385'
//              , '77610'
//              , '34322'
//              , '51061'
//              , '68100'
//              , '61860'
//              , '13695'
//              , '44438'
//              , '90578'
//              , '96888'
//              , '58048'
//              , '99543'
//              , '73171'
//              , '56691'
//              , '64528'
//              , '56910'
//              , '83444'
//              , '30122'
//              , '68014'
//              , '14171'
//              , '16807'
//              , '83041'
//              , '34102'
//              , '51103'
//              , '79777'
//              , '17871'
//              , '12305'
//              , '22685'
//              , '94167'
//              , '28709'
//              , '35258'
//              , '57665'
//              , '71256'
//              , '57047'
//              , '11489'
//              , '31387'
//              , '68341'
//              , '78451'
//              , '14867'
//              , '25103'
//              , '35458'
//              , '25003'
//              , '54364'
//              , '73520'
//              , '32213'
//              , '35576'))
//      )       INTERSECT (
//         SELECT ca_zip
//         FROM
//           (
//            SELECT
//              substr(ca_zip, 1, 5) ca_zip
//            , count(*) cnt
//            FROM
//              customer_address
//            , customer
//            WHERE (ca_address_sk = c_current_addr_sk)
//               AND (c_preferred_cust_flag = 'Y')
//            GROUP BY ca_zip
//            HAVING (count(*) > 10)
//         )  a1
//      )    )  a2
//)  v1
//WHERE (ss_store_sk = s_store_sk)
//   AND (ss_sold_date_sk = d_date_sk)
//   AND (d_qoy = 2)
//   AND (d_year = 1998)
//   AND (substr(s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
//GROUP BY s_store_name
//ORDER BY s_store_name ASC
//LIMIT 100;
//"""
//    order_qt_query8_before "${query8}"
//    async_mv_rewrite_fail(db, mv8, query8, "mv8")
//    order_qt_query8_after "${query8}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8"""
//
//    def mv8_1 = """
//SELECT
//  s_store_name
//, sum(ss_net_profit)
//FROM
//  store_sales
//, date_dim
//, store
//, (
//   SELECT ca_zip
//   FROM
//     (
//(
//         SELECT substr(ca_zip, 1, 5) ca_zip
//         FROM
//           customer_address
//         WHERE (substr(ca_zip, 1, 5) IN (
//                '24128'
//              , '57834'
//              , '13354'
//              , '15734'
//              , '78668'
//              , '76232'
//              , '62878'
//              , '45375'
//              , '63435'
//              , '22245'
//              , '65084'
//              , '49130'
//              , '40558'
//              , '25733'
//              , '15798'
//              , '87816'
//              , '81096'
//              , '56458'
//              , '35474'
//              , '27156'
//              , '83926'
//              , '18840'
//              , '28286'
//              , '24676'
//              , '37930'
//              , '77556'
//              , '27700'
//              , '45266'
//              , '94627'
//              , '62971'
//              , '20548'
//              , '23470'
//              , '47305'
//              , '53535'
//              , '21337'
//              , '26231'
//              , '50412'
//              , '69399'
//              , '17879'
//              , '51622'
//              , '43848'
//              , '21195'
//              , '83921'
//              , '15559'
//              , '67853'
//              , '15126'
//              , '16021'
//              , '26233'
//              , '53268'
//              , '10567'
//              , '91137'
//              , '76107'
//              , '11101'
//              , '59166'
//              , '38415'
//              , '61265'
//              , '71954'
//              , '15371'
//              , '11928'
//              , '15455'
//              , '98294'
//              , '68309'
//              , '69913'
//              , '59402'
//              , '58263'
//              , '25782'
//              , '18119'
//              , '35942'
//              , '33282'
//              , '42029'
//              , '17920'
//              , '98359'
//              , '15882'
//              , '45721'
//              , '60279'
//              , '18426'
//              , '64544'
//              , '25631'
//              , '43933'
//              , '37125'
//              , '98235'
//              , '10336'
//              , '24610'
//              , '68101'
//              , '56240'
//              , '40081'
//              , '86379'
//              , '44165'
//              , '33515'
//              , '88190'
//              , '84093'
//              , '27068'
//              , '99076'
//              , '36634'
//              , '50308'
//              , '28577'
//              , '39736'
//              , '33786'
//              , '71286'
//              , '26859'
//              , '55565'
//              , '98569'
//              , '70738'
//              , '19736'
//              , '64457'
//              , '17183'
//              , '28915'
//              , '26653'
//              , '58058'
//              , '89091'
//              , '54601'
//              , '24206'
//              , '14328'
//              , '55253'
//              , '82136'
//              , '67897'
//              , '56529'
//              , '72305'
//              , '67473'
//              , '62377'
//              , '22752'
//              , '57647'
//              , '62496'
//              , '41918'
//              , '36233'
//              , '86284'
//              , '54917'
//              , '22152'
//              , '19515'
//              , '63837'
//              , '18376'
//              , '42961'
//              , '10144'
//              , '36495'
//              , '58078'
//              , '38607'
//              , '91110'
//              , '64147'
//              , '19430'
//              , '17043'
//              , '45200'
//              , '63981'
//              , '48425'
//              , '22351'
//              , '30010'
//              , '21756'
//              , '14922'
//              , '14663'
//              , '77191'
//              , '60099'
//              , '29741'
//              , '36420'
//              , '21076'
//              , '91393'
//              , '28810'
//              , '96765'
//              , '23006'
//              , '18799'
//              , '49156'
//              , '98025'
//              , '23932'
//              , '67467'
//              , '30450'
//              , '50298'
//              , '29178'
//              , '89360'
//              , '32754'
//              , '63089'
//              , '87501'
//              , '87343'
//              , '29839'
//              , '30903'
//              , '81019'
//              , '18652'
//              , '73273'
//              , '25989'
//              , '20260'
//              , '68893'
//              , '53179'
//              , '30469'
//              , '28898'
//              , '31671'
//              , '24996'
//              , '18767'
//              , '64034'
//              , '91068'
//              , '51798'
//              , '51200'
//              , '63193'
//              , '39516'
//              , '72550'
//              , '72325'
//              , '51211'
//              , '23968'
//              , '86057'
//              , '10390'
//              , '85816'
//              , '45692'
//              , '65164'
//              , '21309'
//              , '18845'
//              , '68621'
//              , '92712'
//              , '68880'
//              , '90257'
//              , '47770'
//              , '13955'
//              , '70466'
//              , '21286'
//              , '67875'
//              , '82636'
//              , '36446'
//              , '79994'
//              , '72823'
//              , '40162'
//              , '41367'
//              , '41766'
//              , '22437'
//              , '58470'
//              , '11356'
//              , '76638'
//              , '68806'
//              , '25280'
//              , '67301'
//              , '73650'
//              , '86198'
//              , '16725'
//              , '38935'
//              , '13394'
//              , '61810'
//              , '81312'
//              , '15146'
//              , '71791'
//              , '31016'
//              , '72013'
//              , '37126'
//              , '22744'
//              , '73134'
//              , '70372'
//              , '30431'
//              , '39192'
//              , '35850'
//              , '56571'
//              , '67030'
//              , '22461'
//              , '88424'
//              , '88086'
//              , '14060'
//              , '40604'
//              , '19512'
//              , '72175'
//              , '51649'
//              , '19505'
//              , '24317'
//              , '13375'
//              , '81426'
//              , '18270'
//              , '72425'
//              , '45748'
//              , '55307'
//              , '53672'
//              , '52867'
//              , '56575'
//              , '39127'
//              , '30625'
//              , '10445'
//              , '39972'
//              , '74351'
//              , '26065'
//              , '83849'
//              , '42666'
//              , '96976'
//              , '68786'
//              , '77721'
//              , '68908'
//              , '66864'
//              , '63792'
//              , '51650'
//              , '31029'
//              , '26689'
//              , '66708'
//              , '11376'
//              , '20004'
//              , '31880'
//              , '96451'
//              , '41248'
//              , '94898'
//              , '18383'
//              , '60576'
//              , '38193'
//              , '48583'
//              , '13595'
//              , '76614'
//              , '24671'
//              , '46820'
//              , '82276'
//              , '10516'
//              , '11634'
//              , '45549'
//              , '88885'
//              , '18842'
//              , '90225'
//              , '18906'
//              , '13376'
//              , '84935'
//              , '78890'
//              , '58943'
//              , '15765'
//              , '50016'
//              , '69035'
//              , '49448'
//              , '39371'
//              , '41368'
//              , '33123'
//              , '83144'
//              , '14089'
//              , '94945'
//              , '73241'
//              , '19769'
//              , '47537'
//              , '38122'
//              , '28587'
//              , '76698'
//              , '22927'
//              , '56616'
//              , '34425'
//              , '96576'
//              , '78567'
//              , '97789'
//              , '94983'
//              , '79077'
//              , '57855'
//              , '97189'
//              , '46081'
//              , '48033'
//              , '19849'
//              , '28488'
//              , '28545'
//              , '72151'
//              , '69952'
//              , '43285'
//              , '26105'
//              , '76231'
//              , '15723'
//              , '25486'
//              , '39861'
//              , '83933'
//              , '75691'
//              , '46136'
//              , '61547'
//              , '66162'
//              , '25858'
//              , '22246'
//              , '51949'
//              , '27385'
//              , '77610'
//              , '34322'
//              , '51061'
//              , '68100'
//              , '61860'
//              , '13695'
//              , '44438'
//              , '90578'
//              , '96888'
//              , '58048'
//              , '99543'
//              , '73171'
//              , '56691'
//              , '64528'
//              , '56910'
//              , '83444'
//              , '30122'
//              , '68014'
//              , '14171'
//              , '16807'
//              , '83041'
//              , '34102'
//              , '51103'
//              , '79777'
//              , '17871'
//              , '12305'
//              , '22685'
//              , '94167'
//              , '28709'
//              , '35258'
//              , '57665'
//              , '71256'
//              , '57047'
//              , '11489'
//              , '31387'
//              , '68341'
//              , '78451'
//              , '14867'
//              , '25103'
//              , '35458'
//              , '25003'
//              , '54364'
//              , '73520'
//              , '32213'
//              , '35576'))
//      )       INTERSECT (
//         SELECT ca_zip
//         FROM
//           (
//            SELECT
//              substr(ca_zip, 1, 5) ca_zip
//            , count(*) cnt
//            FROM
//              customer_address
//            , customer
//            WHERE (ca_address_sk = c_current_addr_sk)
//               AND (c_preferred_cust_flag = 'Y')
//            GROUP BY ca_zip
//            HAVING (count(*) > 10)
//         )  a1
//      )    )  a2
//)  v1
//WHERE (ss_store_sk = s_store_sk)
//   AND (ss_sold_date_sk = d_date_sk)
//   AND (d_qoy = 2)
//   AND (d_year = 1998)
//   AND (substr(s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
//GROUP BY s_store_name;
//"""
//    def query8_1 = """
//SELECT
//  s_store_name
//, sum(ss_net_profit)
//FROM
//  store_sales
//, date_dim
//, store
//, (
//   SELECT ca_zip
//   FROM
//     (
//(
//         SELECT substr(ca_zip, 1, 5) ca_zip
//         FROM
//           customer_address
//         WHERE (substr(ca_zip, 1, 5) IN (
//                '24128'
//              , '57834'
//              , '13354'
//              , '15734'
//              , '78668'
//              , '76232'
//              , '62878'
//              , '45375'
//              , '63435'
//              , '22245'
//              , '65084'
//              , '49130'
//              , '40558'
//              , '25733'
//              , '15798'
//              , '87816'
//              , '81096'
//              , '56458'
//              , '35474'
//              , '27156'
//              , '83926'
//              , '18840'
//              , '28286'
//              , '24676'
//              , '37930'
//              , '77556'
//              , '27700'
//              , '45266'
//              , '94627'
//              , '62971'
//              , '20548'
//              , '23470'
//              , '47305'
//              , '53535'
//              , '21337'
//              , '26231'
//              , '50412'
//              , '69399'
//              , '17879'
//              , '51622'
//              , '43848'
//              , '21195'
//              , '83921'
//              , '15559'
//              , '67853'
//              , '15126'
//              , '16021'
//              , '26233'
//              , '53268'
//              , '10567'
//              , '91137'
//              , '76107'
//              , '11101'
//              , '59166'
//              , '38415'
//              , '61265'
//              , '71954'
//              , '15371'
//              , '11928'
//              , '15455'
//              , '98294'
//              , '68309'
//              , '69913'
//              , '59402'
//              , '58263'
//              , '25782'
//              , '18119'
//              , '35942'
//              , '33282'
//              , '42029'
//              , '17920'
//              , '98359'
//              , '15882'
//              , '45721'
//              , '60279'
//              , '18426'
//              , '64544'
//              , '25631'
//              , '43933'
//              , '37125'
//              , '98235'
//              , '10336'
//              , '24610'
//              , '68101'
//              , '56240'
//              , '40081'
//              , '86379'
//              , '44165'
//              , '33515'
//              , '88190'
//              , '84093'
//              , '27068'
//              , '99076'
//              , '36634'
//              , '50308'
//              , '28577'
//              , '39736'
//              , '33786'
//              , '71286'
//              , '26859'
//              , '55565'
//              , '98569'
//              , '70738'
//              , '19736'
//              , '64457'
//              , '17183'
//              , '28915'
//              , '26653'
//              , '58058'
//              , '89091'
//              , '54601'
//              , '24206'
//              , '14328'
//              , '55253'
//              , '82136'
//              , '67897'
//              , '56529'
//              , '72305'
//              , '67473'
//              , '62377'
//              , '22752'
//              , '57647'
//              , '62496'
//              , '41918'
//              , '36233'
//              , '86284'
//              , '54917'
//              , '22152'
//              , '19515'
//              , '63837'
//              , '18376'
//              , '42961'
//              , '10144'
//              , '36495'
//              , '58078'
//              , '38607'
//              , '91110'
//              , '64147'
//              , '19430'
//              , '17043'
//              , '45200'
//              , '63981'
//              , '48425'
//              , '22351'
//              , '30010'
//              , '21756'
//              , '14922'
//              , '14663'
//              , '77191'
//              , '60099'
//              , '29741'
//              , '36420'
//              , '21076'
//              , '91393'
//              , '28810'
//              , '96765'
//              , '23006'
//              , '18799'
//              , '49156'
//              , '98025'
//              , '23932'
//              , '67467'
//              , '30450'
//              , '50298'
//              , '29178'
//              , '89360'
//              , '32754'
//              , '63089'
//              , '87501'
//              , '87343'
//              , '29839'
//              , '30903'
//              , '81019'
//              , '18652'
//              , '73273'
//              , '25989'
//              , '20260'
//              , '68893'
//              , '53179'
//              , '30469'
//              , '28898'
//              , '31671'
//              , '24996'
//              , '18767'
//              , '64034'
//              , '91068'
//              , '51798'
//              , '51200'
//              , '63193'
//              , '39516'
//              , '72550'
//              , '72325'
//              , '51211'
//              , '23968'
//              , '86057'
//              , '10390'
//              , '85816'
//              , '45692'
//              , '65164'
//              , '21309'
//              , '18845'
//              , '68621'
//              , '92712'
//              , '68880'
//              , '90257'
//              , '47770'
//              , '13955'
//              , '70466'
//              , '21286'
//              , '67875'
//              , '82636'
//              , '36446'
//              , '79994'
//              , '72823'
//              , '40162'
//              , '41367'
//              , '41766'
//              , '22437'
//              , '58470'
//              , '11356'
//              , '76638'
//              , '68806'
//              , '25280'
//              , '67301'
//              , '73650'
//              , '86198'
//              , '16725'
//              , '38935'
//              , '13394'
//              , '61810'
//              , '81312'
//              , '15146'
//              , '71791'
//              , '31016'
//              , '72013'
//              , '37126'
//              , '22744'
//              , '73134'
//              , '70372'
//              , '30431'
//              , '39192'
//              , '35850'
//              , '56571'
//              , '67030'
//              , '22461'
//              , '88424'
//              , '88086'
//              , '14060'
//              , '40604'
//              , '19512'
//              , '72175'
//              , '51649'
//              , '19505'
//              , '24317'
//              , '13375'
//              , '81426'
//              , '18270'
//              , '72425'
//              , '45748'
//              , '55307'
//              , '53672'
//              , '52867'
//              , '56575'
//              , '39127'
//              , '30625'
//              , '10445'
//              , '39972'
//              , '74351'
//              , '26065'
//              , '83849'
//              , '42666'
//              , '96976'
//              , '68786'
//              , '77721'
//              , '68908'
//              , '66864'
//              , '63792'
//              , '51650'
//              , '31029'
//              , '26689'
//              , '66708'
//              , '11376'
//              , '20004'
//              , '31880'
//              , '96451'
//              , '41248'
//              , '94898'
//              , '18383'
//              , '60576'
//              , '38193'
//              , '48583'
//              , '13595'
//              , '76614'
//              , '24671'
//              , '46820'
//              , '82276'
//              , '10516'
//              , '11634'
//              , '45549'
//              , '88885'
//              , '18842'
//              , '90225'
//              , '18906'
//              , '13376'
//              , '84935'
//              , '78890'
//              , '58943'
//              , '15765'
//              , '50016'
//              , '69035'
//              , '49448'
//              , '39371'
//              , '41368'
//              , '33123'
//              , '83144'
//              , '14089'
//              , '94945'
//              , '73241'
//              , '19769'
//              , '47537'
//              , '38122'
//              , '28587'
//              , '76698'
//              , '22927'
//              , '56616'
//              , '34425'
//              , '96576'
//              , '78567'
//              , '97789'
//              , '94983'
//              , '79077'
//              , '57855'
//              , '97189'
//              , '46081'
//              , '48033'
//              , '19849'
//              , '28488'
//              , '28545'
//              , '72151'
//              , '69952'
//              , '43285'
//              , '26105'
//              , '76231'
//              , '15723'
//              , '25486'
//              , '39861'
//              , '83933'
//              , '75691'
//              , '46136'
//              , '61547'
//              , '66162'
//              , '25858'
//              , '22246'
//              , '51949'
//              , '27385'
//              , '77610'
//              , '34322'
//              , '51061'
//              , '68100'
//              , '61860'
//              , '13695'
//              , '44438'
//              , '90578'
//              , '96888'
//              , '58048'
//              , '99543'
//              , '73171'
//              , '56691'
//              , '64528'
//              , '56910'
//              , '83444'
//              , '30122'
//              , '68014'
//              , '14171'
//              , '16807'
//              , '83041'
//              , '34102'
//              , '51103'
//              , '79777'
//              , '17871'
//              , '12305'
//              , '22685'
//              , '94167'
//              , '28709'
//              , '35258'
//              , '57665'
//              , '71256'
//              , '57047'
//              , '11489'
//              , '31387'
//              , '68341'
//              , '78451'
//              , '14867'
//              , '25103'
//              , '35458'
//              , '25003'
//              , '54364'
//              , '73520'
//              , '32213'
//              , '35576'))
//      )       INTERSECT (
//         SELECT ca_zip
//         FROM
//           (
//            SELECT
//              substr(ca_zip, 1, 5) ca_zip
//            , count(*) cnt
//            FROM
//              customer_address
//            , customer
//            WHERE (ca_address_sk = c_current_addr_sk)
//               AND (c_preferred_cust_flag = 'Y')
//            GROUP BY ca_zip
//            HAVING (count(*) > 10)
//         )  a1
//      )    )  a2
//)  v1
//WHERE (ss_store_sk = s_store_sk)
//   AND (ss_sold_date_sk = d_date_sk)
//   AND (d_qoy = 2)
//   AND (d_year = 1998)
//   AND (substr(s_zip, 1, 2) = substr(v1.ca_zip, 1, 2))
//GROUP BY s_store_name
//ORDER BY s_store_name ASC
//LIMIT 100;
//"""
//    order_qt_query8_1_before "${query8_1}"
//    async_mv_rewrite_fail(db, mv8_1, query8_1, "mv8_1")
//    order_qt_query8_1_after "${query8_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv8_1"""
//
//    def mv9 = """
//SELECT
//  (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 1 AND 20)
//   ) > 74129) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 1 AND 20)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 1 AND 20)
//) END) bucket1
//, (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 21 AND 40)
//   ) > 122840) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 21 AND 40)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 21 AND 40)
//) END) bucket2
//, (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 41 AND 60)
//   ) > 56580) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 41 AND 60)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 41 AND 60)
//) END) bucket3
//, (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 61 AND 80)
//   ) > 10097) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 61 AND 80)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 61 AND 80)
//) END) bucket4
//, (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 81 AND 100)
//   ) > 165306) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 81 AND 100)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 81 AND 100)
//) END) bucket5
//FROM
//  reason
//WHERE (r_reason_sk = 1);
//"""
//    def query9 = """
//SELECT
//  (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 1 AND 20)
//   ) > 74129) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 1 AND 20)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 1 AND 20)
//) END) bucket1
//, (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 21 AND 40)
//   ) > 122840) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 21 AND 40)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 21 AND 40)
//) END) bucket2
//, (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 41 AND 60)
//   ) > 56580) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 41 AND 60)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 41 AND 60)
//) END) bucket3
//, (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 61 AND 80)
//   ) > 10097) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 61 AND 80)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 61 AND 80)
//) END) bucket4
//, (CASE WHEN ((
//      SELECT count(*)
//      FROM
//        store_sales
//      WHERE (ss_quantity BETWEEN 81 AND 100)
//   ) > 165306) THEN (
//   SELECT avg(ss_ext_discount_amt)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 81 AND 100)
//) ELSE (
//   SELECT avg(ss_net_paid)
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 81 AND 100)
//) END) bucket5
//FROM
//  reason
//WHERE (r_reason_sk = 1);
//"""
//    order_qt_query9_before "${query9}"
//    async_mv_rewrite_fail(db, mv9, query9, "mv9")
//    order_qt_query9_after "${query9}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv9"""
//
//
//    def mv10 = """
//SELECT
//  cd_gender
//, cd_marital_status
//, cd_education_status
//, count(*) cnt1
//, cd_purchase_estimate
//, count(*) cnt2
//, cd_credit_rating
//, count(*) cnt3
//, cd_dep_count
//, count(*) cnt4
//, cd_dep_employed_count
//, count(*) cnt5
//, cd_dep_college_count
//, count(*) cnt6
//FROM
//  customer c
//, customer_address ca
//, customer_demographics
//WHERE (c.c_current_addr_sk = ca.ca_address_sk)
//   AND (ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'))
//   AND (cd_demo_sk = c.c_current_cdemo_sk)
//   AND (EXISTS (
//   SELECT *
//   FROM
//     store_sales
//   , date_dim
//   WHERE (c.c_customer_sk = ss_customer_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//      AND (d_year = 2002)
//      AND (d_moy BETWEEN 1 AND (1 + 3))
//))
//   AND ((EXISTS (
//      SELECT *
//      FROM
//        web_sales
//      , date_dim
//      WHERE (c.c_customer_sk = ws_bill_customer_sk)
//         AND (ws_sold_date_sk = d_date_sk)
//         AND (d_year = 2002)
//         AND (d_moy BETWEEN 1 AND (1 + 3))
//   ))
//      OR (EXISTS (
//      SELECT *
//      FROM
//        catalog_sales
//      , date_dim
//      WHERE (c.c_customer_sk = cs_ship_customer_sk)
//         AND (cs_sold_date_sk = d_date_sk)
//         AND (d_year = 2002)
//         AND (d_moy BETWEEN 1 AND (1 + 3))
//   )))
//GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
//ORDER BY cd_gender ASC, cd_marital_status ASC, cd_education_status ASC, cd_purchase_estimate ASC, cd_credit_rating ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
//LIMIT 100;
//"""
//    def query10 = """
//SELECT
//  cd_gender
//, cd_marital_status
//, cd_education_status
//, count(*) cnt1
//, cd_purchase_estimate
//, count(*) cnt2
//, cd_credit_rating
//, count(*) cnt3
//, cd_dep_count
//, count(*) cnt4
//, cd_dep_employed_count
//, count(*) cnt5
//, cd_dep_college_count
//, count(*) cnt6
//FROM
//  customer c
//, customer_address ca
//, customer_demographics
//WHERE (c.c_current_addr_sk = ca.ca_address_sk)
//   AND (ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'))
//   AND (cd_demo_sk = c.c_current_cdemo_sk)
//   AND (EXISTS (
//   SELECT *
//   FROM
//     store_sales
//   , date_dim
//   WHERE (c.c_customer_sk = ss_customer_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//      AND (d_year = 2002)
//      AND (d_moy BETWEEN 1 AND (1 + 3))
//))
//   AND ((EXISTS (
//      SELECT *
//      FROM
//        web_sales
//      , date_dim
//      WHERE (c.c_customer_sk = ws_bill_customer_sk)
//         AND (ws_sold_date_sk = d_date_sk)
//         AND (d_year = 2002)
//         AND (d_moy BETWEEN 1 AND (1 + 3))
//   ))
//      OR (EXISTS (
//      SELECT *
//      FROM
//        catalog_sales
//      , date_dim
//      WHERE (c.c_customer_sk = cs_ship_customer_sk)
//         AND (cs_sold_date_sk = d_date_sk)
//         AND (d_year = 2002)
//         AND (d_moy BETWEEN 1 AND (1 + 3))
//   )))
//GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
//ORDER BY cd_gender ASC, cd_marital_status ASC, cd_education_status ASC, cd_purchase_estimate ASC, cd_credit_rating ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
//LIMIT 100;
//"""
//    order_qt_query10_before "${query10}"
//    async_mv_rewrite_fail(db, mv10, query10, "mv10")
//    order_qt_query10_after "${query10}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10"""
//
//    def mv10_1 = """
//SELECT
//  cd_gender
//, cd_marital_status
//, cd_education_status
//, count(*) cnt1
//, cd_purchase_estimate
//, count(*) cnt2
//, cd_credit_rating
//, count(*) cnt3
//, cd_dep_count
//, count(*) cnt4
//, cd_dep_employed_count
//, count(*) cnt5
//, cd_dep_college_count
//, count(*) cnt6
//FROM
//  customer c
//, customer_address ca
//, customer_demographics
//WHERE (c.c_current_addr_sk = ca.ca_address_sk)
//   AND (ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'))
//   AND (cd_demo_sk = c.c_current_cdemo_sk)
//   AND (EXISTS (
//   SELECT *
//   FROM
//     store_sales
//   , date_dim
//   WHERE (c.c_customer_sk = ss_customer_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//      AND (d_year = 2002)
//      AND (d_moy BETWEEN 1 AND (1 + 3))
//))
//   AND ((EXISTS (
//      SELECT *
//      FROM
//        web_sales
//      , date_dim
//      WHERE (c.c_customer_sk = ws_bill_customer_sk)
//         AND (ws_sold_date_sk = d_date_sk)
//         AND (d_year = 2002)
//         AND (d_moy BETWEEN 1 AND (1 + 3))
//   ))
//      OR (EXISTS (
//      SELECT *
//      FROM
//        catalog_sales
//      , date_dim
//      WHERE (c.c_customer_sk = cs_ship_customer_sk)
//         AND (cs_sold_date_sk = d_date_sk)
//         AND (d_year = 2002)
//         AND (d_moy BETWEEN 1 AND (1 + 3))
//   )))
//GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count;
//
//"""
//    def query10_1 = """
//SELECT
//  cd_gender
//, cd_marital_status
//, cd_education_status
//, count(*) cnt1
//, cd_purchase_estimate
//, count(*) cnt2
//, cd_credit_rating
//, count(*) cnt3
//, cd_dep_count
//, count(*) cnt4
//, cd_dep_employed_count
//, count(*) cnt5
//, cd_dep_college_count
//, count(*) cnt6
//FROM
//  customer c
//, customer_address ca
//, customer_demographics
//WHERE (c.c_current_addr_sk = ca.ca_address_sk)
//   AND (ca_county IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County'))
//   AND (cd_demo_sk = c.c_current_cdemo_sk)
//   AND (EXISTS (
//   SELECT *
//   FROM
//     store_sales
//   , date_dim
//   WHERE (c.c_customer_sk = ss_customer_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//      AND (d_year = 2002)
//      AND (d_moy BETWEEN 1 AND (1 + 3))
//))
//   AND ((EXISTS (
//      SELECT *
//      FROM
//        web_sales
//      , date_dim
//      WHERE (c.c_customer_sk = ws_bill_customer_sk)
//         AND (ws_sold_date_sk = d_date_sk)
//         AND (d_year = 2002)
//         AND (d_moy BETWEEN 1 AND (1 + 3))
//   ))
//      OR (EXISTS (
//      SELECT *
//      FROM
//        catalog_sales
//      , date_dim
//      WHERE (c.c_customer_sk = cs_ship_customer_sk)
//         AND (cs_sold_date_sk = d_date_sk)
//         AND (d_year = 2002)
//         AND (d_moy BETWEEN 1 AND (1 + 3))
//   )))
//GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count
//ORDER BY cd_gender ASC, cd_marital_status ASC, cd_education_status ASC, cd_purchase_estimate ASC, cd_credit_rating ASC, cd_dep_count ASC, cd_dep_employed_count ASC, cd_dep_college_count ASC
//LIMIT 100;
//"""
//    order_qt_query10_1_before "${query10_1}"
//    async_mv_rewrite_fail(db, mv10_1, query10_1, "mv10_1")
//    order_qt_query10_1_after "${query10_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv10_1"""
//
//    def mv11 = """
//WITH
//  year_total AS (
//   SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum((ss_ext_list_price - ss_ext_discount_amt)) year_total
//   , 's' sale_type
//   FROM
//     customer
//   , store_sales
//   , date_dim
//   WHERE (c_customer_sk = ss_customer_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//UNION ALL    SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum((ws_ext_list_price - ws_ext_discount_amt)) year_total
//   , 'w' sale_type
//   FROM
//     customer
//   , web_sales
//   , date_dim
//   WHERE (c_customer_sk = ws_bill_customer_sk)
//      AND (ws_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//)
//SELECT
//  t_s_secyear.customer_id
//, t_s_secyear.customer_first_name
//, t_s_secyear.customer_last_name
//, t_s_secyear.customer_preferred_cust_flag
//, t_s_secyear.customer_birth_country
//, t_s_secyear.customer_login
//FROM
//  year_total t_s_firstyear
//, year_total t_s_secyear
//, year_total t_w_firstyear
//, year_total t_w_secyear
//WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
//   AND (t_s_firstyear.sale_type = 's')
//   AND (t_w_firstyear.sale_type = 'w')
//   AND (t_s_secyear.sale_type = 's')
//   AND (t_w_secyear.sale_type = 'w')
//   AND (t_s_firstyear.dyear = 2001)
//   AND (t_s_secyear.dyear = (2001 + 1))
//   AND (t_w_firstyear.dyear = 2001)
//   AND (t_w_secyear.dyear = (2001 + 1))
//   AND (t_s_firstyear.year_total > 0)
//   AND (t_w_firstyear.year_total > 0)
//   AND ((CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL(2,1)) END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL(2,1)) END))
//ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
//LIMIT 100;
//"""
//    def query11 = """
//WITH
//  year_total AS (
//   SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum((ss_ext_list_price - ss_ext_discount_amt)) year_total
//   , 's' sale_type
//   FROM
//     customer
//   , store_sales
//   , date_dim
//   WHERE (c_customer_sk = ss_customer_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//UNION ALL    SELECT
//     c_customer_id customer_id
//   , c_first_name customer_first_name
//   , c_last_name customer_last_name
//   , c_preferred_cust_flag customer_preferred_cust_flag
//   , c_birth_country customer_birth_country
//   , c_login customer_login
//   , c_email_address customer_email_address
//   , d_year dyear
//   , sum((ws_ext_list_price - ws_ext_discount_amt)) year_total
//   , 'w' sale_type
//   FROM
//     customer
//   , web_sales
//   , date_dim
//   WHERE (c_customer_sk = ws_bill_customer_sk)
//      AND (ws_sold_date_sk = d_date_sk)
//   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
//)
//SELECT
//  t_s_secyear.customer_id
//, t_s_secyear.customer_first_name
//, t_s_secyear.customer_last_name
//, t_s_secyear.customer_preferred_cust_flag
//, t_s_secyear.customer_birth_country
//, t_s_secyear.customer_login
//FROM
//  year_total t_s_firstyear
//, year_total t_s_secyear
//, year_total t_w_firstyear
//, year_total t_w_secyear
//WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
//   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
//   AND (t_s_firstyear.sale_type = 's')
//   AND (t_w_firstyear.sale_type = 'w')
//   AND (t_s_secyear.sale_type = 's')
//   AND (t_w_secyear.sale_type = 'w')
//   AND (t_s_firstyear.dyear = 2001)
//   AND (t_s_secyear.dyear = (2001 + 1))
//   AND (t_w_firstyear.dyear = 2001)
//   AND (t_w_secyear.dyear = (2001 + 1))
//   AND (t_s_firstyear.year_total > 0)
//   AND (t_w_firstyear.year_total > 0)
//   AND ((CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL(2,1)) END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL(2,1)) END))
//ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
//LIMIT 100;
//"""
//    order_qt_query11_before "${query11}"
//    async_mv_rewrite_fail(db, mv11, query11, "mv11")
//    order_qt_query11_after "${query11}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv11"""
//
//
//    def mv12 = """
//SELECT
//  i_item_id
//, i_item_desc
//, i_category
//, i_class
//, i_current_price
//, sum(ws_ext_sales_price) itemrevenue
//, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
//FROM
//  web_sales
//, item
//, date_dim
//WHERE (ws_item_sk = i_item_sk)
//   AND (i_category IN ('Sports', 'Books', 'Home'))
//   AND (ws_sold_date_sk = d_date_sk)
//   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
//GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
//ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
//LIMIT 100;
//"""
//    def query12 = """
//SELECT
//  i_item_id
//, i_item_desc
//, i_category
//, i_class
//, i_current_price
//, sum(ws_ext_sales_price) itemrevenue
//, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
//FROM
//  web_sales
//, item
//, date_dim
//WHERE (ws_item_sk = i_item_sk)
//   AND (i_category IN ('Sports', 'Books', 'Home'))
//   AND (ws_sold_date_sk = d_date_sk)
//   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
//GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
//ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
//LIMIT 100;
//"""
//    order_qt_query12_before "${query12}"
//    async_mv_rewrite_fail(db, mv12, query12, "mv12")
//    order_qt_query12_after "${query12}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv12"""
//
//    def mv12_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, i_category
//, i_class
//, i_current_price
//, sum(ws_ext_sales_price) itemrevenue
//, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
//FROM
//  web_sales
//, item
//, date_dim
//WHERE (ws_item_sk = i_item_sk)
//   AND (i_category IN ('Sports', 'Books', 'Home'))
//   AND (ws_sold_date_sk = d_date_sk)
//   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
//GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price;
//"""
//    def query12_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, i_category
//, i_class
//, i_current_price
//, sum(ws_ext_sales_price) itemrevenue
//, ((sum(ws_ext_sales_price) * 100) / sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
//FROM
//  web_sales
//, item
//, date_dim
//WHERE (ws_item_sk = i_item_sk)
//   AND (i_category IN ('Sports', 'Books', 'Home'))
//   AND (ws_sold_date_sk = d_date_sk)
//   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
//GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
//ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
//LIMIT 100;
//"""
//    order_qt_query12_1_before "${query12_1}"
//    async_mv_rewrite_fail(db, mv12_1, query12_1, "mv12_1")
//    order_qt_query12_1_after "${query12_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv12_1"""

//    def mv13 = """
//SELECT
//  avg(ss_quantity)
//, avg(ss_ext_sales_price)
//, avg(ss_ext_wholesale_cost)
//, sum(ss_ext_wholesale_cost)
//FROM
//  store_sales
//, store
//, customer_demographics
//, household_demographics
//, customer_address
//, date_dim
//WHERE (s_store_sk = ss_store_sk)
//   AND (ss_sold_date_sk = d_date_sk)
//   AND (d_year = 2001)
//   AND (((ss_hdemo_sk = hd_demo_sk)
//         AND (cd_demo_sk = ss_cdemo_sk)
//         AND (cd_marital_status = 'M')
//         AND (cd_education_status = 'Advanced Degree')
//         AND (ss_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2)))
//         AND (hd_dep_count = 3))
//      OR ((ss_hdemo_sk = hd_demo_sk)
//         AND (cd_demo_sk = ss_cdemo_sk)
//         AND (cd_marital_status = 'S')
//         AND (cd_education_status = 'College')
//         AND (ss_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2)))
//         AND (hd_dep_count = 1))
//      OR ((ss_hdemo_sk = hd_demo_sk)
//         AND (cd_demo_sk = ss_cdemo_sk)
//         AND (cd_marital_status = 'W')
//         AND (cd_education_status = '2 yr Degree')
//         AND (ss_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))
//         AND (hd_dep_count = 1)))
//   AND (((ss_addr_sk = ca_address_sk)
//         AND (ca_country = 'United States')
//         AND (ca_state IN ('TX'      , 'OH'      , 'TX'))
//         AND (ss_net_profit BETWEEN 100 AND 200))
//      OR ((ss_addr_sk = ca_address_sk)
//         AND (ca_country = 'United States')
//         AND (ca_state IN ('OR'      , 'NM'      , 'KY'))
//         AND (ss_net_profit BETWEEN 150 AND 300))
//      OR ((ss_addr_sk = ca_address_sk)
//         AND (ca_country = 'United States')
//         AND (ca_state IN ('VA'      , 'TX'      , 'MS'))
//         AND (ss_net_profit BETWEEN 50 AND 250)));
//"""
//    def query13 = """
//SELECT
//  avg(ss_quantity)
//, avg(ss_ext_sales_price)
//, avg(ss_ext_wholesale_cost)
//, sum(ss_ext_wholesale_cost)
//FROM
//  store_sales
//, store
//, customer_demographics
//, household_demographics
//, customer_address
//, date_dim
//WHERE (s_store_sk = ss_store_sk)
//   AND (ss_sold_date_sk = d_date_sk)
//   AND (d_year = 2001)
//   AND (((ss_hdemo_sk = hd_demo_sk)
//         AND (cd_demo_sk = ss_cdemo_sk)
//         AND (cd_marital_status = 'M')
//         AND (cd_education_status = 'Advanced Degree')
//         AND (ss_sales_price BETWEEN CAST('100.00' AS DECIMAL(5,2)) AND CAST('150.00' AS DECIMAL(5,2)))
//         AND (hd_dep_count = 3))
//      OR ((ss_hdemo_sk = hd_demo_sk)
//         AND (cd_demo_sk = ss_cdemo_sk)
//         AND (cd_marital_status = 'S')
//         AND (cd_education_status = 'College')
//         AND (ss_sales_price BETWEEN CAST('50.00' AS DECIMAL(5,2)) AND CAST('100.00' AS DECIMAL(5,2)))
//         AND (hd_dep_count = 1))
//      OR ((ss_hdemo_sk = hd_demo_sk)
//         AND (cd_demo_sk = ss_cdemo_sk)
//         AND (cd_marital_status = 'W')
//         AND (cd_education_status = '2 yr Degree')
//         AND (ss_sales_price BETWEEN CAST('150.00' AS DECIMAL(5,2)) AND CAST('200.00' AS DECIMAL(5,2)))
//         AND (hd_dep_count = 1)))
//   AND (((ss_addr_sk = ca_address_sk)
//         AND (ca_country = 'United States')
//         AND (ca_state IN ('TX'      , 'OH'      , 'TX'))
//         AND (ss_net_profit BETWEEN 100 AND 200))
//      OR ((ss_addr_sk = ca_address_sk)
//         AND (ca_country = 'United States')
//         AND (ca_state IN ('OR'      , 'NM'      , 'KY'))
//         AND (ss_net_profit BETWEEN 150 AND 300))
//      OR ((ss_addr_sk = ca_address_sk)
//         AND (ca_country = 'United States')
//         AND (ca_state IN ('VA'      , 'TX'      , 'MS'))
//         AND (ss_net_profit BETWEEN 50 AND 250)));
//"""
//    order_qt_query13_before "${query13}"
//    // Should success
//    async_mv_rewrite_fail(db, mv13, query13, "mv13")
//    order_qt_query13_after "${query13}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv13"""

    // Should create success but fail
//    def mv14 = """
//WITH
//  cross_items AS (
//   SELECT i_item_sk ss_item_sk
//   FROM
//     item
//   , (
//      SELECT
//        iss.i_brand_id brand_id
//      , iss.i_class_id class_id
//      , iss.i_category_id category_id
//      FROM
//        store_sales
//      , item iss
//      , date_dim d1
//      WHERE (ss_item_sk = iss.i_item_sk)
//         AND (ss_sold_date_sk = d1.d_date_sk)
//         AND (d1.d_year BETWEEN 1999 AND (1999 + 2))
//INTERSECT       SELECT
//        ics.i_brand_id
//      , ics.i_class_id
//      , ics.i_category_id
//      FROM
//        catalog_sales
//      , item ics
//      , date_dim d2
//      WHERE (cs_item_sk = ics.i_item_sk)
//         AND (cs_sold_date_sk = d2.d_date_sk)
//         AND (d2.d_year BETWEEN 1999 AND (1999 + 2))
//INTERSECT       SELECT
//        iws.i_brand_id
//      , iws.i_class_id
//      , iws.i_category_id
//      FROM
//        web_sales
//      , item iws
//      , date_dim d3
//      WHERE (ws_item_sk = iws.i_item_sk)
//         AND (ws_sold_date_sk = d3.d_date_sk)
//         AND (d3.d_year BETWEEN 1999 AND (1999 + 2))
//   )  x
//   WHERE (i_brand_id = brand_id)
//      AND (i_class_id = class_id)
//      AND (i_category_id = category_id)
//)
//, avg_sales AS (
//   SELECT avg((quantity * list_price)) average_sales
//   FROM
//     (
//      SELECT
//        ss_quantity quantity
//      , ss_list_price list_price
//      FROM
//        store_sales
//      , date_dim
//      WHERE (ss_sold_date_sk = d_date_sk)
//         AND (d_year BETWEEN 1999 AND (1999 + 2))
//UNION ALL       SELECT
//        cs_quantity quantity
//      , cs_list_price list_price
//      FROM
//        catalog_sales
//      , date_dim
//      WHERE (cs_sold_date_sk = d_date_sk)
//         AND (d_year BETWEEN 1999 AND (1999 + 2))
//UNION ALL       SELECT
//        ws_quantity quantity
//      , ws_list_price list_price
//      FROM
//        web_sales
//      , date_dim
//      WHERE (ws_sold_date_sk = d_date_sk)
//         AND (d_year BETWEEN 1999 AND (1999 + 2))
//   ) y
//)
//SELECT *
//FROM
//  (
//   SELECT
//     'store' channel
//   , i_brand_id
//   , i_class_id
//   , i_category_id
//   , sum((ss_quantity * ss_list_price)) sales
//   , count(*) number_sales
//   FROM
//     store_sales
//   , item
//   , date_dim
//   WHERE (ss_item_sk IN (
//      SELECT ss_item_sk
//      FROM
//        cross_items
//   ))
//      AND (ss_item_sk = i_item_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//      AND (d_week_seq = (
//         SELECT d_week_seq
//         FROM
//           date_dim
//         WHERE (d_year = (1999 + 1))
//            AND (d_moy = 12)
//            AND (d_dom = 11)
//      ))
//   GROUP BY i_brand_id, i_class_id, i_category_id
//   HAVING (sum((ss_quantity * ss_list_price)) > (
//         SELECT average_sales
//         FROM
//           avg_sales
//      ))
//)  this_year
//, (
//   SELECT
//     'store' channel
//   , i_brand_id
//   , i_class_id
//   , i_category_id
//   , sum((ss_quantity * ss_list_price)) sales
//   , count(*) number_sales
//   FROM
//     store_sales
//   , item
//   , date_dim
//   WHERE (ss_item_sk IN (
//      SELECT ss_item_sk
//      FROM
//        cross_items
//   ))
//      AND (ss_item_sk = i_item_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//      AND (d_week_seq = (
//         SELECT d_week_seq
//         FROM
//           date_dim
//         WHERE (d_year = 1999)
//            AND (d_moy = 12)
//            AND (d_dom = 11)
//      ))
//   GROUP BY i_brand_id, i_class_id, i_category_id
//   HAVING (sum((ss_quantity * ss_list_price)) > (
//         SELECT average_sales
//         FROM
//           avg_sales
//      ))
//)  last_year
//WHERE (this_year.i_brand_id = last_year.i_brand_id)
//   AND (this_year.i_class_id = last_year.i_class_id)
//   AND (this_year.i_category_id = last_year.i_category_id)
//ORDER BY this_year.channel ASC, this_year.i_brand_id ASC, this_year.i_class_id ASC, this_year.i_category_id ASC
//LIMIT 100;
//"""
//    def query14 = """
//WITH
//  cross_items AS (
//   SELECT i_item_sk ss_item_sk
//   FROM
//     item
//   , (
//      SELECT
//        iss.i_brand_id brand_id
//      , iss.i_class_id class_id
//      , iss.i_category_id category_id
//      FROM
//        store_sales
//      , item iss
//      , date_dim d1
//      WHERE (ss_item_sk = iss.i_item_sk)
//         AND (ss_sold_date_sk = d1.d_date_sk)
//         AND (d1.d_year BETWEEN 1999 AND (1999 + 2))
//INTERSECT       SELECT
//        ics.i_brand_id
//      , ics.i_class_id
//      , ics.i_category_id
//      FROM
//        catalog_sales
//      , item ics
//      , date_dim d2
//      WHERE (cs_item_sk = ics.i_item_sk)
//         AND (cs_sold_date_sk = d2.d_date_sk)
//         AND (d2.d_year BETWEEN 1999 AND (1999 + 2))
//INTERSECT       SELECT
//        iws.i_brand_id
//      , iws.i_class_id
//      , iws.i_category_id
//      FROM
//        web_sales
//      , item iws
//      , date_dim d3
//      WHERE (ws_item_sk = iws.i_item_sk)
//         AND (ws_sold_date_sk = d3.d_date_sk)
//         AND (d3.d_year BETWEEN 1999 AND (1999 + 2))
//   )  x
//   WHERE (i_brand_id = brand_id)
//      AND (i_class_id = class_id)
//      AND (i_category_id = category_id)
//)
//, avg_sales AS (
//   SELECT avg((quantity * list_price)) average_sales
//   FROM
//     (
//      SELECT
//        ss_quantity quantity
//      , ss_list_price list_price
//      FROM
//        store_sales
//      , date_dim
//      WHERE (ss_sold_date_sk = d_date_sk)
//         AND (d_year BETWEEN 1999 AND (1999 + 2))
//UNION ALL       SELECT
//        cs_quantity quantity
//      , cs_list_price list_price
//      FROM
//        catalog_sales
//      , date_dim
//      WHERE (cs_sold_date_sk = d_date_sk)
//         AND (d_year BETWEEN 1999 AND (1999 + 2))
//UNION ALL       SELECT
//        ws_quantity quantity
//      , ws_list_price list_price
//      FROM
//        web_sales
//      , date_dim
//      WHERE (ws_sold_date_sk = d_date_sk)
//         AND (d_year BETWEEN 1999 AND (1999 + 2))
//   ) y
//)
//SELECT *
//FROM
//  (
//   SELECT
//     'store' channel
//   , i_brand_id
//   , i_class_id
//   , i_category_id
//   , sum((ss_quantity * ss_list_price)) sales
//   , count(*) number_sales
//   FROM
//     store_sales
//   , item
//   , date_dim
//   WHERE (ss_item_sk IN (
//      SELECT ss_item_sk
//      FROM
//        cross_items
//   ))
//      AND (ss_item_sk = i_item_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//      AND (d_week_seq = (
//         SELECT d_week_seq
//         FROM
//           date_dim
//         WHERE (d_year = (1999 + 1))
//            AND (d_moy = 12)
//            AND (d_dom = 11)
//      ))
//   GROUP BY i_brand_id, i_class_id, i_category_id
//   HAVING (sum((ss_quantity * ss_list_price)) > (
//         SELECT average_sales
//         FROM
//           avg_sales
//      ))
//)  this_year
//, (
//   SELECT
//     'store' channel
//   , i_brand_id
//   , i_class_id
//   , i_category_id
//   , sum((ss_quantity * ss_list_price)) sales
//   , count(*) number_sales
//   FROM
//     store_sales
//   , item
//   , date_dim
//   WHERE (ss_item_sk IN (
//      SELECT ss_item_sk
//      FROM
//        cross_items
//   ))
//      AND (ss_item_sk = i_item_sk)
//      AND (ss_sold_date_sk = d_date_sk)
//      AND (d_week_seq = (
//         SELECT d_week_seq
//         FROM
//           date_dim
//         WHERE (d_year = 1999)
//            AND (d_moy = 12)
//            AND (d_dom = 11)
//      ))
//   GROUP BY i_brand_id, i_class_id, i_category_id
//   HAVING (sum((ss_quantity * ss_list_price)) > (
//         SELECT average_sales
//         FROM
//           avg_sales
//      ))
//)  last_year
//WHERE (this_year.i_brand_id = last_year.i_brand_id)
//   AND (this_year.i_class_id = last_year.i_class_id)
//   AND (this_year.i_category_id = last_year.i_category_id)
//ORDER BY this_year.channel ASC, this_year.i_brand_id ASC, this_year.i_class_id ASC, this_year.i_category_id ASC
//LIMIT 100;
//
//"""
//    order_qt_query14_before "${query14}"
//    async_mv_rewrite_fail(db, mv14, query14, "mv14")
//    order_qt_query14_after "${query14}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv14"""
//
//    def mv15 = """
//SELECT
//  ca_zip
//, sum(cs_sales_price)
//FROM
//  catalog_sales
//, customer
//, customer_address
//, date_dim
//WHERE (cs_bill_customer_sk = c_customer_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
//      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
//      OR (cs_sales_price > 500))
//   AND (cs_sold_date_sk = d_date_sk)
//   AND (d_qoy = 2)
//   AND (d_year = 2001)
//GROUP BY ca_zip
//ORDER BY ca_zip ASC
//LIMIT 100;
//"""
//    def query15 = """
//SELECT
//  ca_zip
//, sum(cs_sales_price)
//FROM
//  catalog_sales
//, customer
//, customer_address
//, date_dim
//WHERE (cs_bill_customer_sk = c_customer_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
//      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
//      OR (cs_sales_price > 500))
//   AND (cs_sold_date_sk = d_date_sk)
//   AND (d_qoy = 2)
//   AND (d_year = 2001)
//GROUP BY ca_zip
//ORDER BY ca_zip ASC
//LIMIT 100;
//"""
//    order_qt_query15_before "${query15}"
//    async_mv_rewrite_fail(db, mv15, query15, "mv15")
//    order_qt_query15_after "${query15}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15"""
//
//    def mv15_1 = """
//SELECT
//  ca_zip
//, sum(cs_sales_price)
//FROM
//  catalog_sales
//, customer
//, customer_address
//, date_dim
//WHERE (cs_bill_customer_sk = c_customer_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
//      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
//      OR (cs_sales_price > 500))
//   AND (cs_sold_date_sk = d_date_sk)
//   AND (d_qoy = 2)
//   AND (d_year = 2001)
//GROUP BY ca_zip;
//"""
//    def query15_1 = """
//SELECT
//  ca_zip
//, sum(cs_sales_price)
//FROM
//  catalog_sales
//, customer
//, customer_address
//, date_dim
//WHERE (cs_bill_customer_sk = c_customer_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
//      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
//      OR (cs_sales_price > 500))
//   AND (cs_sold_date_sk = d_date_sk)
//   AND (d_qoy = 2)
//   AND (d_year = 2001)
//GROUP BY ca_zip
//ORDER BY ca_zip ASC
//LIMIT 100;
//"""
//    order_qt_query15_1_before "${query15_1}"
//    async_mv_rewrite_success(db, mv15_1, query15_1, "mv15_1")
//    order_qt_query15_1_after "${query15_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv15_1"""
//
//    def mv16 = """
//SELECT
//  count(DISTINCT cs_order_number) 'order count'
//, sum(cs_ext_ship_cost) 'total shipping cost'
//, sum(cs_net_profit) 'total net profit'
//FROM
//  catalog_sales cs1
//, date_dim
//, customer_address
//, call_center
//WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
//   AND (cs1.cs_ship_date_sk = d_date_sk)
//   AND (cs1.cs_ship_addr_sk = ca_address_sk)
//   AND (ca_state = 'GA')
//   AND (cs1.cs_call_center_sk = cc_call_center_sk)
//   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
//   AND (EXISTS (
//   SELECT *
//   FROM
//     catalog_sales cs2
//   WHERE (cs1.cs_order_number = cs2.cs_order_number)
//      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
//))
//   AND (NOT (EXISTS (
//   SELECT *
//   FROM
//     catalog_returns cr1
//   WHERE (cs1.cs_order_number = cr1.cr_order_number)
//)))
//ORDER BY count(DISTINCT cs_order_number) ASC
//LIMIT 100;
//"""
//    def query16 = """
//SELECT
//  count(DISTINCT cs_order_number) 'order count'
//, sum(cs_ext_ship_cost) 'total shipping cost'
//, sum(cs_net_profit) 'total net profit'
//FROM
//  catalog_sales cs1
//, date_dim
//, customer_address
//, call_center
//WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
//   AND (cs1.cs_ship_date_sk = d_date_sk)
//   AND (cs1.cs_ship_addr_sk = ca_address_sk)
//   AND (ca_state = 'GA')
//   AND (cs1.cs_call_center_sk = cc_call_center_sk)
//   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
//   AND (EXISTS (
//   SELECT *
//   FROM
//     catalog_sales cs2
//   WHERE (cs1.cs_order_number = cs2.cs_order_number)
//      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
//))
//   AND (NOT (EXISTS (
//   SELECT *
//   FROM
//     catalog_returns cr1
//   WHERE (cs1.cs_order_number = cr1.cr_order_number)
//)))
//ORDER BY count(DISTINCT cs_order_number) ASC
//LIMIT 100;
//"""
//    order_qt_query16_before "${query16}"
//    async_mv_rewrite_fail(db, mv16, query16, "mv16")
//    order_qt_query16_after "${query16}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16"""
//
//    def mv16_1 = """
//SELECT
//  count(DISTINCT cs_order_number) 'order count'
//, sum(cs_ext_ship_cost) 'total shipping cost'
//, sum(cs_net_profit) 'total net profit'
//FROM
//  catalog_sales cs1
//, date_dim
//, customer_address
//, call_center
//WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
//   AND (cs1.cs_ship_date_sk = d_date_sk)
//   AND (cs1.cs_ship_addr_sk = ca_address_sk)
//   AND (ca_state = 'GA')
//   AND (cs1.cs_call_center_sk = cc_call_center_sk)
//   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
//   AND (EXISTS (
//   SELECT *
//   FROM
//     catalog_sales cs2
//   WHERE (cs1.cs_order_number = cs2.cs_order_number)
//      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
//))
//   AND (NOT (EXISTS (
//   SELECT *
//   FROM
//     catalog_returns cr1
//   WHERE (cs1.cs_order_number = cr1.cr_order_number)
//)));
//"""
//    def query16_1 = """
//SELECT
//  count(DISTINCT cs_order_number) 'order count'
//, sum(cs_ext_ship_cost) 'total shipping cost'
//, sum(cs_net_profit) 'total net profit'
//FROM
//  catalog_sales cs1
//, date_dim
//, customer_address
//, call_center
//WHERE (d_date BETWEEN CAST('2002-2-01' AS DATE) AND (CAST('2002-2-01' AS DATE) + INTERVAL  '60' DAY))
//   AND (cs1.cs_ship_date_sk = d_date_sk)
//   AND (cs1.cs_ship_addr_sk = ca_address_sk)
//   AND (ca_state = 'GA')
//   AND (cs1.cs_call_center_sk = cc_call_center_sk)
//   AND (cc_county IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))
//   AND (EXISTS (
//   SELECT *
//   FROM
//     catalog_sales cs2
//   WHERE (cs1.cs_order_number = cs2.cs_order_number)
//      AND (cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
//))
//   AND (NOT (EXISTS (
//   SELECT *
//   FROM
//     catalog_returns cr1
//   WHERE (cs1.cs_order_number = cr1.cr_order_number)
//)))
//ORDER BY count(DISTINCT cs_order_number) ASC
//LIMIT 100;
//"""
//    order_qt_query16_1_before "${query16_1}"
//    async_mv_rewrite_success(db, mv16_1, query16_1, "mv16_1")
//    order_qt_query16_1_after "${query16_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv16_1"""
//
//    def mv17 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_state
//, count(ss_quantity) store_sales_quantitycount
//, avg(ss_quantity) store_sales_quantityave
//, stddev_samp(ss_quantity) store_sales_quantitystdev
//, (stddev_samp(ss_quantity) / avg(ss_quantity)) store_sales_quantitycov
//, count(sr_return_quantity) store_returns_quantitycount
//, avg(sr_return_quantity) store_returns_quantityave
//, stddev_samp(sr_return_quantity) store_returns_quantitystdev
//, (stddev_samp(sr_return_quantity) / avg(sr_return_quantity)) store_returns_quantitycov
//, count(cs_quantity) catalog_sales_quantitycount
//, avg(cs_quantity) catalog_sales_quantityave
//, stddev_samp(cs_quantity) catalog_sales_quantitystdev
//, (stddev_samp(cs_quantity) / avg(cs_quantity)) catalog_sales_quantitycov
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_quarter_name = '2001Q1')
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
//GROUP BY i_item_id, i_item_desc, s_state
//ORDER BY i_item_id ASC, i_item_desc ASC, s_state ASC
//LIMIT 100;
//"""
//    def query17 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_state
//, count(ss_quantity) store_sales_quantitycount
//, avg(ss_quantity) store_sales_quantityave
//, stddev_samp(ss_quantity) store_sales_quantitystdev
//, (stddev_samp(ss_quantity) / avg(ss_quantity)) store_sales_quantitycov
//, count(sr_return_quantity) store_returns_quantitycount
//, avg(sr_return_quantity) store_returns_quantityave
//, stddev_samp(sr_return_quantity) store_returns_quantitystdev
//, (stddev_samp(sr_return_quantity) / avg(sr_return_quantity)) store_returns_quantitycov
//, count(cs_quantity) catalog_sales_quantitycount
//, avg(cs_quantity) catalog_sales_quantityave
//, stddev_samp(cs_quantity) catalog_sales_quantitystdev
//, (stddev_samp(cs_quantity) / avg(cs_quantity)) catalog_sales_quantitycov
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_quarter_name = '2001Q1')
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
//GROUP BY i_item_id, i_item_desc, s_state
//ORDER BY i_item_id ASC, i_item_desc ASC, s_state ASC
//LIMIT 100;
//"""
//    order_qt_query17_before "${query17}"
//    async_mv_rewrite_fail(db, mv17, query17, "mv17")
//    order_qt_query17_after "${query17}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17"""
//
//    def mv17_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_state
//, count(ss_quantity) store_sales_quantitycount
//, avg(ss_quantity) store_sales_quantityave
//, stddev_samp(ss_quantity) store_sales_quantitystdev
//, (stddev_samp(ss_quantity) / avg(ss_quantity)) store_sales_quantitycov
//, count(sr_return_quantity) store_returns_quantitycount
//, avg(sr_return_quantity) store_returns_quantityave
//, stddev_samp(sr_return_quantity) store_returns_quantitystdev
//, (stddev_samp(sr_return_quantity) / avg(sr_return_quantity)) store_returns_quantitycov
//, count(cs_quantity) catalog_sales_quantitycount
//, avg(cs_quantity) catalog_sales_quantityave
//, stddev_samp(cs_quantity) catalog_sales_quantitystdev
//, (stddev_samp(cs_quantity) / avg(cs_quantity)) catalog_sales_quantitycov
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_quarter_name = '2001Q1')
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
//GROUP BY i_item_id, i_item_desc, s_state;
//"""
//    def query17_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_state
//, count(ss_quantity) store_sales_quantitycount
//, avg(ss_quantity) store_sales_quantityave
//, stddev_samp(ss_quantity) store_sales_quantitystdev
//, (stddev_samp(ss_quantity) / avg(ss_quantity)) store_sales_quantitycov
//, count(sr_return_quantity) store_returns_quantitycount
//, avg(sr_return_quantity) store_returns_quantityave
//, stddev_samp(sr_return_quantity) store_returns_quantitystdev
//, (stddev_samp(sr_return_quantity) / avg(sr_return_quantity)) store_returns_quantitycov
//, count(cs_quantity) catalog_sales_quantitycount
//, avg(cs_quantity) catalog_sales_quantityave
//, stddev_samp(cs_quantity) catalog_sales_quantitystdev
//, (stddev_samp(cs_quantity) / avg(cs_quantity)) catalog_sales_quantitycov
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_quarter_name = '2001Q1')
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
//GROUP BY i_item_id, i_item_desc, s_state
//ORDER BY i_item_id ASC, i_item_desc ASC, s_state ASC
//LIMIT 100;
//"""
//    order_qt_query17_1_before "${query17_1}"
//    async_mv_rewrite_success(db, mv17_1, query17_1, "mv17_1")
//    order_qt_query17_1_after "${query17_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv17_1"""
//
//    def mv18 = """
//SELECT
//  i_item_id
//, ca_country
//, ca_state
//, ca_county
//, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
//, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
//, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
//, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
//, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
//, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
//, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
//FROM
//  catalog_sales
//, customer_demographics cd1
//, customer_demographics cd2
//, customer
//, customer_address
//, date_dim
//, item
//WHERE (cs_sold_date_sk = d_date_sk)
//   AND (cs_item_sk = i_item_sk)
//   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
//   AND (cs_bill_customer_sk = c_customer_sk)
//   AND (cd1.cd_gender = 'F')
//   AND (cd1.cd_education_status = 'Unknown')
//   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
//   AND (d_year = 1998)
//   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
//GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
//ORDER BY ca_country ASC, ca_state ASC, ca_county ASC, i_item_id ASC
//LIMIT 100;
//"""
//    def query18 = """
//SELECT
//  i_item_id
//, ca_country
//, ca_state
//, ca_county
//, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
//, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
//, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
//, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
//, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
//, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
//, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
//FROM
//  catalog_sales
//, customer_demographics cd1
//, customer_demographics cd2
//, customer
//, customer_address
//, date_dim
//, item
//WHERE (cs_sold_date_sk = d_date_sk)
//   AND (cs_item_sk = i_item_sk)
//   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
//   AND (cs_bill_customer_sk = c_customer_sk)
//   AND (cd1.cd_gender = 'F')
//   AND (cd1.cd_education_status = 'Unknown')
//   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
//   AND (d_year = 1998)
//   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
//GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
//ORDER BY ca_country ASC, ca_state ASC, ca_county ASC, i_item_id ASC
//LIMIT 100;
//"""
//    order_qt_query18_before "${query18}"
//    async_mv_rewrite_fail(db, mv18, query18, "mv18")
//    order_qt_query18_after "${query18}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18"""
//
//    def mv18_1 = """
//SELECT
//  i_item_id
//, ca_country
//, ca_state
//, ca_county
//, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
//, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
//, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
//, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
//, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
//, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
//, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
//FROM
//  catalog_sales
//, customer_demographics cd1
//, customer_demographics cd2
//, customer
//, customer_address
//, date_dim
//, item
//WHERE (cs_sold_date_sk = d_date_sk)
//   AND (cs_item_sk = i_item_sk)
//   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
//   AND (cs_bill_customer_sk = c_customer_sk)
//   AND (cd1.cd_gender = 'F')
//   AND (cd1.cd_education_status = 'Unknown')
//   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
//   AND (d_year = 1998)
//   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
//GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county);
//"""
//    def query18_1 = """
//SELECT
//  i_item_id
//, ca_country
//, ca_state
//, ca_county
//, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
//, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
//, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
//, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
//, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
//, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
//, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
//FROM
//  catalog_sales
//, customer_demographics cd1
//, customer_demographics cd2
//, customer
//, customer_address
//, date_dim
//, item
//WHERE (cs_sold_date_sk = d_date_sk)
//   AND (cs_item_sk = i_item_sk)
//   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
//   AND (cs_bill_customer_sk = c_customer_sk)
//   AND (cd1.cd_gender = 'F')
//   AND (cd1.cd_education_status = 'Unknown')
//   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
//   AND (d_year = 1998)
//   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
//GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
//ORDER BY ca_country ASC, ca_state ASC, ca_county ASC, i_item_id ASC
//LIMIT 100;
//"""
//    order_qt_query18_1_before "${query18_1}"
//    // should success but not
//    async_mv_rewrite_fail(db, mv18_1, query18_1, "mv18_1")
//    order_qt_query18_1_after "${query18_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv18_1"""

//    def mv19 = """
//SELECT
//  i_brand_id brand_id
//, i_brand brand
//, i_manufact_id
//, i_manufact
//, sum(ss_ext_sales_price) ext_price
//FROM
//  date_dim
//, store_sales
//, item
//, customer
//, customer_address
//, store
//WHERE (d_date_sk = ss_sold_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (i_manager_id = 8)
//   AND (d_moy = 11)
//   AND (d_year = 1998)
//   AND (ss_customer_sk = c_customer_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
//   AND (ss_store_sk = s_store_sk)
//GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
//ORDER BY ext_price DESC, i_brand ASC, i_brand_id ASC, i_manufact_id ASC, i_manufact ASC
//LIMIT 100;
//"""
//    def query19 = """
//SELECT
//  i_brand_id brand_id
//, i_brand brand
//, i_manufact_id
//, i_manufact
//, sum(ss_ext_sales_price) ext_price
//FROM
//  date_dim
//, store_sales
//, item
//, customer
//, customer_address
//, store
//WHERE (d_date_sk = ss_sold_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (i_manager_id = 8)
//   AND (d_moy = 11)
//   AND (d_year = 1998)
//   AND (ss_customer_sk = c_customer_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
//   AND (ss_store_sk = s_store_sk)
//GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
//ORDER BY ext_price DESC, i_brand ASC, i_brand_id ASC, i_manufact_id ASC, i_manufact ASC
//LIMIT 100;
//"""
//    order_qt_query19_before "${query19}"
//    async_mv_rewrite_fail(db, mv19, query19, "mv19")
//    order_qt_query19_after "${query19}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19"""
//
//    def mv19_1 = """
//SELECT
//  i_brand_id brand_id
//, i_brand brand
//, i_manufact_id
//, i_manufact
//, sum(ss_ext_sales_price) ext_price
//FROM
//  date_dim
//, store_sales
//, item
//, customer
//, customer_address
//, store
//WHERE (d_date_sk = ss_sold_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (i_manager_id = 8)
//   AND (d_moy = 11)
//   AND (d_year = 1998)
//   AND (ss_customer_sk = c_customer_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
//   AND (ss_store_sk = s_store_sk)
//GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact;
//"""
//    def query19_1 = """
//SELECT
//  i_brand_id brand_id
//, i_brand brand
//, i_manufact_id
//, i_manufact
//, sum(ss_ext_sales_price) ext_price
//FROM
//  date_dim
//, store_sales
//, item
//, customer
//, customer_address
//, store
//WHERE (d_date_sk = ss_sold_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (i_manager_id = 8)
//   AND (d_moy = 11)
//   AND (d_year = 1998)
//   AND (ss_customer_sk = c_customer_sk)
//   AND (c_current_addr_sk = ca_address_sk)
//   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
//   AND (ss_store_sk = s_store_sk)
//GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
//ORDER BY ext_price DESC, i_brand ASC, i_brand_id ASC, i_manufact_id ASC, i_manufact ASC
//LIMIT 100;
//"""
//    order_qt_query19_1_before "${query19_1}"
//    async_mv_rewrite_success(db, mv19_1, query19_1, "mv19_1")
//    order_qt_query19_1_after "${query19_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv19_1"""
//
//    def mv20 = """
//SELECT
//  i_item_id
//, i_item_desc
//, i_category
//, i_class
//, i_current_price
//, sum(cs_ext_sales_price) itemrevenue
//, ((sum(cs_ext_sales_price) * 100) / sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
//FROM
//  catalog_sales
//, item
//, date_dim
//WHERE (cs_item_sk = i_item_sk)
//   AND (i_category IN ('Sports', 'Books', 'Home'))
//   AND (cs_sold_date_sk = d_date_sk)
//   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
//GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
//ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
//LIMIT 100;
//"""
//    def query20 = """
//SELECT
//  i_item_id
//, i_item_desc
//, i_category
//, i_class
//, i_current_price
//, sum(cs_ext_sales_price) itemrevenue
//, ((sum(cs_ext_sales_price) * 100) / sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
//FROM
//  catalog_sales
//, item
//, date_dim
//WHERE (cs_item_sk = i_item_sk)
//   AND (i_category IN ('Sports', 'Books', 'Home'))
//   AND (cs_sold_date_sk = d_date_sk)
//   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
//GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
//ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
//LIMIT 100;
//"""
//    order_qt_query20_before "${query20}"
//    async_mv_rewrite_fail(db, mv20, query20, "mv20")
//    order_qt_query20_after "${query20}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20"""
//
//    def mv20_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, i_category
//, i_class
//, i_current_price
//, sum(cs_ext_sales_price) itemrevenue
//, ((sum(cs_ext_sales_price) * 100) / sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
//FROM
//  catalog_sales
//, item
//, date_dim
//WHERE (cs_item_sk = i_item_sk)
//   AND (i_category IN ('Sports', 'Books', 'Home'))
//   AND (cs_sold_date_sk = d_date_sk)
//   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
//GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price;
//"""
//    def query20_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, i_category
//, i_class
//, i_current_price
//, sum(cs_ext_sales_price) itemrevenue
//, ((sum(cs_ext_sales_price) * 100) / sum(sum(cs_ext_sales_price)) OVER (PARTITION BY i_class)) revenueratio
//FROM
//  catalog_sales
//, item
//, date_dim
//WHERE (cs_item_sk = i_item_sk)
//   AND (i_category IN ('Sports', 'Books', 'Home'))
//   AND (cs_sold_date_sk = d_date_sk)
//   AND (CAST(d_date AS DATE) BETWEEN CAST('1999-02-22' AS DATE) AND (CAST('1999-02-22' AS DATE) + INTERVAL  '30' DAY))
//GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price
//ORDER BY i_category ASC, i_class ASC, i_item_id ASC, i_item_desc ASC, revenueratio ASC
//LIMIT 100;
//"""
//    order_qt_query20_1_before "${query20_1}"
//    async_mv_rewrite_fail(db, mv20_1, query20_1, "mv20_1")
//    order_qt_query20_1_after "${query20_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv20_1"""

//    def mv21 = """
//SELECT *
//FROM
//  (
//   SELECT
//     w_warehouse_name
//   , i_item_id
//   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
//   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
//   FROM
//     inventory
//   , warehouse
//   , item
//   , date_dim
//   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
//      AND (i_item_sk = inv_item_sk)
//      AND (inv_warehouse_sk = w_warehouse_sk)
//      AND (inv_date_sk = d_date_sk)
//      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
//   GROUP BY w_warehouse_name, i_item_id
//)  x
//WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))))
//ORDER BY w_warehouse_name ASC, i_item_id ASC
//LIMIT 100;
//"""
//    def query21 = """
//SELECT *
//FROM
//  (
//   SELECT
//     w_warehouse_name
//   , i_item_id
//   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
//   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
//   FROM
//     inventory
//   , warehouse
//   , item
//   , date_dim
//   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
//      AND (i_item_sk = inv_item_sk)
//      AND (inv_warehouse_sk = w_warehouse_sk)
//      AND (inv_date_sk = d_date_sk)
//      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
//   GROUP BY w_warehouse_name, i_item_id
//)  x
//WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))))
//ORDER BY w_warehouse_name ASC, i_item_id ASC
//LIMIT 100;
//"""
//    order_qt_query21_before "${query21}"
//    async_mv_rewrite_fail(db, mv21, query21, "mv21")
//    order_qt_query21_after "${query21}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv21"""
//
//    def mv21_1 = """
//SELECT *
//FROM
//  (
//   SELECT
//     w_warehouse_name
//   , i_item_id
//   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
//   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
//   FROM
//     inventory
//   , warehouse
//   , item
//   , date_dim
//   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
//      AND (i_item_sk = inv_item_sk)
//      AND (inv_warehouse_sk = w_warehouse_sk)
//      AND (inv_date_sk = d_date_sk)
//      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
//   GROUP BY w_warehouse_name, i_item_id
//)  x
//WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))));
//"""
//    def query21_1 = """
//SELECT *
//FROM
//  (
//   SELECT
//     w_warehouse_name
//   , i_item_id
//   , sum((CASE WHEN (CAST(d_date AS DATE) < CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_before
//   , sum((CASE WHEN (CAST(d_date AS DATE) >= CAST('2000-03-11' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END)) inv_after
//   FROM
//     inventory
//   , warehouse
//   , item
//   , date_dim
//   WHERE (i_current_price BETWEEN CAST('0.99' AS DECIMAL(3,2)) AND CAST('1.49' AS DECIMAL(3,2)))
//      AND (i_item_sk = inv_item_sk)
//      AND (inv_warehouse_sk = w_warehouse_sk)
//      AND (inv_date_sk = d_date_sk)
//      AND (d_date BETWEEN (CAST('2000-03-11' AS DATE) - INTERVAL  '30' DAY) AND (CAST('2000-03-11' AS DATE) + INTERVAL  '30' DAY))
//   GROUP BY w_warehouse_name, i_item_id
//)  x
//WHERE ((CASE WHEN (inv_before > 0) THEN (CAST(inv_after AS DECIMAL(7,2)) / inv_before) ELSE null END) BETWEEN (CAST('2.00' AS DECIMAL(3,2)) / CAST('3.00' AS DECIMAL(3,2))) AND (CAST('3.00' AS DECIMAL(3,2)) / CAST('2.00' AS DECIMAL(3,2))))
//ORDER BY w_warehouse_name ASC, i_item_id ASC
//LIMIT 100;
//"""
//    order_qt_query21_1_before "${query21_1}"
//    async_mv_rewrite_success(db, mv21_1, query21_1, "mv21_1")
//    order_qt_query21_1_after "${query21_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv21_1"""
//
//    def mv22 = """
//SELECT
//  i_product_name
//, i_brand
//, i_class
//, i_category
//, avg(inv_quantity_on_hand) qoh
//FROM
//  inventory
//, date_dim
//, item
//WHERE (inv_date_sk = d_date_sk)
//   AND (inv_item_sk = i_item_sk)
//   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
//GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
//ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC
//LIMIT 100;
//"""
//    def query22 = """
//SELECT
//  i_product_name
//, i_brand
//, i_class
//, i_category
//, avg(inv_quantity_on_hand) qoh
//FROM
//  inventory
//, date_dim
//, item
//WHERE (inv_date_sk = d_date_sk)
//   AND (inv_item_sk = i_item_sk)
//   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
//GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
//ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC
//LIMIT 100;
//"""
//    order_qt_query22_before "${query22}"
//    async_mv_rewrite_fail(db, mv22, query22, "mv22")
//    order_qt_query22_after "${query22}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv22"""
//
//    def mv22_1 = """
//SELECT
//  i_product_name
//, i_brand
//, i_class
//, i_category
//, avg(inv_quantity_on_hand) qoh
//FROM
//  inventory
//, date_dim
//, item
//WHERE (inv_date_sk = d_date_sk)
//   AND (inv_item_sk = i_item_sk)
//   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
//GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category);
//"""
//    def query22_1 = """
//SELECT
//  i_product_name
//, i_brand
//, i_class
//, i_category
//, avg(inv_quantity_on_hand) qoh
//FROM
//  inventory
//, date_dim
//, item
//WHERE (inv_date_sk = d_date_sk)
//   AND (inv_item_sk = i_item_sk)
//   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
//GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
//ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC
//LIMIT 100;
//"""
//    order_qt_query22_1_before "${query22_1}"
//    // should success but fail
//    async_mv_rewrite_fail(db, mv22_1, query22_1, "mv22_1")
//    order_qt_query22_1_after "${query22_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv22_1"""
//
//    def mv23 = """
//WITH
//  frequent_ss_items AS (
//   SELECT
//     substr(i_item_desc, 1, 30) itemdesc
//   , i_item_sk item_sk
//   , d_date solddate
//   , count(*) cnt
//   FROM
//     store_sales
//   , date_dim
//   , item
//   WHERE (ss_sold_date_sk = d_date_sk)
//      AND (ss_item_sk = i_item_sk)
//      AND (d_year IN (2000   , (2000 + 1)   , (2000 + 2)   , (2000 + 3)))
//   GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date
//   HAVING (count(*) > 4)
//)
//, max_store_sales AS (
//   SELECT max(csales) tpcds_cmax
//   FROM
//     (
//      SELECT
//        c_customer_sk
//      , sum((ss_quantity * ss_sales_price)) csales
//      FROM
//        store_sales
//      , customer
//      , date_dim
//      WHERE (ss_customer_sk = c_customer_sk)
//         AND (ss_sold_date_sk = d_date_sk)
//         AND (d_year IN (2000      , (2000 + 1)      , (2000 + 2)      , (2000 + 3)))
//      GROUP BY c_customer_sk
//   ) x
//)
//, best_ss_customer AS (
//   SELECT
//     c_customer_sk
//   , sum((ss_quantity * ss_sales_price)) ssales
//   FROM
//     store_sales
//   , customer
//   WHERE (ss_customer_sk = c_customer_sk)
//   GROUP BY c_customer_sk
//   HAVING (sum((ss_quantity * ss_sales_price)) > ((50 / CAST('100.0' AS DECIMAL(5,2))) * (
//            SELECT *
//            FROM
//              max_store_sales
//         )))
//)
//SELECT sum(sales)
//FROM
//  (
//   SELECT (cs_quantity * cs_list_price) sales
//   FROM
//     catalog_sales
//   , date_dim
//   WHERE (d_year = 2000)
//      AND (d_moy = 2)
//      AND (cs_sold_date_sk = d_date_sk)
//      AND (cs_item_sk IN (
//      SELECT item_sk
//      FROM
//        frequent_ss_items
//   ))
//      AND (cs_bill_customer_sk IN (
//      SELECT c_customer_sk
//      FROM
//        best_ss_customer
//   ))
//UNION ALL    SELECT (ws_quantity * ws_list_price) sales
//   FROM
//     web_sales
//   , date_dim
//   WHERE (d_year = 2000)
//      AND (d_moy = 2)
//      AND (ws_sold_date_sk = d_date_sk)
//      AND (ws_item_sk IN (
//      SELECT item_sk
//      FROM
//        frequent_ss_items
//   ))
//      AND (ws_bill_customer_sk IN (
//      SELECT c_customer_sk
//      FROM
//        best_ss_customer
//   ))
//) y
//LIMIT 100;
//"""
//    def query23 = """
//WITH
//  frequent_ss_items AS (
//   SELECT
//     substr(i_item_desc, 1, 30) itemdesc
//   , i_item_sk item_sk
//   , d_date solddate
//   , count(*) cnt
//   FROM
//     store_sales
//   , date_dim
//   , item
//   WHERE (ss_sold_date_sk = d_date_sk)
//      AND (ss_item_sk = i_item_sk)
//      AND (d_year IN (2000   , (2000 + 1)   , (2000 + 2)   , (2000 + 3)))
//   GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date
//   HAVING (count(*) > 4)
//)
//, max_store_sales AS (
//   SELECT max(csales) tpcds_cmax
//   FROM
//     (
//      SELECT
//        c_customer_sk
//      , sum((ss_quantity * ss_sales_price)) csales
//      FROM
//        store_sales
//      , customer
//      , date_dim
//      WHERE (ss_customer_sk = c_customer_sk)
//         AND (ss_sold_date_sk = d_date_sk)
//         AND (d_year IN (2000      , (2000 + 1)      , (2000 + 2)      , (2000 + 3)))
//      GROUP BY c_customer_sk
//   ) x
//)
//, best_ss_customer AS (
//   SELECT
//     c_customer_sk
//   , sum((ss_quantity * ss_sales_price)) ssales
//   FROM
//     store_sales
//   , customer
//   WHERE (ss_customer_sk = c_customer_sk)
//   GROUP BY c_customer_sk
//   HAVING (sum((ss_quantity * ss_sales_price)) > ((50 / CAST('100.0' AS DECIMAL(5,2))) * (
//            SELECT *
//            FROM
//              max_store_sales
//         )))
//)
//SELECT sum(sales)
//FROM
//  (
//   SELECT (cs_quantity * cs_list_price) sales
//   FROM
//     catalog_sales
//   , date_dim
//   WHERE (d_year = 2000)
//      AND (d_moy = 2)
//      AND (cs_sold_date_sk = d_date_sk)
//      AND (cs_item_sk IN (
//      SELECT item_sk
//      FROM
//        frequent_ss_items
//   ))
//      AND (cs_bill_customer_sk IN (
//      SELECT c_customer_sk
//      FROM
//        best_ss_customer
//   ))
//UNION ALL    SELECT (ws_quantity * ws_list_price) sales
//   FROM
//     web_sales
//   , date_dim
//   WHERE (d_year = 2000)
//      AND (d_moy = 2)
//      AND (ws_sold_date_sk = d_date_sk)
//      AND (ws_item_sk IN (
//      SELECT item_sk
//      FROM
//        frequent_ss_items
//   ))
//      AND (ws_bill_customer_sk IN (
//      SELECT c_customer_sk
//      FROM
//        best_ss_customer
//   ))
//) y
//LIMIT 100;
//"""
//    order_qt_query23_before "${query23}"
//    async_mv_rewrite_fail(db, mv23, query23, "mv23")
//    order_qt_query23_after "${query23}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv23"""
//
//    def mv24 = """
//WITH
//  ssales AS (
//   SELECT
//     c_last_name
//   , c_first_name
//   , s_store_name
//   , ca_state
//   , s_state
//   , i_color
//   , i_current_price
//   , i_manager_id
//   , i_units
//   , i_size
//   , sum(ss_net_paid) netpaid
//   FROM
//     store_sales
//   , store_returns
//   , store
//   , item
//   , customer
//   , customer_address
//   WHERE (ss_ticket_number = sr_ticket_number)
//      AND (ss_item_sk = sr_item_sk)
//      AND (ss_customer_sk = c_customer_sk)
//      AND (ss_item_sk = i_item_sk)
//      AND (ss_store_sk = s_store_sk)
//      AND (c_birth_country = upper(ca_country))
//      AND (s_zip = ca_zip)
//      AND (s_market_id = 8)
//   GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size
//)
//SELECT
//  c_last_name
//, c_first_name
//, s_store_name
//, sum(netpaid) paid
//FROM
//  ssales
//WHERE (i_color = 'pale')
//GROUP BY c_last_name, c_first_name, s_store_name
//HAVING (sum(netpaid) > (
//      SELECT (CAST('0.05' AS DECIMAL(5,2)) * avg(netpaid))
//      FROM
//        ssales
//   ))
//ORDER BY c_last_name, c_first_name, s_store_name;
//"""
//    def query24 = """
//WITH
//  ssales AS (
//   SELECT
//     c_last_name
//   , c_first_name
//   , s_store_name
//   , ca_state
//   , s_state
//   , i_color
//   , i_current_price
//   , i_manager_id
//   , i_units
//   , i_size
//   , sum(ss_net_paid) netpaid
//   FROM
//     store_sales
//   , store_returns
//   , store
//   , item
//   , customer
//   , customer_address
//   WHERE (ss_ticket_number = sr_ticket_number)
//      AND (ss_item_sk = sr_item_sk)
//      AND (ss_customer_sk = c_customer_sk)
//      AND (ss_item_sk = i_item_sk)
//      AND (ss_store_sk = s_store_sk)
//      AND (c_birth_country = upper(ca_country))
//      AND (s_zip = ca_zip)
//      AND (s_market_id = 8)
//   GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size
//)
//SELECT
//  c_last_name
//, c_first_name
//, s_store_name
//, sum(netpaid) paid
//FROM
//  ssales
//WHERE (i_color = 'pale')
//GROUP BY c_last_name, c_first_name, s_store_name
//HAVING (sum(netpaid) > (
//      SELECT (CAST('0.05' AS DECIMAL(5,2)) * avg(netpaid))
//      FROM
//        ssales
//   ))
//ORDER BY c_last_name, c_first_name, s_store_name;
//"""
//    order_qt_query24_before "${query24}"
//    async_mv_rewrite_fail(db, mv24, query24, "mv24")
//    order_qt_query24_after "${query24}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv24"""
//
//
//    def mv25 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_store_id
//, s_store_name
//, sum(ss_net_profit) store_sales_profit
//, sum(sr_net_loss) store_returns_loss
//, sum(cs_net_profit) catalog_sales_profit
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_moy = 4)
//   AND (d1.d_year = 2001)
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_moy BETWEEN 4 AND 10)
//   AND (d2.d_year = 2001)
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_moy BETWEEN 4 AND 10)
//   AND (d3.d_year = 2001)
//GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
//ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
//LIMIT 100;
//"""
//    def query25 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_store_id
//, s_store_name
//, sum(ss_net_profit) store_sales_profit
//, sum(sr_net_loss) store_returns_loss
//, sum(cs_net_profit) catalog_sales_profit
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_moy = 4)
//   AND (d1.d_year = 2001)
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_moy BETWEEN 4 AND 10)
//   AND (d2.d_year = 2001)
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_moy BETWEEN 4 AND 10)
//   AND (d3.d_year = 2001)
//GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
//ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
//LIMIT 100;
//"""
//    order_qt_query25_before "${query25}"
//    async_mv_rewrite_fail(db, mv25, query25, "mv25")
//    order_qt_query25_after "${query25}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25"""
//
//    def mv25_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_store_id
//, s_store_name
//, sum(ss_net_profit) store_sales_profit
//, sum(sr_net_loss) store_returns_loss
//, sum(cs_net_profit) catalog_sales_profit
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_moy = 4)
//   AND (d1.d_year = 2001)
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_moy BETWEEN 4 AND 10)
//   AND (d2.d_year = 2001)
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_moy BETWEEN 4 AND 10)
//   AND (d3.d_year = 2001)
//GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name;
//"""
//    def query25_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_store_id
//, s_store_name
//, sum(ss_net_profit) store_sales_profit
//, sum(sr_net_loss) store_returns_loss
//, sum(cs_net_profit) catalog_sales_profit
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_moy = 4)
//   AND (d1.d_year = 2001)
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_moy BETWEEN 4 AND 10)
//   AND (d2.d_year = 2001)
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_moy BETWEEN 4 AND 10)
//   AND (d3.d_year = 2001)
//GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
//ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
//LIMIT 100;
//"""
//    order_qt_query25_1_before "${query25_1}"
//    async_mv_rewrite_success(db, mv25_1, query25_1, "mv25_1")
//    order_qt_query25_1_after "${query25_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv25_1"""
//
//    def mv26 = """
//SELECT
//  i_item_id
//, avg(cs_quantity) agg1
//, avg(cs_list_price) agg2
//, avg(cs_coupon_amt) agg3
//, avg(cs_sales_price) agg4
//FROM
//  catalog_sales
//, customer_demographics
//, date_dim
//, item
//, promotion
//WHERE (cs_sold_date_sk = d_date_sk)
//   AND (cs_item_sk = i_item_sk)
//   AND (cs_bill_cdemo_sk = cd_demo_sk)
//   AND (cs_promo_sk = p_promo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND ((p_channel_email = 'N')
//      OR (p_channel_event = 'N'))
//   AND (d_year = 2000)
//GROUP BY i_item_id
//ORDER BY i_item_id ASC
//LIMIT 100;
//"""
//    def query26 = """
//SELECT
//  i_item_id
//, avg(cs_quantity) agg1
//, avg(cs_list_price) agg2
//, avg(cs_coupon_amt) agg3
//, avg(cs_sales_price) agg4
//FROM
//  catalog_sales
//, customer_demographics
//, date_dim
//, item
//, promotion
//WHERE (cs_sold_date_sk = d_date_sk)
//   AND (cs_item_sk = i_item_sk)
//   AND (cs_bill_cdemo_sk = cd_demo_sk)
//   AND (cs_promo_sk = p_promo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND ((p_channel_email = 'N')
//      OR (p_channel_event = 'N'))
//   AND (d_year = 2000)
//GROUP BY i_item_id
//ORDER BY i_item_id ASC
//LIMIT 100;
//"""
//    order_qt_query26_before "${query26}"
//    async_mv_rewrite_fail(db, mv26, query26, "mv26")
//    order_qt_query26_after "${query26}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv26"""
//
//    def mv26_1 = """
//SELECT
//  i_item_id
//, avg(cs_quantity) agg1
//, avg(cs_list_price) agg2
//, avg(cs_coupon_amt) agg3
//, avg(cs_sales_price) agg4
//FROM
//  catalog_sales
//, customer_demographics
//, date_dim
//, item
//, promotion
//WHERE (cs_sold_date_sk = d_date_sk)
//   AND (cs_item_sk = i_item_sk)
//   AND (cs_bill_cdemo_sk = cd_demo_sk)
//   AND (cs_promo_sk = p_promo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND ((p_channel_email = 'N')
//      OR (p_channel_event = 'N'))
//   AND (d_year = 2000)
//GROUP BY i_item_id;
//"""
//    def query26_1 = """
//SELECT
//  i_item_id
//, avg(cs_quantity) agg1
//, avg(cs_list_price) agg2
//, avg(cs_coupon_amt) agg3
//, avg(cs_sales_price) agg4
//FROM
//  catalog_sales
//, customer_demographics
//, date_dim
//, item
//, promotion
//WHERE (cs_sold_date_sk = d_date_sk)
//   AND (cs_item_sk = i_item_sk)
//   AND (cs_bill_cdemo_sk = cd_demo_sk)
//   AND (cs_promo_sk = p_promo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND ((p_channel_email = 'N')
//      OR (p_channel_event = 'N'))
//   AND (d_year = 2000)
//GROUP BY i_item_id
//ORDER BY i_item_id ASC
//LIMIT 100;
//"""
//    order_qt_query26_1_before "${query26_1}"
//    async_mv_rewrite_success(db, mv26_1, query26_1, "mv26_1")
//    order_qt_query26_1_after "${query26_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv26_1"""
//
//    def mv27 = """
//SELECT
//  i_item_id
//, s_state
//, GROUPING (s_state) g_state
//, avg(ss_quantity) agg1
//, avg(ss_list_price) agg2
//, avg(ss_coupon_amt) agg3
//, avg(ss_sales_price) agg4
//FROM
//  store_sales
//, customer_demographics
//, date_dim
//, store
//, item
//WHERE (ss_sold_date_sk = d_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (ss_store_sk = s_store_sk)
//   AND (ss_cdemo_sk = cd_demo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND (d_year = 2002)
//   AND (s_state IN (
//     'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'))
//GROUP BY ROLLUP (i_item_id, s_state)
//ORDER BY i_item_id ASC, s_state ASC
//LIMIT 100;
//"""
//    def query27 = """
//SELECT
//  i_item_id
//, s_state
//, GROUPING (s_state) g_state
//, avg(ss_quantity) agg1
//, avg(ss_list_price) agg2
//, avg(ss_coupon_amt) agg3
//, avg(ss_sales_price) agg4
//FROM
//  store_sales
//, customer_demographics
//, date_dim
//, store
//, item
//WHERE (ss_sold_date_sk = d_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (ss_store_sk = s_store_sk)
//   AND (ss_cdemo_sk = cd_demo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND (d_year = 2002)
//   AND (s_state IN (
//     'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'))
//GROUP BY ROLLUP (i_item_id, s_state)
//ORDER BY i_item_id ASC, s_state ASC
//LIMIT 100;
//"""
//    order_qt_query27_before "${query27}"
//    async_mv_rewrite_fail(db, mv27, query27, "mv27")
//    order_qt_query27_after "${query27}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv27"""

//    def mv27_1 = """
//SELECT
//  i_item_id
//, s_state
//, GROUPING (s_state) g_state
//, avg(ss_quantity) agg1
//, avg(ss_list_price) agg2
//, avg(ss_coupon_amt) agg3
//, avg(ss_sales_price) agg4
//FROM
//  store_sales
//, customer_demographics
//, date_dim
//, store
//, item
//WHERE (ss_sold_date_sk = d_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (ss_store_sk = s_store_sk)
//   AND (ss_cdemo_sk = cd_demo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND (d_year = 2002)
//   AND (s_state IN (
//     'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'))
//GROUP BY ROLLUP (i_item_id, s_state);
//"""
//    def query27_1 = """
//SELECT
//  i_item_id
//, s_state
//, GROUPING (s_state) g_state
//, avg(ss_quantity) agg1
//, avg(ss_list_price) agg2
//, avg(ss_coupon_amt) agg3
//, avg(ss_sales_price) agg4
//FROM
//  store_sales
//, customer_demographics
//, date_dim
//, store
//, item
//WHERE (ss_sold_date_sk = d_date_sk)
//   AND (ss_item_sk = i_item_sk)
//   AND (ss_store_sk = s_store_sk)
//   AND (ss_cdemo_sk = cd_demo_sk)
//   AND (cd_gender = 'M')
//   AND (cd_marital_status = 'S')
//   AND (cd_education_status = 'College')
//   AND (d_year = 2002)
//   AND (s_state IN (
//     'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'
//   , 'TN'))
//GROUP BY ROLLUP (i_item_id, s_state)
//ORDER BY i_item_id ASC, s_state ASC
//LIMIT 100;
//"""
//    order_qt_query27_1_before "${query27_1}"
//    // should success but fail
//    async_mv_rewrite_fail(db, mv27_1, query27_1, "mv27_1")
//    order_qt_query27_1_after "${query27_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv27_1"""
//
//    def mv28 = """
//SELECT *
//FROM
//  (
//   SELECT
//     avg(ss_list_price) b1_lp
//   , count(ss_list_price) b1_cnt
//   , count(DISTINCT ss_list_price) b1_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 0 AND 5)
//      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
//         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
//         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
//)  b1
//, (
//   SELECT
//     avg(ss_list_price) b2_lp
//   , count(ss_list_price) b2_cnt
//   , count(DISTINCT ss_list_price) b2_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 6 AND 10)
//      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
//         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
//         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
//)  b2
//, (
//   SELECT
//     avg(ss_list_price) b3_lp
//   , count(ss_list_price) b3_cnt
//   , count(DISTINCT ss_list_price) b3_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 11 AND 15)
//      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
//         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
//         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
//)  b3
//, (
//   SELECT
//     avg(ss_list_price) b4_lp
//   , count(ss_list_price) b4_cnt
//   , count(DISTINCT ss_list_price) b4_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 16 AND 20)
//      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
//         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
//         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
//)  b4
//, (
//   SELECT
//     avg(ss_list_price) b5_lp
//   , count(ss_list_price) b5_cnt
//   , count(DISTINCT ss_list_price) b5_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 21 AND 25)
//      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
//         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
//         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
//)  b5
//, (
//   SELECT
//     avg(ss_list_price) b6_lp
//   , count(ss_list_price) b6_cnt
//   , count(DISTINCT ss_list_price) b6_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 26 AND 30)
//      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
//         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
//         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
//)  b6
//LIMIT 100;
//"""
//    def query28 = """
//SELECT *
//FROM
//  (
//   SELECT
//     avg(ss_list_price) b1_lp
//   , count(ss_list_price) b1_cnt
//   , count(DISTINCT ss_list_price) b1_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 0 AND 5)
//      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
//         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
//         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
//)  b1
//, (
//   SELECT
//     avg(ss_list_price) b2_lp
//   , count(ss_list_price) b2_cnt
//   , count(DISTINCT ss_list_price) b2_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 6 AND 10)
//      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
//         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
//         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
//)  b2
//, (
//   SELECT
//     avg(ss_list_price) b3_lp
//   , count(ss_list_price) b3_cnt
//   , count(DISTINCT ss_list_price) b3_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 11 AND 15)
//      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
//         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
//         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
//)  b3
//, (
//   SELECT
//     avg(ss_list_price) b4_lp
//   , count(ss_list_price) b4_cnt
//   , count(DISTINCT ss_list_price) b4_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 16 AND 20)
//      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
//         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
//         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
//)  b4
//, (
//   SELECT
//     avg(ss_list_price) b5_lp
//   , count(ss_list_price) b5_cnt
//   , count(DISTINCT ss_list_price) b5_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 21 AND 25)
//      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
//         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
//         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
//)  b5
//, (
//   SELECT
//     avg(ss_list_price) b6_lp
//   , count(ss_list_price) b6_cnt
//   , count(DISTINCT ss_list_price) b6_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 26 AND 30)
//      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
//         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
//         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
//)  b6
//LIMIT 100;
//"""
//    order_qt_query28_before "${query28}"
//    async_mv_rewrite_fail(db, mv28, query28, "mv28")
//    order_qt_query28_after "${query28}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv28"""
//
//    def mv28_1 = """
//SELECT *
//FROM
//  (
//   SELECT
//     avg(ss_list_price) b1_lp
//   , count(ss_list_price) b1_cnt
//   , count(DISTINCT ss_list_price) b1_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 0 AND 5)
//      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
//         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
//         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
//)  b1
//, (
//   SELECT
//     avg(ss_list_price) b2_lp
//   , count(ss_list_price) b2_cnt
//   , count(DISTINCT ss_list_price) b2_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 6 AND 10)
//      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
//         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
//         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
//)  b2
//, (
//   SELECT
//     avg(ss_list_price) b3_lp
//   , count(ss_list_price) b3_cnt
//   , count(DISTINCT ss_list_price) b3_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 11 AND 15)
//      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
//         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
//         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
//)  b3
//, (
//   SELECT
//     avg(ss_list_price) b4_lp
//   , count(ss_list_price) b4_cnt
//   , count(DISTINCT ss_list_price) b4_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 16 AND 20)
//      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
//         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
//         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
//)  b4
//, (
//   SELECT
//     avg(ss_list_price) b5_lp
//   , count(ss_list_price) b5_cnt
//   , count(DISTINCT ss_list_price) b5_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 21 AND 25)
//      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
//         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
//         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
//)  b5
//, (
//   SELECT
//     avg(ss_list_price) b6_lp
//   , count(ss_list_price) b6_cnt
//   , count(DISTINCT ss_list_price) b6_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 26 AND 30)
//      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
//         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
//         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
//)  b6;
//"""
//    def query28_1 = """
//SELECT *
//FROM
//  (
//   SELECT
//     avg(ss_list_price) b1_lp
//   , count(ss_list_price) b1_cnt
//   , count(DISTINCT ss_list_price) b1_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 0 AND 5)
//      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
//         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
//         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
//)  b1
//, (
//   SELECT
//     avg(ss_list_price) b2_lp
//   , count(ss_list_price) b2_cnt
//   , count(DISTINCT ss_list_price) b2_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 6 AND 10)
//      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
//         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
//         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
//)  b2
//, (
//   SELECT
//     avg(ss_list_price) b3_lp
//   , count(ss_list_price) b3_cnt
//   , count(DISTINCT ss_list_price) b3_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 11 AND 15)
//      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
//         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
//         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
//)  b3
//, (
//   SELECT
//     avg(ss_list_price) b4_lp
//   , count(ss_list_price) b4_cnt
//   , count(DISTINCT ss_list_price) b4_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 16 AND 20)
//      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
//         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
//         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
//)  b4
//, (
//   SELECT
//     avg(ss_list_price) b5_lp
//   , count(ss_list_price) b5_cnt
//   , count(DISTINCT ss_list_price) b5_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 21 AND 25)
//      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
//         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
//         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
//)  b5
//, (
//   SELECT
//     avg(ss_list_price) b6_lp
//   , count(ss_list_price) b6_cnt
//   , count(DISTINCT ss_list_price) b6_cntd
//   FROM
//     store_sales
//   WHERE (ss_quantity BETWEEN 26 AND 30)
//      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
//         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
//         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
//)  b6
//LIMIT 100;
//"""
//    order_qt_query28_1_before "${query28_1}"
//    async_mv_rewrite_fail(db, mv28_1, query28_1, "mv28_1")
//    order_qt_query28_1_after "${query28_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv28_1"""
//
//    def mv29 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_store_id
//, s_store_name
//, sum(ss_quantity) store_sales_quantity
//, sum(sr_return_quantity) store_returns_quantity
//, sum(cs_quantity) catalog_sales_quantity
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_moy = 9)
//   AND (d1.d_year = 1999)
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_moy BETWEEN 9 AND (9 + 3))
//   AND (d2.d_year = 1999)
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_year IN (1999, (1999 + 1), (1999 + 2)))
//GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
//ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
//LIMIT 100;
//"""
//    def query29 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_store_id
//, s_store_name
//, sum(ss_quantity) store_sales_quantity
//, sum(sr_return_quantity) store_returns_quantity
//, sum(cs_quantity) catalog_sales_quantity
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_moy = 9)
//   AND (d1.d_year = 1999)
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_moy BETWEEN 9 AND (9 + 3))
//   AND (d2.d_year = 1999)
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_year IN (1999, (1999 + 1), (1999 + 2)))
//GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
//ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
//LIMIT 100;
//"""
//    order_qt_query29_before "${query29}"
//    async_mv_rewrite_fail(db, mv29, query29, "mv29")
//    order_qt_query29_after "${query29}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv29"""
//
//    def mv29_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_store_id
//, s_store_name
//, sum(ss_quantity) store_sales_quantity
//, sum(sr_return_quantity) store_returns_quantity
//, sum(cs_quantity) catalog_sales_quantity
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_moy = 9)
//   AND (d1.d_year = 1999)
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_moy BETWEEN 9 AND (9 + 3))
//   AND (d2.d_year = 1999)
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_year IN (1999, (1999 + 1), (1999 + 2)))
//GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name;
//"""
//    def query29_1 = """
//SELECT
//  i_item_id
//, i_item_desc
//, s_store_id
//, s_store_name
//, sum(ss_quantity) store_sales_quantity
//, sum(sr_return_quantity) store_returns_quantity
//, sum(cs_quantity) catalog_sales_quantity
//FROM
//  store_sales
//, store_returns
//, catalog_sales
//, date_dim d1
//, date_dim d2
//, date_dim d3
//, store
//, item
//WHERE (d1.d_moy = 9)
//   AND (d1.d_year = 1999)
//   AND (d1.d_date_sk = ss_sold_date_sk)
//   AND (i_item_sk = ss_item_sk)
//   AND (s_store_sk = ss_store_sk)
//   AND (ss_customer_sk = sr_customer_sk)
//   AND (ss_item_sk = sr_item_sk)
//   AND (ss_ticket_number = sr_ticket_number)
//   AND (sr_returned_date_sk = d2.d_date_sk)
//   AND (d2.d_moy BETWEEN 9 AND (9 + 3))
//   AND (d2.d_year = 1999)
//   AND (sr_customer_sk = cs_bill_customer_sk)
//   AND (sr_item_sk = cs_item_sk)
//   AND (cs_sold_date_sk = d3.d_date_sk)
//   AND (d3.d_year IN (1999, (1999 + 1), (1999 + 2)))
//GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
//ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
//LIMIT 100;
//"""
//    order_qt_query29_1_before "${query29_1}"
//    async_mv_rewrite_success(db, mv29_1, query29_1, "mv29_1")
//    order_qt_query29_1_after "${query29_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv29_1"""
//
//    def mv30 = """
//WITH
//  customer_total_return AS (
//   SELECT
//     wr_returning_customer_sk ctr_customer_sk
//   , ca_state ctr_state
//   , sum(wr_return_amt) ctr_total_return
//   FROM
//     web_returns
//   , date_dim
//   , customer_address
//   WHERE (wr_returned_date_sk = d_date_sk)
//      AND (d_year = 2002)
//      AND (wr_returning_addr_sk = ca_address_sk)
//   GROUP BY wr_returning_customer_sk, ca_state
//)
//SELECT
//  c_customer_id
//, c_salutation
//, c_first_name
//, c_last_name
//, c_preferred_cust_flag
//, c_birth_day
//, c_birth_month
//, c_birth_year
//, c_birth_country
//, c_login
//, c_email_address
//, c_last_review_date_sk
//, ctr_total_return
//FROM
//  customer_total_return ctr1
//, customer_address
//, customer
//WHERE (ctr1.ctr_total_return > (
//      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL))
//      FROM
//        customer_total_return ctr2
//      WHERE (ctr1.ctr_state = ctr2.ctr_state)
//   ))
//   AND (ca_address_sk = c_current_addr_sk)
//   AND (ca_state = 'GA')
//   AND (ctr1.ctr_customer_sk = c_customer_sk)
//ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, c_preferred_cust_flag ASC, c_birth_day ASC, c_birth_month ASC, c_birth_year ASC, c_birth_country ASC, c_login ASC, c_email_address ASC, c_last_review_date_sk ASC, ctr_total_return ASC
//LIMIT 100;
//"""
//    def query30 = """
//WITH
//  customer_total_return AS (
//   SELECT
//     wr_returning_customer_sk ctr_customer_sk
//   , ca_state ctr_state
//   , sum(wr_return_amt) ctr_total_return
//   FROM
//     web_returns
//   , date_dim
//   , customer_address
//   WHERE (wr_returned_date_sk = d_date_sk)
//      AND (d_year = 2002)
//      AND (wr_returning_addr_sk = ca_address_sk)
//   GROUP BY wr_returning_customer_sk, ca_state
//)
//SELECT
//  c_customer_id
//, c_salutation
//, c_first_name
//, c_last_name
//, c_preferred_cust_flag
//, c_birth_day
//, c_birth_month
//, c_birth_year
//, c_birth_country
//, c_login
//, c_email_address
//, c_last_review_date_sk
//, ctr_total_return
//FROM
//  customer_total_return ctr1
//, customer_address
//, customer
//WHERE (ctr1.ctr_total_return > (
//      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL))
//      FROM
//        customer_total_return ctr2
//      WHERE (ctr1.ctr_state = ctr2.ctr_state)
//   ))
//   AND (ca_address_sk = c_current_addr_sk)
//   AND (ca_state = 'GA')
//   AND (ctr1.ctr_customer_sk = c_customer_sk)
//ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, c_preferred_cust_flag ASC, c_birth_day ASC, c_birth_month ASC, c_birth_year ASC, c_birth_country ASC, c_login ASC, c_email_address ASC, c_last_review_date_sk ASC, ctr_total_return ASC
//LIMIT 100;
//"""
//    order_qt_query30_before "${query30}"
//    async_mv_rewrite_fail(db, mv30, query30, "mv30")
//    order_qt_query30_after "${query30}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv30"""


    def mv31 = """
"""
    def query31 = """
"""
    order_qt_query31_before "${query31}"
    async_mv_rewrite_success(db, mv31, query31, "mv31")
    order_qt_query31_after "${query31}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv31"""

    def mv31_1 = """
"""
    def query31_1 = """
"""
    order_qt_query31_1_before "${query31_1}"
    async_mv_rewrite_success(db, mv31_1, query31_1, "mv31_1")
    order_qt_query31_1_after "${query31_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv31_1"""

    def mv32 = """
"""
    def query32 = """
"""
    order_qt_query32_before "${query32}"
    async_mv_rewrite_success(db, mv32, query32, "mv32")
    order_qt_query32_after "${query32}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv32"""

    def mv32_1 = """
"""
    def query32_1 = """
"""
    order_qt_query32_1_before "${query32_1}"
    async_mv_rewrite_success(db, mv32_1, query32_1, "mv32_1")
    order_qt_query32_1_after "${query32_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv32_1"""

    def mv33 = """
"""
    def query33 = """
"""
    order_qt_query33_before "${query33}"
    async_mv_rewrite_success(db, mv33, query33, "mv33")
    order_qt_query33_after "${query33}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv33"""

    def mv33_1 = """
"""
    def query33_1 = """
"""
    order_qt_query33_1_before "${query33_1}"
    async_mv_rewrite_success(db, mv33_1, query33_1, "mv33_1")
    order_qt_query33_1_after "${query33_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv33_1"""

    def mv34 = """
"""
    def query34 = """
"""
    order_qt_query34_before "${query34}"
    async_mv_rewrite_success(db, mv34, query34, "mv34")
    order_qt_query34_after "${query34}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv34"""

    def mv34_1 = """
"""
    def query34_1 = """
"""
    order_qt_query34_1_before "${query34_1}"
    async_mv_rewrite_success(db, mv34_1, query34_1, "mv34_1")
    order_qt_query34_1_after "${query34_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv34_1"""

    def mv35 = """
"""
    def query35 = """
"""
    order_qt_query35_before "${query35}"
    async_mv_rewrite_success(db, mv35, query35, "mv35")
    order_qt_query35_after "${query35}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv35"""

    def mv35_1 = """
"""
    def query35_1 = """
"""
    order_qt_query35_1_before "${query35_1}"
    async_mv_rewrite_success(db, mv35_1, query35_1, "mv35_1")
    order_qt_query35_1_after "${query35_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv35_1"""

    def mv36 = """
"""
    def query36 = """
"""
    order_qt_query36_before "${query36}"
    async_mv_rewrite_success(db, mv36, query36, "mv36")
    order_qt_query36_after "${query36}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv36"""

    def mv36_1 = """
"""
    def query36_1 = """
"""
    order_qt_query36_1_before "${query36_1}"
    async_mv_rewrite_success(db, mv36_1, query36_1, "mv36_1")
    order_qt_query36_1_after "${query36_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv36_1"""

    def mv37 = """
"""
    def query37 = """
"""
    order_qt_query37_before "${query37}"
    async_mv_rewrite_success(db, mv37, query37, "mv37")
    order_qt_query37_after "${query37}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv37"""

    def mv37_1 = """
"""
    def query37_1 = """
"""
    order_qt_query37_1_before "${query37_1}"
    async_mv_rewrite_success(db, mv37_1, query37_1, "mv37_1")
    order_qt_query37_1_after "${query37_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv37_1"""

    def mv38 = """
"""
    def query38 = """
"""
    order_qt_query38_before "${query38}"
    async_mv_rewrite_success(db, mv38, query38, "mv38")
    order_qt_query38_after "${query38}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv38"""

    def mv38_1 = """
"""
    def query38_1 = """
"""
    order_qt_query38_1_before "${query38_1}"
    async_mv_rewrite_success(db, mv38_1, query38_1, "mv38_1")
    order_qt_query38_1_after "${query38_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv38_1"""

    def mv39 = """
"""
    def query39 = """
"""
    order_qt_query39_before "${query39}"
    async_mv_rewrite_success(db, mv39, query39, "mv39")
    order_qt_query39_after "${query39}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv39"""

    def mv39_1 = """
"""
    def query39_1 = """
"""
    order_qt_query39_1_before "${query39_1}"
    async_mv_rewrite_success(db, mv39_1, query39_1, "mv39_1")
    order_qt_query39_1_after "${query39_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv39_1"""

    def mv40 = """
"""
    def query40 = """
"""
    order_qt_query40_before "${query40}"
    async_mv_rewrite_success(db, mv40, query40, "mv40")
    order_qt_query40_after "${query40}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv40"""

    def mv40_1 = """
"""
    def query40_1 = """
"""
    order_qt_query40_1_before "${query40_1}"
    async_mv_rewrite_success(db, mv40_1, query40_1, "mv40_1")
    order_qt_query40_1_after "${query40_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv40_1"""

    def mv41 = """
"""
    def query41 = """
"""
    order_qt_query41_before "${query41}"
    async_mv_rewrite_success(db, mv41, query41, "mv41")
    order_qt_query41_after "${query41}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv41"""

    def mv41_1 = """
"""
    def query41_1 = """
"""
    order_qt_query41_1_before "${query41_1}"
    async_mv_rewrite_success(db, mv41_1, query41_1, "mv41_1")
    order_qt_query41_1_after "${query41_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv41_1"""

    def mv42 = """
"""
    def query42 = """
"""
    order_qt_query42_before "${query42}"
    async_mv_rewrite_success(db, mv42, query42, "mv42")
    order_qt_query42_after "${query42}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv42"""

    def mv42_1 = """
"""
    def query42_1 = """
"""
    order_qt_query42_1_before "${query42_1}"
    async_mv_rewrite_success(db, mv42_1, query42_1, "mv42_1")
    order_qt_query42_1_after "${query42_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv42_1"""

    def mv43 = """
"""
    def query43 = """
"""
    order_qt_query43_before "${query43}"
    async_mv_rewrite_success(db, mv43, query43, "mv43")
    order_qt_query43_after "${query43}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv43"""

    def mv43_1 = """
"""
    def query43_1 = """
"""
    order_qt_query43_1_before "${query43_1}"
    async_mv_rewrite_success(db, mv43_1, query43_1, "mv43_1")
    order_qt_query43_1_after "${query43_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv43_1"""

    def mv44 = """
"""
    def query44 = """
"""
    order_qt_query44_before "${query44}"
    async_mv_rewrite_success(db, mv44, query44, "mv44")
    order_qt_query44_after "${query44}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv44"""

    def mv44_1 = """
"""
    def query44_1 = """
"""
    order_qt_query44_1_before "${query44_1}"
    async_mv_rewrite_success(db, mv44_1, query44_1, "mv44_1")
    order_qt_query44_1_after "${query44_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv44_1"""

    def mv45 = """
"""
    def query45 = """
"""
    order_qt_query45_before "${query45}"
    async_mv_rewrite_success(db, mv45, query45, "mv45")
    order_qt_query45_after "${query45}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv45"""

    def mv45_1 = """
"""
    def query45_1 = """
"""
    order_qt_query45_1_before "${query45_1}"
    async_mv_rewrite_success(db, mv45_1, query45_1, "mv45_1")
    order_qt_query45_1_after "${query45_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv45_1"""

    def mv46 = """
"""
    def query46 = """
"""
    order_qt_query46_before "${query46}"
    async_mv_rewrite_success(db, mv46, query46, "mv46")
    order_qt_query46_after "${query46}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv46"""

    def mv46_1 = """
"""
    def query46_1 = """
"""
    order_qt_query46_1_before "${query46_1}"
    async_mv_rewrite_success(db, mv46_1, query46_1, "mv46_1")
    order_qt_query46_1_after "${query46_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv46_1"""

    def mv47 = """
"""
    def query47 = """
"""
    order_qt_query47_before "${query47}"
    async_mv_rewrite_success(db, mv47, query47, "mv47")
    order_qt_query47_after "${query47}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv47"""

    def mv47_1 = """
"""
    def query47_1 = """
"""
    order_qt_query47_1_before "${query47_1}"
    async_mv_rewrite_success(db, mv47_1, query47_1, "mv47_1")
    order_qt_query47_1_after "${query47_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv47_1"""

    def mv48 = """
"""
    def query48 = """
"""
    order_qt_query48_before "${query48}"
    async_mv_rewrite_success(db, mv48, query48, "mv48")
    order_qt_query48_after "${query48}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv48"""

    def mv48_1 = """
"""
    def query48_1 = """
"""
    order_qt_query48_1_before "${query48_1}"
    async_mv_rewrite_success(db, mv48_1, query48_1, "mv48_1")
    order_qt_query48_1_after "${query48_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv48_1"""

    def mv49 = """
"""
    def query49 = """
"""
    order_qt_query49_before "${query49}"
    async_mv_rewrite_success(db, mv49, query49, "mv49")
    order_qt_query49_after "${query49}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv49"""

    def mv49_1 = """
"""
    def query49_1 = """
"""
    order_qt_query49_1_before "${query49_1}"
    async_mv_rewrite_success(db, mv49_1, query49_1, "mv49_1")
    order_qt_query49_1_after "${query49_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv49_1"""

    def mv50 = """
"""
    def query50 = """
"""
    order_qt_query50_before "${query50}"
    async_mv_rewrite_success(db, mv50, query50, "mv50")
    order_qt_query50_after "${query50}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv50"""

    def mv50_1 = """
"""
    def query50_1 = """
"""
    order_qt_query50_1_before "${query50_1}"
    async_mv_rewrite_success(db, mv50_1, query50_1, "mv50_1")
    order_qt_query50_1_after "${query50_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv50_1"""

    def mv51 = """
"""
    def query51 = """
"""
    order_qt_query51_before "${query51}"
    async_mv_rewrite_success(db, mv51, query51, "mv51")
    order_qt_query51_after "${query51}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv51"""

    def mv51_1 = """
"""
    def query51_1 = """
"""
    order_qt_query51_1_before "${query51_1}"
    async_mv_rewrite_success(db, mv51_1, query51_1, "mv51_1")
    order_qt_query51_1_after "${query51_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv51_1"""

    def mv52 = """
"""
    def query52 = """
"""
    order_qt_query52_before "${query52}"
    async_mv_rewrite_success(db, mv52, query52, "mv52")
    order_qt_query52_after "${query52}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv52"""

    def mv52_1 = """
"""
    def query52_1 = """
"""
    order_qt_query52_1_before "${query52_1}"
    async_mv_rewrite_success(db, mv52_1, query52_1, "mv52_1")
    order_qt_query52_1_after "${query52_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv52_1"""

    def mv53 = """
"""
    def query53 = """
"""
    order_qt_query53_before "${query53}"
    async_mv_rewrite_success(db, mv53, query53, "mv53")
    order_qt_query53_after "${query53}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv53"""

    def mv53_1 = """
"""
    def query53_1 = """
"""
    order_qt_query53_1_before "${query53_1}"
    async_mv_rewrite_success(db, mv53_1, query53_1, "mv53_1")
    order_qt_query53_1_after "${query53_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv53_1"""

    def mv54 = """
"""
    def query54 = """
"""
    order_qt_query54_before "${query54}"
    async_mv_rewrite_success(db, mv54, query54, "mv54")
    order_qt_query54_after "${query54}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv54"""

    def mv54_1 = """
"""
    def query54_1 = """
"""
    order_qt_query54_1_before "${query54_1}"
    async_mv_rewrite_success(db, mv54_1, query54_1, "mv54_1")
    order_qt_query54_1_after "${query54_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv54_1"""

    def mv55 = """
"""
    def query55 = """
"""
    order_qt_query55_before "${query55}"
    async_mv_rewrite_success(db, mv55, query55, "mv55")
    order_qt_query55_after "${query55}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv55"""

    def mv55_1 = """
"""
    def query55_1 = """
"""
    order_qt_query55_1_before "${query55_1}"
    async_mv_rewrite_success(db, mv55_1, query55_1, "mv55_1")
    order_qt_query55_1_after "${query55_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv55_1"""

    def mv56 = """
"""
    def query56 = """
"""
    order_qt_query56_before "${query56}"
    async_mv_rewrite_success(db, mv56, query56, "mv56")
    order_qt_query56_after "${query56}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv56"""

    def mv56_1 = """
"""
    def query56_1 = """
"""
    order_qt_query56_1_before "${query56_1}"
    async_mv_rewrite_success(db, mv56_1, query56_1, "mv56_1")
    order_qt_query56_1_after "${query56_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv56_1"""

    def mv57 = """
"""
    def query57 = """
"""
    order_qt_query57_before "${query57}"
    async_mv_rewrite_success(db, mv57, query57, "mv57")
    order_qt_query57_after "${query57}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv57"""

    def mv57_1 = """
"""
    def query57_1 = """
"""
    order_qt_query57_1_before "${query57_1}"
    async_mv_rewrite_success(db, mv57_1, query57_1, "mv57_1")
    order_qt_query57_1_after "${query57_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv57_1"""

    def mv58 = """
"""
    def query58 = """
"""
    order_qt_query58_before "${query58}"
    async_mv_rewrite_success(db, mv58, query58, "mv58")
    order_qt_query58_after "${query58}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv58"""

    def mv58_1 = """
"""
    def query58_1 = """
"""
    order_qt_query58_1_before "${query58_1}"
    async_mv_rewrite_success(db, mv58_1, query58_1, "mv58_1")
    order_qt_query58_1_after "${query58_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv58_1"""

    def mv59 = """
"""
    def query59 = """
"""
    order_qt_query59_before "${query59}"
    async_mv_rewrite_success(db, mv59, query59, "mv59")
    order_qt_query59_after "${query59}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv59"""

    def mv59_1 = """
"""
    def query59_1 = """
"""
    order_qt_query59_1_before "${query59_1}"
    async_mv_rewrite_success(db, mv59_1, query59_1, "mv59_1")
    order_qt_query59_1_after "${query59_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv59_1"""

    def mv60 = """
"""
    def query60 = """
"""
    order_qt_query60_before "${query60}"
    async_mv_rewrite_success(db, mv60, query60, "mv60")
    order_qt_query60_after "${query60}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv60"""

    def mv60_1 = """
"""
    def query60_1 = """
"""
    order_qt_query60_1_before "${query60_1}"
    async_mv_rewrite_success(db, mv60_1, query60_1, "mv60_1")
    order_qt_query60_1_after "${query60_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv60_1"""

    def mv61 = """
"""
    def query61 = """
"""
    order_qt_query61_before "${query61}"
    async_mv_rewrite_success(db, mv61, query61, "mv61")
    order_qt_query61_after "${query61}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv61"""

    def mv61_1 = """
"""
    def query61_1 = """
"""
    order_qt_query61_1_before "${query61_1}"
    async_mv_rewrite_success(db, mv61_1, query61_1, "mv61_1")
    order_qt_query61_1_after "${query61_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv61_1"""

    def mv62 = """
"""
    def query62 = """
"""
    order_qt_query62_before "${query62}"
    async_mv_rewrite_success(db, mv62, query62, "mv62")
    order_qt_query62_after "${query62}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv62"""

    def mv62_1 = """
"""
    def query62_1 = """
"""
    order_qt_query62_1_before "${query62_1}"
    async_mv_rewrite_success(db, mv62_1, query62_1, "mv62_1")
    order_qt_query62_1_after "${query62_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv62_1"""

    def mv63 = """
"""
    def query63 = """
"""
    order_qt_query63_before "${query63}"
    async_mv_rewrite_success(db, mv63, query63, "mv63")
    order_qt_query63_after "${query63}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv63"""

    def mv63_1 = """
"""
    def query63_1 = """
"""
    order_qt_query63_1_before "${query63_1}"
    async_mv_rewrite_success(db, mv63_1, query63_1, "mv63_1")
    order_qt_query63_1_after "${query63_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv63_1"""

    def mv64 = """
"""
    def query64 = """
"""
    order_qt_query64_before "${query64}"
    async_mv_rewrite_success(db, mv64, query64, "mv64")
    order_qt_query64_after "${query64}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv64"""

    def mv64_1 = """
"""
    def query64_1 = """
"""
    order_qt_query64_1_before "${query64_1}"
    async_mv_rewrite_success(db, mv64_1, query64_1, "mv64_1")
    order_qt_query64_1_after "${query64_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv64_1"""

    def mv65 = """
"""
    def query65 = """
"""
    order_qt_query65_before "${query65}"
    async_mv_rewrite_success(db, mv65, query65, "mv65")
    order_qt_query65_after "${query65}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv65"""

    def mv65_1 = """
"""
    def query65_1 = """
"""
    order_qt_query65_1_before "${query65_1}"
    async_mv_rewrite_success(db, mv65_1, query65_1, "mv65_1")
    order_qt_query65_1_after "${query65_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv65_1"""

    def mv66 = """
"""
    def query66 = """
"""
    order_qt_query66_before "${query66}"
    async_mv_rewrite_success(db, mv66, query66, "mv66")
    order_qt_query66_after "${query66}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv66"""

    def mv66_1 = """
"""
    def query66_1 = """
"""
    order_qt_query66_1_before "${query66_1}"
    async_mv_rewrite_success(db, mv66_1, query66_1, "mv66_1")
    order_qt_query66_1_after "${query66_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv66_1"""

    def mv67 = """
"""
    def query67 = """
"""
    order_qt_query67_before "${query67}"
    async_mv_rewrite_success(db, mv67, query67, "mv67")
    order_qt_query67_after "${query67}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv67"""

    def mv67_1 = """
"""
    def query67_1 = """
"""
    order_qt_query67_1_before "${query67_1}"
    async_mv_rewrite_success(db, mv67_1, query67_1, "mv67_1")
    order_qt_query67_1_after "${query67_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv67_1"""

    def mv68 = """
"""
    def query68 = """
"""
    order_qt_query68_before "${query68}"
    async_mv_rewrite_success(db, mv68, query68, "mv68")
    order_qt_query68_after "${query68}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv68"""

    def mv68_1 = """
"""
    def query68_1 = """
"""
    order_qt_query68_1_before "${query68_1}"
    async_mv_rewrite_success(db, mv68_1, query68_1, "mv68_1")
    order_qt_query68_1_after "${query68_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv68_1"""

    def mv69 = """
"""
    def query69 = """
"""
    order_qt_query69_before "${query69}"
    async_mv_rewrite_success(db, mv69, query69, "mv69")
    order_qt_query69_after "${query69}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv69"""

    def mv69_1 = """
"""
    def query69_1 = """
"""
    order_qt_query69_1_before "${query69_1}"
    async_mv_rewrite_success(db, mv69_1, query69_1, "mv69_1")
    order_qt_query69_1_after "${query69_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv69_1"""

    def mv70 = """
"""
    def query70 = """
"""
    order_qt_query70_before "${query70}"
    async_mv_rewrite_success(db, mv70, query70, "mv70")
    order_qt_query70_after "${query70}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv70"""

    def mv70_1 = """
"""
    def query70_1 = """
"""
    order_qt_query70_1_before "${query70_1}"
    async_mv_rewrite_success(db, mv70_1, query70_1, "mv70_1")
    order_qt_query70_1_after "${query70_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv70_1"""

    def mv71 = """
"""
    def query71 = """
"""
    order_qt_query71_before "${query71}"
    async_mv_rewrite_success(db, mv71, query71, "mv71")
    order_qt_query71_after "${query71}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv71"""

    def mv71_1 = """
"""
    def query71_1 = """
"""
    order_qt_query71_1_before "${query71_1}"
    async_mv_rewrite_success(db, mv71_1, query71_1, "mv71_1")
    order_qt_query71_1_after "${query71_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv71_1"""

    def mv72 = """
"""
    def query72 = """
"""
    order_qt_query72_before "${query72}"
    async_mv_rewrite_success(db, mv72, query72, "mv72")
    order_qt_query72_after "${query72}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv72"""

    def mv72_1 = """
"""
    def query72_1 = """
"""
    order_qt_query72_1_before "${query72_1}"
    async_mv_rewrite_success(db, mv72_1, query72_1, "mv72_1")
    order_qt_query72_1_after "${query72_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv72_1"""

    def mv73 = """
"""
    def query73 = """
"""
    order_qt_query73_before "${query73}"
    async_mv_rewrite_success(db, mv73, query73, "mv73")
    order_qt_query73_after "${query73}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv73"""

    def mv73_1 = """
"""
    def query73_1 = """
"""
    order_qt_query73_1_before "${query73_1}"
    async_mv_rewrite_success(db, mv73_1, query73_1, "mv73_1")
    order_qt_query73_1_after "${query73_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv73_1"""

    def mv74 = """
"""
    def query74 = """
"""
    order_qt_query74_before "${query74}"
    async_mv_rewrite_success(db, mv74, query74, "mv74")
    order_qt_query74_after "${query74}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv74"""

    def mv74_1 = """
"""
    def query74_1 = """
"""
    order_qt_query74_1_before "${query74_1}"
    async_mv_rewrite_success(db, mv74_1, query74_1, "mv74_1")
    order_qt_query74_1_after "${query74_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv74_1"""

    def mv75 = """
"""
    def query75 = """
"""
    order_qt_query75_before "${query75}"
    async_mv_rewrite_success(db, mv75, query75, "mv75")
    order_qt_query75_after "${query75}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv75"""

    def mv75_1 = """
"""
    def query75_1 = """
"""
    order_qt_query75_1_before "${query75_1}"
    async_mv_rewrite_success(db, mv75_1, query75_1, "mv75_1")
    order_qt_query75_1_after "${query75_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv75_1"""

    def mv76 = """
"""
    def query76 = """
"""
    order_qt_query76_before "${query76}"
    async_mv_rewrite_success(db, mv76, query76, "mv76")
    order_qt_query76_after "${query76}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv76"""

    def mv76_1 = """
"""
    def query76_1 = """
"""
    order_qt_query76_1_before "${query76_1}"
    async_mv_rewrite_success(db, mv76_1, query76_1, "mv76_1")
    order_qt_query76_1_after "${query76_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv76_1"""

    def mv77 = """
"""
    def query77 = """
"""
    order_qt_query77_before "${query77}"
    async_mv_rewrite_success(db, mv77, query77, "mv77")
    order_qt_query77_after "${query77}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv77"""

    def mv77_1 = """
"""
    def query77_1 = """
"""
    order_qt_query77_1_before "${query77_1}"
    async_mv_rewrite_success(db, mv77_1, query77_1, "mv77_1")
    order_qt_query77_1_after "${query77_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv77_1"""

    def mv78 = """
"""
    def query78 = """
"""
    order_qt_query78_before "${query78}"
    async_mv_rewrite_success(db, mv78, query78, "mv78")
    order_qt_query78_after "${query78}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv78"""

    def mv78_1 = """
"""
    def query78_1 = """
"""
    order_qt_query78_1_before "${query78_1}"
    async_mv_rewrite_success(db, mv78_1, query78_1, "mv78_1")
    order_qt_query78_1_after "${query78_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv78_1"""

    def mv79 = """
"""
    def query79 = """
"""
    order_qt_query79_before "${query79}"
    async_mv_rewrite_success(db, mv79, query79, "mv79")
    order_qt_query79_after "${query79}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv79"""

    def mv79_1 = """
"""
    def query79_1 = """
"""
    order_qt_query79_1_before "${query79_1}"
    async_mv_rewrite_success(db, mv79_1, query79_1, "mv79_1")
    order_qt_query79_1_after "${query79_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv79_1"""

    def mv80 = """
"""
    def query80 = """
"""
    order_qt_query80_before "${query80}"
    async_mv_rewrite_success(db, mv80, query80, "mv80")
    order_qt_query80_after "${query80}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv80"""

    def mv80_1 = """
"""
    def query80_1 = """
"""
    order_qt_query80_1_before "${query80_1}"
    async_mv_rewrite_success(db, mv80_1, query80_1, "mv80_1")
    order_qt_query80_1_after "${query80_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv80_1"""

    def mv81 = """
"""
    def query81 = """
"""
    order_qt_query81_before "${query81}"
    async_mv_rewrite_success(db, mv81, query81, "mv81")
    order_qt_query81_after "${query81}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv81"""

    def mv81_1 = """
"""
    def query81_1 = """
"""
    order_qt_query81_1_before "${query81_1}"
    async_mv_rewrite_success(db, mv81_1, query81_1, "mv81_1")
    order_qt_query81_1_after "${query81_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv81_1"""

    def mv82 = """
"""
    def query82 = """
"""
    order_qt_query82_before "${query82}"
    async_mv_rewrite_success(db, mv82, query82, "mv82")
    order_qt_query82_after "${query82}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv82"""

    def mv82_1 = """
"""
    def query82_1 = """
"""
    order_qt_query82_1_before "${query82_1}"
    async_mv_rewrite_success(db, mv82_1, query82_1, "mv82_1")
    order_qt_query82_1_after "${query82_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv82_1"""

    def mv83 = """
"""
    def query83 = """
"""
    order_qt_query83_before "${query83}"
    async_mv_rewrite_success(db, mv83, query83, "mv83")
    order_qt_query83_after "${query83}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv83"""

    def mv83_1 = """
"""
    def query83_1 = """
"""
    order_qt_query83_1_before "${query83_1}"
    async_mv_rewrite_success(db, mv83_1, query83_1, "mv83_1")
    order_qt_query83_1_after "${query83_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv83_1"""

    def mv84 = """
"""
    def query84 = """
"""
    order_qt_query84_before "${query84}"
    async_mv_rewrite_success(db, mv84, query84, "mv84")
    order_qt_query84_after "${query84}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv84"""

    def mv84_1 = """
"""
    def query84_1 = """
"""
    order_qt_query84_1_before "${query84_1}"
    async_mv_rewrite_success(db, mv84_1, query84_1, "mv84_1")
    order_qt_query84_1_after "${query84_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv84_1"""

    def mv85 = """
"""
    def query85 = """
"""
    order_qt_query85_before "${query85}"
    async_mv_rewrite_success(db, mv85, query85, "mv85")
    order_qt_query85_after "${query85}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv85"""

    def mv85_1 = """
"""
    def query85_1 = """
"""
    order_qt_query85_1_before "${query85_1}"
    async_mv_rewrite_success(db, mv85_1, query85_1, "mv85_1")
    order_qt_query85_1_after "${query85_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv85_1"""

    def mv86 = """
"""
    def query86 = """
"""
    order_qt_query86_before "${query86}"
    async_mv_rewrite_success(db, mv86, query86, "mv86")
    order_qt_query86_after "${query86}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv86"""

    def mv86_1 = """
"""
    def query86_1 = """
"""
    order_qt_query86_1_before "${query86_1}"
    async_mv_rewrite_success(db, mv86_1, query86_1, "mv86_1")
    order_qt_query86_1_after "${query86_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv86_1"""

    def mv87 = """
"""
    def query87 = """
"""
    order_qt_query87_before "${query87}"
    async_mv_rewrite_success(db, mv87, query87, "mv87")
    order_qt_query87_after "${query87}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv87"""

    def mv87_1 = """
"""
    def query87_1 = """
"""
    order_qt_query87_1_before "${query87_1}"
    async_mv_rewrite_success(db, mv87_1, query87_1, "mv87_1")
    order_qt_query87_1_after "${query87_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv87_1"""

    def mv88 = """
"""
    def query88 = """
"""
    order_qt_query88_before "${query88}"
    async_mv_rewrite_success(db, mv88, query88, "mv88")
    order_qt_query88_after "${query88}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv88"""

    def mv88_1 = """
"""
    def query88_1 = """
"""
    order_qt_query88_1_before "${query88_1}"
    async_mv_rewrite_success(db, mv88_1, query88_1, "mv88_1")
    order_qt_query88_1_after "${query88_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv88_1"""

    def mv89 = """
"""
    def query89 = """
"""
    order_qt_query89_before "${query89}"
    async_mv_rewrite_success(db, mv89, query89, "mv89")
    order_qt_query89_after "${query89}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv89"""

    def mv89_1 = """
"""
    def query89_1 = """
"""
    order_qt_query89_1_before "${query89_1}"
    async_mv_rewrite_success(db, mv89_1, query89_1, "mv89_1")
    order_qt_query89_1_after "${query89_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv89_1"""

    def mv90 = """
"""
    def query90 = """
"""
    order_qt_query90_before "${query90}"
    async_mv_rewrite_success(db, mv90, query90, "mv90")
    order_qt_query90_after "${query90}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv90"""

    def mv90_1 = """
"""
    def query90_1 = """
"""
    order_qt_query90_1_before "${query90_1}"
    async_mv_rewrite_success(db, mv90_1, query90_1, "mv90_1")
    order_qt_query90_1_after "${query90_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv90_1"""

    def mv91 = """
"""
    def query91 = """
"""
    order_qt_query91_before "${query91}"
    async_mv_rewrite_success(db, mv91, query91, "mv91")
    order_qt_query91_after "${query91}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv91"""

    def mv91_1 = """
"""
    def query91_1 = """
"""
    order_qt_query91_1_before "${query91_1}"
    async_mv_rewrite_success(db, mv91_1, query91_1, "mv91_1")
    order_qt_query91_1_after "${query91_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv91_1"""

    def mv92 = """
"""
    def query92 = """
"""
    order_qt_query92_before "${query92}"
    async_mv_rewrite_success(db, mv92, query92, "mv92")
    order_qt_query92_after "${query92}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv92"""

    def mv92_1 = """
"""
    def query92_1 = """
"""
    order_qt_query92_1_before "${query92_1}"
    async_mv_rewrite_success(db, mv92_1, query92_1, "mv92_1")
    order_qt_query92_1_after "${query92_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv92_1"""

    def mv93 = """
"""
    def query93 = """
"""
    order_qt_query93_before "${query93}"
    async_mv_rewrite_success(db, mv93, query93, "mv93")
    order_qt_query93_after "${query93}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv93"""

    def mv93_1 = """
"""
    def query93_1 = """
"""
    order_qt_query93_1_before "${query93_1}"
    async_mv_rewrite_success(db, mv93_1, query93_1, "mv93_1")
    order_qt_query93_1_after "${query93_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv93_1"""

    def mv94 = """
"""
    def query94 = """
"""
    order_qt_query94_before "${query94}"
    async_mv_rewrite_success(db, mv94, query94, "mv94")
    order_qt_query94_after "${query94}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv94"""

    def mv94_1 = """
"""
    def query94_1 = """
"""
    order_qt_query94_1_before "${query94_1}"
    async_mv_rewrite_success(db, mv94_1, query94_1, "mv94_1")
    order_qt_query94_1_after "${query94_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv94_1"""

    def mv95 = """
"""
    def query95 = """
"""
    order_qt_query95_before "${query95}"
    async_mv_rewrite_success(db, mv95, query95, "mv95")
    order_qt_query95_after "${query95}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv95"""

    def mv95_1 = """
"""
    def query95_1 = """
"""
    order_qt_query95_1_before "${query95_1}"
    async_mv_rewrite_success(db, mv95_1, query95_1, "mv95_1")
    order_qt_query95_1_after "${query95_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv95_1"""

    def mv96 = """
"""
    def query96 = """
"""
    order_qt_query96_before "${query96}"
    async_mv_rewrite_success(db, mv96, query96, "mv96")
    order_qt_query96_after "${query96}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv96"""

    def mv96_1 = """
"""
    def query96_1 = """
"""
    order_qt_query96_1_before "${query96_1}"
    async_mv_rewrite_success(db, mv96_1, query96_1, "mv96_1")
    order_qt_query96_1_after "${query96_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv96_1"""

    def mv97 = """
"""
    def query97 = """
"""
    order_qt_query97_before "${query97}"
    async_mv_rewrite_success(db, mv97, query97, "mv97")
    order_qt_query97_after "${query97}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv97"""

    def mv97_1 = """
"""
    def query97_1 = """
"""
    order_qt_query97_1_before "${query97_1}"
    async_mv_rewrite_success(db, mv97_1, query97_1, "mv97_1")
    order_qt_query97_1_after "${query97_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv97_1"""

    def mv98 = """
"""
    def query98 = """
"""
    order_qt_query98_before "${query98}"
    async_mv_rewrite_success(db, mv98, query98, "mv98")
    order_qt_query98_after "${query98}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv98"""

    def mv98_1 = """
"""
    def query98_1 = """
"""
    order_qt_query98_1_before "${query98_1}"
    async_mv_rewrite_success(db, mv98_1, query98_1, "mv98_1")
    order_qt_query98_1_after "${query98_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv98_1"""

    def mv99 = """
"""
    def query99 = """
"""
    order_qt_query99_before "${query99}"
    async_mv_rewrite_success(db, mv99, query99, "mv99")
    order_qt_query99_after "${query99}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv99"""

    def mv99_1 = """
"""
    def query99_1 = """
"""
    order_qt_query99_1_before "${query99_1}"
    async_mv_rewrite_success(db, mv99_1, query99_1, "mv99_1")
    order_qt_query99_1_after "${query99_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv99_1"""
}