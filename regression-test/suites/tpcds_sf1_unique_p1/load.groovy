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
suite("load") {
    def tables=["store", "store_returns", "customer", "date_dim", "web_sales",
                "catalog_sales", "store_sales", "item", "web_returns", "catalog_returns",
                "catalog_page", "web_site", "customer_address", "customer_demographics",
                "ship_mode", "promotion", "inventory", "time_dim", "income_band",
                "call_center", "reason", "household_demographics", "warehouse", "web_page"]
    def columnsMap = [
        "item": """tmp_item_sk, tmp_item_id, tmp_rec_start_date, tmp_rec_end_date, tmp_item_desc,
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
        "catalog_returns": """cr_returned_date_sk,cr_returned_time_sk,cr_item_sk,cr_refunded_customer_sk,
                              cr_refunded_cdemo_sk,cr_refunded_hdemo_sk,cr_refunded_addr_sk,cr_returning_customer_sk,
                              cr_returning_cdemo_sk,cr_returning_hdemo_sk,cr_returning_addr_sk,cr_call_center_sk,
                              cr_catalog_page_sk,cr_ship_mode_sk,cr_warehouse_sk,cr_reason_sk,cr_order_number,
                              cr_return_quantity,cr_return_amount,cr_return_tax,cr_return_amt_inc_tax,cr_fee,
                              cr_return_ship_cost,cr_refunded_cash,cr_reversed_charge,cr_store_credit,cr_net_loss""",
        "catalog_sales": """cs_sold_date_sk,cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,
                            cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,
                            cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,
                            cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,
                            cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,
                            cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,
                            cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit""",
        "store_returns": """sr_returned_date_sk,sr_return_time_sk,sr_item_sk,sr_customer_sk,sr_cdemo_sk,
                            sr_hdemo_sk,sr_addr_sk,sr_store_sk,sr_reason_sk,sr_ticket_number,
                            sr_return_quantity,sr_return_amt,sr_return_tax,sr_return_amt_inc_tax,sr_fee,
                            sr_return_ship_cost,sr_refunded_cash,sr_reversed_charge,sr_store_credit,sr_net_loss""",
        "store_sales": """ss_sold_date_sk,ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,
                          ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,
                          ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,
                          ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,
                          ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit""",
        "web_returns": """wr_returned_date_sk,wr_returned_time_sk,wr_item_sk,wr_refunded_customer_sk,
                          wr_refunded_cdemo_sk,wr_refunded_hdemo_sk,wr_refunded_addr_sk,wr_returning_customer_sk,
                          wr_returning_cdemo_sk,wr_returning_hdemo_sk,wr_returning_addr_sk,wr_web_page_sk,
                          wr_reason_sk,wr_order_number,wr_return_quantity,wr_return_amt,wr_return_tax,
                          wr_return_amt_inc_tax,wr_fee,wr_return_ship_cost,wr_refunded_cash,
                          wr_reversed_charge,wr_account_credit,wr_net_loss""",
        "web_sales": """ws_sold_date_sk,ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,
                        ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,
                        ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,
                        ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,
                        ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,
                        ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,
                        ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,
                        ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,
                        ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,
                        ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit""",
    ]

    def specialTables = ["item", "customer_address", "catalog_returns", "catalog_sales",
                         "store_returns", "store_sales", "web_returns", "web_sales"]

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
        sql """SET query_timeout=1800"""
        sql """ ANALYZE TABLE $tableName WITH SYNC """
    }

    // CREATE-TABLE-AS-SELECT
    sql "drop table if exists t;"
    sql "create table if not exists t properties('replication_num'='1') as select * from item;"
    def origin_count = sql "select count(*) from item"
    def new_count = sql "select count(*) from t"
    assertEquals(origin_count, new_count)
    sql "drop table if exists tt;"
    sql "create table if not exists tt like item"
    sql "insert into tt select * from t"
    new_count = sql "select count(*) from tt"
    assertEquals(origin_count, new_count)

    sql """ sync """
}
