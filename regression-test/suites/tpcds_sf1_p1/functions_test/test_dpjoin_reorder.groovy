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

suite("test_dp_join_reorder") {
    sql """set enable_nereids_planner=true"""
    sql """set enable_dphyp_optimizer=true"""
    sql """set enable_fallback_to_original_planner=false"""
    sql "use regression_test_tpcds_sf1_p1"

    explain {
        sql("""
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
                ), 
                cross_sales AS (
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
                , cs2.syear
            FROM
                cross_sales cs1
                , cross_sales cs2
                , cross_sales cs3
            WHERE (cs1.item_sk = cs2.item_sk)
                AND (cs1.syear = 1999)
                AND (cs2.syear = (1999 + 1))
                AND (cs1.store_name = cs2.store_name)
                AND (cs1.store_zip = cs2.store_zip)
                AND (cs1.store_name = cs3.store_name)
                AND (cs1.store_zip = cs3.store_zip);
        """)
    }
}