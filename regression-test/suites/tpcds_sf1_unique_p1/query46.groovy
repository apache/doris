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

suite("tpcds_sf1_unique_p1_q46") {
    def stats = sql "show column stats  store_sales"
    logger.info("${stats}")
    stats = sql "show column stats  date_dim"
    logger.info("${stats}")
    stats = sql "show column stats  store"
    logger.info("${stats}")
    stats = sql "show column stats  household_demographics"
    logger.info("${stats}")
    stats = sql "show column stats  customer_address"
    logger.info("${stats}")
    def ds46 = """
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
        LIMIT 100
        """

    def memo46 = sql "explain memo plan ${ds46}"
    logger.info("${memo46}")
    qt_order_ds46 "${ds46}"
}