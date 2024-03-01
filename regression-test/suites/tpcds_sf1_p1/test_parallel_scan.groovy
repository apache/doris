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

suite("test_parallel_scan") {
    sql "use regression_test_tpcds_sf1_p1"

    qt_select0 """
        select count() from store_sales_agg;
    """

//     qt_select1 """
//             select
//                 ss_sold_date_sk
//                 , ss_sold_time_sk
//                 , max(ss_item_sk)
//                 , max(ss_customer_sk)
//                 , max(ss_cdemo_sk)
//                 , max(ss_hdemo_sk)
//                 , max(ss_addr_sk)
//                 , min(ss_store_sk)
//                 , min(ss_promo_sk)
//                 , min(ss_ticket_number)
//                 , min(ss_quantity)
//                 , sum(ss_wholesale_cost)
//                 , sum(ss_list_price)
//                 , sum(ss_sales_price)
//                 , sum(ss_ext_discount_amt)
//                 , sum(ss_ext_sales_price)
//                 , sum(ss_ext_wholesale_cost)
//                 , max(ss_ext_list_price)
//                 , max(ss_ext_tax)
//                 , max(ss_coupon_amt)
//                 , min(ss_net_paid)
//                 , min(ss_net_paid_inc_tax)
//                 , min(ss_net_profit)
//             from store_sales
//             group by ss_sold_date_sk, ss_sold_time_sk
//             order by 1, 2, 3
//             limit 13185, 20;
//         """

    qt_select1 """
        select
            *
        from store_sales_agg
        order by 1, 2, 3
        limit 13185, 20;
    """

    qt_select2 """
        select
            *
        from store_sales_agg
        where 2452123 < ss_sold_date_sk and 2452500 > ss_sold_date_sk
        order by 1, 2, 3
        limit 25, 20;
    """

//     qt_select2 """
//         select
//             ss_sold_date_sk
//             , ss_sold_time_sk
//             , max(ss_item_sk)
//             , max(ss_customer_sk)
//             , max(ss_cdemo_sk)
//             , max(ss_hdemo_sk)
//             , max(ss_addr_sk)
//             , min(ss_store_sk)
//             , min(ss_promo_sk)
//             , min(ss_ticket_number)
//             , min(ss_quantity)
//             , sum(ss_wholesale_cost)
//             , sum(ss_list_price)
//             , sum(ss_sales_price)
//             , sum(ss_ext_discount_amt)
//             , sum(ss_ext_sales_price)
//             , sum(ss_ext_wholesale_cost)
//             , max(ss_ext_list_price)
//             , max(ss_ext_tax)
//             , max(ss_coupon_amt)
//             , min(ss_net_paid)
//             , min(ss_net_paid_inc_tax)
//             , min(ss_net_profit)
//         from store_sales
//         where 2452123 < ss_sold_date_sk and 2452500 > ss_sold_date_sk
//         group by ss_sold_date_sk, ss_sold_time_sk
//         order by 1, 2, 3
//         limit 25, 20;
//     """
}