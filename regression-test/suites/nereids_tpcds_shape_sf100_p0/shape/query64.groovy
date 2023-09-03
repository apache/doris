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

suite("query64") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_pipeline_task_num=8'
    sql 'set forbid_unknown_col_stats=true'
    sql 'set broadcast_row_count_limit = 30000000'
    sql 'set enable_nereids_timeout = false'
    sql 'SET enable_pipeline_engine = true'

    def ds64 = '''
 with cs_ui as
  (select cs_item_sk
         ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
   from catalog_sales
       ,catalog_returns
   where cs_item_sk = cr_item_sk
     and cs_order_number = cr_order_number
   group by cs_item_sk
   having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
 cross_sales as
  (select i_product_name product_name
      ,i_item_sk item_sk
      ,s_store_name store_name
      ,s_zip store_zip
      ,ad1.ca_street_number b_street_number
      ,ad1.ca_street_name b_street_name
      ,ad1.ca_city b_city
      ,ad1.ca_zip b_zip
      ,ad2.ca_street_number c_street_number
      ,ad2.ca_street_name c_street_name
      ,ad2.ca_city c_city
      ,ad2.ca_zip c_zip
      ,d1.d_year as syear
      ,d2.d_year as fsyear
      ,d3.d_year s2year
      ,count(*) cnt
      ,sum(ss_wholesale_cost) s1
      ,sum(ss_list_price) s2
      ,sum(ss_coupon_amt) s3
   FROM   store_sales
         ,store_returns
         ,cs_ui
         ,date_dim d1
         ,date_dim d2
         ,date_dim d3
         ,store
         ,customer
         ,customer_demographics cd1
         ,customer_demographics cd2
         ,promotion
         ,household_demographics hd1
         ,household_demographics hd2
         ,customer_address ad1
         ,customer_address ad2
         ,income_band ib1
         ,income_band ib2
         ,item
   WHERE  ss_store_sk = s_store_sk AND
          ss_sold_date_sk = d1.d_date_sk AND
          ss_customer_sk = c_customer_sk AND
          ss_cdemo_sk= cd1.cd_demo_sk AND
          ss_hdemo_sk = hd1.hd_demo_sk AND
          ss_addr_sk = ad1.ca_address_sk and
          ss_item_sk = i_item_sk and
          ss_item_sk = sr_item_sk and
          ss_ticket_number = sr_ticket_number and
          ss_item_sk = cs_ui.cs_item_sk and
          c_current_cdemo_sk = cd2.cd_demo_sk AND
          c_current_hdemo_sk = hd2.hd_demo_sk AND
          c_current_addr_sk = ad2.ca_address_sk and
          c_first_sales_date_sk = d2.d_date_sk and
          c_first_shipto_date_sk = d3.d_date_sk and
          ss_promo_sk = p_promo_sk and
          hd1.hd_income_band_sk = ib1.ib_income_band_sk and
          hd2.hd_income_band_sk = ib2.ib_income_band_sk and
          cd1.cd_marital_status <> cd2.cd_marital_status and
          i_color in ('blanched','medium','brown','chocolate','burlywood','drab') and
          i_current_price between 23 and 23 + 10 and
          i_current_price between 23 + 1 and 23 + 15
 group by i_product_name
        ,i_item_sk
        ,s_store_name
        ,s_zip
        ,ad1.ca_street_number
        ,ad1.ca_street_name
        ,ad1.ca_city
        ,ad1.ca_zip
        ,ad2.ca_street_number
        ,ad2.ca_street_name
        ,ad2.ca_city
        ,ad2.ca_zip
        ,d1.d_year
        ,d2.d_year
        ,d3.d_year
 )
 select cs1.product_name
      ,cs1.store_name
      ,cs1.store_zip
      ,cs1.b_street_number
      ,cs1.b_street_name
      ,cs1.b_city
      ,cs1.b_zip
      ,cs1.c_street_number
      ,cs1.c_street_name
      ,cs1.c_city
      ,cs1.c_zip
      ,cs1.syear
      ,cs1.cnt
      ,cs1.s1 as s11
      ,cs1.s2 as s21
      ,cs1.s3 as s31
      ,cs2.s1 as s12
      ,cs2.s2 as s22
      ,cs2.s3 as s32
      ,cs2.syear
      ,cs2.cnt
 from cross_sales cs1,cross_sales cs2
 where cs1.item_sk=cs2.item_sk and
      cs1.syear = 2001 and
      cs2.syear = 2001 + 1 and
      cs2.cnt <= cs1.cnt and
      cs1.store_name = cs2.store_name and
      cs1.store_zip = cs2.store_zip
 order by cs1.product_name
        ,cs1.store_name
        ,cs2.cnt
        ,cs1.s1
        ,cs2.s1;

    '''

//    qt_ds_shape_64 'explain shape plan ' + ds64

}
