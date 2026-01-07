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

suite("eager_agg") {
    sql """
        set eager_aggregation_mode=1;
        set eager_aggregation_on_join=true;
    """

    // push to ss-join-ws
    qt_a """
    explain shape plan
    select /*+leading({ss ws} dt)*/ dt.d_year 
       ,sum(ws_list_price) brand
       ,sum(ss_sales_price) sum_agg
    from  date_dim dt 
        ,store_sales ss
        ,web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by dt.d_year
    """

    // push to ss-join-ws
    qt_a2 """
    explain shape plan
    select /*+leading({ss ws} dt)*/ dt.d_year 
       ,sum(ws_list_price + ss_sales_price) brand

    from  date_dim dt 
        ,store_sales ss
        ,web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by dt.d_year
    """

    // push sum/min/max aggFunc
    qt_sum_min_max """
    explain shape plan
    select /*+leading({ss ws} dt)*/ dt.d_year 
       ,sum(ws_list_price) brand
       ,min(ss_sales_price) min_agg
       ,max(ss_sales_price) max_agg
    from  date_dim dt 
        ,store_sales ss
        ,web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by dt.d_year
    """


    // do not push avg/count aggFunc
    qt_avg_count """
    explain shape plan
    select /*+leading({ss ws} dt)*/ dt.d_year 
       ,avg(ws_list_price) 
    from  date_dim dt 
        ,store_sales ss
        ,web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by dt.d_year
    """

    // agg push to ss-d
    qt_groupkey_push_SS_JOIN_D """
    explain shape plan
    select /*+leading({ss dt} ws)*/  dt.d_year 
        ,sum(ss_wholesale_cost) brand
        ,sum(ss_sales_price + d_moy) sum_agg
    from  store_sales ss
        join date_dim dt
        join web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by dt.d_year, ss_hdemo_sk + ws_quantity
    """

    // group key: ss_hdemo_sk + d_moy => push to ss-d
    qt_groupkey_push """
    explain shape plan
    select /*+leading({ss dt} ws)*/  dt.d_year 
        ,sum(ss_wholesale_cost) brand
        ,sum(ss_sales_price) sum_agg
    from  store_sales ss
        join date_dim dt
        join web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by dt.d_year, ss_hdemo_sk + d_moy
    """
}
