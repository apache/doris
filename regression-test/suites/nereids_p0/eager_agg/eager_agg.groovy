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
        set runtime_filter_mode=OFF;
        set broadcast_row_count_limit=-1;
        set disable_nereids_rules="SALT_JOIN";
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

   qt_a_exe"""
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

    qt_a2_exe """
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

    qt_sum_min_max_exe """
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

    qt_avg_count_exe """
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

    qt_groupkey_push_SS_JOIN_D_exe """
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

    qt_groupkey_push_exe """
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

    qt_sum_if_push """
        explain shape plan
        select /*+leading({web_sales item} date_dim)*/ d_week_seq,
                sum(case when (d_day_name='Monday') then ws_sales_price else null end) mon_sales,
                sum(case when (d_day_name='Tuesday') then ws_sales_price else  null end) tue_sales,
                sum(case when (d_day_name='Wednesday') then ws_sales_price else null end) wed_sales,
                sum(case when (d_day_name='Thursday') then ws_sales_price else null end) thu_sales,
                sum(case when (d_day_name='Friday') then ws_sales_price else null end) fri_sales,
                sum(case when (d_day_name='Saturday') then ws_sales_price else null end) sat_sales
        from web_sales join item on ws_item_sk = i_item_sk
                        join date_dim on d_date_sk = ws_sold_date_sk
        group by d_week_seq, ws_item_sk;
        """

    qt_sum_if_push_exe """
        select /*+leading({web_sales item} date_dim)*/ d_week_seq,
                sum(case when (d_day_name='Monday') then ws_sales_price else null end) mon_sales,
                sum(case when (d_day_name='Tuesday') then ws_sales_price else  null end) tue_sales,
                sum(case when (d_day_name='Wednesday') then ws_sales_price else null end) wed_sales,
                sum(case when (d_day_name='Thursday') then ws_sales_price else null end) thu_sales,
                sum(case when (d_day_name='Friday') then ws_sales_price else null end) fri_sales,
                sum(case when (d_day_name='Saturday') then ws_sales_price else null end) sat_sales
        from web_sales join item on ws_item_sk = i_item_sk
                        join date_dim on d_date_sk = ws_sold_date_sk
        group by d_week_seq, ws_item_sk;
        """

    qt_check_nullable """
    explain shape plan
    select /*+SET_VAR(eager_aggregation_mode=1, disable_join_reorder = true)*/ a + ss_sales_price 
    from (
    select sum(case when ss_item_sk =1 then 1 else 0 end) a, ss_sales_price
    from store_sales 
      right join date_dim on d_date_sk = ss_sold_date_sk
    group by ss_sales_price
    )t;
    """

    qt_check_nullable_exe """
    select /*+SET_VAR(eager_aggregation_mode=1, disable_join_reorder = true)*/ a + ss_sales_price 
    from (
    select sum(case when ss_item_sk =1 then 1 else 0 end) a, ss_sales_price
    from store_sales 
      right join date_dim on d_date_sk = ss_sold_date_sk
    group by ss_sales_price
    )t;
    """

    qt_check_no_push_value_slots_contains_if_slots """
    explain shape plan
    select /*+SET_VAR(eager_aggregation_mode=1, disable_join_reorder = false)*/ 
        sum(case when ss_item_sk =1 then ss_item_sk else 0 end) a 
    from store_sales 
      join  date_dim on d_date_sk = ss_sold_date_sk
    group by d_year;
    """


    qt_min_sum_same_slot """
    explain shape plan
    select /*+leading({ss dt} ws)*/  dt.d_moy 
        ,min(d_year) brand
        ,sum(d_year) sum_agg 
    from  store_sales ss
        join date_dim dt
        join web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by d_moy
    having brand is null;
    """

    qt_min_sum_same_slot_exe """
    select /*+leading({ss dt} ws)*/  dt.d_moy 
        ,min(d_year) brand
        ,sum(d_year) sum_agg 
    from  store_sales ss
        join date_dim dt
        join web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by d_moy
    having brand is null;
    """

    qt_sum_min_same_slot_exe """
    select /*+leading({ss dt} ws)*/  dt.d_moy 
        ,sum(d_year) sum_agg 
        ,min(d_year) brand  
    from  store_sales ss
        join date_dim dt
        join web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by d_moy
    having brand is null;
    """

    qt_no_push_aggkey_groupKey_overlap """
    explain shape plan
    select /*+leading({ss dt} ws)*/  dt.d_year 
        ,min(d_year) brand
        ,sum(d_year) sum_agg 
    from  store_sales ss
        join date_dim dt
        join web_sales ws
    where dt.d_date_sk = ss_sold_date_sk
    and ss_item_sk = ws_item_sk
    group by d_year
    """

    // if any union child can not push, do not push for all union children
    qt_no_push_not_all_union_children_push """
    explain shape plan
    select  d_year 
        ,min(d_moy) 
        ,sum(d_moy)  
    from (
        select /*+leading({ss dt})*/  d_year, d_moy
        from store_sales ss
            join date_dim dt on dt.d_date_sk = ss_sold_date_sk
        union all
        select d_year, d_moy
        from date_dim
    ) t
    group by d_year;
    """
}
