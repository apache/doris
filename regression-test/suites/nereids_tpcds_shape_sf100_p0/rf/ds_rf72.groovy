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

suite("ds_rf72") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_fragment_exec_instance_num=8; '
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set broadcast_row_count_limit = 30000000'
    sql 'set enable_nereids_timeout = false'
    sql 'set enable_pipeline_engine=true'
    sql 'set enable_runtime_filter_prune=false'
    sql 'set expand_runtime_filter_by_inner_join=false'
    String stmt = '''
    explain physical plan
    select  i_item_desc
      ,w_warehouse_name
      ,d1.d_week_seq
      ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
      ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
      ,count(*) total_cnt
from catalog_sales
join inventory on (cs_item_sk = inv_item_sk)
join warehouse on (w_warehouse_sk=inv_warehouse_sk)
join item on (i_item_sk = cs_item_sk)
join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
join date_dim d2 on (inv_date_sk = d2.d_date_sk)
join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
left outer join promotion on (cs_promo_sk=p_promo_sk)
left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
where d1.d_week_seq = d2.d_week_seq
  and inv_quantity_on_hand < cs_quantity 
  and (d3.d_date > (d1.d_date + INTERVAL  '5' DAY))
  and hd_buy_potential = '501-1000'
  and d1.d_year = 2002
  and cd_marital_status = 'W'
group by i_item_desc,w_warehouse_name,d1.d_week_seq
order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
limit 100;

    '''
    String plan = sql "${stmt}"
    log.info(plan)
    def getRuntimeFilters = { plantree ->
        {
            def lst = []
            plantree.eachMatch("RF\\d+\\[[^#]+#\\d+->\\[[^\\]]+\\]") {
                ch ->
                    {
                        lst.add(ch.replaceAll("#\\d+", ''))
                    }
            }
            return lst.join(',')
        }
    }
    
    // def outFile = "regression-test/suites/nereids_tpcds_shape_sf100_p0/ddl/rf/rf.72"
    // File file = new File(outFile)
    // file.write(getRuntimeFilters(plan))

     assertEquals("RF10[w_warehouse_sk->[inv_warehouse_sk],RF8[d_date_sk->[inv_date_sk],RF9[cs_item_sk->[inv_item_sk],RF7[d_week_seq->[d_week_seq],RF5[cs_item_sk->[cr_item_sk],RF6[cs_order_number->[cr_order_number],RF4[i_item_sk->[cs_item_sk],RF3[d_date_sk->[cs_ship_date_sk],RF2[cd_demo_sk->[cs_bill_cdemo_sk],RF1[d_date_sk->[cs_sold_date_sk],RF0[hd_demo_sk->[cs_bill_hdemo_sk]", getRuntimeFilters(plan))
}
