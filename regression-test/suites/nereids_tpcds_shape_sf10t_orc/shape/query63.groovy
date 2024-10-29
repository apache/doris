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

suite("query63") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    if (isCloudMode()) {
        return
    }
    sql """
         use ${db};
         set enable_nereids_planner=true;
         set enable_nereids_distribute_planner=false;
         set enable_fallback_to_original_planner=false;
         set exec_mem_limit=21G;
         set be_number_for_test=3;
         set parallel_fragment_exec_instance_num=8; ;
         set parallel_pipeline_task_num=8;
         set forbid_unknown_col_stats=true;
         set enable_nereids_timeout = false;
         set enable_runtime_filter_prune=false;
         set runtime_filter_type=8;
         set dump_nereids_memo=false;
         set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
         set enable_fold_constant_by_be = false;
         set push_topn_to_agg = true;
         set TOPN_OPT_LIMIT_THRESHOLD = 1024;
         set enable_parallel_result_sink=true;
         """
    qt_ds_shape_63 '''
    explain shape plan
    select  * 
from (select i_manager_id
             ,sum(ss_sales_price) sum_sales
             ,avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
      from item
          ,store_sales
          ,date_dim
          ,store
      where ss_item_sk = i_item_sk
        and ss_sold_date_sk = d_date_sk
        and ss_store_sk = s_store_sk
        and d_month_seq in (1211,1211+1,1211+2,1211+3,1211+4,1211+5,1211+6,1211+7,1211+8,1211+9,1211+10,1211+11)
        and ((    i_category in ('Books','Children','Electronics')
              and i_class in ('personal','portable','reference','self-help')
              and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		                  'exportiunivamalg #9','scholaramalgamalg #9'))
           or(    i_category in ('Women','Music','Men')
              and i_class in ('accessories','classical','fragrances','pants')
              and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		                 'importoamalg #1')))
group by i_manager_id, d_moy) tmp1
where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by i_manager_id
        ,avg_monthly_sales
        ,sum_sales
limit 100
    '''
}
