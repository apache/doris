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

suite("query97") {
    if (isCloudMode()) {
        return
    }
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    multi_sql """
        use ${db};
        set enable_nereids_planner=true;
        set enable_nereids_distribute_planner=true;
        set enable_fallback_to_original_planner=false;
        set exec_mem_limit=21G;
        set be_number_for_test=3;
        set enable_runtime_filter_prune=false;
        set parallel_pipeline_task_num=8;
        set forbid_unknown_col_stats=false;
        set enable_stats=false;
        set runtime_filter_type=8;
        set broadcast_row_count_limit = 30000000;
        set enable_nereids_timeout = false;
        set enable_pipeline_engine = true;
        set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        set push_topn_to_agg = true;
        set topn_opt_limit_threshold=1024;
        """

    qt_ds_shape_97 '''
    explain shape plan




with ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from store_sales,date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between 1214 and 1214 + 11
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from catalog_sales,date_dim
where cs_sold_date_sk = d_date_sk
  and d_month_seq between 1214 and 1214 + 11
group by cs_bill_customer_sk
        ,cs_item_sk)
 select  sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
limit 100;

    '''
}
