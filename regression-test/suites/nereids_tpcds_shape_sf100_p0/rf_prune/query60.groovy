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

suite("query60") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=false'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_fragment_exec_instance_num=8; '
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set enable_nereids_timeout = false'
    sql 'set runtime_filter_type=8'
    sql 'set enable_runtime_filter_prune=true'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    def ds = """with ss as (
 select
          i_item_id,sum(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id in (select
  i_item_id
from
 item
where i_category in ('Children'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 8
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -7 
 group by i_item_id),
 cs as (
 select
          i_item_id,sum(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from
 item
where i_category in ('Children'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 8
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -7 
 group by i_item_id),
 ws as (
 select
          i_item_id,sum(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from
 item
where i_category in ('Children'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 8
 and     ws_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -7
 group by i_item_id)
  select   
  i_item_id
,sum(total_sales) total_sales
 from  (select * from ss 
        union all
        select * from cs 
        union all
        select * from ws) tmp1
 group by i_item_id
 order by i_item_id
      ,total_sales
 limit 100"""
    qt_ds_shape_60 """
    explain shape plan
    ${ds}
    """
}
