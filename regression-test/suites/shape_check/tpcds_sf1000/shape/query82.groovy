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

suite("query82") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    if (isCloudMode()) {
        return
    }
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
    sql 'set enable_runtime_filter_prune=false'
    sql 'set runtime_filter_type=8'
    sql 'set dump_nereids_memo=false'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    def ds = """select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between 58 and 58+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2001-01-13' as date) and (cast('2001-01-13' as date) +  interval 60 day)
 and i_manufact_id in (259,559,580,485)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100"""
    qt_ds_shape_82 '''
    explain shape plan
    select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between 58 and 58+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2001-01-13' as date) and (cast('2001-01-13' as date) +  interval 60 day)
 and i_manufact_id in (259,559,580,485)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 limit 100
    '''
}
