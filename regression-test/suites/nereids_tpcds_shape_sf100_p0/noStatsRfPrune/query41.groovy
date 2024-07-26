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

suite("query41") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=false'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
sql 'set enable_runtime_filter_prune=true'
    sql 'set parallel_pipeline_task_num=8'
    sql 'set forbid_unknown_col_stats=false'
    sql 'set enable_stats=false'
    sql "set runtime_filter_type=8"
    sql 'set broadcast_row_count_limit = 30000000'
    sql 'set enable_nereids_timeout = false'
    sql 'SET enable_pipeline_engine = true'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    qt_ds_shape_41 '''
    explain shape plan




select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 748 and 748+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'gainsboro' or i_color = 'aquamarine') and 
        (i_units = 'Ounce' or i_units = 'Dozen') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'chiffon' or i_color = 'violet') and
        (i_units = 'Ton' or i_units = 'Pound') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'chartreuse' or i_color = 'blue') and
        (i_units = 'Each' or i_units = 'Oz') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'tan' or i_color = 'dodger') and
        (i_units = 'Bunch' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'blanched' or i_color = 'tomato') and 
        (i_units = 'Tbl' or i_units = 'Case') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'almond' or i_color = 'lime') and
        (i_units = 'Box' or i_units = 'Dram') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'peru' or i_color = 'saddle') and
        (i_units = 'Pallet' or i_units = 'Gram') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'indian' or i_color = 'spring') and
        (i_units = 'Unknown' or i_units = 'Carton') and
        (i_size = 'medium' or i_size = 'economy')
        )))) > 0
 order by i_product_name
 limit 100;

    '''
}
