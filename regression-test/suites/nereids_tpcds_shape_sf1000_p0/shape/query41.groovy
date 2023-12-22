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
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_fragment_exec_instance_num=8; '
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set enable_nereids_timeout = false'
    sql 'set enable_runtime_filter_prune=false'
    sql 'set dump_nereids_memo=true'
    def ds = """select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 704 and 704+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'forest' or i_color = 'lime') and 
        (i_units = 'Pallet' or i_units = 'Pound') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'navy' or i_color = 'slate') and
        (i_units = 'Gross' or i_units = 'Bunch') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'powder' or i_color = 'sky') and
        (i_units = 'Dozen' or i_units = 'Lb') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'maroon' or i_color = 'smoke') and
        (i_units = 'Ounce' or i_units = 'Case') and
        (i_size = 'economy' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'dark' or i_color = 'aquamarine') and 
        (i_units = 'Ton' or i_units = 'Tbl') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'frosted' or i_color = 'plum') and
        (i_units = 'Dram' or i_units = 'Box') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'papaya' or i_color = 'peach') and
        (i_units = 'Bundle' or i_units = 'Carton') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'firebrick' or i_color = 'sienna') and
        (i_units = 'Cup' or i_units = 'Each') and
        (i_size = 'economy' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100"""
    qt_ds_shape_41 '''
    explain shape plan
    select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 704 and 704+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'forest' or i_color = 'lime') and 
        (i_units = 'Pallet' or i_units = 'Pound') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'navy' or i_color = 'slate') and
        (i_units = 'Gross' or i_units = 'Bunch') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'powder' or i_color = 'sky') and
        (i_units = 'Dozen' or i_units = 'Lb') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'maroon' or i_color = 'smoke') and
        (i_units = 'Ounce' or i_units = 'Case') and
        (i_size = 'economy' or i_size = 'small')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'dark' or i_color = 'aquamarine') and 
        (i_units = 'Ton' or i_units = 'Tbl') and
        (i_size = 'economy' or i_size = 'small')
        ) or
        (i_category = 'Women' and
        (i_color = 'frosted' or i_color = 'plum') and
        (i_units = 'Dram' or i_units = 'Box') and
        (i_size = 'extra large' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'papaya' or i_color = 'peach') and
        (i_units = 'Bundle' or i_units = 'Carton') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'firebrick' or i_color = 'sienna') and
        (i_units = 'Cup' or i_units = 'Each') and
        (i_size = 'economy' or i_size = 'small')
        )))) > 0
 order by i_product_name
 limit 100
    '''
}
