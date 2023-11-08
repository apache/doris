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

package infer_expr_name

suite("nereids_test_create_blocked") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    def queryResult = sql """
explain analyzed plan
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
"""

    def topPlan = queryResult[0][0].toString()
    assertTrue(topPlan.contains("LogicalResultSink"))
    assertTrue(topPlan.contains("i_product_name"))
}