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
select  ca_zip, ca_city, sum(ws_sales_price)
 from web_sales, customer, customer_address, date_dim, item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ws_item_sk = i_item_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or 
 	      i_item_id in (select i_item_id
                             from item
                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             )
 	    )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2000
 group by ca_zip, ca_city
 order by ca_zip, ca_city
 limit 100;
"""

    def topPlan = queryResult[0][0].toString()
    assertTrue(topPlan.contains("LogicalResultSink"))
    assertTrue(topPlan.contains("ca_zip"))
    assertTrue(topPlan.contains("ca_city"))
    assertTrue(topPlan.contains("__sum_2"))
}