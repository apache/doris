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

suite("test_regression_test_tpcds_sf1_p1_q93", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  ss_customer_sk
		, sum(act_sales) sumsales
		FROM
		  (
		   SELECT
		     ss_item_sk
		   , ss_ticket_number
		   , ss_customer_sk
		   , (CASE WHEN (sr_return_quantity IS NOT NULL) THEN ((ss_quantity - sr_return_quantity) * ss_sales_price) ELSE (ss_quantity * ss_sales_price) END) act_sales
		   FROM
		     store_sales
		   LEFT JOIN store_returns ON (sr_item_sk = ss_item_sk)
		      AND (sr_ticket_number = ss_ticket_number)
		   , reason
		   WHERE (sr_reason_sk = r_reason_sk)
		      AND (r_reason_desc = 'reason 28')
		)  t
		GROUP BY ss_customer_sk
		ORDER BY sumsales ASC, ss_customer_sk ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 17> <slot 16> sum(`act_sales`) ASC, <slot 18> <slot 15> `ss_customer_sk` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 16> sum(`act_sales`))\n" + 
				"  |  group by: <slot 15> `ss_customer_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (<slot 28> IS NOT NULL) THEN ((<slot 22> - <slot 28>) * <slot 23>) ELSE (<slot 22> * <slot 23>) END))\n" + 
				"  |  group by: <slot 21>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `sr_item_sk`)\n" + 
				"  |  equal join conjunct: (`ss_ticket_number` = `sr_ticket_number`)\n" + 
				"  |  other predicates: (<slot 40> = <slot 35>)") && 
		explainStr.contains("other predicates: (<slot 40> = <slot 35>)") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 21 22 23 28 \n" + 
				"  |  hash output slot ids: 4 5 6 7 8 9 ") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=403256560") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: reason(reason), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`r_reason_desc` = 'reason 28')") 
            
        }
    }
}