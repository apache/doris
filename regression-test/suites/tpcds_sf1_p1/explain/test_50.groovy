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

suite("test_regression_test_tpcds_sf1_p1_q50", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  s_store_name
		, s_company_id
		, s_street_number
		, s_street_name
		, s_street_type
		, s_suite_number
		, s_city
		, s_county
		, s_state
		, s_zip
		, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) <= 30) THEN 1 ELSE 0 END)) '30 days'
		, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 30)
		   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 60) THEN 1 ELSE 0 END)) '31-60 days'
		, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 60)
		   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 90) THEN 1 ELSE 0 END)) '61-90 days'
		, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 90)
		   AND ((sr_returned_date_sk - ss_sold_date_sk) <= 120) THEN 1 ELSE 0 END)) '91-120 days'
		, sum((CASE WHEN ((sr_returned_date_sk - ss_sold_date_sk) > 120) THEN 1 ELSE 0 END)) '>120 days'
		FROM
		  store_sales
		, store_returns
		, store
		, date_dim d1
		, date_dim d2
		WHERE (d2.d_year = 2001)
		   AND (d2.d_moy = 8)
		   AND (ss_ticket_number = sr_ticket_number)
		   AND (ss_item_sk = sr_item_sk)
		   AND (ss_sold_date_sk = d1.d_date_sk)
		   AND (sr_returned_date_sk = d2.d_date_sk)
		   AND (ss_customer_sk = sr_customer_sk)
		   AND (ss_store_sk = s_store_sk)
		GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip
		ORDER BY s_store_name ASC, s_company_id ASC, s_street_number ASC, s_street_name ASC, s_street_type ASC, s_suite_number ASC, s_city ASC, s_county ASC, s_state ASC, s_zip ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 39> <slot 24> `s_store_name` ASC, <slot 40> <slot 25> `s_company_id` ASC, <slot 41> <slot 26> `s_street_number` ASC, <slot 42> <slot 27> `s_street_name` ASC, <slot 43> <slot 28> `s_street_type` ASC, <slot 44> <slot 29> `s_suite_number` ASC, <slot 45> <slot 30> `s_city` ASC, <slot 46> <slot 31> `s_county` ASC, <slot 47> <slot 32> `s_state` ASC, <slot 48> <slot 33> `s_zip` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 34> sum((CASE WHEN ((`sr_returned_date_sk` - `ss_sold_date_sk`) <= 30) THEN 1 ELSE 0 END))), sum(<slot 35> sum((CASE WHEN ((`sr_returned_date_sk` - `ss_sold_date_sk`) > 30) AND ((`sr_returned_date_sk` - `ss_sold_date_sk`) <= 60) THEN 1 ELSE 0 END))), sum(<slot 36> sum((CASE WHEN ((`sr_returned_date_sk` - `ss_sold_date_sk`) > 60) AND ((`sr_returned_date_sk` - `ss_sold_date_sk`) <= 90) THEN 1 ELSE 0 END))), sum(<slot 37> sum((CASE WHEN ((`sr_returned_date_sk` - `ss_sold_date_sk`) > 90) AND ((`sr_returned_date_sk` - `ss_sold_date_sk`) <= 120) THEN 1 ELSE 0 END))), sum(<slot 38> sum((CASE WHEN ((`sr_returned_date_sk` - `ss_sold_date_sk`) > 120) THEN 1 ELSE 0 END)))\n" + 
				"  |  group by: <slot 24> `s_store_name`, <slot 25> `s_company_id`, <slot 26> `s_street_number`, <slot 27> `s_street_name`, <slot 28> `s_street_type`, <slot 29> `s_suite_number`, <slot 30> `s_city`, <slot 31> `s_county`, <slot 32> `s_state`, <slot 33> `s_zip`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN ((<slot 99> - <slot 94>) <= 30) THEN 1 ELSE 0 END)), sum((CASE WHEN ((<slot 99> - <slot 94>) > 30) AND ((<slot 99> - <slot 94>) <= 60) THEN 1 ELSE 0 END)), sum((CASE WHEN ((<slot 99> - <slot 94>) > 60) AND ((<slot 99> - <slot 94>) <= 90) THEN 1 ELSE 0 END)), sum((CASE WHEN ((<slot 99> - <slot 94>) > 90) AND ((<slot 99> - <slot 94>) <= 120) THEN 1 ELSE 0 END)), sum((CASE WHEN ((<slot 99> - <slot 94>) > 120) THEN 1 ELSE 0 END))\n" + 
				"  |  group by: <slot 104>, <slot 105>, <slot 106>, <slot 107>, <slot 108>, <slot 109>, <slot 110>, <slot 111>, <slot 112>, <slot 113>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 78> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 94 99 104 105 106 107 108 109 110 111 112 113 \n" + 
				"  |  hash output slot ids: 83 84 85 86 87 88 73 89 90 91 92 78 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 67> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 73 78 83 84 85 86 87 88 89 90 91 92 \n" + 
				"  |  hash output slot ids: 0 1 2 3 68 4 5 6 7 8 9 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 54> = `d1`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 63 67 68 \n" + 
				"  |  hash output slot ids: 54 58 59 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_ticket_number` = `sr_ticket_number`)\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `sr_item_sk`)\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `sr_customer_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `sr_ticket_number`, RF001[in_or_bloom] <- `sr_item_sk`, RF002[in_or_bloom] <- `sr_customer_sk`") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 54 58 59 \n" + 
				"  |  hash output slot ids: 22 10 11 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_ticket_number`, RF001[in_or_bloom] -> `ss_item_sk`, RF002[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d2`.`d_year` = 2001), (`d2`.`d_moy` = 8)") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") 
            
        }
    }
}