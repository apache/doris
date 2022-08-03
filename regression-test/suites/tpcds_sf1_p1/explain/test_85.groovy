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

suite("test_regression_test_tpcds_sf1_p1_q85", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  substr(r_reason_desc, 1, 20)
		, avg(ws_quantity)
		, avg(wr_refunded_cash)
		, avg(wr_fee)
		FROM
		  web_sales
		, web_returns
		, web_page
		, customer_demographics cd1
		, customer_demographics cd2
		, customer_address
		, date_dim
		, reason
		WHERE (ws_web_page_sk = wp_web_page_sk)
		   AND (ws_item_sk = wr_item_sk)
		   AND (ws_order_number = wr_order_number)
		   AND (ws_sold_date_sk = d_date_sk)
		   AND (d_year = 2000)
		   AND (cd1.cd_demo_sk = wr_refunded_cdemo_sk)
		   AND (cd2.cd_demo_sk = wr_returning_cdemo_sk)
		   AND (ca_address_sk = wr_refunded_addr_sk)
		   AND (r_reason_sk = wr_reason_sk)
		   AND (((cd1.cd_marital_status = 'M')
		         AND (cd1.cd_marital_status = cd2.cd_marital_status)
		         AND (cd1.cd_education_status = 'Advanced Degree')
		         AND (cd1.cd_education_status = cd2.cd_education_status)
		         AND (ws_sales_price BETWEEN CAST('100.00' AS DECIMAL) AND CAST('150.00' AS DECIMAL)))
		      OR ((cd1.cd_marital_status = 'S')
		         AND (cd1.cd_marital_status = cd2.cd_marital_status)
		         AND (cd1.cd_education_status = 'College')
		         AND (cd1.cd_education_status = cd2.cd_education_status)
		         AND (ws_sales_price BETWEEN CAST('50.00' AS DECIMAL) AND CAST('100.00' AS DECIMAL)))
		      OR ((cd1.cd_marital_status = 'W')
		         AND (cd1.cd_marital_status = cd2.cd_marital_status)
		         AND (cd1.cd_education_status = '2 yr Degree')
		         AND (cd1.cd_education_status = cd2.cd_education_status)
		         AND (ws_sales_price BETWEEN CAST('150.00' AS DECIMAL) AND CAST('200.00' AS DECIMAL))))
		   AND (((ca_country = 'United States')
		         AND (ca_state IN ('IN'      , 'OH'      , 'NJ'))
		         AND (ws_net_profit BETWEEN 100 AND 200))
		      OR ((ca_country = 'United States')
		         AND (ca_state IN ('WI'      , 'CT'      , 'KY'))
		         AND (ws_net_profit BETWEEN 150 AND 300))
		      OR ((ca_country = 'United States')
		         AND (ca_state IN ('LA'      , 'IA'      , 'AR'))
		         AND (ws_net_profit BETWEEN 50 AND 250)))
		GROUP BY r_reason_desc
		ORDER BY substr(r_reason_desc, 1, 20) ASC, avg(ws_quantity) ASC, avg(wr_refunded_cash) ASC, avg(wr_fee) ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 37> substr(<slot 33> `r_reason_desc`, 1, 20) ASC, <slot 38> <slot 34> avg(`ws_quantity`) ASC, <slot 39> <slot 35> avg(`wr_refunded_cash`) ASC, <slot 40> <slot 36> avg(`wr_fee`) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 30> avg(`ws_quantity`)), avg(<slot 31> avg(`wr_refunded_cash`)), avg(<slot 32> avg(`wr_fee`))\n" + 
				"  |  group by: <slot 29> `r_reason_desc`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(<slot 178>), avg(<slot 167>), avg(<slot 168>)\n" + 
				"  |  group by: <slot 188>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 155> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 167 168 178 188 \n" + 
				"  |  hash output slot ids: 161 151 140 141 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 126> = `wp_web_page_sk`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 140 141 151 155 161 \n" + 
				"  |  hash output slot ids: 129 114 115 135 125 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 97> = `r_reason_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 114 115 125 126 129 135 \n" + 
				"  |  hash output slot ids: 0 101 102 105 90 91 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 75> = `ca_address_sk`)\n" + 
				"  |  other predicates: (((<slot 241> IN ('IN', 'OH', 'NJ')) AND <slot 238> >= 100 AND <slot 238> <= 200) OR ((<slot 241> IN ('WI', 'CT', 'KY')) AND <slot 238> >= 150 AND <slot 238> <= 300) OR ((<slot 241> IN ('LA', 'IA', 'AR')) AND <slot 238> >= 50 AND <slot 238> <= 250))") && 
		explainStr.contains("other predicates: (((<slot 241> IN ('IN', 'OH', 'NJ')) AND <slot 238> >= 100 AND <slot 238> <= 200) OR ((<slot 241> IN ('WI', 'CT', 'KY')) AND <slot 238> >= 150 AND <slot 238> <= 300) OR ((<slot 241> IN ('LA', 'IA', 'AR')) AND <slot 238> >= 50 AND <slot 238> <= 250))") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 90 91 97 101 102 105 \n" + 
				"  |  hash output slot ids: 80 81 84 69 70 86 27 76 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 57> = `ws_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 58> = `ws_order_number`)\n" + 
				"  |  other predicates: (((<slot 216> = 'M') AND (<slot 217> = 'Advanced Degree') AND <slot 229> >= 100.00 AND <slot 229> <= 150.00) OR ((<slot 216> = 'S') AND (<slot 217> = 'College') AND <slot 229> >= 50.00 AND <slot 229> <= 100.00) OR ((<slot 216> = 'W') AND (<slot 217> = '2 yr Degree') AND <slot 229> >= 150.00 AND <slot 229> <= 200.00))") && 
		explainStr.contains("other predicates: (((<slot 216> = 'M') AND (<slot 217> = 'Advanced Degree') AND <slot 229> >= 100.00 AND <slot 229> <= 150.00) OR ((<slot 216> = 'S') AND (<slot 217> = 'College') AND <slot 229> >= 50.00 AND <slot 229> <= 100.00) OR ((<slot 216> = 'W') AND (<slot 217> = '2 yr Degree') AND <slot 229> >= 150.00 AND <slot 229> <= 200.00))") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 69 70 75 76 80 81 84 86 \n" + 
				"  |  hash output slot ids: 1 4 53 54 55 56 25 10 28 61 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 49> = `cd2`.`cd_demo_sk`)\n" + 
				"  |  equal join conjunct: (<slot 42> = `cd2`.`cd_marital_status`)\n" + 
				"  |  equal join conjunct: (<slot 43> = `cd2`.`cd_education_status`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 53 54 55 56 57 58 61 62 \n" + 
				"  |  hash output slot ids: 50 51 42 43 44 45 46 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cd1`.`cd_demo_sk` = `wr_refunded_cdemo_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `wr_refunded_cdemo_sk`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 42 43 44 45 46 47 49 50 51 \n" + 
				"  |  hash output slot ids: 16 2 18 3 20 21 23 7 9 ") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cd1`.`cd_marital_status` = 'M' OR `cd1`.`cd_marital_status` = 'S' OR `cd1`.`cd_marital_status` = 'W'), (`cd1`.`cd_education_status` = '2 yr Degree' OR `cd1`.`cd_education_status` = 'Advanced Degree' OR `cd1`.`cd_education_status` = 'College')\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cd1`.`cd_demo_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000)") && 
		explainStr.contains("TABLE: web_page(web_page), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: reason(reason), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_country` = 'United States'), `ca_state` IN ('IN', 'OH', 'NJ', 'WI', 'CT', 'KY', 'LA', 'IA', 'AR')") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON") 
            
        }
    }
}