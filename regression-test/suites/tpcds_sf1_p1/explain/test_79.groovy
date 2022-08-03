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

suite("test_regression_test_tpcds_sf1_p1_q79", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  c_last_name
		, c_first_name
		, substr(s_city, 1, 30)
		, ss_ticket_number
		, amt
		, profit
		FROM
		  (
		   SELECT
		     ss_ticket_number
		   , ss_customer_sk
		   , store.s_city
		   , sum(ss_coupon_amt) amt
		   , sum(ss_net_profit) profit
		   FROM
		     store_sales
		   , date_dim
		   , store
		   , household_demographics
		   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
		      AND (store_sales.ss_store_sk = store.s_store_sk)
		      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND ((household_demographics.hd_dep_count = 6)
		         OR (household_demographics.hd_vehicle_count > 2))
		      AND (date_dim.d_dow = 1)
		      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
		      AND (store.s_number_employees BETWEEN 200 AND 295)
		   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, store.s_city
		)  ms
		, customer
		WHERE (ss_customer_sk = c_customer_sk)
		ORDER BY c_last_name ASC, c_first_name ASC, substr(s_city, 1, 30) ASC, profit ASC, ss_ticket_number, amt
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 31> `c_last_name` ASC, <slot 32> `c_first_name` ASC, <slot 33> substr(`s_city`, 1, 30) ASC, <slot 34> `profit` ASC, <slot 35> `ss_ticket_number` ASC, <slot 36> `amt` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 18> `ss_customer_sk` = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 79 81 82 83 84 85 \n" + 
				"  |  hash output slot ids: 17 20 21 22 28 29 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 21> sum(`ss_coupon_amt`)), sum(<slot 22> sum(`ss_net_profit`))\n" + 
				"  |  group by: <slot 17> `ss_ticket_number`, <slot 18> `ss_customer_sk`, <slot 19> `ss_addr_sk`, <slot 20> `store`.`s_city`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 64>), sum(<slot 65>)\n" + 
				"  |  group by: <slot 62>, <slot 63>, <slot 69>, <slot 73>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 54> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 62 63 64 65 69 73 \n" + 
				"  |  hash output slot ids: 48 49 50 51 55 59 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 42> = `store`.`s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 48 49 50 51 54 55 59 \n" + 
				"  |  hash output slot ids: 2 37 38 39 40 43 44 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`store_sales`.`ss_sold_date_sk` = `date_dim`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `date_dim`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 37 38 39 40 42 43 44 \n" + 
				"  |  hash output slot ids: 0 16 1 3 4 7 9 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `store_sales`.`ss_sold_date_sk`") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: ((`household_demographics`.`hd_dep_count` = 6) OR (`household_demographics`.`hd_vehicle_count` > 2))") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `store`.`s_number_employees` >= 200, `store`.`s_number_employees` <= 295") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`date_dim`.`d_dow` = 1), (`date_dim`.`d_year` IN (1999, 2000, 2001))") 
            
        }
    }
}