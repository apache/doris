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

suite("test_regression_test_tpcds_sf1_p1_q46", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  c_last_name
		, c_first_name
		, ca_city
		, bought_city
		, ss_ticket_number
		, amt
		, profit
		FROM
		  (
		   SELECT
		     ss_ticket_number
		   , ss_customer_sk
		   , ca_city bought_city
		   , sum(ss_coupon_amt) amt
		   , sum(ss_net_profit) profit
		   FROM
		     store_sales
		   , date_dim
		   , store
		   , household_demographics
		   , customer_address
		   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
		      AND (store_sales.ss_store_sk = store.s_store_sk)
		      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (store_sales.ss_addr_sk = customer_address.ca_address_sk)
		      AND ((household_demographics.hd_dep_count = 4)
		         OR (household_demographics.hd_vehicle_count = 3))
		      AND (date_dim.d_dow IN (6   , 0))
		      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
		      AND (store.s_city IN ('Fairview'   , 'Midway'   , 'Fairview'   , 'Fairview'   , 'Fairview'))
		   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
		)  dn
		, customer
		, customer_address current_addr
		WHERE (ss_customer_sk = c_customer_sk)
		   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
		   AND (current_addr.ca_city <> bought_city)
		ORDER BY c_last_name ASC, c_first_name ASC, ca_city ASC, bought_city ASC, ss_ticket_number ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 35> `c_last_name` ASC, <slot 36> `c_first_name` ASC, <slot 37> `ca_city` ASC, <slot 38> `bought_city` ASC, <slot 39> `ss_ticket_number` ASC") && 
		explainStr.contains("join op: INNER JOIN(PARTITIONED)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 108> = `current_addr`.`ca_address_sk`)\n" + 
				"  |  other predicates: (<slot 173> != <slot 167>)") && 
		explainStr.contains("other predicates: (<slot 173> != <slot 167>)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 109 111 112 113 114 115 118 \n" + 
				"  |  hash output slot ids: 100 102 103 104 105 106 31 ") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 19> `ss_customer_sk` = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 100 102 103 104 105 106 108 \n" + 
				"  |  hash output slot ids: 33 18 21 22 23 29 30 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 22> sum(`ss_coupon_amt`)), sum(<slot 23> sum(`ss_net_profit`))\n" + 
				"  |  group by: <slot 18> `ss_ticket_number`, <slot 19> `ss_customer_sk`, <slot 20> `ss_addr_sk`, <slot 21> `ca_city`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 84>), sum(<slot 85>)\n" + 
				"  |  group by: <slot 82>, <slot 83>, <slot 89>, <slot 98>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 73> = `customer_address`.`ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 82 83 84 85 89 98 \n" + 
				"  |  hash output slot ids: 66 2 67 68 69 73 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 59> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 66 67 68 69 73 \n" + 
				"  |  hash output slot ids: 53 54 55 56 60 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 47> = `store`.`s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 53 54 55 56 59 60 \n" + 
				"  |  hash output slot ids: 48 49 42 43 44 45 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`store_sales`.`ss_sold_date_sk` = `date_dim`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `date_dim`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 42 43 44 45 47 48 49 \n" + 
				"  |  hash output slot ids: 0 1 3 4 7 9 11 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `store_sales`.`ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: ((`household_demographics`.`hd_dep_count` = 4) OR (`household_demographics`.`hd_vehicle_count` = 3))") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_city` IN ('Fairview', 'Midway', 'Fairview', 'Fairview', 'Fairview'))") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`date_dim`.`d_dow` IN (6, 0)), (`date_dim`.`d_year` IN (1999, 2000, 2001))") 
            
        }
    }
}