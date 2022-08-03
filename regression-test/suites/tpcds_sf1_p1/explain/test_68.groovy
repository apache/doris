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

suite("test_regression_test_tpcds_sf1_p1_q68", "tpch_sf1") {
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
		, extended_price
		, extended_tax
		, list_price
		FROM
		  (
		   SELECT
		     ss_ticket_number
		   , ss_customer_sk
		   , ca_city bought_city
		   , sum(ss_ext_sales_price) extended_price
		   , sum(ss_ext_list_price) list_price
		   , sum(ss_ext_tax) extended_tax
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
		      AND (date_dim.d_dom BETWEEN 1 AND 2)
		      AND ((household_demographics.hd_dep_count = 4)
		         OR (household_demographics.hd_vehicle_count = 3))
		      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
		      AND (store.s_city IN ('Midway'   , 'Fairview'))
		   GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city
		)  dn
		, customer
		, customer_address current_addr
		WHERE (ss_customer_sk = c_customer_sk)
		   AND (customer.c_current_addr_sk = current_addr.ca_address_sk)
		   AND (current_addr.ca_city <> bought_city)
		ORDER BY c_last_name ASC, ss_ticket_number ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 38> `c_last_name` ASC, <slot 39> `ss_ticket_number` ASC") && 
		explainStr.contains("join op: INNER JOIN(PARTITIONED)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 117> = `current_addr`.`ca_address_sk`)\n" + 
				"  |  other predicates: (<slot 189> != <slot 182>)") && 
		explainStr.contains("other predicates: (<slot 189> != <slot 182>)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 118 120 121 122 123 124 125 128 \n" + 
				"  |  hash output slot ids: 112 113 114 34 115 108 110 111 ") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 20> `ss_customer_sk` = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 108 110 111 112 113 114 115 117 \n" + 
				"  |  hash output slot ids: 32 33 19 36 22 23 24 25 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 23> sum(`ss_ext_sales_price`)), sum(<slot 24> sum(`ss_ext_list_price`)), sum(<slot 25> sum(`ss_ext_tax`))\n" + 
				"  |  group by: <slot 19> `ss_ticket_number`, <slot 20> `ss_customer_sk`, <slot 21> `ss_addr_sk`, <slot 22> `ca_city`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 91>), sum(<slot 92>), sum(<slot 93>)\n" + 
				"  |  group by: <slot 89>, <slot 90>, <slot 97>, <slot 106>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 80> = `customer_address`.`ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 89 90 91 92 93 97 106 \n" + 
				"  |  hash output slot ids: 80 2 72 73 74 75 76 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 65> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 72 73 74 75 76 80 \n" + 
				"  |  hash output slot ids: 66 58 59 60 61 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 52> = `store`.`s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 58 59 60 61 62 65 66 \n" + 
				"  |  hash output slot ids: 48 49 50 53 54 46 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`store_sales`.`ss_sold_date_sk` = `date_dim`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `date_dim`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 46 47 48 49 50 52 53 54 \n" + 
				"  |  hash output slot ids: 0 1 3 4 5 8 10 12 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `store_sales`.`ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: ((`household_demographics`.`hd_dep_count` = 4) OR (`household_demographics`.`hd_vehicle_count` = 3))") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_city` IN ('Midway', 'Fairview'))") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `date_dim`.`d_dom` >= 1, `date_dim`.`d_dom` <= 2, (`date_dim`.`d_year` IN (1999, 2000, 2001))") 
            
        }
    }
}