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

suite("test_regression_test_tpcds_sf1_p1_q34", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  c_last_name
		, c_first_name
		, c_salutation
		, c_preferred_cust_flag
		, ss_ticket_number
		, cnt
		FROM
		  (
		   SELECT
		     ss_ticket_number
		   , ss_customer_sk
		   , count(*) cnt
		   FROM
		     store_sales
		   , date_dim
		   , store
		   , household_demographics
		   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
		      AND (store_sales.ss_store_sk = store.s_store_sk)
		      AND (store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND ((date_dim.d_dom BETWEEN 1 AND 3)
		         OR (date_dim.d_dom BETWEEN 25 AND 28))
		      AND ((household_demographics.hd_buy_potential = '>10000')
		         OR (household_demographics.hd_buy_potential = 'Unknown'))
		      AND (household_demographics.hd_vehicle_count > 0)
		      AND ((CASE WHEN (household_demographics.hd_vehicle_count > 0) THEN (CAST(household_demographics.hd_dep_count AS DECIMAL(7,2)) / household_demographics.hd_vehicle_count) ELSE null END) > CAST('1.2' AS DECIMAL))
		      AND (date_dim.d_year IN (1999   , (1999 + 1)   , (1999 + 2)))
		      AND (store.s_county IN ('Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'   , 'Williamson County'))
		   GROUP BY ss_ticket_number, ss_customer_sk
		)  dn
		, customer
		WHERE (ss_customer_sk = c_customer_sk)
		   AND (cnt BETWEEN 15 AND 20)
		ORDER BY c_last_name ASC, c_first_name ASC, c_salutation ASC, c_preferred_cust_flag DESC, ss_ticket_number ASC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 25> `c_last_name` ASC, <slot 26> `c_first_name` ASC, <slot 27> `c_salutation` ASC, <slot 28> `c_preferred_cust_flag` DESC, <slot 29> `ss_ticket_number` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 15> `ss_customer_sk` = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 63 65 66 67 68 69 \n" + 
				"  |  hash output slot ids: 16 20 21 22 23 14 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 16> count(*))\n" + 
				"  |  group by: <slot 14> `ss_ticket_number`, <slot 15> `ss_customer_sk`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: <slot 49>, <slot 50>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 43> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 49 50 \n" + 
				"  |  hash output slot ids: 39 40 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 34> = `store`.`s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 39 40 43 \n" + 
				"  |  hash output slot ids: 32 35 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`store_sales`.`ss_sold_date_sk` = `date_dim`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `date_dim`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 31 32 34 35 \n" + 
				"  |  hash output slot ids: 0 1 4 6 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `store_sales`.`ss_sold_date_sk`") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_buy_potential` = '>10000' OR `household_demographics`.`hd_buy_potential` = 'Unknown'), (`household_demographics`.`hd_vehicle_count` > 0), ((CASE WHEN (`household_demographics`.`hd_vehicle_count` > 0) THEN (CAST(`household_demographics`.`hd_dep_count` AS DECIMAL(7,2)) / `household_demographics`.`hd_vehicle_count`) ELSE NULL END) > 1.2)") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_county` IN ('Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County', 'Williamson County'))") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: ((`date_dim`.`d_dom` >= 1 AND `date_dim`.`d_dom` <= 3) OR (`date_dim`.`d_dom` >= 25 AND `date_dim`.`d_dom` <= 28)), (`date_dim`.`d_year` IN (1999, 2000, 2001))") 
            
        }
    }
}