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

suite("test_regression_test_tpcds_sf1_p1_q84", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  c_customer_id customer_id
		, concat(concat(c_last_name, ', '), c_first_name) customername
		FROM
		  customer
		, customer_address
		, customer_demographics
		, household_demographics
		, income_band
		, store_returns
		WHERE (ca_city = 'Edgewood')
		   AND (c_current_addr_sk = ca_address_sk)
		   AND (ib_lower_bound >= 38128)
		   AND (ib_upper_bound <= (38128 + 50000))
		   AND (ib_income_band_sk = hd_income_band_sk)
		   AND (cd_demo_sk = c_current_cdemo_sk)
		   AND (hd_demo_sk = c_current_hdemo_sk)
		   AND (sr_cdemo_sk = cd_demo_sk)
		ORDER BY c_customer_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 15> `c_customer_id` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 53> = `ib_income_band_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 56 57 58 \n" + 
				"  |  hash output slot ids: 44 45 46 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 39> = `hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 44 45 46 53 \n" + 
				"  |  hash output slot ids: 34 35 36 9 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 29> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 34 35 36 39 \n" + 
				"  |  hash output slot ids: 26 27 28 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 18> = `sr_cdemo_sk`)") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 26 27 28 29 31 \n" + 
				"  |  hash output slot ids: 19 20 21 22 24 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cd_demo_sk` = `c_current_cdemo_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `c_current_cdemo_sk`") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 18 19 20 21 22 24 \n" + 
				"  |  hash output slot ids: 0 1 2 4 10 13 ") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cd_demo_sk`") && 
		explainStr.contains("TABLE: income_band(income_band), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ib_lower_bound` >= 38128), (`ib_upper_bound` <= 88128)") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_city` = 'Edgewood')") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") 
            
        }
    }
}