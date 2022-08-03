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

suite("test_regression_test_tpcds_sf1_p1_q91", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  cc_call_center_id Call_Center
		, cc_name Call_Center_Name
		, cc_manager Manager
		, sum(cr_net_loss) Returns_Loss
		FROM
		  call_center
		, catalog_returns
		, date_dim
		, customer
		, customer_address
		, customer_demographics
		, household_demographics
		WHERE (cr_call_center_sk = cc_call_center_sk)
		   AND (cr_returned_date_sk = d_date_sk)
		   AND (cr_returning_customer_sk = c_customer_sk)
		   AND (cd_demo_sk = c_current_cdemo_sk)
		   AND (hd_demo_sk = c_current_hdemo_sk)
		   AND (ca_address_sk = c_current_addr_sk)
		   AND (d_year = 1998)
		   AND (d_moy = 11)
		   AND (((cd_marital_status = 'M')
		         AND (cd_education_status = 'Unknown'))
		      OR ((cd_marital_status = 'W')
		         AND (cd_education_status = 'Advanced Degree')))
		   AND (hd_buy_potential LIKE 'Unknown%')
		   AND (ca_gmt_offset = -7)
		GROUP BY cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status
		ORDER BY sum(cr_net_loss) DESC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 28> <slot 27> sum(`cr_net_loss`) DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 27> sum(`cr_net_loss`))\n" + 
				"  |  group by: <slot 22> `cc_call_center_id`, <slot 23> `cc_name`, <slot 24> `cc_manager`, <slot 25> `cd_marital_status`, <slot 26> `cd_education_status`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 104>)\n" + 
				"  |  group by: <slot 112>, <slot 113>, <slot 114>, <slot 98>, <slot 99>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 87> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 98 99 104 112 113 114 \n" + 
				"  |  hash output slot ids: 80 85 93 94 79 95 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 71> = `cc_call_center_sk`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 79 80 85 87 93 94 95 \n" + 
				"  |  hash output slot ids: 64 0 65 1 2 70 72 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 56> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 64 65 70 71 72 \n" + 
				"  |  hash output slot ids: 51 52 57 58 59 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 44> = `hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 51 52 56 57 58 59 \n" + 
				"  |  hash output slot ids: 48 40 41 45 46 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 35> = `cr_returning_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 40 41 44 45 46 47 48 \n" + 
				"  |  hash output slot ids: 33 34 3 4 37 38 6 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cd_demo_sk` = `c_current_cdemo_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `c_current_cdemo_sk`") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 33 34 35 37 38 \n" + 
				"  |  hash output slot ids: 18 19 9 13 15 ") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (((`cd_marital_status` = 'M') AND (`cd_education_status` = 'Unknown')) OR ((`cd_marital_status` = 'W') AND (`cd_education_status` = 'Advanced Degree')))\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cd_demo_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998), (`d_moy` = 11)") && 
		explainStr.contains("TABLE: call_center(call_center), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_gmt_offset` = -7)") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`hd_buy_potential` LIKE 'Unknown%')") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") 
            
        }
    }
}