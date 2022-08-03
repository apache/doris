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

suite("test_regression_test_tpcds_sf1_p1_q81", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  customer_total_return AS (
		   SELECT
		     cr_returning_customer_sk ctr_customer_sk
		   , ca_state ctr_state
		   , sum(cr_return_amt_inc_tax) ctr_total_return
		   FROM
		     catalog_returns
		   , date_dim
		   , customer_address
		   WHERE (cr_returned_date_sk = d_date_sk)
		      AND (d_year = 2000)
		      AND (cr_returning_addr_sk = ca_address_sk)
		   GROUP BY cr_returning_customer_sk, ca_state
		)
		SELECT
		  c_customer_id
		, c_salutation
		, c_first_name
		, c_last_name
		, ca_street_number
		, ca_street_name
		, ca_street_type
		, ca_suite_number
		, ca_city
		, ca_county
		, ca_state
		, ca_zip
		, ca_country
		, ca_gmt_offset
		, ca_location_type
		, ctr_total_return
		FROM
		  customer_total_return ctr1
		, customer_address
		, customer
		WHERE (ctr1.ctr_total_return > (
		      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL))
		      FROM
		        customer_total_return ctr2
		      WHERE (ctr1.ctr_state = ctr2.ctr_state)
		   ))
		   AND (ca_address_sk = c_current_addr_sk)
		   AND (ca_state = 'GA')
		   AND (ctr1.ctr_customer_sk = c_customer_sk)
		ORDER BY c_customer_id ASC, c_salutation ASC, c_first_name ASC, c_last_name ASC, ca_street_number ASC, ca_street_name ASC, ca_street_type ASC, ca_suite_number ASC, ca_city ASC, ca_county ASC, ca_state ASC, ca_zip ASC, ca_country ASC, ca_gmt_offset ASC, ca_location_type ASC, ctr_total_return ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 50> `c_customer_id` ASC, <slot 51> `c_salutation` ASC, <slot 52> `c_first_name` ASC, <slot 53> `c_last_name` ASC, <slot 54> `ca_street_number` ASC, <slot 55> `ca_street_name` ASC, <slot 56> `ca_street_type` ASC, <slot 57> `ca_suite_number` ASC, <slot 58> `ca_city` ASC, <slot 59> `ca_county` ASC, <slot 60> `ca_state` ASC, <slot 61> `ca_zip` ASC, <slot 62> `ca_country` ASC, <slot 63> `ca_gmt_offset` ASC, <slot 64> `ca_location_type` ASC, <slot 65> `ctr_total_return` ASC") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 117> = <slot 28> `ctr2`.`ctr_state`)\n" + 
				"  |  other join predicates: (<slot 229> > (<slot 231> * 1.2))") && 
		explainStr.contains("other join predicates: (<slot 229> > (<slot 231> * 1.2))") && 
		explainStr.contains("vec output tuple id: 21") && 
		explainStr.contains("output slot ids: 133 134 135 136 139 140 141 142 143 144 145 146 147 148 149 153 \n" + 
				"  |  hash output slot ids: 98 99 100 101 104 105 106 107 108 109 110 111 112 113 114 118 29 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 71> = <slot 8> `cr_returning_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 98 99 100 101 104 105 106 107 108 109 110 111 112 113 114 117 118 \n" + 
				"  |  hash output slot ids: 66 67 68 69 72 73 9 74 10 75 76 77 78 79 80 81 82 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`c_current_addr_sk` = `ca_address_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `ca_address_sk`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 66 67 68 69 71 72 73 74 75 76 77 78 79 80 81 82 \n" + 
				"  |  hash output slot ids: 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 49 ") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `c_current_addr_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 29> avg(`ctr_total_return`))\n" + 
				"  |  group by: <slot 28> `ctr2`.`ctr_state`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(<slot 24> sum(`cr_return_amt_inc_tax`))\n" + 
				"  |  group by: <slot 23> `ca_state`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 24> sum(`cr_return_amt_inc_tax`))\n" + 
				"  |  group by: <slot 22> `cr_returning_customer_sk`, <slot 23> `ca_state`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 126>)\n" + 
				"  |  group by: <slot 125>, <slot 131>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 122> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 125 126 131 \n" + 
				"  |  hash output slot ids: 119 120 15 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cr_returned_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 119 120 122 \n" + 
				"  |  hash output slot ids: 16 20 14 ") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `cr_returned_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 10> sum(`cr_return_amt_inc_tax`))\n" + 
				"  |  group by: <slot 8> `cr_returning_customer_sk`, <slot 9> `ca_state`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 91>)\n" + 
				"  |  group by: <slot 90>, <slot 96>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 87> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 90 91 96 \n" + 
				"  |  hash output slot ids: 1 84 85 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cr_returned_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 84 85 87 \n" + 
				"  |  hash output slot ids: 0 2 6 ") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cr_returned_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000)") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_state` = 'GA')") 
            
        }
    }
}