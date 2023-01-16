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

suite("test_regression_test_tpcds_sf1_p1_q13", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  avg(ss_quantity)
		, avg(ss_ext_sales_price)
		, avg(ss_ext_wholesale_cost)
		, sum(ss_ext_wholesale_cost)
		FROM
		  store_sales
		, store
		, customer_demographics
		, household_demographics
		, customer_address
		, date_dim
		WHERE (s_store_sk = ss_store_sk)
		   AND (ss_sold_date_sk = d_date_sk)
		   AND (d_year = 2001)
		   AND (((ss_hdemo_sk = hd_demo_sk)
		         AND (cd_demo_sk = ss_cdemo_sk)
		         AND (cd_marital_status = 'M')
		         AND (cd_education_status = 'Advanced Degree')
		         AND (ss_sales_price BETWEEN CAST('100.00' AS DECIMAL) AND CAST('150.00' AS DECIMAL))
		         AND (hd_dep_count = 3))
		      OR ((ss_hdemo_sk = hd_demo_sk)
		         AND (cd_demo_sk = ss_cdemo_sk)
		         AND (cd_marital_status = 'S')
		         AND (cd_education_status = 'College')
		         AND (ss_sales_price BETWEEN CAST('50.00' AS DECIMAL) AND CAST('100.00' AS DECIMAL))
		         AND (hd_dep_count = 1))
		      OR ((ss_hdemo_sk = hd_demo_sk)
		         AND (cd_demo_sk = ss_cdemo_sk)
		         AND (cd_marital_status = 'W')
		         AND (cd_education_status = '2 yr Degree')
		         AND (ss_sales_price BETWEEN CAST('150.00' AS DECIMAL) AND CAST('200.00' AS DECIMAL))
		         AND (hd_dep_count = 1)))
		   AND (((ss_addr_sk = ca_address_sk)
		         AND (ca_country = 'United States')
		         AND (ca_state IN ('TX'      , 'OH'      , 'TX'))
		         AND (ss_net_profit BETWEEN 100 AND 200))
		      OR ((ss_addr_sk = ca_address_sk)
		         AND (ca_country = 'United States')
		         AND (ca_state IN ('OR'      , 'NM'      , 'KY'))
		         AND (ss_net_profit BETWEEN 150 AND 300))
		      OR ((ss_addr_sk = ca_address_sk)
		         AND (ca_country = 'United States')
		         AND (ca_state IN ('VA'      , 'TX'      , 'MS'))
		         AND (ss_net_profit BETWEEN 50 AND 250)))

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 21> avg(`ss_quantity`)), avg(<slot 22> avg(`ss_ext_sales_price`)), avg(<slot 23> avg(`ss_ext_wholesale_cost`)), sum(<slot 24> sum(`ss_ext_wholesale_cost`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: avg(<slot 86>), avg(<slot 87>), avg(<slot 88>), sum(<slot 88>)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 76> = `ca_address_sk`)\n" + 
				"  |  other predicates: (((<slot 157> IN ('TX', 'OH', 'TX')) AND <slot 154> >= 100 AND <slot 154> <= 200) OR ((<slot 157> IN ('OR', 'NM', 'KY')) AND <slot 154> >= 150 AND <slot 154> <= 300) OR ((<slot 157> IN ('VA', 'TX', 'MS')) AND <slot 154> >= 50 AND <slot 154> <= 250))") && 
		explainStr.contains("other predicates: (((<slot 157> IN ('TX', 'OH', 'TX')) AND <slot 154> >= 100 AND <slot 154> <= 200) OR ((<slot 157> IN ('OR', 'NM', 'KY')) AND <slot 154> >= 150 AND <slot 154> <= 300) OR ((<slot 157> IN ('VA', 'TX', 'MS')) AND <slot 154> >= 50 AND <slot 154> <= 250))") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 86 87 88 \n" + 
				"  |  hash output slot ids: 19 68 69 70 77 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 59> = `cd_demo_sk`)\n" + 
				"  |  other predicates: (((<slot 148> = 'M') AND (<slot 149> = 'Advanced Degree') AND <slot 143> >= 100.00 AND <slot 143> <= 150.00 AND (<slot 146> = 3)) OR ((<slot 148> = 'S') AND (<slot 149> = 'College') AND <slot 143> >= 50.00 AND <slot 143> <= 100.00 AND (<slot 146> = 1)) OR ((<slot 148> = 'W') AND (<slot 149> = '2 yr Degree') AND <slot 143> >= 150.00 AND <slot 143> <= 200.00 AND (<slot 146> = 1)))") && 
		explainStr.contains("other predicates: (((<slot 148> = 'M') AND (<slot 149> = 'Advanced Degree') AND <slot 143> >= 100.00 AND <slot 143> <= 150.00 AND (<slot 146> = 3)) OR ((<slot 148> = 'S') AND (<slot 149> = 'College') AND <slot 143> >= 50.00 AND <slot 143> <= 100.00 AND (<slot 146> = 1)) OR ((<slot 148> = 'W') AND (<slot 149> = '2 yr Degree') AND <slot 143> >= 150.00 AND <slot 143> <= 200.00 AND (<slot 146> = 1)))") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 68 69 70 76 77 \n" + 
				"  |  hash output slot ids: 67 53 54 55 12 60 61 13 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 45> = `hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 53 54 55 59 60 61 62 67 \n" + 
				"  |  hash output slot ids: 48 49 40 41 42 46 14 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 33> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 40 41 42 45 46 47 48 49 \n" + 
				"  |  hash output slot ids: 34 35 36 37 38 29 30 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_store_sk` = `s_store_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `s_store_sk`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 29 30 31 33 34 35 36 37 38 \n" + 
				"  |  hash output slot ids: 0 16 1 2 20 5 8 11 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_store_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_country` = 'United States'), `ca_state` IN ('OH', 'OR', 'NM', 'KY', 'VA', 'TX', 'MS')") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cd_marital_status` = 'M' OR `cd_marital_status` = 'S' OR `cd_marital_status` = 'W'), (`cd_education_status` = '2 yr Degree' OR `cd_education_status` = 'Advanced Degree' OR `cd_education_status` = 'College')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`hd_dep_count` = 1 OR `hd_dep_count` = 3)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") 
            
        }
    }
}