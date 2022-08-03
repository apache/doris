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

suite("test_regression_test_tpcds_sf1_p1_q48", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT sum(ss_quantity)
		FROM
		  store_sales
		, store
		, customer_demographics
		, customer_address
		, date_dim
		WHERE (s_store_sk = ss_store_sk)
		   AND (ss_sold_date_sk = d_date_sk)
		   AND (d_year = 2000)
		   AND (((cd_demo_sk = ss_cdemo_sk)
		         AND (cd_marital_status = 'M')
		         AND (cd_education_status = '4 yr Degree')
		         AND (ss_sales_price BETWEEN CAST('100.00' AS DECIMAL) AND CAST('150.00' AS DECIMAL)))
		      OR ((cd_demo_sk = ss_cdemo_sk)
		         AND (cd_marital_status = 'D')
		         AND (cd_education_status = '2 yr Degree')
		         AND (ss_sales_price BETWEEN CAST('50.00' AS DECIMAL) AND CAST('100.00' AS DECIMAL)))
		      OR ((cd_demo_sk = ss_cdemo_sk)
		         AND (cd_marital_status = 'S')
		         AND (cd_education_status = 'College')
		         AND (ss_sales_price BETWEEN CAST('150.00' AS DECIMAL) AND CAST('200.00' AS DECIMAL))))
		   AND (((ss_addr_sk = ca_address_sk)
		         AND (ca_country = 'United States')
		         AND (ca_state IN ('CO'      , 'OH'      , 'TX'))
		         AND (ss_net_profit BETWEEN 0 AND 2000))
		      OR ((ss_addr_sk = ca_address_sk)
		         AND (ca_country = 'United States')
		         AND (ca_state IN ('OR'      , 'MN'      , 'KY'))
		         AND (ss_net_profit BETWEEN 150 AND 3000))
		      OR ((ss_addr_sk = ca_address_sk)
		         AND (ca_country = 'United States')
		         AND (ca_state IN ('VA'      , 'CA'      , 'MS'))
		         AND (ss_net_profit BETWEEN 50 AND 25000)))

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 16> sum(`ss_quantity`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(<slot 49>)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 41> = `ca_address_sk`)\n" + 
				"  |  other predicates: (((<slot 94> IN ('CO', 'OH', 'TX')) AND <slot 91> >= 0 AND <slot 91> <= 2000) OR ((<slot 94> IN ('OR', 'MN', 'KY')) AND <slot 91> >= 150 AND <slot 91> <= 3000) OR ((<slot 94> IN ('VA', 'CA', 'MS')) AND <slot 91> >= 50 AND <slot 91> <= 25000))") && 
		explainStr.contains("other predicates: (((<slot 94> IN ('CO', 'OH', 'TX')) AND <slot 91> >= 0 AND <slot 91> <= 2000) OR ((<slot 94> IN ('OR', 'MN', 'KY')) AND <slot 91> >= 150 AND <slot 91> <= 3000) OR ((<slot 94> IN ('VA', 'CA', 'MS')) AND <slot 91> >= 50 AND <slot 91> <= 25000))") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 49 \n" + 
				"  |  hash output slot ids: 36 42 14 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 29> = `cd_demo_sk`)\n" + 
				"  |  other predicates: (((<slot 87> = 'M') AND (<slot 88> = '4 yr Degree') AND <slot 83> >= 100.00 AND <slot 83> <= 150.00) OR ((<slot 87> = 'D') AND (<slot 88> = '2 yr Degree') AND <slot 83> >= 50.00 AND <slot 83> <= 100.00) OR ((<slot 87> = 'S') AND (<slot 88> = 'College') AND <slot 83> >= 150.00 AND <slot 83> <= 200.00))") && 
		explainStr.contains("other predicates: (((<slot 87> = 'M') AND (<slot 88> = '4 yr Degree') AND <slot 83> >= 100.00 AND <slot 83> <= 150.00) OR ((<slot 87> = 'D') AND (<slot 88> = '2 yr Degree') AND <slot 83> >= 50.00 AND <slot 83> <= 100.00) OR ((<slot 87> = 'S') AND (<slot 88> = 'College') AND <slot 83> >= 150.00 AND <slot 83> <= 200.00))") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 36 41 42 \n" + 
				"  |  hash output slot ids: 32 8 9 26 30 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 20> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 26 29 30 31 32 \n" + 
				"  |  hash output slot ids: 18 21 22 23 24 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_store_sk` = `s_store_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `s_store_sk`") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 18 20 21 22 23 24 \n" + 
				"  |  hash output slot ids: 0 3 7 10 11 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_store_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_country` = 'United States'), `ca_state` IN ('CO', 'OH', 'TX', 'OR', 'MN', 'KY', 'VA', 'CA', 'MS')") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cd_marital_status` = 'D' OR `cd_marital_status` = 'M' OR `cd_marital_status` = 'S'), (`cd_education_status` = '2 yr Degree' OR `cd_education_status` = '4 yr Degree' OR `cd_education_status` = 'College')") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000)") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") 
            
        }
    }
}