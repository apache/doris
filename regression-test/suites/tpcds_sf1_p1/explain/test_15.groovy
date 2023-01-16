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

suite("test_regression_test_tpcds_sf1_p1_q15", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  ca_zip
		, sum(cs_sales_price)
		FROM
		  catalog_sales
		, customer
		, customer_address
		, date_dim
		WHERE (cs_bill_customer_sk = c_customer_sk)
		   AND (c_current_addr_sk = ca_address_sk)
		   AND ((substr(ca_zip, 1, 5) IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
		      OR (ca_state IN ('CA'   , 'WA'   , 'GA'))
		      OR (cs_sales_price > 500))
		   AND (cs_sold_date_sk = d_date_sk)
		   AND (d_qoy = 2)
		   AND (d_year = 2001)
		GROUP BY ca_zip
		ORDER BY ca_zip ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 13> <slot 11> `ca_zip` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 12> sum(`cs_sales_price`))\n" + 
				"  |  group by: <slot 11> `ca_zip`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 28>)\n" + 
				"  |  group by: <slot 36>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 24> = `ca_address_sk`)\n" + 
				"  |  other predicates: ((substr(<slot 52>, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')) OR (<slot 54> IN ('CA', 'WA', 'GA')) OR (<slot 50> > 500))") && 
		explainStr.contains("other predicates: ((substr(<slot 52>, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')) OR (<slot 54> IN ('CA', 'WA', 'GA')) OR (<slot 50> > 500))") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 28 36 \n" + 
				"  |  hash output slot ids: 0 20 6 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 17> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 20 24 \n" + 
				"  |  hash output slot ids: 19 15 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_bill_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 15 17 19 \n" + 
				"  |  hash output slot ids: 1 4 7 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cs_bill_customer_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_qoy` = 2), (`d_year` = 2001)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") 
            
        }
    }
}