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

suite("test_regression_test_tpcds_sf1_p1_q19", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_brand_id brand_id
		, i_brand brand
		, i_manufact_id
		, i_manufact
		, sum(ss_ext_sales_price) ext_price
		FROM
		  date_dim
		, store_sales
		, item
		, customer
		, customer_address
		, store
		WHERE (d_date_sk = ss_sold_date_sk)
		   AND (ss_item_sk = i_item_sk)
		   AND (i_manager_id = 8)
		   AND (d_moy = 11)
		   AND (d_year = 1998)
		   AND (ss_customer_sk = c_customer_sk)
		   AND (c_current_addr_sk = ca_address_sk)
		   AND (substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5))
		   AND (ss_store_sk = s_store_sk)
		GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
		ORDER BY ext_price DESC, i_brand ASC, i_brand_id ASC, i_manufact_id ASC, i_manufact ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 25> <slot 24> sum(`ss_ext_sales_price`) DESC, <slot 26> <slot 20> `i_brand` ASC, <slot 27> <slot 21> `i_brand_id` ASC, <slot 28> <slot 22> `i_manufact_id` ASC, <slot 29> <slot 23> `i_manufact` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 24> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 20> `i_brand`, <slot 21> `i_brand_id`, <slot 22> `i_manufact_id`, <slot 23> `i_manufact`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 86>)\n" + 
				"  |  group by: <slot 95>, <slot 94>, <slot 96>, <slot 97>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 83> = `ca_address_sk`)\n" + 
				"  |  other predicates: (substr(<slot 150>, 1, 5) != substr(<slot 148>, 1, 5))") && 
		explainStr.contains("other predicates: (substr(<slot 150>, 1, 5) != substr(<slot 148>, 1, 5))") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 86 94 95 96 97 \n" + 
				"  |  hash output slot ids: 16 68 84 76 77 78 79 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 56> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 68 76 77 78 79 83 84 \n" + 
				"  |  hash output slot ids: 17 67 52 60 61 62 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 41> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 52 56 60 61 62 63 67 \n" + 
				"  |  hash output slot ids: 48 49 38 42 46 14 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 32> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 38 41 42 46 47 48 49 \n" + 
				"  |  hash output slot ids: 0 33 1 34 2 3 30 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 30 32 33 34 \n" + 
				"  |  hash output slot ids: 18 4 7 12 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_manager_id` = 8)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_moy` = 11), (`d_year` = 1998)") 
            
        }
    }
}