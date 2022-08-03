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

suite("test_regression_test_tpcds_sf1_p1_q27", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_item_id
		, s_state
		, GROUPING (s_state) g_state
		, avg(ss_quantity) agg1
		, avg(ss_list_price) agg2
		, avg(ss_coupon_amt) agg3
		, avg(ss_sales_price) agg4
		FROM
		  store_sales
		, customer_demographics
		, date_dim
		, store
		, item
		WHERE (ss_sold_date_sk = d_date_sk)
		   AND (ss_item_sk = i_item_sk)
		   AND (ss_store_sk = s_store_sk)
		   AND (ss_cdemo_sk = cd_demo_sk)
		   AND (cd_gender = 'M')
		   AND (cd_marital_status = 'S')
		   AND (cd_education_status = 'College')
		   AND (d_year = 2002)
		   AND (s_state IN (
		     'TN'
		   , 'TN'
		   , 'TN'
		   , 'TN'
		   , 'TN'
		   , 'TN'))
		GROUP BY ROLLUP (i_item_id, s_state)
		ORDER BY i_item_id ASC, s_state ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 44> <slot 36> `i_item_id` ASC, <slot 45> <slot 37> `s_state` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 32> avg(`ss_quantity`)), avg(<slot 33> avg(`ss_list_price`)), avg(<slot 34> avg(`ss_coupon_amt`)), avg(<slot 35> avg(`ss_sales_price`))\n" + 
				"  |  group by: <slot 28> `i_item_id`, <slot 29> `s_state`, <slot 30> `GROUPING_ID`, <slot 31> `GROUPING_PREFIX_`s_state``") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(<slot 22> `ss_quantity`), avg(<slot 23> `ss_list_price`), avg(<slot 24> `ss_coupon_amt`), avg(<slot 25> `ss_sales_price`)\n" + 
				"  |  group by: <slot 20> `i_item_id`, <slot 21> `s_state`, <slot 26> `GROUPING_ID`, <slot 27> `GROUPING_PREFIX_`s_state``") && 
		explainStr.contains("output slots: ``i_item_id``, ``s_state``, ``ss_quantity``, ``ss_list_price``, ``ss_coupon_amt``, ``ss_sales_price``, ``GROUPING_ID``, ``GROUPING_PREFIX_`s_state```") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 80> = `cd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 \n" + 
				"  |  hash output slot ids: 73 74 75 76 77 78 79 15 80 16 81 17 82 18 83 84 85 86 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 67> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 73 74 75 76 77 78 79 80 81 82 83 84 85 86 \n" + 
				"  |  hash output slot ids: 64 65 1 66 67 68 69 70 71 72 13 61 62 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 56> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 61 62 63 64 65 66 67 68 69 70 71 72 \n" + 
				"  |  hash output slot ids: 0 51 52 53 54 55 56 57 58 59 11 60 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 51 52 53 54 55 56 57 58 59 60 \n" + 
				"  |  hash output slot ids: 2 3 19 4 5 8 9 10 12 14 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cd_gender` = 'M'), (`cd_marital_status` = 'S'), (`cd_education_status` = 'College')") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`s_state` IN ('TN', 'TN', 'TN', 'TN', 'TN', 'TN'))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") 
            
        }
    }
}