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

suite("test_regression_test_tpcds_sf1_p1_q22", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_product_name
		, i_brand
		, i_class
		, i_category
		, avg(inv_quantity_on_hand) qoh
		FROM
		  inventory
		, date_dim
		, item
		WHERE (inv_date_sk = d_date_sk)
		   AND (inv_item_sk = i_item_sk)
		   AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
		GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
		ORDER BY qoh ASC, i_product_name ASC, i_brand ASC, i_class ASC, i_category ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 29> <slot 28> avg(`inv_quantity_on_hand`) ASC, <slot 30> <slot 23> `i_product_name` ASC, <slot 31> <slot 24> `i_brand` ASC, <slot 32> <slot 25> `i_class` ASC, <slot 33> <slot 26> `i_category` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 22> avg(`inv_quantity_on_hand`))\n" + 
				"  |  group by: <slot 17> `i_product_name`, <slot 18> `i_brand`, <slot 19> `i_class`, <slot 20> `i_category`, <slot 21> `GROUPING_ID`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(<slot 15> `inv_quantity_on_hand`)\n" + 
				"  |  group by: <slot 11> `i_product_name`, <slot 12> `i_brand`, <slot 13> `i_class`, <slot 14> `i_category`, <slot 16> `GROUPING_ID`") && 
		explainStr.contains("output slots: ``i_product_name``, ``i_brand``, ``i_class``, ``i_category``, ``inv_quantity_on_hand``, ``GROUPING_ID``") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 36> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 39 40 41 42 43 44 45 46 47 48 \n" + 
				"  |  hash output slot ids: 0 1 34 2 35 3 36 37 38 9 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`inv_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 34 35 36 37 38 \n" + 
				"  |  hash output slot ids: 4 6 7 8 10 ") && 
		explainStr.contains("TABLE: inventory(inventory), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `inv_date_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") 
            
        }
    }
}