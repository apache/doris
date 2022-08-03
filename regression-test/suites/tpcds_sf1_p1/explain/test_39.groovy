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

suite("test_regression_test_tpcds_sf1_p1_q39", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  inv AS (
		   SELECT
		     w_warehouse_name
		   , w_warehouse_sk
		   , i_item_sk
		   , d_moy
		   , stdev
		   , mean
		   , (CASE mean WHEN 0 THEN null ELSE (stdev / mean) END) cov
		   FROM
		     (
		      SELECT
		        w_warehouse_name
		      , w_warehouse_sk
		      , i_item_sk
		      , d_moy
		      , stddev_samp(inv_quantity_on_hand) stdev
		      , avg(inv_quantity_on_hand) mean
		      FROM
		        inventory
		      , item
		      , warehouse
		      , date_dim
		      WHERE (inv_item_sk = i_item_sk)
		         AND (inv_warehouse_sk = w_warehouse_sk)
		         AND (inv_date_sk = d_date_sk)
		         AND (d_year = 2001)
		      GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy
		   )  foo
		   WHERE ((CASE mean WHEN 0 THEN 0 ELSE (stdev / mean) END) > 1)
		)
		SELECT
		  inv1.w_warehouse_sk
		, inv1.i_item_sk
		, inv1.d_moy
		, inv1.mean
		, inv1.cov
		, inv2.w_warehouse_sk
		, inv2.i_item_sk
		, inv2.d_moy
		, inv2.mean
		, inv2.cov
		FROM
		  inv inv1
		, inv inv2
		WHERE (inv1.i_item_sk = inv2.i_item_sk)
		   AND (inv1.w_warehouse_sk = inv2.w_warehouse_sk)
		   AND (inv1.d_moy = 1)
		   AND (inv2.d_moy = (1 + 1))
		   AND (inv1.cov > CAST('1.5' AS DECIMAL))
		ORDER BY inv1.w_warehouse_sk ASC, inv1.i_item_sk ASC, inv1.d_moy ASC, inv1.mean ASC, inv1.cov ASC, inv2.d_moy ASC, inv2.mean ASC, inv2.cov ASC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 70> `inv1`.`w_warehouse_sk` ASC, <slot 71> `inv1`.`i_item_sk` ASC, <slot 72> `inv1`.`d_moy` ASC, <slot 73> `inv1`.`mean` ASC, <slot 74> `inv1`.`cov` ASC, <slot 75> `inv2`.`d_moy` ASC, <slot 76> `inv2`.`mean` ASC, <slot 77> `inv2`.`cov` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 18> `i_item_sk` = <slot 53> `i_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 17> `w_warehouse_sk` = <slot 52> `w_warehouse_sk`)") && 
		explainStr.contains("vec output tuple id: 23") && 
		explainStr.contains("output slot ids: 125 126 127 129 130 132 133 134 136 137 \n" + 
				"  |  hash output slot ids: 17 18 19 20 52 21 53 54 55 56 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: stddev_samp(<slot 14> stddev_samp(`inv_quantity_on_hand`)), avg(<slot 15> avg(`inv_quantity_on_hand`))\n" + 
				"  |  group by: <slot 10> `w_warehouse_name`, <slot 11> `w_warehouse_sk`, <slot 12> `i_item_sk`, <slot 13> `d_moy`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: stddev_samp(<slot 49> stddev_samp(`inv_quantity_on_hand`)), avg(<slot 50> avg(`inv_quantity_on_hand`))\n" + 
				"  |  group by: <slot 45> `w_warehouse_name`, <slot 46> `w_warehouse_sk`, <slot 47> `i_item_sk`, <slot 48> `d_moy`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: stddev_samp(<slot 114>), avg(<slot 114>)\n" + 
				"  |  group by: <slot 119>, <slot 120>, <slot 118>, <slot 121>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 110> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 22") && 
		explainStr.contains("output slot ids: 114 118 119 120 121 \n" + 
				"  |  hash output slot ids: 112 113 38 107 111 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 104> = `w_warehouse_sk`)") && 
		explainStr.contains("vec output tuple id: 21") && 
		explainStr.contains("output slot ids: 107 110 111 112 113 \n" + 
				"  |  hash output slot ids: 35 36 102 105 106 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`inv_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 102 104 105 106 \n" + 
				"  |  hash output slot ids: 37 39 41 42 ") && 
		explainStr.contains("TABLE: inventory(inventory), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `inv_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001), (`d_moy` = 2)") && 
		explainStr.contains("TABLE: warehouse(warehouse), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: stddev_samp(<slot 92>), avg(<slot 92>)\n" + 
				"  |  group by: <slot 97>, <slot 98>, <slot 96>, <slot 99>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 88> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 92 96 97 98 99 \n" + 
				"  |  hash output slot ids: 3 85 89 90 91 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 82> = `w_warehouse_sk`)") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 85 88 89 90 91 \n" + 
				"  |  hash output slot ids: 80 0 1 83 84 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`inv_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 80 82 83 84 \n" + 
				"  |  hash output slot ids: 2 4 6 7 ") && 
		explainStr.contains("TABLE: inventory(inventory), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `inv_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001), (`d_moy` = 1)") && 
		explainStr.contains("TABLE: warehouse(warehouse), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") 
            
        }
    }
}