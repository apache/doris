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

suite("test_regression_test_tpcds_sf1_p1_q44", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  asceding.rnk
		, i1.i_product_name best_performing
		, i2.i_product_name worst_performing
		FROM
		  (
		   SELECT *
		   FROM
		     (
		      SELECT
		        item_sk
		      , rank() OVER (ORDER BY rank_col ASC) rnk
		      FROM
		        (
		         SELECT
		           ss_item_sk item_sk
		         , avg(ss_net_profit) rank_col
		         FROM
		           store_sales ss1
		         WHERE (ss_store_sk = 4)
		         GROUP BY ss_item_sk
		         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL) * (
		                  SELECT avg(ss_net_profit) rank_col
		                  FROM
		                    store_sales
		                  WHERE (ss_store_sk = 4)
		                     AND (ss_addr_sk IS NULL)
		                  GROUP BY ss_store_sk
		               )))
		      )  v1
		   )  v11
		   WHERE (rnk < 11)
		)  asceding
		, (
		   SELECT *
		   FROM
		     (
		      SELECT
		        item_sk
		      , rank() OVER (ORDER BY rank_col DESC) rnk
		      FROM
		        (
		         SELECT
		           ss_item_sk item_sk
		         , avg(ss_net_profit) rank_col
		         FROM
		           store_sales ss1
		         WHERE (ss_store_sk = 4)
		         GROUP BY ss_item_sk
		         HAVING (avg(ss_net_profit) > (CAST('0.9' AS DECIMAL) * (
		                  SELECT avg(ss_net_profit) rank_col
		                  FROM
		                    store_sales
		                  WHERE (ss_store_sk = 4)
		                     AND (ss_addr_sk IS NULL)
		                  GROUP BY ss_store_sk
		               )))
		      )  v2
		   )  v21
		   WHERE (rnk < 11)
		)  descending
		, item i1
		, item i2
		WHERE (asceding.rnk = descending.rnk)
		   AND (i1.i_item_sk = asceding.item_sk)
		   AND (i2.i_item_sk = descending.item_sk)
		ORDER BY asceding.rnk ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 46> `asceding`.`rnk` ASC") && 
		explainStr.contains("join op: INNER JOIN(PARTITIONED)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 79> = `i2`.`i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 35") && 
		explainStr.contains("output slot ids: 84 87 89 \n" + 
				"  |  hash output slot ids: 81 43 78 ") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(PARTITIONED)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 73> = `i1`.`i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 34") && 
		explainStr.contains("output slot ids: 78 79 81 \n" + 
				"  |  hash output slot ids: 74 42 75 ") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 49> = <slot 61>)") && 
		explainStr.contains("vec output tuple id: 33") && 
		explainStr.contains("output slot ids: 73 74 75 \n" + 
				"  |  hash output slot ids: 49 51 63 ") && 
		explainStr.contains("predicates: (<slot 49> < 11)") && 
		explainStr.contains("order by: <slot 52> <slot 4> avg(`ss_net_profit`) ASC NULLS FIRST") && 
		explainStr.contains("predicates: (<slot 61> < 11)") && 
		explainStr.contains("order by: <slot 64> <slot 25> avg(`ss_net_profit`) DESC NULLS LAST") && 
		explainStr.contains("order by: <slot 67> `rank_col` DESC") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: (<slot 25> avg(`ss_net_profit`) > (CAST('0.9' AS DECIMAL(9,0)) * <slot 32> avg(`ss_net_profit`)))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 25> avg(`ss_net_profit`))\n" + 
				"  |  group by: <slot 24> `ss_item_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 32> avg(`ss_net_profit`))\n" + 
				"  |  group by: <slot 31> `ss_store_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_net_profit`)\n" + 
				"  |  group by: `ss_store_sk`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ss_store_sk` = 4), (`ss_addr_sk` IS NULL)") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_net_profit`)\n" + 
				"  |  group by: `ss_item_sk`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ss_store_sk` = 4)") && 
		explainStr.contains("order by: <slot 55> `rank_col` ASC") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: (<slot 4> avg(`ss_net_profit`) > (CAST('0.9' AS DECIMAL(9,0)) * <slot 11> avg(`ss_net_profit`)))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 4> avg(`ss_net_profit`))\n" + 
				"  |  group by: <slot 3> `ss_item_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 11> avg(`ss_net_profit`))\n" + 
				"  |  group by: <slot 10> `ss_store_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_net_profit`)\n" + 
				"  |  group by: `ss_store_sk`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ss_store_sk` = 4), (`ss_addr_sk` IS NULL)") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_net_profit`)\n" + 
				"  |  group by: `ss_item_sk`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ss_store_sk` = 4)") 
            
        }
    }
}