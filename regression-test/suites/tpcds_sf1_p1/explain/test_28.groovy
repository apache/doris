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

suite("test_regression_test_tpcds_sf1_p1_q28", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT *
		FROM
		  (
		   SELECT
		     avg(ss_list_price) b1_lp
		   , count(ss_list_price) b1_cnt
		   , count(DISTINCT ss_list_price) b1_cntd
		   FROM
		     store_sales
		   WHERE (ss_quantity BETWEEN 0 AND 5)
		      AND ((ss_list_price BETWEEN 8 AND (8 + 10))
		         OR (ss_coupon_amt BETWEEN 459 AND (459 + 1000))
		         OR (ss_wholesale_cost BETWEEN 57 AND (57 + 20)))
		)  b1
		, (
		   SELECT
		     avg(ss_list_price) b2_lp
		   , count(ss_list_price) b2_cnt
		   , count(DISTINCT ss_list_price) b2_cntd
		   FROM
		     store_sales
		   WHERE (ss_quantity BETWEEN 6 AND 10)
		      AND ((ss_list_price BETWEEN 90 AND (90 + 10))
		         OR (ss_coupon_amt BETWEEN 2323 AND (2323 + 1000))
		         OR (ss_wholesale_cost BETWEEN 31 AND (31 + 20)))
		)  b2
		, (
		   SELECT
		     avg(ss_list_price) b3_lp
		   , count(ss_list_price) b3_cnt
		   , count(DISTINCT ss_list_price) b3_cntd
		   FROM
		     store_sales
		   WHERE (ss_quantity BETWEEN 11 AND 15)
		      AND ((ss_list_price BETWEEN 142 AND (142 + 10))
		         OR (ss_coupon_amt BETWEEN 12214 AND (12214 + 1000))
		         OR (ss_wholesale_cost BETWEEN 79 AND (79 + 20)))
		)  b3
		, (
		   SELECT
		     avg(ss_list_price) b4_lp
		   , count(ss_list_price) b4_cnt
		   , count(DISTINCT ss_list_price) b4_cntd
		   FROM
		     store_sales
		   WHERE (ss_quantity BETWEEN 16 AND 20)
		      AND ((ss_list_price BETWEEN 135 AND (135 + 10))
		         OR (ss_coupon_amt BETWEEN 6071 AND (6071 + 1000))
		         OR (ss_wholesale_cost BETWEEN 38 AND (38 + 20)))
		)  b4
		, (
		   SELECT
		     avg(ss_list_price) b5_lp
		   , count(ss_list_price) b5_cnt
		   , count(DISTINCT ss_list_price) b5_cntd
		   FROM
		     store_sales
		   WHERE (ss_quantity BETWEEN 21 AND 25)
		      AND ((ss_list_price BETWEEN 122 AND (122 + 10))
		         OR (ss_coupon_amt BETWEEN 836 AND (836 + 1000))
		         OR (ss_wholesale_cost BETWEEN 17 AND (17 + 20)))
		)  b5
		, (
		   SELECT
		     avg(ss_list_price) b6_lp
		   , count(ss_list_price) b6_cnt
		   , count(DISTINCT ss_list_price) b6_cntd
		   FROM
		     store_sales
		   WHERE (ss_quantity BETWEEN 26 AND 30)
		      AND ((ss_list_price BETWEEN 154 AND (154 + 10))
		         OR (ss_coupon_amt BETWEEN 7326 AND (7326 + 1000))
		         OR (ss_wholesale_cost BETWEEN 7 AND (7 + 20)))
		)  b6
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=1") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=1") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=1") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=1") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=1") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 7> count(<slot 4> `ss_list_price`)), avg(<slot 8> <slot 5> avg(`ss_list_price`)), count(<slot 9> <slot 6> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 87> count(<slot 84> `ss_list_price`)), avg(<slot 88> <slot 85> avg(`ss_list_price`)), count(<slot 89> <slot 86> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(<slot 84> `ss_list_price`), avg(<slot 85> avg(`ss_list_price`)), count(<slot 86> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  output: avg(<slot 85> avg(`ss_list_price`)), count(<slot 86> count(`ss_list_price`))\n" + 
				"  |  group by: <slot 84> `ss_list_price`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_list_price`), count(`ss_list_price`)\n" + 
				"  |  group by: `ss_list_price`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `ss_quantity` >= 26, `ss_quantity` <= 30, ((`ss_list_price` >= 154 AND `ss_list_price` <= 164) OR (`ss_coupon_amt` >= 7326 AND `ss_coupon_amt` <= 8326) OR (`ss_wholesale_cost` >= 7 AND `ss_wholesale_cost` <= 27))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 71> count(<slot 68> `ss_list_price`)), avg(<slot 72> <slot 69> avg(`ss_list_price`)), count(<slot 73> <slot 70> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(<slot 68> `ss_list_price`), avg(<slot 69> avg(`ss_list_price`)), count(<slot 70> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  output: avg(<slot 69> avg(`ss_list_price`)), count(<slot 70> count(`ss_list_price`))\n" + 
				"  |  group by: <slot 68> `ss_list_price`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_list_price`), count(`ss_list_price`)\n" + 
				"  |  group by: `ss_list_price`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `ss_quantity` >= 21, `ss_quantity` <= 25, ((`ss_list_price` >= 122 AND `ss_list_price` <= 132) OR (`ss_coupon_amt` >= 836 AND `ss_coupon_amt` <= 1836) OR (`ss_wholesale_cost` >= 17 AND `ss_wholesale_cost` <= 37))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 55> count(<slot 52> `ss_list_price`)), avg(<slot 56> <slot 53> avg(`ss_list_price`)), count(<slot 57> <slot 54> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(<slot 52> `ss_list_price`), avg(<slot 53> avg(`ss_list_price`)), count(<slot 54> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  output: avg(<slot 53> avg(`ss_list_price`)), count(<slot 54> count(`ss_list_price`))\n" + 
				"  |  group by: <slot 52> `ss_list_price`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_list_price`), count(`ss_list_price`)\n" + 
				"  |  group by: `ss_list_price`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `ss_quantity` >= 16, `ss_quantity` <= 20, ((`ss_list_price` >= 135 AND `ss_list_price` <= 145) OR (`ss_coupon_amt` >= 6071 AND `ss_coupon_amt` <= 7071) OR (`ss_wholesale_cost` >= 38 AND `ss_wholesale_cost` <= 58))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 39> count(<slot 36> `ss_list_price`)), avg(<slot 40> <slot 37> avg(`ss_list_price`)), count(<slot 41> <slot 38> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(<slot 36> `ss_list_price`), avg(<slot 37> avg(`ss_list_price`)), count(<slot 38> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  output: avg(<slot 37> avg(`ss_list_price`)), count(<slot 38> count(`ss_list_price`))\n" + 
				"  |  group by: <slot 36> `ss_list_price`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_list_price`), count(`ss_list_price`)\n" + 
				"  |  group by: `ss_list_price`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `ss_quantity` >= 11, `ss_quantity` <= 15, ((`ss_list_price` >= 142 AND `ss_list_price` <= 152) OR (`ss_coupon_amt` >= 12214 AND `ss_coupon_amt` <= 13214) OR (`ss_wholesale_cost` >= 79 AND `ss_wholesale_cost` <= 99))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 23> count(<slot 20> `ss_list_price`)), avg(<slot 24> <slot 21> avg(`ss_list_price`)), count(<slot 25> <slot 22> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(<slot 20> `ss_list_price`), avg(<slot 21> avg(`ss_list_price`)), count(<slot 22> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  output: avg(<slot 21> avg(`ss_list_price`)), count(<slot 22> count(`ss_list_price`))\n" + 
				"  |  group by: <slot 20> `ss_list_price`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_list_price`), count(`ss_list_price`)\n" + 
				"  |  group by: `ss_list_price`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `ss_quantity` >= 6, `ss_quantity` <= 10, ((`ss_list_price` >= 90 AND `ss_list_price` <= 100) OR (`ss_coupon_amt` >= 2323 AND `ss_coupon_amt` <= 3323) OR (`ss_wholesale_cost` >= 31 AND `ss_wholesale_cost` <= 51))") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(<slot 4> `ss_list_price`), avg(<slot 5> avg(`ss_list_price`)), count(<slot 6> count(`ss_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge serialize)\n" + 
				"  |  output: avg(<slot 5> avg(`ss_list_price`)), count(<slot 6> count(`ss_list_price`))\n" + 
				"  |  group by: <slot 4> `ss_list_price`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(`ss_list_price`), count(`ss_list_price`)\n" + 
				"  |  group by: `ss_list_price`") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `ss_quantity` >= 0, `ss_quantity` <= 5, ((`ss_list_price` >= 8 AND `ss_list_price` <= 18) OR (`ss_coupon_amt` >= 459 AND `ss_coupon_amt` <= 1459) OR (`ss_wholesale_cost` >= 57 AND `ss_wholesale_cost` <= 77))") 
            
        }
    }
}