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

suite("test_regression_test_tpcds_sf1_p1_q88", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT *
		FROM
		  (
		   SELECT count(*) h8_30_to_9
		   FROM
		     store_sales
		   , household_demographics
		   , time_dim
		   , store
		   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
		      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (time_dim.t_hour = 8)
		      AND (time_dim.t_minute >= 30)
		      AND (((household_demographics.hd_dep_count = 4)
		            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
		         OR ((household_demographics.hd_dep_count = 2)
		            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
		         OR ((household_demographics.hd_dep_count = 0)
		            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
		      AND (store.s_store_name = 'ese')
		)  s1
		, (
		   SELECT count(*) h9_to_9_30
		   FROM
		     store_sales
		   , household_demographics
		   , time_dim
		   , store
		   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
		      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (time_dim.t_hour = 9)
		      AND (time_dim.t_minute < 30)
		      AND (((household_demographics.hd_dep_count = 4)
		            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
		         OR ((household_demographics.hd_dep_count = 2)
		            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
		         OR ((household_demographics.hd_dep_count = 0)
		            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
		      AND (store.s_store_name = 'ese')
		)  s2
		, (
		   SELECT count(*) h9_30_to_10
		   FROM
		     store_sales
		   , household_demographics
		   , time_dim
		   , store
		   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
		      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (time_dim.t_hour = 9)
		      AND (time_dim.t_minute >= 30)
		      AND (((household_demographics.hd_dep_count = 4)
		            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
		         OR ((household_demographics.hd_dep_count = 2)
		            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
		         OR ((household_demographics.hd_dep_count = 0)
		            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
		      AND (store.s_store_name = 'ese')
		)  s3
		, (
		   SELECT count(*) h10_to_10_30
		   FROM
		     store_sales
		   , household_demographics
		   , time_dim
		   , store
		   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
		      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (time_dim.t_hour = 10)
		      AND (time_dim.t_minute < 30)
		      AND (((household_demographics.hd_dep_count = 4)
		            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
		         OR ((household_demographics.hd_dep_count = 2)
		            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
		         OR ((household_demographics.hd_dep_count = 0)
		            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
		      AND (store.s_store_name = 'ese')
		)  s4
		, (
		   SELECT count(*) h10_30_to_11
		   FROM
		     store_sales
		   , household_demographics
		   , time_dim
		   , store
		   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
		      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (time_dim.t_hour = 10)
		      AND (time_dim.t_minute >= 30)
		      AND (((household_demographics.hd_dep_count = 4)
		            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
		         OR ((household_demographics.hd_dep_count = 2)
		            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
		         OR ((household_demographics.hd_dep_count = 0)
		            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
		      AND (store.s_store_name = 'ese')
		)  s5
		, (
		   SELECT count(*) h11_to_11_30
		   FROM
		     store_sales
		   , household_demographics
		   , time_dim
		   , store
		   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
		      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (time_dim.t_hour = 11)
		      AND (time_dim.t_minute < 30)
		      AND (((household_demographics.hd_dep_count = 4)
		            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
		         OR ((household_demographics.hd_dep_count = 2)
		            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
		         OR ((household_demographics.hd_dep_count = 0)
		            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
		      AND (store.s_store_name = 'ese')
		)  s6
		, (
		   SELECT count(*) h11_30_to_12
		   FROM
		     store_sales
		   , household_demographics
		   , time_dim
		   , store
		   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
		      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (time_dim.t_hour = 11)
		      AND (time_dim.t_minute >= 30)
		      AND (((household_demographics.hd_dep_count = 4)
		            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
		         OR ((household_demographics.hd_dep_count = 2)
		            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
		         OR ((household_demographics.hd_dep_count = 0)
		            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
		      AND (store.s_store_name = 'ese')
		)  s7
		, (
		   SELECT count(*) h12_to_12_30
		   FROM
		     store_sales
		   , household_demographics
		   , time_dim
		   , store
		   WHERE (ss_sold_time_sk = time_dim.t_time_sk)
		      AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (time_dim.t_hour = 12)
		      AND (time_dim.t_minute < 30)
		      AND (((household_demographics.hd_dep_count = 4)
		            AND (household_demographics.hd_vehicle_count <= (4 + 2)))
		         OR ((household_demographics.hd_dep_count = 2)
		            AND (household_demographics.hd_vehicle_count <= (2 + 2)))
		         OR ((household_demographics.hd_dep_count = 0)
		            AND (household_demographics.hd_vehicle_count <= (0 + 2))))
		      AND (store.s_store_name = 'ese')
		)  s8

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
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=1") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=1") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 11> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 102> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 294> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 71") && 
		explainStr.contains("output slot ids: 301 \n" + 
				"  |  hash output slot ids: 292 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 287> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 70") && 
		explainStr.contains("output slot ids: 292 294 \n" + 
				"  |  hash output slot ids: 288 286 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF007[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 69") && 
		explainStr.contains("output slot ids: 286 287 288 \n" + 
				"  |  hash output slot ids: 91 93 95 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF007[in_or_bloom] -> `ss_sold_time_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_store_name` = 'ese')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 0 OR `household_demographics`.`hd_dep_count` = 2 OR `household_demographics`.`hd_dep_count` = 4), `household_demographics`.`hd_vehicle_count` <= 6, (((`household_demographics`.`hd_dep_count` = 4) AND (`household_demographics`.`hd_vehicle_count` <= 6)) OR ((`household_demographics`.`hd_dep_count` = 2) AND (`household_demographics`.`hd_vehicle_count` <= 4)) OR ((`household_demographics`.`hd_dep_count` = 0) AND (`household_demographics`.`hd_vehicle_count` <= 2)))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`time_dim`.`t_hour` = 12), (`time_dim`.`t_minute` < 30)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 89> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 268> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 68") && 
		explainStr.contains("output slot ids: 275 \n" + 
				"  |  hash output slot ids: 266 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 261> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 67") && 
		explainStr.contains("output slot ids: 266 268 \n" + 
				"  |  hash output slot ids: 260 262 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF006[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 66") && 
		explainStr.contains("output slot ids: 260 261 262 \n" + 
				"  |  hash output slot ids: 80 82 78 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF006[in_or_bloom] -> `ss_sold_time_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_store_name` = 'ese')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 0 OR `household_demographics`.`hd_dep_count` = 2 OR `household_demographics`.`hd_dep_count` = 4), `household_demographics`.`hd_vehicle_count` <= 6, (((`household_demographics`.`hd_dep_count` = 4) AND (`household_demographics`.`hd_vehicle_count` <= 6)) OR ((`household_demographics`.`hd_dep_count` = 2) AND (`household_demographics`.`hd_vehicle_count` <= 4)) OR ((`household_demographics`.`hd_dep_count` = 0) AND (`household_demographics`.`hd_vehicle_count` <= 2)))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`time_dim`.`t_hour` = 11), (`time_dim`.`t_minute` >= 30)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 76> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 242> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 65") && 
		explainStr.contains("output slot ids: 249 \n" + 
				"  |  hash output slot ids: 240 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 235> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 64") && 
		explainStr.contains("output slot ids: 240 242 \n" + 
				"  |  hash output slot ids: 234 236 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 63") && 
		explainStr.contains("output slot ids: 234 235 236 \n" + 
				"  |  hash output slot ids: 65 67 69 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF005[in_or_bloom] -> `ss_sold_time_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_store_name` = 'ese')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 0 OR `household_demographics`.`hd_dep_count` = 2 OR `household_demographics`.`hd_dep_count` = 4), `household_demographics`.`hd_vehicle_count` <= 6, (((`household_demographics`.`hd_dep_count` = 4) AND (`household_demographics`.`hd_vehicle_count` <= 6)) OR ((`household_demographics`.`hd_dep_count` = 2) AND (`household_demographics`.`hd_vehicle_count` <= 4)) OR ((`household_demographics`.`hd_dep_count` = 0) AND (`household_demographics`.`hd_vehicle_count` <= 2)))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`time_dim`.`t_hour` = 11), (`time_dim`.`t_minute` < 30)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 63> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 216> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 62") && 
		explainStr.contains("output slot ids: 223 \n" + 
				"  |  hash output slot ids: 214 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 209> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 61") && 
		explainStr.contains("output slot ids: 214 216 \n" + 
				"  |  hash output slot ids: 208 210 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 60") && 
		explainStr.contains("output slot ids: 208 209 210 \n" + 
				"  |  hash output slot ids: 52 54 56 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `ss_sold_time_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_store_name` = 'ese')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 0 OR `household_demographics`.`hd_dep_count` = 2 OR `household_demographics`.`hd_dep_count` = 4), `household_demographics`.`hd_vehicle_count` <= 6, (((`household_demographics`.`hd_dep_count` = 4) AND (`household_demographics`.`hd_vehicle_count` <= 6)) OR ((`household_demographics`.`hd_dep_count` = 2) AND (`household_demographics`.`hd_vehicle_count` <= 4)) OR ((`household_demographics`.`hd_dep_count` = 0) AND (`household_demographics`.`hd_vehicle_count` <= 2)))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`time_dim`.`t_hour` = 10), (`time_dim`.`t_minute` >= 30)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 50> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 190> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 59") && 
		explainStr.contains("output slot ids: 197 \n" + 
				"  |  hash output slot ids: 188 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 183> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 58") && 
		explainStr.contains("output slot ids: 188 190 \n" + 
				"  |  hash output slot ids: 182 184 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 57") && 
		explainStr.contains("output slot ids: 182 183 184 \n" + 
				"  |  hash output slot ids: 39 41 43 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `ss_sold_time_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_store_name` = 'ese')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `household_demographics`.`hd_vehicle_count` <= 6, (`household_demographics`.`hd_dep_count` = 0 OR `household_demographics`.`hd_dep_count` = 2 OR `household_demographics`.`hd_dep_count` = 4), (((`household_demographics`.`hd_dep_count` = 4) AND (`household_demographics`.`hd_vehicle_count` <= 6)) OR ((`household_demographics`.`hd_dep_count` = 2) AND (`household_demographics`.`hd_vehicle_count` <= 4)) OR ((`household_demographics`.`hd_dep_count` = 0) AND (`household_demographics`.`hd_vehicle_count` <= 2)))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`time_dim`.`t_hour` = 10), (`time_dim`.`t_minute` < 30)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 37> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 164> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 56") && 
		explainStr.contains("output slot ids: 171 \n" + 
				"  |  hash output slot ids: 162 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 157> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 55") && 
		explainStr.contains("output slot ids: 162 164 \n" + 
				"  |  hash output slot ids: 156 158 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 54") && 
		explainStr.contains("output slot ids: 156 157 158 \n" + 
				"  |  hash output slot ids: 26 28 30 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ss_sold_time_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_store_name` = 'ese')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 0 OR `household_demographics`.`hd_dep_count` = 2 OR `household_demographics`.`hd_dep_count` = 4), `household_demographics`.`hd_vehicle_count` <= 6, (((`household_demographics`.`hd_dep_count` = 4) AND (`household_demographics`.`hd_vehicle_count` <= 6)) OR ((`household_demographics`.`hd_dep_count` = 2) AND (`household_demographics`.`hd_vehicle_count` <= 4)) OR ((`household_demographics`.`hd_dep_count` = 0) AND (`household_demographics`.`hd_vehicle_count` <= 2)))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`time_dim`.`t_hour` = 9), (`time_dim`.`t_minute` >= 30)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 24> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 138> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 53") && 
		explainStr.contains("output slot ids: 145 \n" + 
				"  |  hash output slot ids: 136 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 131> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 52") && 
		explainStr.contains("output slot ids: 136 138 \n" + 
				"  |  hash output slot ids: 130 132 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 51") && 
		explainStr.contains("output slot ids: 130 131 132 \n" + 
				"  |  hash output slot ids: 17 13 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_sold_time_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_store_name` = 'ese')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 0 OR `household_demographics`.`hd_dep_count` = 2 OR `household_demographics`.`hd_dep_count` = 4), `household_demographics`.`hd_vehicle_count` <= 6, (((`household_demographics`.`hd_dep_count` = 4) AND (`household_demographics`.`hd_vehicle_count` <= 6)) OR ((`household_demographics`.`hd_dep_count` = 2) AND (`household_demographics`.`hd_vehicle_count` <= 4)) OR ((`household_demographics`.`hd_dep_count` = 0) AND (`household_demographics`.`hd_vehicle_count` <= 2)))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`time_dim`.`t_hour` = 9), (`time_dim`.`t_minute` < 30)") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 112> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 50") && 
		explainStr.contains("output slot ids: 119 \n" + 
				"  |  hash output slot ids: 110 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 105> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 49") && 
		explainStr.contains("output slot ids: 110 112 \n" + 
				"  |  hash output slot ids: 104 106 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 48") && 
		explainStr.contains("output slot ids: 104 105 106 \n" + 
				"  |  hash output slot ids: 0 2 4 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_time_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_store_name` = 'ese')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 0 OR `household_demographics`.`hd_dep_count` = 2 OR `household_demographics`.`hd_dep_count` = 4), `household_demographics`.`hd_vehicle_count` <= 6, (((`household_demographics`.`hd_dep_count` = 4) AND (`household_demographics`.`hd_vehicle_count` <= 6)) OR ((`household_demographics`.`hd_dep_count` = 2) AND (`household_demographics`.`hd_vehicle_count` <= 4)) OR ((`household_demographics`.`hd_dep_count` = 0) AND (`household_demographics`.`hd_vehicle_count` <= 2)))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`time_dim`.`t_hour` = 8), (`time_dim`.`t_minute` >= 30)") 
            
        }
    }
}