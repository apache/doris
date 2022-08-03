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

suite("test_regression_test_tpcds_sf1_p1_q96", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT count(*)
		FROM
		  store_sales
		, household_demographics
		, time_dim
		, store
		WHERE (ss_sold_time_sk = time_dim.t_time_sk)
		   AND (ss_hdemo_sk = household_demographics.hd_demo_sk)
		   AND (ss_store_sk = s_store_sk)
		   AND (time_dim.t_hour = 20)
		   AND (time_dim.t_minute >= 30)
		   AND (household_demographics.hd_dep_count = 7)
		   AND (store.s_store_name = 'ese')
		ORDER BY count(*) ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 11> <slot 10> count(*) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 10> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 20> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 26 \n" + 
				"  |  hash output slot ids: 18 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 13> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 7") && 
		explainStr.contains("output slot ids: 18 20 \n" + 
				"  |  hash output slot ids: 12 14 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 12 13 14 \n" + 
				"  |  hash output slot ids: 0 2 4 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_time_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`store`.`s_store_name` = 'ese')") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 7)") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`time_dim`.`t_hour` = 20), (`time_dim`.`t_minute` >= 30)") 
            
        }
    }
}