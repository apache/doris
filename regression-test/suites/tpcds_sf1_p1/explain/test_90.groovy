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

suite("test_regression_test_tpcds_sf1_p1_q90", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT (CAST(amc AS DECIMAL(15,4)) / CAST(pmc AS DECIMAL(15,4))) am_pm_ratio
		FROM
		  (
		   SELECT count(*) amc
		   FROM
		     web_sales
		   , household_demographics
		   , time_dim
		   , web_page
		   WHERE (ws_sold_time_sk = time_dim.t_time_sk)
		      AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ws_web_page_sk = web_page.wp_web_page_sk)
		      AND (time_dim.t_hour BETWEEN 8 AND (8 + 1))
		      AND (household_demographics.hd_dep_count = 6)
		      AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
		)  at
		, (
		   SELECT count(*) pmc
		   FROM
		     web_sales
		   , household_demographics
		   , time_dim
		   , web_page
		   WHERE (ws_sold_time_sk = time_dim.t_time_sk)
		      AND (ws_ship_hdemo_sk = household_demographics.hd_demo_sk)
		      AND (ws_web_page_sk = web_page.wp_web_page_sk)
		      AND (time_dim.t_hour BETWEEN 19 AND (19 + 1))
		      AND (household_demographics.hd_dep_count = 6)
		      AND (web_page.wp_char_count BETWEEN 5000 AND 5200)
		)  pt
		ORDER BY am_pm_ratio ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 22> (CAST(`amc` AS DECIMAL(15,4)) / CAST(`pmc` AS DECIMAL(15,4))) ASC") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=1") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 9> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 20> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 51> = `web_page`.`wp_web_page_sk`)") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 56 \n" + 
				"  |  hash output slot ids: 49 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 45> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 49 51 \n" + 
				"  |  hash output slot ids: 44 46 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 44 45 46 \n" + 
				"  |  hash output slot ids: 11 13 15 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ws_sold_time_sk`") && 
		explainStr.contains("TABLE: web_page(web_page), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `web_page`.`wp_char_count` >= 5000, `web_page`.`wp_char_count` <= 5200") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 6)") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `time_dim`.`t_hour` >= 19, `time_dim`.`t_hour` <= 20") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 30> = `web_page`.`wp_web_page_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 35 \n" + 
				"  |  hash output slot ids: 28 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 24> = `household_demographics`.`hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 28 30 \n" + 
				"  |  hash output slot ids: 23 25 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_time_sk` = `time_dim`.`t_time_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `time_dim`.`t_time_sk`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 23 24 25 \n" + 
				"  |  hash output slot ids: 0 2 4 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws_sold_time_sk`") && 
		explainStr.contains("TABLE: web_page(web_page), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `web_page`.`wp_char_count` >= 5000, `web_page`.`wp_char_count` <= 5200") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`household_demographics`.`hd_dep_count` = 6)") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `time_dim`.`t_hour` >= 8, `time_dim`.`t_hour` <= 9") 
            
        }
    }
}