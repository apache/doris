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

suite("test_regression_test_tpcds_sf1_p1_q43", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  s_store_name
		, s_store_id
		, sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
		, sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
		, sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
		, sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
		, sum((CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE null END)) thu_sales
		, sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
		, sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
		FROM
		  date_dim
		, store_sales
		, store
		WHERE (d_date_sk = ss_sold_date_sk)
		   AND (s_store_sk = ss_store_sk)
		   AND (s_gmt_offset = -5)
		   AND (d_year = 2000)
		GROUP BY s_store_name, s_store_id
		ORDER BY s_store_name ASC, s_store_id ASC, sun_sales ASC, mon_sales ASC, tue_sales ASC, wed_sales ASC, thu_sales ASC, fri_sales ASC, sat_sales ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 19> <slot 10> `s_store_name` ASC, <slot 20> <slot 11> `s_store_id` ASC, <slot 21> <slot 12> sum((CASE WHEN (`d_day_name` = 'Sunday') THEN `ss_sales_price` ELSE NULL END)) ASC, <slot 22> <slot 13> sum((CASE WHEN (`d_day_name` = 'Monday') THEN `ss_sales_price` ELSE NULL END)) ASC, <slot 23> <slot 14> sum((CASE WHEN (`d_day_name` = 'Tuesday') THEN `ss_sales_price` ELSE NULL END)) ASC, <slot 24> <slot 15> sum((CASE WHEN (`d_day_name` = 'Wednesday') THEN `ss_sales_price` ELSE NULL END)) ASC, <slot 25> <slot 16> sum((CASE WHEN (`d_day_name` = 'Thursday') THEN `ss_sales_price` ELSE NULL END)) ASC, <slot 26> <slot 17> sum((CASE WHEN (`d_day_name` = 'Friday') THEN `ss_sales_price` ELSE NULL END)) ASC, <slot 27> <slot 18> sum((CASE WHEN (`d_day_name` = 'Saturday') THEN `ss_sales_price` ELSE NULL END)) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 12> sum((CASE WHEN (`d_day_name` = 'Sunday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 13> sum((CASE WHEN (`d_day_name` = 'Monday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 14> sum((CASE WHEN (`d_day_name` = 'Tuesday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 15> sum((CASE WHEN (`d_day_name` = 'Wednesday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 16> sum((CASE WHEN (`d_day_name` = 'Thursday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 17> sum((CASE WHEN (`d_day_name` = 'Friday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 18> sum((CASE WHEN (`d_day_name` = 'Saturday') THEN `ss_sales_price` ELSE NULL END)))\n" + 
				"  |  group by: <slot 10> `s_store_name`, <slot 11> `s_store_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (<slot 37> = 'Sunday') THEN <slot 34> ELSE NULL END)), sum((CASE WHEN (<slot 37> = 'Monday') THEN <slot 34> ELSE NULL END)), sum((CASE WHEN (<slot 37> = 'Tuesday') THEN <slot 34> ELSE NULL END)), sum((CASE WHEN (<slot 37> = 'Wednesday') THEN <slot 34> ELSE NULL END)), sum((CASE WHEN (<slot 37> = 'Thursday') THEN <slot 34> ELSE NULL END)), sum((CASE WHEN (<slot 37> = 'Friday') THEN <slot 34> ELSE NULL END)), sum((CASE WHEN (<slot 37> = 'Saturday') THEN <slot 34> ELSE NULL END))\n" + 
				"  |  group by: <slot 40>, <slot 41>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 30> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 6") && 
		explainStr.contains("output slot ids: 34 37 40 41 \n" + 
				"  |  hash output slot ids: 0 1 28 31 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 5") && 
		explainStr.contains("output slot ids: 28 30 31 \n" + 
				"  |  hash output slot ids: 2 3 7 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`s_gmt_offset` = -5)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000)") 
            
        }
    }
}