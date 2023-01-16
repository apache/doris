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

suite("test_regression_test_tpcds_sf1_p1_q65", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  s_store_name
		, i_item_desc
		, sc.revenue
		, i_current_price
		, i_wholesale_cost
		, i_brand
		FROM
		  store
		, item
		, (
		   SELECT
		     ss_store_sk
		   , avg(revenue) ave
		   FROM
		     (
		      SELECT
		        ss_store_sk
		      , ss_item_sk
		      , sum(ss_sales_price) revenue
		      FROM
		        store_sales
		      , date_dim
		      WHERE (ss_sold_date_sk = d_date_sk)
		         AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
		      GROUP BY ss_store_sk, ss_item_sk
		   )  sa
		   GROUP BY ss_store_sk
		)  sb
		, (
		   SELECT
		     ss_store_sk
		   , ss_item_sk
		   , sum(ss_sales_price) revenue
		   FROM
		     store_sales
		   , date_dim
		   WHERE (ss_sold_date_sk = d_date_sk)
		      AND (d_month_seq BETWEEN 1176 AND (1176 + 11))
		   GROUP BY ss_store_sk, ss_item_sk
		)  sc
		WHERE (sb.ss_store_sk = sc.ss_store_sk)
		   AND (sc.revenue <= (CAST('0.1' AS DECIMAL) * sb.ave))
		   AND (s_store_sk = sc.ss_store_sk)
		   AND (i_item_sk = sc.ss_item_sk)
		ORDER BY s_store_name ASC, i_item_desc ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 35> `s_store_name` ASC, <slot 36> `i_item_desc` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 66> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 71 72 73 74 78 81 \n" + 
				"  |  hash output slot ids: 64 68 28 61 62 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 52> = <slot 24> `ss_store_sk`)\n" + 
				"  |  other predicates: (<slot 108> <= (0.1 * <slot 110>))") && 
		explainStr.contains("other predicates: (<slot 108> <= (0.1 * <slot 110>))") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 61 62 63 64 66 68 \n" + 
				"  |  hash output slot ids: 48 49 50 52 54 25 47 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (`i_item_sk` = <slot 7> `ss_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 7> `ss_item_sk`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 47 48 49 50 52 54 \n" + 
				"  |  hash output slot ids: 32 6 8 29 30 31 ") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `i_item_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 25> avg(`revenue`))\n" + 
				"  |  group by: <slot 24> `ss_store_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(<slot 20> sum(`ss_sales_price`))\n" + 
				"  |  group by: <slot 18> `ss_store_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 20> sum(`ss_sales_price`))\n" + 
				"  |  group by: <slot 18> `ss_store_sk`, <slot 19> `ss_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 57>)\n" + 
				"  |  group by: <slot 55>, <slot 56>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 55 56 57 \n" + 
				"  |  hash output slot ids: 12 13 14 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1176, `d_month_seq` <= 1187") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 8> sum(`ss_sales_price`))\n" + 
				"  |  group by: <slot 6> `ss_store_sk`, <slot 7> `ss_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 43>)\n" + 
				"  |  group by: <slot 41>, <slot 42>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 41 42 43 \n" + 
				"  |  hash output slot ids: 0 1 2 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1176, `d_month_seq` <= 1187") 
            
        }
    }
}