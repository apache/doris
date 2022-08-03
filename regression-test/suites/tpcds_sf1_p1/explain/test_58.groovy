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

suite("test_regression_test_tpcds_sf1_p1_q58", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  ss_items AS (
		   SELECT
		     i_item_id item_id
		   , sum(ss_ext_sales_price) ss_item_rev
		   FROM
		     store_sales
		   , item
		   , date_dim
		   WHERE (ss_item_sk = i_item_sk)
		      AND (d_date IN (
		      SELECT d_date
		      FROM
		        date_dim
		      WHERE (d_week_seq = (
		            SELECT d_week_seq
		            FROM
		              date_dim
		            WHERE (d_date = CAST('2000-01-03' AS DATE))
		         ))
		   ))
		      AND (ss_sold_date_sk = d_date_sk)
		   GROUP BY i_item_id
		)
		, cs_items AS (
		   SELECT
		     i_item_id item_id
		   , sum(cs_ext_sales_price) cs_item_rev
		   FROM
		     catalog_sales
		   , item
		   , date_dim
		   WHERE (cs_item_sk = i_item_sk)
		      AND (d_date IN (
		      SELECT d_date
		      FROM
		        date_dim
		      WHERE (d_week_seq = (
		            SELECT d_week_seq
		            FROM
		              date_dim
		            WHERE (d_date = CAST('2000-01-03' AS DATE))
		         ))
		   ))
		      AND (cs_sold_date_sk = d_date_sk)
		   GROUP BY i_item_id
		)
		, ws_items AS (
		   SELECT
		     i_item_id item_id
		   , sum(ws_ext_sales_price) ws_item_rev
		   FROM
		     web_sales
		   , item
		   , date_dim
		   WHERE (ws_item_sk = i_item_sk)
		      AND (d_date IN (
		      SELECT d_date
		      FROM
		        date_dim
		      WHERE (d_week_seq = (
		            SELECT d_week_seq
		            FROM
		              date_dim
		            WHERE (d_date = CAST('2000-01-03' AS DATE))
		         ))
		   ))
		      AND (ws_sold_date_sk = d_date_sk)
		   GROUP BY i_item_id
		)
		SELECT
		  ss_items.item_id
		, ss_item_rev
		, CAST((((ss_item_rev / ((CAST(ss_item_rev AS DECIMAL(16,7)) + cs_item_rev) + ws_item_rev)) / 3) * 100) AS DECIMAL(7,2)) ss_dev
		, cs_item_rev
		, CAST((((cs_item_rev / ((CAST(ss_item_rev AS DECIMAL(16,7)) + cs_item_rev) + ws_item_rev)) / 3) * 100) AS DECIMAL(7,2)) cs_dev
		, ws_item_rev
		, CAST((((ws_item_rev / ((CAST(ss_item_rev AS DECIMAL(16,7)) + cs_item_rev) + ws_item_rev)) / 3) * 100) AS DECIMAL(7,2)) ws_dev
		, (((ss_item_rev + cs_item_rev) + ws_item_rev) / 3) average
		FROM
		  ss_items
		, cs_items
		, ws_items
		WHERE (ss_items.item_id = cs_items.item_id)
		   AND (ss_items.item_id = ws_items.item_id)
		   AND (ss_item_rev BETWEEN (CAST('0.9' AS DECIMAL) * cs_item_rev) AND (CAST('1.1' AS DECIMAL) * cs_item_rev))
		   AND (ss_item_rev BETWEEN (CAST('0.9' AS DECIMAL) * ws_item_rev) AND (CAST('1.1' AS DECIMAL) * ws_item_rev))
		   AND (cs_item_rev BETWEEN (CAST('0.9' AS DECIMAL) * ss_item_rev) AND (CAST('1.1' AS DECIMAL) * ss_item_rev))
		   AND (cs_item_rev BETWEEN (CAST('0.9' AS DECIMAL) * ws_item_rev) AND (CAST('1.1' AS DECIMAL) * ws_item_rev))
		   AND (ws_item_rev BETWEEN (CAST('0.9' AS DECIMAL) * ss_item_rev) AND (CAST('1.1' AS DECIMAL) * ss_item_rev))
		   AND (ws_item_rev BETWEEN (CAST('0.9' AS DECIMAL) * cs_item_rev) AND (CAST('1.1' AS DECIMAL) * cs_item_rev))
		ORDER BY ss_items.item_id ASC, ss_item_rev ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 51> `ss_items`.`item_id` ASC, <slot 52> `ss_item_rev` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 97> = <slot 47> `i_item_id`)\n" + 
				"  |  other predicates: <slot 178> >= (0.9 * <slot 181>), <slot 178> <= (1.1 * <slot 181>), <slot 179> >= (0.9 * <slot 181>), <slot 179> <= (1.1 * <slot 181>), <slot 181> >= (0.9 * <slot 178>), <slot 181> <= (1.1 * <slot 178>), <slot 181> >= (0.9 * <slot 179>), <slot 181> <= (1.1 * <slot 179>)") && 
		explainStr.contains("other predicates: <slot 178> >= (0.9 * <slot 181>), <slot 178> <= (1.1 * <slot 181>), <slot 179> >= (0.9 * <slot 181>), <slot 179> <= (1.1 * <slot 181>), <slot 181> >= (0.9 * <slot 178>), <slot 181> <= (1.1 * <slot 178>), <slot 181> >= (0.9 * <slot 179>), <slot 181> <= (1.1 * <slot 179>)") && 
		explainStr.contains("vec output tuple id: 41") && 
		explainStr.contains("output slot ids: 122 123 125 127 \n" + 
				"  |  hash output slot ids: 48 97 98 100 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 13> `i_item_id` = <slot 30> `i_item_id`)\n" + 
				"  |  other predicates: <slot 159> >= (0.9 * <slot 161>), <slot 159> <= (1.1 * <slot 161>), <slot 161> >= (0.9 * <slot 159>), <slot 161> <= (1.1 * <slot 159>)") && 
		explainStr.contains("other predicates: <slot 159> >= (0.9 * <slot 161>), <slot 159> <= (1.1 * <slot 161>), <slot 161> >= (0.9 * <slot 159>), <slot 161> <= (1.1 * <slot 159>)") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 97 98 100 \n" + 
				"  |  hash output slot ids: 13 14 31 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 14> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 13> `i_item_id`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 48> sum(`ws_ext_sales_price`))\n" + 
				"  |  group by: <slot 47> `i_item_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 115>)\n" + 
				"  |  group by: <slot 118>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 111> = <slot 114>") && 
		explainStr.contains("vec output tuple id: 40") && 
		explainStr.contains("output slot ids: 115 118 \n" + 
				"  |  hash output slot ids: 106 109 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 103> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 38") && 
		explainStr.contains("output slot ids: 106 109 111 \n" + 
				"  |  hash output slot ids: 101 104 40 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 101 103 104 \n" + 
				"  |  hash output slot ids: 41 42 45 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `ws_item_sk`") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Inconsistent distribution of table and queries]\n" + 
				"  |  equal join conjunct: (`d_week_seq` = `d_week_seq`)\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `d_week_seq`") && 
		explainStr.contains("vec output tuple id: 39") && 
		explainStr.contains("output slot ids: 114 \n" + 
				"  |  hash output slot ids: 38 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF005[in_or_bloom] -> `d_week_seq`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_date` = '2000-01-03 00:00:00')") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 31> sum(`cs_ext_sales_price`))\n" + 
				"  |  group by: <slot 30> `i_item_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 90>)\n" + 
				"  |  group by: <slot 93>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 86> = <slot 89>") && 
		explainStr.contains("vec output tuple id: 35") && 
		explainStr.contains("output slot ids: 90 93 \n" + 
				"  |  hash output slot ids: 81 84 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 78> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 33") && 
		explainStr.contains("output slot ids: 81 84 86 \n" + 
				"  |  hash output slot ids: 23 76 79 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 32") && 
		explainStr.contains("output slot ids: 76 78 79 \n" + 
				"  |  hash output slot ids: 24 25 28 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Inconsistent distribution of table and queries]\n" + 
				"  |  equal join conjunct: (`d_week_seq` = `d_week_seq`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `d_week_seq`") && 
		explainStr.contains("vec output tuple id: 34") && 
		explainStr.contains("output slot ids: 89 \n" + 
				"  |  hash output slot ids: 21 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `d_week_seq`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_date` = '2000-01-03 00:00:00')") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 69>)\n" + 
				"  |  group by: <slot 72>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 65> = <slot 68>") && 
		explainStr.contains("vec output tuple id: 31") && 
		explainStr.contains("output slot ids: 69 72 \n" + 
				"  |  hash output slot ids: 60 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 57> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 29") && 
		explainStr.contains("output slot ids: 60 63 65 \n" + 
				"  |  hash output slot ids: 6 55 58 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 28") && 
		explainStr.contains("output slot ids: 55 57 58 \n" + 
				"  |  hash output slot ids: 7 8 11 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Inconsistent distribution of table and queries]\n" + 
				"  |  equal join conjunct: (`d_week_seq` = `d_week_seq`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_week_seq`") && 
		explainStr.contains("vec output tuple id: 30") && 
		explainStr.contains("output slot ids: 68 \n" + 
				"  |  hash output slot ids: 4 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `d_week_seq`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_date` = '2000-01-03 00:00:00')") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") 
            
        }
    }
}