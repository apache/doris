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

suite("test_regression_test_tpcds_sf1_p1_q83", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  sr_items AS (
		   SELECT
		     i_item_id item_id
		   , sum(sr_return_quantity) sr_item_qty
		   FROM
		     store_returns
		   , item
		   , date_dim
		   WHERE (sr_item_sk = i_item_sk)
		      AND (d_date IN (
		      SELECT d_date
		      FROM
		        date_dim
		      WHERE (d_week_seq IN (
		         SELECT d_week_seq
		         FROM
		           date_dim
		         WHERE (d_date IN (CAST('2000-06-30' AS DATE)         , CAST('2000-09-27' AS DATE)         , CAST('2000-11-17' AS DATE)))
		      ))
		   ))
		      AND (sr_returned_date_sk = d_date_sk)
		   GROUP BY i_item_id
		)
		, cr_items AS (
		   SELECT
		     i_item_id item_id
		   , sum(cr_return_quantity) cr_item_qty
		   FROM
		     catalog_returns
		   , item
		   , date_dim
		   WHERE (cr_item_sk = i_item_sk)
		      AND (d_date IN (
		      SELECT d_date
		      FROM
		        date_dim
		      WHERE (d_week_seq IN (
		         SELECT d_week_seq
		         FROM
		           date_dim
		         WHERE (d_date IN (CAST('2000-06-30' AS DATE)         , CAST('2000-09-27' AS DATE)         , CAST('2000-11-17' AS DATE)))
		      ))
		   ))
		      AND (cr_returned_date_sk = d_date_sk)
		   GROUP BY i_item_id
		)
		, wr_items AS (
		   SELECT
		     i_item_id item_id
		   , sum(wr_return_quantity) wr_item_qty
		   FROM
		     web_returns
		   , item
		   , date_dim
		   WHERE (wr_item_sk = i_item_sk)
		      AND (d_date IN (
		      SELECT d_date
		      FROM
		        date_dim
		      WHERE (d_week_seq IN (
		         SELECT d_week_seq
		         FROM
		           date_dim
		         WHERE (d_date IN (CAST('2000-06-30' AS DATE)         , CAST('2000-09-27' AS DATE)         , CAST('2000-11-17' AS DATE)))
		      ))
		   ))
		      AND (wr_returned_date_sk = d_date_sk)
		   GROUP BY i_item_id
		)
		SELECT
		  sr_items.item_id
		, sr_item_qty
		, CAST((((sr_item_qty / ((CAST(sr_item_qty AS DECIMAL(9,4)) + cr_item_qty) + wr_item_qty)) / CAST('3.0' AS DECIMAL)) * 100) AS DECIMAL(7,2)) sr_dev
		, cr_item_qty
		, CAST((((cr_item_qty / ((CAST(sr_item_qty AS DECIMAL(9,4)) + cr_item_qty) + wr_item_qty)) / CAST('3.0' AS DECIMAL)) * 100) AS DECIMAL(7,2)) cr_dev
		, wr_item_qty
		, CAST((((wr_item_qty / ((CAST(sr_item_qty AS DECIMAL(9,4)) + cr_item_qty) + wr_item_qty)) / CAST('3.0' AS DECIMAL)) * 100) AS DECIMAL(7,2)) wr_dev
		, (((sr_item_qty + cr_item_qty) + wr_item_qty) / CAST('3.00' AS DECIMAL)) average
		FROM
		  sr_items
		, cr_items
		, wr_items
		WHERE (sr_items.item_id = cr_items.item_id)
		   AND (sr_items.item_id = wr_items.item_id)
		ORDER BY sr_items.item_id ASC, sr_item_qty ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 51> `sr_items`.`item_id` ASC, <slot 52> `sr_item_qty` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 97> = <slot 47> `i_item_id`)") && 
		explainStr.contains("vec output tuple id: 41") && 
		explainStr.contains("output slot ids: 122 123 125 127 \n" + 
				"  |  hash output slot ids: 48 97 98 100 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 13> `i_item_id` = <slot 30> `i_item_id`)") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 97 98 100 \n" + 
				"  |  hash output slot ids: 13 14 31 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 14> sum(`sr_return_quantity`))\n" + 
				"  |  group by: <slot 13> `i_item_id`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 48> sum(`wr_return_quantity`))\n" + 
				"  |  group by: <slot 47> `i_item_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 117>)\n" + 
				"  |  group by: <slot 120>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 106> = <slot 114>") && 
		explainStr.contains("vec output tuple id: 40") && 
		explainStr.contains("output slot ids: 117 120 \n" + 
				"  |  hash output slot ids: 108 111 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 104> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 38") && 
		explainStr.contains("output slot ids: 106 108 111 \n" + 
				"  |  hash output slot ids: 101 103 41 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`d_date_sk` = `wr_returned_date_sk`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `wr_returned_date_sk`") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 101 103 104 \n" + 
				"  |  hash output slot ids: 40 42 43 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `d_date_sk`") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Inconsistent distribution of table and queries]\n" + 
				"  |  equal join conjunct: `d_week_seq` = `d_week_seq`\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `d_week_seq`") && 
		explainStr.contains("vec output tuple id: 39") && 
		explainStr.contains("output slot ids: 114 \n" + 
				"  |  hash output slot ids: 38 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF005[in_or_bloom] -> `d_week_seq`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_date` IN ('2000-06-30', '2000-09-27', '2000-11-17'))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 31> sum(`cr_return_quantity`))\n" + 
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
				"  |  equal join conjunct: (`cr_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 32") && 
		explainStr.contains("output slot ids: 76 78 79 \n" + 
				"  |  hash output slot ids: 24 25 28 ") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `cr_item_sk`") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Inconsistent distribution of table and queries]\n" + 
				"  |  equal join conjunct: `d_week_seq` = `d_week_seq`\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `d_week_seq`") && 
		explainStr.contains("vec output tuple id: 34") && 
		explainStr.contains("output slot ids: 89 \n" + 
				"  |  hash output slot ids: 21 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `d_week_seq`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_date` IN ('2000-06-30', '2000-09-27', '2000-11-17'))") && 
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
				"  |  equal join conjunct: (`sr_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 28") && 
		explainStr.contains("output slot ids: 55 57 58 \n" + 
				"  |  hash output slot ids: 7 8 11 ") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `sr_item_sk`") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[Inconsistent distribution of table and queries]\n" + 
				"  |  equal join conjunct: `d_week_seq` = `d_week_seq`\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_week_seq`") && 
		explainStr.contains("vec output tuple id: 30") && 
		explainStr.contains("output slot ids: 68 \n" + 
				"  |  hash output slot ids: 4 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `d_week_seq`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_date` IN ('2000-06-30', '2000-09-27', '2000-11-17'))") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") 
            
        }
    }
}