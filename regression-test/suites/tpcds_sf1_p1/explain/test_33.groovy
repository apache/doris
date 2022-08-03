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

suite("test_regression_test_tpcds_sf1_p1_q33", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  ss AS (
		   SELECT
		     i_manufact_id
		   , sum(ss_ext_sales_price) total_sales
		   FROM
		     store_sales
		   , date_dim
		   , customer_address
		   , item
		   WHERE (i_manufact_id IN (
		      SELECT i_manufact_id
		      FROM
		        item
		      WHERE (i_category IN ('Electronics'))
		   ))
		      AND (ss_item_sk = i_item_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (d_year = 1998)
		      AND (d_moy = 5)
		      AND (ss_addr_sk = ca_address_sk)
		      AND (ca_gmt_offset = -5)
		   GROUP BY i_manufact_id
		)
		, cs AS (
		   SELECT
		     i_manufact_id
		   , sum(cs_ext_sales_price) total_sales
		   FROM
		     catalog_sales
		   , date_dim
		   , customer_address
		   , item
		   WHERE (i_manufact_id IN (
		      SELECT i_manufact_id
		      FROM
		        item
		      WHERE (i_category IN ('Electronics'))
		   ))
		      AND (cs_item_sk = i_item_sk)
		      AND (cs_sold_date_sk = d_date_sk)
		      AND (d_year = 1998)
		      AND (d_moy = 5)
		      AND (cs_bill_addr_sk = ca_address_sk)
		      AND (ca_gmt_offset = -5)
		   GROUP BY i_manufact_id
		)
		, ws AS (
		   SELECT
		     i_manufact_id
		   , sum(ws_ext_sales_price) total_sales
		   FROM
		     web_sales
		   , date_dim
		   , customer_address
		   , item
		   WHERE (i_manufact_id IN (
		      SELECT i_manufact_id
		      FROM
		        item
		      WHERE (i_category IN ('Electronics'))
		   ))
		      AND (ws_item_sk = i_item_sk)
		      AND (ws_sold_date_sk = d_date_sk)
		      AND (d_year = 1998)
		      AND (d_moy = 5)
		      AND (ws_bill_addr_sk = ca_address_sk)
		      AND (ca_gmt_offset = -5)
		   GROUP BY i_manufact_id
		)
		SELECT
		  i_manufact_id
		, sum(total_sales) total_sales
		FROM
		  (
		   SELECT *
		   FROM
		     ss
		UNION ALL    SELECT *
		   FROM
		     cs
		UNION ALL    SELECT *
		   FROM
		     ws
		)  tmp1
		GROUP BY i_manufact_id
		ORDER BY total_sales ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 60> <slot 59> sum(`total_sales`) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 59> sum(`total_sales`))\n" + 
				"  |  group by: <slot 58> `i_manufact_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 55> `ss`.`total_sales` `cs`.`total_sales` `ws`.`total_sales`)\n" + 
				"  |  group by: <slot 54> `ss`.`i_manufact_id` `cs`.`i_manufact_id` `ws`.`i_manufact_id`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 51> sum(`ws_ext_sales_price`))\n" + 
				"  |  group by: <slot 50> `i_manufact_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 162>)\n" + 
				"  |  group by: <slot 166>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 155> = `i_manufact_id`") && 
		explainStr.contains("vec output tuple id: 39") && 
		explainStr.contains("output slot ids: 162 166 \n" + 
				"  |  hash output slot ids: 151 155 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 145> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 38") && 
		explainStr.contains("output slot ids: 151 155 \n" + 
				"  |  hash output slot ids: 146 142 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 138> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 142 145 146 \n" + 
				"  |  hash output slot ids: 136 139 140 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 136 138 139 140 \n" + 
				"  |  hash output slot ids: 39 40 43 47 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ws_item_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` IN ('Electronics'))") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_gmt_offset` = -5)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998), (`d_moy` = 5)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 33> sum(`cs_ext_sales_price`))\n" + 
				"  |  group by: <slot 32> `i_manufact_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 125>)\n" + 
				"  |  group by: <slot 129>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 118> = `i_manufact_id`") && 
		explainStr.contains("vec output tuple id: 35") && 
		explainStr.contains("output slot ids: 125 129 \n" + 
				"  |  hash output slot ids: 114 118 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 108> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 34") && 
		explainStr.contains("output slot ids: 114 118 \n" + 
				"  |  hash output slot ids: 105 109 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 101> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 33") && 
		explainStr.contains("output slot ids: 105 108 109 \n" + 
				"  |  hash output slot ids: 99 102 103 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 32") && 
		explainStr.contains("output slot ids: 99 101 102 103 \n" + 
				"  |  hash output slot ids: 21 22 25 29 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` IN ('Electronics'))") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_gmt_offset` = -5)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998), (`d_moy` = 5)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 15> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 14> `i_manufact_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 88>)\n" + 
				"  |  group by: <slot 92>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 81> = `i_manufact_id`") && 
		explainStr.contains("vec output tuple id: 31") && 
		explainStr.contains("output slot ids: 88 92 \n" + 
				"  |  hash output slot ids: 81 77 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 71> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 30") && 
		explainStr.contains("output slot ids: 77 81 \n" + 
				"  |  hash output slot ids: 68 72 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 64> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 29") && 
		explainStr.contains("output slot ids: 68 71 72 \n" + 
				"  |  hash output slot ids: 65 66 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 28") && 
		explainStr.contains("output slot ids: 62 64 65 66 \n" + 
				"  |  hash output slot ids: 3 4 7 11 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` IN ('Electronics'))") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_gmt_offset` = -5)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998), (`d_moy` = 5)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") 
            
        }
    }
}