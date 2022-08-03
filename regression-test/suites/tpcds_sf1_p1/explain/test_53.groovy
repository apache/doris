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

suite("test_regression_test_tpcds_sf1_p1_q53", "tpch_sf1") {
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
		     i_manufact_id
		   , sum(ss_sales_price) sum_sales
		   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
		   FROM
		     item
		   , store_sales
		   , date_dim
		   , store
		   WHERE (ss_item_sk = i_item_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (d_month_seq IN (1200   , (1200 + 1)   , (1200 + 2)   , (1200 + 3)   , (1200 + 4)   , (1200 + 5)   , (1200 + 6)   , (1200 + 7)   , (1200 + 8)   , (1200 + 9)   , (1200 + 10)   , (1200 + 11)))
		      AND (((i_category IN ('Books'         , 'Children'         , 'Electronics'))
		            AND (i_class IN ('personal'         , 'portable'         , 'reference'         , 'self-help'))
		            AND (i_brand IN ('scholaramalgamalg #14'         , 'scholaramalgamalg #7'         , 'exportiunivamalg #9'         , 'scholaramalgamalg #9')))
		         OR ((i_category IN ('Women'         , 'Music'         , 'Men'))
		            AND (i_class IN ('accessories'         , 'classical'         , 'fragrances'         , 'pants'))
		            AND (i_brand IN ('amalgimporto #1'         , 'edu packscholar #1'         , 'exportiimporto #1'         , 'importoamalg #1'))))
		   GROUP BY i_manufact_id, d_qoy
		)  tmp1
		WHERE ((CASE WHEN (avg_quarterly_sales > 0) THEN (abs((CAST(sum_sales AS DECIMAL(27,4)) - avg_quarterly_sales)) / avg_quarterly_sales) ELSE null END) > CAST('0.1' AS DECIMAL))
		ORDER BY avg_quarterly_sales ASC, sum_sales ASC, i_manufact_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 20> `avg_quarterly_sales` ASC, <slot 21> `sum_sales` ASC, <slot 22> `i_manufact_id` ASC") && 
		explainStr.contains("predicates: ((CASE WHEN (<slot 57> > 0) THEN (abs((CAST(<slot 60> <slot 15> sum(`ss_sales_price`) AS DECIMAL(27,4)) - <slot 57>)) / <slot 57>) ELSE NULL END) > 0.1)") && 
		explainStr.contains("order by: <slot 58> <slot 13> `i_manufact_id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 15> sum(`ss_sales_price`))\n" + 
				"  |  group by: <slot 13> `i_manufact_id`, <slot 14> `d_qoy`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 44>)\n" + 
				"  |  group by: <slot 48>, <slot 55>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 35> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 44 48 55 \n" + 
				"  |  hash output slot ids: 32 36 43 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 25> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 32 35 36 43 \n" + 
				"  |  hash output slot ids: 23 26 27 12 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 23 25 26 27 \n" + 
				"  |  hash output slot ids: 0 1 4 6 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_month_seq` IN (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `i_category` IN ('Books', 'Children', 'Electronics', 'Women', 'Music', 'Men'), `i_class` IN ('personal', 'portable', 'reference', 'self-help', 'accessories', 'classical', 'fragrances', 'pants'), `i_brand` IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9', 'amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1'), (((`i_category` IN ('Books', 'Children', 'Electronics')) AND (`i_class` IN ('personal', 'portable', 'reference', 'self-help')) AND (`i_brand` IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))) OR ((`i_category` IN ('Women', 'Music', 'Men')) AND (`i_class` IN ('accessories', 'classical', 'fragrances', 'pants')) AND (`i_brand` IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1'))))") 
            
        }
    }
}