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

suite("test_regression_test_tpcds_sf1_p1_q57", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  v1 AS (
		   SELECT
		     i_category
		   , i_brand
		   , cc_name
		   , d_year
		   , d_moy
		   , sum(cs_sales_price) sum_sales
		   , avg(sum(cs_sales_price)) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) avg_monthly_sales
		   , rank() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year ASC, d_moy ASC) rn
		   FROM
		     item
		   , catalog_sales
		   , date_dim
		   , call_center
		   WHERE (cs_item_sk = i_item_sk)
		      AND (cs_sold_date_sk = d_date_sk)
		      AND (cc_call_center_sk = cs_call_center_sk)
		      AND ((d_year = 1999)
		         OR ((d_year = (1999 - 1))
		            AND (d_moy = 12))
		         OR ((d_year = (1999 + 1))
		            AND (d_moy = 1)))
		   GROUP BY i_category, i_brand, cc_name, d_year, d_moy
		)
		, v2 AS (
		   SELECT
		     v1.i_category
		   , v1.i_brand
		   , v1.cc_name
		   , v1.d_year
		   , v1.d_moy
		   , v1.avg_monthly_sales
		   , v1.sum_sales
		   , v1_lag.sum_sales psum
		   , v1_lead.sum_sales nsum
		   FROM
		     v1
		   , v1 v1_lag
		   , v1 v1_lead
		   WHERE (v1.i_category = v1_lag.i_category)
		      AND (v1.i_category = v1_lead.i_category)
		      AND (v1.i_brand = v1_lag.i_brand)
		      AND (v1.i_brand = v1_lead.i_brand)
		      AND (v1.cc_name = v1_lag.cc_name)
		      AND (v1.cc_name = v1_lead.cc_name)
		      AND (v1.rn = (v1_lag.rn + 1))
		      AND (v1.rn = (v1_lead.rn - 1))
		)
		SELECT *
		FROM
		  v2
		WHERE (d_year = 1999)
		   AND (avg_monthly_sales > 0)
		   AND ((CASE WHEN (avg_monthly_sales > 0) THEN (abs((sum_sales - avg_monthly_sales)) / avg_monthly_sales) ELSE null END) > CAST('0.1' AS DECIMAL))
		ORDER BY (sum_sales - avg_monthly_sales) ASC, 3 ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 99> (`sum_sales` - `avg_monthly_sales`) ASC, <slot 100> `v2`.`cc_name` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 248> = <slot 294> <slot 72> `i_category`)\n" + 
				"  |  equal join conjunct: (<slot 249> = <slot 295> <slot 73> `i_brand`)\n" + 
				"  |  equal join conjunct: (<slot 250> = <slot 296> <slot 74> `cc_name`)\n" + 
				"  |  equal join conjunct: (<slot 255> = (<slot 292> - 1))") && 
		explainStr.contains("vec output tuple id: 51") && 
		explainStr.contains("output slot ids: 316 317 318 319 320 321 322 329 336 \n" + 
				"  |  hash output slot ids: 261 248 249 250 251 299 252 253 254 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 163> <slot 141> <slot 12> `i_category` = <slot 226> <slot 42> `i_category`)\n" + 
				"  |  equal join conjunct: (<slot 164> <slot 142> <slot 13> `i_brand` = <slot 227> <slot 43> `i_brand`)\n" + 
				"  |  equal join conjunct: (<slot 165> <slot 143> <slot 14> `cc_name` = <slot 228> <slot 44> `cc_name`)\n" + 
				"  |  equal join conjunct: (<slot 174> <slot 139> = (<slot 224> + 1))") && 
		explainStr.contains("vec output tuple id: 43") && 
		explainStr.contains("output slot ids: 248 249 250 251 252 253 254 255 261 \n" + 
				"  |  hash output slot ids: 163 164 165 166 167 231 168 138 174 ") && 
		explainStr.contains("predicates: (<slot 166> <slot 144> <slot 15> `d_year` = 1999), (<slot 138> > 0), ((CASE WHEN (<slot 138> > 0) THEN (abs((<slot 168> <slot 146> <slot 17> sum(`cs_sales_price`) - <slot 138>)) / <slot 138>) ELSE NULL END) > 0.1)") && 
		explainStr.contains("order by: <slot 175> <slot 12> `i_category` ASC, <slot 176> <slot 13> `i_brand` ASC, <slot 177> <slot 14> `cc_name` ASC, <slot 178> <slot 15> `d_year` ASC") && 
		explainStr.contains("order by: <slot 297> <slot 75> `d_year` ASC NULLS FIRST, <slot 298> <slot 76> `d_moy` ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 294> <slot 72> `i_category` ASC, <slot 295> <slot 73> `i_brand` ASC, <slot 296> <slot 74> `cc_name` ASC, <slot 297> <slot 75> `d_year` ASC, <slot 298> <slot 76> `d_moy` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 77> sum(`cs_sales_price`))\n" + 
				"  |  group by: <slot 72> `i_category`, <slot 73> `i_brand`, <slot 74> `cc_name`, <slot 75> `d_year`, <slot 76> `d_moy`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 280>)\n" + 
				"  |  group by: <slot 284>, <slot 285>, <slot 290>, <slot 287>, <slot 288>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 273> = `cc_call_center_sk`)") && 
		explainStr.contains("vec output tuple id: 46") && 
		explainStr.contains("output slot ids: 280 284 285 287 288 290 \n" + 
				"  |  hash output slot ids: 274 275 277 278 270 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 265> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 45") && 
		explainStr.contains("output slot ids: 270 273 274 275 277 278 \n" + 
				"  |  hash output slot ids: 64 263 266 267 268 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 44") && 
		explainStr.contains("output slot ids: 263 265 266 267 268 \n" + 
				"  |  hash output slot ids: 65 68 71 60 61 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: call_center(call_center), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998 OR `d_year` = 1999 OR `d_year` = 2000), ((`d_year` = 1999) OR ((`d_year` = 1998) AND (`d_moy` = 12)) OR ((`d_year` = 2000) AND (`d_moy` = 1)))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("order by: <slot 229> <slot 45> `d_year` ASC NULLS FIRST, <slot 230> <slot 46> `d_moy` ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 226> <slot 42> `i_category` ASC, <slot 227> <slot 43> `i_brand` ASC, <slot 228> <slot 44> `cc_name` ASC, <slot 229> <slot 45> `d_year` ASC, <slot 230> <slot 46> `d_moy` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 47> sum(`cs_sales_price`))\n" + 
				"  |  group by: <slot 42> `i_category`, <slot 43> `i_brand`, <slot 44> `cc_name`, <slot 45> `d_year`, <slot 46> `d_moy`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 212>)\n" + 
				"  |  group by: <slot 216>, <slot 217>, <slot 222>, <slot 219>, <slot 220>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 205> = `cc_call_center_sk`)") && 
		explainStr.contains("vec output tuple id: 38") && 
		explainStr.contains("output slot ids: 212 216 217 219 220 222 \n" + 
				"  |  hash output slot ids: 32 209 210 202 206 207 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 197> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 202 205 206 207 209 210 \n" + 
				"  |  hash output slot ids: 33 34 195 198 199 200 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 195 197 198 199 200 \n" + 
				"  |  hash output slot ids: 35 38 41 30 31 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: call_center(call_center), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998 OR `d_year` = 1999 OR `d_year` = 2000), ((`d_year` = 1999) OR ((`d_year` = 1998) AND (`d_moy` = 12)) OR ((`d_year` = 2000) AND (`d_moy` = 1)))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("order by: <slot 144> <slot 15> `d_year` ASC NULLS FIRST, <slot 145> <slot 16> `d_moy` ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 141> <slot 12> `i_category` ASC, <slot 142> <slot 13> `i_brand` ASC, <slot 143> <slot 14> `cc_name` ASC, <slot 144> <slot 15> `d_year` ASC, <slot 145> <slot 16> `d_moy` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 17> sum(`cs_sales_price`))\n" + 
				"  |  group by: <slot 12> `i_category`, <slot 13> `i_brand`, <slot 14> `cc_name`, <slot 15> `d_year`, <slot 16> `d_moy`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 126>)\n" + 
				"  |  group by: <slot 130>, <slot 131>, <slot 136>, <slot 133>, <slot 134>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 119> = `cc_call_center_sk`)") && 
		explainStr.contains("vec output tuple id: 28") && 
		explainStr.contains("output slot ids: 126 130 131 133 134 136 \n" + 
				"  |  hash output slot ids: 2 116 120 121 123 124 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 111> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 27") && 
		explainStr.contains("output slot ids: 116 119 120 121 123 124 \n" + 
				"  |  hash output slot ids: 112 113 114 3 4 109 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 26") && 
		explainStr.contains("output slot ids: 109 111 112 113 114 \n" + 
				"  |  hash output slot ids: 0 1 5 8 11 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: call_center(call_center), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998 OR `d_year` = 1999 OR `d_year` = 2000), ((`d_year` = 1999) OR ((`d_year` = 1998) AND (`d_moy` = 12)) OR ((`d_year` = 2000) AND (`d_moy` = 1)))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") 
            
        }
    }
}