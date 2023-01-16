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

suite("test_regression_test_tpcds_sf1_p1_q47", "regression_test_tpcds_sf1_p1") {
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
		   , s_store_name
		   , s_company_name
		   , d_year
		   , d_moy
		   , sum(ss_sales_price) sum_sales
		   , avg(sum(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year) avg_monthly_sales
		   , rank() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year ASC, d_moy ASC) rn
		   FROM
		     item
		   , store_sales
		   , date_dim
		   , store
		   WHERE (ss_item_sk = i_item_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND ((d_year = 1999)
		         OR ((d_year = (1999 - 1))
		            AND (d_moy = 12))
		         OR ((d_year = (1999 + 1))
		            AND (d_moy = 1)))
		   GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
		)
		, v2 AS (
		   SELECT
		     v1.i_category
		   , v1.i_brand
		   , v1.s_store_name
		   , v1.s_company_name
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
		      AND (v1.s_store_name = v1_lag.s_store_name)
		      AND (v1.s_store_name = v1_lead.s_store_name)
		      AND (v1.s_company_name = v1_lag.s_company_name)
		      AND (v1.s_company_name = v1_lead.s_company_name)
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
				"  |  order by: <slot 109> (`sum_sales` - `avg_monthly_sales`) ASC, <slot 110> `v2`.`s_store_name` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 275> = <slot 324> <slot 79> `i_category`)\n" + 
				"  |  equal join conjunct: (<slot 276> = <slot 325> <slot 80> `i_brand`)\n" + 
				"  |  equal join conjunct: (<slot 277> = <slot 326> <slot 81> `s_store_name`)\n" + 
				"  |  equal join conjunct: (<slot 278> = <slot 327> <slot 82> `s_company_name`)\n" + 
				"  |  equal join conjunct: (<slot 283> = (<slot 322> - 1))") && 
		explainStr.contains("vec output tuple id: 51") && 
		explainStr.contains("output slot ids: 350 351 352 353 354 355 356 357 365 373 \n" + 
				"  |  hash output slot ids: 290 275 276 277 278 279 280 281 282 330 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 179> <slot 153> <slot 13> `i_category` = <slot 249> <slot 46> `i_category`)\n" + 
				"  |  equal join conjunct: (<slot 180> <slot 154> <slot 14> `i_brand` = <slot 250> <slot 47> `i_brand`)\n" + 
				"  |  equal join conjunct: (<slot 181> <slot 155> <slot 15> `s_store_name` = <slot 251> <slot 48> `s_store_name`)\n" + 
				"  |  equal join conjunct: (<slot 182> <slot 156> <slot 16> `s_company_name` = <slot 252> <slot 49> `s_company_name`)\n" + 
				"  |  equal join conjunct: (<slot 192> <slot 151> = (<slot 247> + 1))") && 
		explainStr.contains("vec output tuple id: 43") && 
		explainStr.contains("output slot ids: 275 276 277 278 279 280 281 282 283 290 \n" + 
				"  |  hash output slot ids: 192 179 180 181 182 150 183 184 185 255 ") && 
		explainStr.contains("predicates: (<slot 183> <slot 157> <slot 17> `d_year` = 1999), (<slot 150> > 0), ((CASE WHEN (<slot 150> > 0) THEN (abs((<slot 185> <slot 159> <slot 19> sum(`ss_sales_price`) - <slot 150>)) / <slot 150>) ELSE NULL END) > 0.1)") && 
		explainStr.contains("order by: <slot 193> <slot 13> `i_category` ASC, <slot 194> <slot 14> `i_brand` ASC, <slot 195> <slot 15> `s_store_name` ASC, <slot 196> <slot 16> `s_company_name` ASC, <slot 197> <slot 17> `d_year` ASC") && 
		explainStr.contains("order by: <slot 328> <slot 83> `d_year` ASC NULLS FIRST, <slot 329> <slot 84> `d_moy` ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 324> <slot 79> `i_category` ASC, <slot 325> <slot 80> `i_brand` ASC, <slot 326> <slot 81> `s_store_name` ASC, <slot 327> <slot 82> `s_company_name` ASC, <slot 328> <slot 83> `d_year` ASC, <slot 329> <slot 84> `d_moy` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 85> sum(`ss_sales_price`))\n" + 
				"  |  group by: <slot 79> `i_category`, <slot 80> `i_brand`, <slot 81> `s_store_name`, <slot 82> `s_company_name`, <slot 83> `d_year`, <slot 84> `d_moy`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 309>)\n" + 
				"  |  group by: <slot 313>, <slot 314>, <slot 319>, <slot 320>, <slot 316>, <slot 317>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 302> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 46") && 
		explainStr.contains("output slot ids: 309 313 314 316 317 319 320 \n" + 
				"  |  hash output slot ids: 304 306 307 68 69 299 303 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 294> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 45") && 
		explainStr.contains("output slot ids: 299 302 303 304 306 307 \n" + 
				"  |  hash output slot ids: 292 70 295 71 296 297 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 44") && 
		explainStr.contains("output slot ids: 292 294 295 296 297 \n" + 
				"  |  hash output slot ids: 66 67 72 75 77 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998 OR `d_year` = 1999 OR `d_year` = 2000), ((`d_year` = 1999) OR ((`d_year` = 1998) AND (`d_moy` = 12)) OR ((`d_year` = 2000) AND (`d_moy` = 1)))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("order by: <slot 253> <slot 50> `d_year` ASC NULLS FIRST, <slot 254> <slot 51> `d_moy` ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 249> <slot 46> `i_category` ASC, <slot 250> <slot 47> `i_brand` ASC, <slot 251> <slot 48> `s_store_name` ASC, <slot 252> <slot 49> `s_company_name` ASC, <slot 253> <slot 50> `d_year` ASC, <slot 254> <slot 51> `d_moy` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 52> sum(`ss_sales_price`))\n" + 
				"  |  group by: <slot 46> `i_category`, <slot 47> `i_brand`, <slot 48> `s_store_name`, <slot 49> `s_company_name`, <slot 50> `d_year`, <slot 51> `d_moy`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 234>)\n" + 
				"  |  group by: <slot 238>, <slot 239>, <slot 244>, <slot 245>, <slot 241>, <slot 242>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 227> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 38") && 
		explainStr.contains("output slot ids: 234 238 239 241 242 244 245 \n" + 
				"  |  hash output slot ids: 224 35 228 36 229 231 232 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 219> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 224 227 228 229 231 232 \n" + 
				"  |  hash output slot ids: 37 38 217 220 221 222 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 217 219 220 221 222 \n" + 
				"  |  hash output slot ids: 33 34 39 42 44 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998 OR `d_year` = 1999 OR `d_year` = 2000), ((`d_year` = 1999) OR ((`d_year` = 1998) AND (`d_moy` = 12)) OR ((`d_year` = 2000) AND (`d_moy` = 1)))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("order by: <slot 157> <slot 17> `d_year` ASC NULLS FIRST, <slot 158> <slot 18> `d_moy` ASC NULLS FIRST") && 
		explainStr.contains("order by: <slot 153> <slot 13> `i_category` ASC, <slot 154> <slot 14> `i_brand` ASC, <slot 155> <slot 15> `s_store_name` ASC, <slot 156> <slot 16> `s_company_name` ASC, <slot 157> <slot 17> `d_year` ASC, <slot 158> <slot 18> `d_moy` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 19> sum(`ss_sales_price`))\n" + 
				"  |  group by: <slot 13> `i_category`, <slot 14> `i_brand`, <slot 15> `s_store_name`, <slot 16> `s_company_name`, <slot 17> `d_year`, <slot 18> `d_moy`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 137>)\n" + 
				"  |  group by: <slot 141>, <slot 142>, <slot 147>, <slot 148>, <slot 144>, <slot 145>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 130> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 28") && 
		explainStr.contains("output slot ids: 137 141 142 144 145 147 148 \n" + 
				"  |  hash output slot ids: 2 131 3 132 134 135 127 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 122> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 27") && 
		explainStr.contains("output slot ids: 127 130 131 132 134 135 \n" + 
				"  |  hash output slot ids: 4 5 120 123 124 125 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 26") && 
		explainStr.contains("output slot ids: 120 122 123 124 125 \n" + 
				"  |  hash output slot ids: 0 1 6 9 11 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998 OR `d_year` = 1999 OR `d_year` = 2000), ((`d_year` = 1999) OR ((`d_year` = 1998) AND (`d_moy` = 12)) OR ((`d_year` = 2000) AND (`d_moy` = 1)))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") 
            
        }
    }
}