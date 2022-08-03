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

suite("test_regression_test_tpcds_sf1_p1_q23", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  frequent_ss_items AS (
		   SELECT
		     substr(i_item_desc, 1, 30) itemdesc
		   , i_item_sk item_sk
		   , d_date solddate
		   , count(*) cnt
		   FROM
		     store_sales
		   , date_dim
		   , item
		   WHERE (ss_sold_date_sk = d_date_sk)
		      AND (ss_item_sk = i_item_sk)
		      AND (d_year IN (2000   , (2000 + 1)   , (2000 + 2)   , (2000 + 3)))
		   GROUP BY substr(i_item_desc, 1, 30), i_item_sk, d_date
		   HAVING (count(*) > 4)
		)
		, max_store_sales AS (
		   SELECT max(csales) tpcds_cmax
		   FROM
		     (
		      SELECT
		        c_customer_sk
		      , sum((ss_quantity * ss_sales_price)) csales
		      FROM
		        store_sales
		      , customer
		      , date_dim
		      WHERE (ss_customer_sk = c_customer_sk)
		         AND (ss_sold_date_sk = d_date_sk)
		         AND (d_year IN (2000      , (2000 + 1)      , (2000 + 2)      , (2000 + 3)))
		      GROUP BY c_customer_sk
		   ) x
		)
		, best_ss_customer AS (
		   SELECT
		     c_customer_sk
		   , sum((ss_quantity * ss_sales_price)) ssales
		   FROM
		     store_sales
		   , customer
		   WHERE (ss_customer_sk = c_customer_sk)
		   GROUP BY c_customer_sk
		   HAVING (sum((ss_quantity * ss_sales_price)) > ((50 / CAST('100.0' AS DECIMAL)) * (
		            SELECT *
		            FROM
		              max_store_sales
		         )))
		)
		SELECT
		  c_last_name
		, c_first_name
		, sales
		FROM
		  (
		   SELECT
		     c_last_name
		   , c_first_name
		   , sum((cs_quantity * cs_list_price)) sales
		   FROM
		     catalog_sales
		   , customer
		   , date_dim
		   WHERE (d_year = 2000)
		      AND (d_moy = 2)
		      AND (cs_sold_date_sk = d_date_sk)
		      AND (cs_item_sk IN (
		      SELECT item_sk
		      FROM
		        frequent_ss_items
		   ))
		      AND (cs_bill_customer_sk IN (
		      SELECT c_customer_sk
		      FROM
		        best_ss_customer
		   ))
		      AND (cs_bill_customer_sk = c_customer_sk)
		   GROUP BY c_last_name, c_first_name
		UNION ALL    SELECT
		     c_last_name
		   , c_first_name
		   , sum((ws_quantity * ws_list_price)) sales
		   FROM
		     web_sales
		   , customer
		   , date_dim
		   WHERE (d_year = 2000)
		      AND (d_moy = 2)
		      AND (ws_sold_date_sk = d_date_sk)
		      AND (ws_item_sk IN (
		      SELECT item_sk
		      FROM
		        frequent_ss_items
		   ))
		      AND (ws_bill_customer_sk IN (
		      SELECT c_customer_sk
		      FROM
		        best_ss_customer
		   ))
		      AND (ws_bill_customer_sk = c_customer_sk)
		   GROUP BY c_last_name, c_first_name
		) z 
		ORDER BY c_last_name ASC, c_first_name ASC, sales ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 170> `c_last_name` ASC, <slot 171> `c_first_name` ASC, <slot 172> `sales` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 163> sum((`ws_quantity` * `ws_list_price`)))\n" + 
				"  |  group by: <slot 161> `c_last_name`, <slot 162> `c_first_name`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 356> * <slot 357>))\n" + 
				"  |  group by: <slot 363>, <slot 364>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 335> = <slot 130> `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 108") && 
		explainStr.contains("output slot ids: 356 357 363 364 \n" + 
				"  |  hash output slot ids: 336 337 329 330 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 301> = <slot 103> `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 104") && 
		explainStr.contains("output slot ids: 329 330 335 336 337 \n" + 
				"  |  hash output slot ids: 308 309 310 302 303 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 277> = <slot 90> `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 100") && 
		explainStr.contains("output slot ids: 301 302 303 308 309 310 \n" + 
				"  |  hash output slot ids: 278 279 280 285 286 287 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 270> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 97") && 
		explainStr.contains("output slot ids: 277 278 279 280 285 286 287 \n" + 
				"  |  hash output slot ids: 272 152 153 154 269 270 271 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF006[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 96") && 
		explainStr.contains("output slot ids: 269 270 271 272 \n" + 
				"  |  hash output slot ids: 98 155 156 125 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF006[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: (<slot 131> sum((`ss_quantity` * `ss_sales_price`)) > ((50 / CAST('100.0' AS DECIMAL(9,0))) * <slot 146> max(`csales`)))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 131> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: <slot 130> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: max(<slot 145> max(`csales`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: max(<slot 142> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 142> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: <slot 141> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 347> * <slot 348>))\n" + 
				"  |  group by: <slot 351>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 345> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 107") && 
		explainStr.contains("output slot ids: 347 348 351 \n" + 
				"  |  hash output slot ids: 342 343 346 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF011[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 106") && 
		explainStr.contains("output slot ids: 342 343 345 346 \n" + 
				"  |  hash output slot ids: 134 135 136 138 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF011[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2000, 2001, 2002, 2003))") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 338> * <slot 339>))\n" + 
				"  |  group by: <slot 341>") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF010[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 105") && 
		explainStr.contains("output slot ids: 338 339 341 \n" + 
				"  |  hash output slot ids: 128 126 127 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF010[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: (<slot 104> sum((`ss_quantity` * `ss_sales_price`)) > ((50 / CAST('100.0' AS DECIMAL(9,0))) * <slot 119> max(`csales`)))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 104> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: <slot 103> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: max(<slot 118> max(`csales`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: max(<slot 115> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 115> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: <slot 114> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 320> * <slot 321>))\n" + 
				"  |  group by: <slot 324>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 318> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 103") && 
		explainStr.contains("output slot ids: 320 321 324 \n" + 
				"  |  hash output slot ids: 315 316 319 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF009[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 102") && 
		explainStr.contains("output slot ids: 315 316 318 319 \n" + 
				"  |  hash output slot ids: 107 108 109 111 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF009[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2000, 2001, 2002, 2003))") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 311> * <slot 312>))\n" + 
				"  |  group by: <slot 314>") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF008[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 101") && 
		explainStr.contains("output slot ids: 311 312 314 \n" + 
				"  |  hash output slot ids: 99 100 101 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF008[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 92> count(*))\n" + 
				"  |  group by: <slot 89> substr(`i_item_desc`, 1, 30), <slot 90> `i_item_sk`, <slot 91> `d_date`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: substr(<slot 298>, 1, 30), <slot 299>, <slot 295>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 289> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 99") && 
		explainStr.contains("output slot ids: 295 298 299 \n" + 
				"  |  hash output slot ids: 290 82 83 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF007[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 98") && 
		explainStr.contains("output slot ids: 289 290 \n" + 
				"  |  hash output slot ids: 84 87 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF007[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2000, 2001, 2002, 2003))") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000), (`d_moy` = 2)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 81> sum((`cs_quantity` * `cs_list_price`)))\n" + 
				"  |  group by: <slot 79> `c_last_name`, <slot 80> `c_first_name`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 260> * <slot 261>))\n" + 
				"  |  group by: <slot 267>, <slot 268>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 239> = <slot 48> `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 95") && 
		explainStr.contains("output slot ids: 260 261 267 268 \n" + 
				"  |  hash output slot ids: 240 241 233 234 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 205> = <slot 21> `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 91") && 
		explainStr.contains("output slot ids: 233 234 239 240 241 \n" + 
				"  |  hash output slot ids: 212 213 214 206 207 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 181> = <slot 8> `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 87") && 
		explainStr.contains("output slot ids: 205 206 207 212 213 214 \n" + 
				"  |  hash output slot ids: 182 183 184 189 190 191 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 174> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 84") && 
		explainStr.contains("output slot ids: 181 182 183 184 189 190 191 \n" + 
				"  |  hash output slot ids: 176 70 71 72 173 174 175 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 83") && 
		explainStr.contains("output slot ids: 173 174 175 176 \n" + 
				"  |  hash output slot ids: 16 73 74 43 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: (<slot 49> sum((`ss_quantity` * `ss_sales_price`)) > ((50 / CAST('100.0' AS DECIMAL(9,0))) * <slot 64> max(`csales`)))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 49> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: <slot 48> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: max(<slot 63> max(`csales`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: max(<slot 60> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 60> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: <slot 59> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 251> * <slot 252>))\n" + 
				"  |  group by: <slot 255>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 249> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 94") && 
		explainStr.contains("output slot ids: 251 252 255 \n" + 
				"  |  hash output slot ids: 246 247 250 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 93") && 
		explainStr.contains("output slot ids: 246 247 249 250 \n" + 
				"  |  hash output slot ids: 52 53 54 56 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF005[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2000, 2001, 2002, 2003))") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 242> * <slot 243>))\n" + 
				"  |  group by: <slot 245>") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 92") && 
		explainStr.contains("output slot ids: 242 243 245 \n" + 
				"  |  hash output slot ids: 44 45 46 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: (<slot 22> sum((`ss_quantity` * `ss_sales_price`)) > ((50 / CAST('100.0' AS DECIMAL(9,0))) * <slot 37> max(`csales`)))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 22> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: <slot 21> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: max(<slot 36> max(`csales`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: max(<slot 33> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 33> sum((`ss_quantity` * `ss_sales_price`)))\n" + 
				"  |  group by: <slot 32> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 224> * <slot 225>))\n" + 
				"  |  group by: <slot 228>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 222> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 90") && 
		explainStr.contains("output slot ids: 224 225 228 \n" + 
				"  |  hash output slot ids: 219 220 223 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 89") && 
		explainStr.contains("output slot ids: 219 220 222 223 \n" + 
				"  |  hash output slot ids: 25 26 27 29 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2000, 2001, 2002, 2003))") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 215> * <slot 216>))\n" + 
				"  |  group by: <slot 218>") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 88") && 
		explainStr.contains("output slot ids: 215 216 218 \n" + 
				"  |  hash output slot ids: 17 18 19 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 10> count(*))\n" + 
				"  |  group by: <slot 7> substr(`i_item_desc`, 1, 30), <slot 8> `i_item_sk`, <slot 9> `d_date`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: substr(<slot 202>, 1, 30), <slot 203>, <slot 199>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 193> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 86") && 
		explainStr.contains("output slot ids: 199 202 203 \n" + 
				"  |  hash output slot ids: 0 1 194 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 85") && 
		explainStr.contains("output slot ids: 193 194 \n" + 
				"  |  hash output slot ids: 2 5 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2000, 2001, 2002, 2003))") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000), (`d_moy` = 2)") 
            
        }
    }
}