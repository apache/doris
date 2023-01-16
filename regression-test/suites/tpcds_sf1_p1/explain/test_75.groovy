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

suite("test_regression_test_tpcds_sf1_p1_q75", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  all_sales AS (
		   SELECT
		     d_year
		   , i_brand_id
		   , i_class_id
		   , i_category_id
		   , i_manufact_id
		   , sum(sales_cnt) sales_cnt
		   , sum(sales_amt) sales_amt
		   FROM
		     (
		      SELECT
		        d_year
		      , i_brand_id
		      , i_class_id
		      , i_category_id
		      , i_manufact_id
		      , (cs_quantity - COALESCE(cr_return_quantity, 0)) sales_cnt
		      , (cs_ext_sales_price - COALESCE(cr_return_amount, CAST('0.0' AS DECIMAL))) sales_amt
		      FROM
		        catalog_sales
		      INNER JOIN item ON (i_item_sk = cs_item_sk)
		      INNER JOIN date_dim ON (d_date_sk = cs_sold_date_sk)
		      LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number)
		         AND (cs_item_sk = cr_item_sk)
		      WHERE (i_category = 'Books')
		UNION       SELECT
		        d_year
		      , i_brand_id
		      , i_class_id
		      , i_category_id
		      , i_manufact_id
		      , (ss_quantity - COALESCE(sr_return_quantity, 0)) sales_cnt
		      , (ss_ext_sales_price - COALESCE(sr_return_amt, CAST('0.0' AS DECIMAL))) sales_amt
		      FROM
		        store_sales
		      INNER JOIN item ON (i_item_sk = ss_item_sk)
		      INNER JOIN date_dim ON (d_date_sk = ss_sold_date_sk)
		      LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number)
		         AND (ss_item_sk = sr_item_sk)
		      WHERE (i_category = 'Books')
		UNION       SELECT
		        d_year
		      , i_brand_id
		      , i_class_id
		      , i_category_id
		      , i_manufact_id
		      , (ws_quantity - COALESCE(wr_return_quantity, 0)) sales_cnt
		      , (ws_ext_sales_price - COALESCE(wr_return_amt, CAST('0.0' AS DECIMAL))) sales_amt
		      FROM
		        web_sales
		      INNER JOIN item ON (i_item_sk = ws_item_sk)
		      INNER JOIN date_dim ON (d_date_sk = ws_sold_date_sk)
		      LEFT JOIN web_returns ON (ws_order_number = wr_order_number)
		         AND (ws_item_sk = wr_item_sk)
		      WHERE (i_category = 'Books')
		   )  sales_detail
		   GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id
		)
		SELECT
		  prev_yr.d_year prev_year
		, curr_yr.d_year year
		, curr_yr.i_brand_id
		, curr_yr.i_class_id
		, curr_yr.i_category_id
		, curr_yr.i_manufact_id
		, prev_yr.sales_cnt prev_yr_cnt
		, curr_yr.sales_cnt curr_yr_cnt
		, (curr_yr.sales_cnt - prev_yr.sales_cnt) sales_cnt_diff
		, (curr_yr.sales_amt - prev_yr.sales_amt) sales_amt_diff
		FROM
		  all_sales curr_yr
		, all_sales prev_yr
		WHERE (curr_yr.i_brand_id = prev_yr.i_brand_id)
		   AND (curr_yr.i_class_id = prev_yr.i_class_id)
		   AND (curr_yr.i_category_id = prev_yr.i_category_id)
		   AND (curr_yr.i_manufact_id = prev_yr.i_manufact_id)
		   AND (curr_yr.d_year = 2002)
		   AND (prev_yr.d_year = (2002 - 1))
		   AND ((CAST(curr_yr.sales_cnt AS DECIMAL(17,2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17,2))) < CAST('0.9' AS DECIMAL))
		ORDER BY sales_cnt_diff ASC, sales_amt_diff ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 158> (`curr_yr`.`sales_cnt` - `prev_yr`.`sales_cnt`) ASC, <slot 159> (`curr_yr`.`sales_amt` - `prev_yr`.`sales_amt`) ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 66> `i_brand_id` = <slot 145> `i_brand_id`)\n" + 
				"  |  equal join conjunct: (<slot 67> `i_class_id` = <slot 146> `i_class_id`)\n" + 
				"  |  equal join conjunct: (<slot 68> `i_category_id` = <slot 147> `i_category_id`)\n" + 
				"  |  equal join conjunct: (<slot 69> `i_manufact_id` = <slot 148> `i_manufact_id`)\n" + 
				"  |  other predicates: ((CAST(<slot 679> AS DECIMAL(17,2)) / CAST(<slot 686> AS DECIMAL(17,2))) < 0.9)") && 
		explainStr.contains("other predicates: ((CAST(<slot 679> AS DECIMAL(17,2)) / CAST(<slot 686> AS DECIMAL(17,2))) < 0.9)") && 
		explainStr.contains("vec output tuple id: 51") && 
		explainStr.contains("output slot ids: 414 415 416 417 418 419 420 421 426 427 \n" + 
				"  |  hash output slot ids: 144 65 66 67 68 69 149 70 150 71 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 70> sum(`sales_cnt`)), sum(<slot 71> sum(`sales_amt`))\n" + 
				"  |  group by: <slot 65> `d_year`, <slot 66> `i_brand_id`, <slot 67> `i_class_id`, <slot 68> `i_category_id`, <slot 69> `i_manufact_id`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 149> sum(`sales_cnt`)), sum(<slot 150> sum(`sales_amt`))\n" + 
				"  |  group by: <slot 144> `d_year`, <slot 145> `i_brand_id`, <slot 146> `i_class_id`, <slot 147> `i_category_id`, <slot 148> `i_manufact_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 135> (`cs_quantity` - coalesce(`cr_return_quantity`, 0)) (`ss_quantity` - coalesce(`sr_return_quantity`, 0)) (`ws_quantity` - coalesce(`wr_return_quantity`, 0))), sum(<slot 136> (`cs_ext_sales_price` - coalesce(`cr_return_amount`, 0.0)) (`ss_ext_sales_price` - coalesce(`sr_return_amt`, 0.0)) (`ws_ext_sales_price` - coalesce(`wr_return_amt`, 0.0)))\n" + 
				"  |  group by: <slot 130> `d_year` `d_year` `d_year`, <slot 131> `i_brand_id` `i_brand_id` `i_brand_id`, <slot 132> `i_class_id` `i_class_id` `i_class_id`, <slot 133> `i_category_id` `i_category_id` `i_category_id`, <slot 134> `i_manufact_id` `i_manufact_id` `i_manufact_id`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 130> `d_year` `d_year` `d_year`, <slot 131> `i_brand_id` `i_brand_id` `i_brand_id`, <slot 132> `i_class_id` `i_class_id` `i_class_id`, <slot 133> `i_category_id` `i_category_id` `i_category_id`, <slot 134> `i_manufact_id` `i_manufact_id` `i_manufact_id`, <slot 135> (`cs_quantity` - coalesce(`cr_return_quantity`, 0)) (`ss_quantity` - coalesce(`sr_return_quantity`, 0)) (`ws_quantity` - coalesce(`wr_return_quantity`, 0)), <slot 136> (`cs_ext_sales_price` - coalesce(`cr_return_amount`, 0.0)) (`ss_ext_sales_price` - coalesce(`sr_return_amt`, 0.0)) (`ws_ext_sales_price` - coalesce(`wr_return_amt`, 0.0))") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 130> `d_year` `d_year` `d_year`, <slot 131> `i_brand_id` `i_brand_id` `i_brand_id`, <slot 132> `i_class_id` `i_class_id` `i_class_id`, <slot 133> `i_category_id` `i_category_id` `i_category_id`, <slot 134> `i_manufact_id` `i_manufact_id` `i_manufact_id`, <slot 135> (`cs_quantity` - coalesce(`cr_return_quantity`, 0)) (`ss_quantity` - coalesce(`sr_return_quantity`, 0)) (`ws_quantity` - coalesce(`wr_return_quantity`, 0)), <slot 136> (`cs_ext_sales_price` - coalesce(`cr_return_amount`, 0.0)) (`ss_ext_sales_price` - coalesce(`sr_return_amt`, 0.0)) (`ws_ext_sales_price` - coalesce(`wr_return_amt`, 0.0))") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 386> = `wr_order_number`)\n" + 
				"  |  equal join conjunct: (<slot 384> = `wr_item_sk`)") && 
		explainStr.contains("vec output tuple id: 50") && 
		explainStr.contains("output slot ids: 397 398 399 400 401 402 403 404 405 406 407 408 409 410 411 412 413 \n" + 
				"  |  hash output slot ids: 384 128 385 386 387 388 389 390 391 392 393 394 395 396 118 119 126 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 374> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 49") && 
		explainStr.contains("output slot ids: 384 385 386 387 388 389 390 391 392 393 394 395 396 \n" + 
				"  |  hash output slot ids: 115 373 374 375 376 120 377 378 379 380 381 382 383 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 48") && 
		explainStr.contains("output slot ids: 373 374 375 376 377 378 379 380 381 382 383 \n" + 
				"  |  hash output slot ids: 113 129 114 116 117 121 122 123 124 125 127 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF005[in_or_bloom] -> `ws_item_sk`") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` = 'Books')") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 345> = `sr_ticket_number`)\n" + 
				"  |  equal join conjunct: (<slot 343> = `sr_item_sk`)") && 
		explainStr.contains("vec output tuple id: 47") && 
		explainStr.contains("output slot ids: 356 357 358 359 360 361 362 363 364 365 366 367 368 369 370 371 372 \n" + 
				"  |  hash output slot ids: 352 353 354 355 101 102 109 111 343 344 345 346 347 348 349 350 351 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 333> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 46") && 
		explainStr.contains("output slot ids: 343 344 345 346 347 348 349 350 351 352 353 354 355 \n" + 
				"  |  hash output slot ids: 98 103 332 333 334 335 336 337 338 339 340 341 342 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 45") && 
		explainStr.contains("output slot ids: 332 333 334 335 336 337 338 339 340 341 342 \n" + 
				"  |  hash output slot ids: 96 112 97 99 100 104 105 106 107 108 110 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` = 'Books')") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 304> = `cr_order_number`)\n" + 
				"  |  equal join conjunct: (<slot 302> = `cr_item_sk`)") && 
		explainStr.contains("vec output tuple id: 44") && 
		explainStr.contains("output slot ids: 315 316 317 318 319 320 321 322 323 324 325 326 327 328 329 330 331 \n" + 
				"  |  hash output slot ids: 302 303 304 305 306 307 308 84 309 85 310 311 312 313 314 92 94 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 292> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 43") && 
		explainStr.contains("output slot ids: 302 303 304 305 306 307 308 309 310 311 312 313 314 \n" + 
				"  |  hash output slot ids: 291 292 293 294 295 296 297 298 299 300 301 81 86 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 42") && 
		explainStr.contains("output slot ids: 291 292 293 294 295 296 297 298 299 300 301 \n" + 
				"  |  hash output slot ids: 80 82 83 87 88 89 90 91 93 79 95 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` = 'Books')") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 56> (`cs_quantity` - coalesce(`cr_return_quantity`, 0)) (`ss_quantity` - coalesce(`sr_return_quantity`, 0)) (`ws_quantity` - coalesce(`wr_return_quantity`, 0))), sum(<slot 57> (`cs_ext_sales_price` - coalesce(`cr_return_amount`, 0.0)) (`ss_ext_sales_price` - coalesce(`sr_return_amt`, 0.0)) (`ws_ext_sales_price` - coalesce(`wr_return_amt`, 0.0)))\n" + 
				"  |  group by: <slot 51> `d_year` `d_year` `d_year`, <slot 52> `i_brand_id` `i_brand_id` `i_brand_id`, <slot 53> `i_class_id` `i_class_id` `i_class_id`, <slot 54> `i_category_id` `i_category_id` `i_category_id`, <slot 55> `i_manufact_id` `i_manufact_id` `i_manufact_id`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 51> `d_year` `d_year` `d_year`, <slot 52> `i_brand_id` `i_brand_id` `i_brand_id`, <slot 53> `i_class_id` `i_class_id` `i_class_id`, <slot 54> `i_category_id` `i_category_id` `i_category_id`, <slot 55> `i_manufact_id` `i_manufact_id` `i_manufact_id`, <slot 56> (`cs_quantity` - coalesce(`cr_return_quantity`, 0)) (`ss_quantity` - coalesce(`sr_return_quantity`, 0)) (`ws_quantity` - coalesce(`wr_return_quantity`, 0)), <slot 57> (`cs_ext_sales_price` - coalesce(`cr_return_amount`, 0.0)) (`ss_ext_sales_price` - coalesce(`sr_return_amt`, 0.0)) (`ws_ext_sales_price` - coalesce(`wr_return_amt`, 0.0))") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 51> `d_year` `d_year` `d_year`, <slot 52> `i_brand_id` `i_brand_id` `i_brand_id`, <slot 53> `i_class_id` `i_class_id` `i_class_id`, <slot 54> `i_category_id` `i_category_id` `i_category_id`, <slot 55> `i_manufact_id` `i_manufact_id` `i_manufact_id`, <slot 56> (`cs_quantity` - coalesce(`cr_return_quantity`, 0)) (`ss_quantity` - coalesce(`sr_return_quantity`, 0)) (`ws_quantity` - coalesce(`wr_return_quantity`, 0)), <slot 57> (`cs_ext_sales_price` - coalesce(`cr_return_amount`, 0.0)) (`ss_ext_sales_price` - coalesce(`sr_return_amt`, 0.0)) (`ws_ext_sales_price` - coalesce(`wr_return_amt`, 0.0))") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 263> = `wr_order_number`)\n" + 
				"  |  equal join conjunct: (<slot 261> = `wr_item_sk`)") && 
		explainStr.contains("vec output tuple id: 41") && 
		explainStr.contains("output slot ids: 274 275 276 277 278 279 280 281 282 283 284 285 286 287 288 289 290 \n" + 
				"  |  hash output slot ids: 261 262 263 39 264 40 265 266 267 268 269 270 271 47 272 273 49 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 251> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 40") && 
		explainStr.contains("output slot ids: 261 262 263 264 265 266 267 268 269 270 271 272 273 \n" + 
				"  |  hash output slot ids: 256 257 258 259 260 36 41 250 251 252 253 254 255 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 39") && 
		explainStr.contains("output slot ids: 250 251 252 253 254 255 256 257 258 259 260 \n" + 
				"  |  hash output slot ids: 48 34 50 35 37 38 42 43 44 45 46 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ws_item_sk`") && 
		explainStr.contains("TABLE: web_returns(web_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` = 'Books')") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 222> = `sr_ticket_number`)\n" + 
				"  |  equal join conjunct: (<slot 220> = `sr_item_sk`)") && 
		explainStr.contains("vec output tuple id: 38") && 
		explainStr.contains("output slot ids: 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 \n" + 
				"  |  hash output slot ids: 224 32 225 226 227 228 229 230 231 232 22 23 220 221 222 30 223 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 210> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 220 221 222 223 224 225 226 227 228 229 230 231 232 \n" + 
				"  |  hash output slot ids: 209 210 211 19 212 213 214 215 216 24 217 218 219 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 209 210 211 212 213 214 215 216 217 218 219 \n" + 
				"  |  hash output slot ids: 17 33 18 20 21 25 26 27 28 29 31 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` = 'Books')") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 181> = `cr_order_number`)\n" + 
				"  |  equal join conjunct: (<slot 179> = `cr_item_sk`)") && 
		explainStr.contains("vec output tuple id: 35") && 
		explainStr.contains("output slot ids: 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 \n" + 
				"  |  hash output slot ids: 5 6 13 15 179 180 181 182 183 184 185 186 187 188 189 190 191 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 169> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 34") && 
		explainStr.contains("output slot ids: 179 180 181 182 183 184 185 186 187 188 189 190 191 \n" + 
				"  |  hash output slot ids: 2 7 168 169 170 171 172 173 174 175 176 177 178 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 33") && 
		explainStr.contains("output slot ids: 168 169 170 171 172 173 174 175 176 177 178 \n" + 
				"  |  hash output slot ids: 0 16 1 3 4 8 9 10 11 12 14 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` = 'Books')") 
            
        }
    }
}