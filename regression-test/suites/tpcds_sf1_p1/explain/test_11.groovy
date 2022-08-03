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

suite("test_regression_test_tpcds_sf1_p1_q11", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  year_total AS (
		   SELECT
		     c_customer_id customer_id
		   , c_first_name customer_first_name
		   , c_last_name customer_last_name
		   , c_preferred_cust_flag customer_preferred_cust_flag
		   , c_birth_country customer_birth_country
		   , c_login customer_login
		   , c_email_address customer_email_address
		   , d_year dyear
		   , sum((ss_ext_list_price - ss_ext_discount_amt)) year_total
		   , 's' sale_type
		   FROM
		     customer
		   , store_sales
		   , date_dim
		   WHERE (c_customer_sk = ss_customer_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
		UNION ALL    SELECT
		     c_customer_id customer_id
		   , c_first_name customer_first_name
		   , c_last_name customer_last_name
		   , c_preferred_cust_flag customer_preferred_cust_flag
		   , c_birth_country customer_birth_country
		   , c_login customer_login
		   , c_email_address customer_email_address
		   , d_year dyear
		   , sum((ws_ext_list_price - ws_ext_discount_amt)) year_total
		   , 'w' sale_type
		   FROM
		     customer
		   , web_sales
		   , date_dim
		   WHERE (c_customer_sk = ws_bill_customer_sk)
		      AND (ws_sold_date_sk = d_date_sk)
		   GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year
		)
		SELECT
		  t_s_secyear.customer_id
		, t_s_secyear.customer_first_name
		, t_s_secyear.customer_last_name
		, t_s_secyear.customer_preferred_cust_flag
		, t_s_secyear.customer_birth_country
		, t_s_secyear.customer_login
		FROM
		  year_total t_s_firstyear
		, year_total t_s_secyear
		, year_total t_w_firstyear
		, year_total t_w_secyear
		WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
		   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
		   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
		   AND (t_s_firstyear.sale_type = 's')
		   AND (t_w_firstyear.sale_type = 'w')
		   AND (t_s_secyear.sale_type = 's')
		   AND (t_w_secyear.sale_type = 'w')
		   AND (t_s_firstyear.dyear = 2001)
		   AND (t_s_secyear.dyear = (2001 + 1))
		   AND (t_w_firstyear.dyear = 2001)
		   AND (t_w_secyear.dyear = (2001 + 1))
		   AND (t_s_firstyear.year_total > 0)
		   AND (t_w_firstyear.year_total > 0)
		   AND ((CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL) END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE CAST('0.0' AS DECIMAL) END))
		ORDER BY t_s_secyear.customer_id ASC, t_s_secyear.customer_first_name ASC, t_s_secyear.customer_last_name ASC, t_s_secyear.customer_preferred_cust_flag ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 264> `t_s_secyear`.`customer_id` ASC, <slot 265> `t_s_secyear`.`customer_first_name` ASC, <slot 266> `t_s_secyear`.`customer_last_name` ASC, <slot 267> `t_s_secyear`.`customer_preferred_cust_flag` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 361> = <slot 244> <slot 212> `c_customer_id` <slot 235> `c_customer_id`)\n" + 
				"  |  other predicates: ((CASE WHEN (<slot 559> > 0) THEN (<slot 556> / <slot 559>) ELSE 0.0 END) > (CASE WHEN (<slot 548> > 0) THEN (<slot 555> / <slot 548>) ELSE 0.0 END))") && 
		explainStr.contains("other predicates: ((CASE WHEN (<slot 559> > 0) THEN (<slot 556> / <slot 559>) ELSE 0.0 END) > (CASE WHEN (<slot 548> > 0) THEN (<slot 555> / <slot 548>) ELSE 0.0 END))") && 
		explainStr.contains("vec output tuple id: 51") && 
		explainStr.contains("output slot ids: 408 409 410 411 412 413 \n" + 
				"  |  hash output slot ids: 368 369 370 372 376 363 252 365 366 367 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 322> = <slot 178> <slot 146> `c_customer_id` <slot 169> `c_customer_id`)") && 
		explainStr.contains("vec output tuple id: 48") && 
		explainStr.contains("output slot ids: 361 363 365 366 367 368 369 370 372 376 \n" + 
				"  |  hash output slot ids: 322 324 326 327 328 329 330 186 331 333 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (<slot 46> <slot 14> `c_customer_id` <slot 37> `c_customer_id` = <slot 112> <slot 80> `c_customer_id` <slot 103> `c_customer_id`)") && 
		explainStr.contains("vec output tuple id: 45") && 
		explainStr.contains("output slot ids: 322 324 326 327 328 329 330 331 333 \n" + 
				"  |  hash output slot ids: 112 113 114 115 116 117 54 120 46 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 243> sum((`ws_ext_list_price` - `ws_ext_discount_amt`)))\n" + 
				"  |  group by: <slot 235> `c_customer_id`, <slot 236> `c_first_name`, <slot 237> `c_last_name`, <slot 238> `c_preferred_cust_flag`, <slot 239> `c_birth_country`, <slot 240> `c_login`, <slot 241> `c_email_address`, <slot 242> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 390> - <slot 391>))\n" + 
				"  |  group by: <slot 394>, <slot 395>, <slot 396>, <slot 397>, <slot 398>, <slot 399>, <slot 400>, <slot 402>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 381> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 50") && 
		explainStr.contains("output slot ids: 390 391 394 395 396 397 398 399 400 402 \n" + 
				"  |  hash output slot ids: 384 385 386 387 388 228 378 379 382 383 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_bill_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 49") && 
		explainStr.contains("output slot ids: 378 379 381 382 383 384 385 386 387 388 \n" + 
				"  |  hash output slot ids: 224 225 226 227 229 230 233 221 222 223 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `ws_bill_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 177> sum((`ws_ext_list_price` - `ws_ext_discount_amt`)))\n" + 
				"  |  group by: <slot 169> `c_customer_id`, <slot 170> `c_first_name`, <slot 171> `c_last_name`, <slot 172> `c_preferred_cust_flag`, <slot 173> `c_birth_country`, <slot 174> `c_login`, <slot 175> `c_email_address`, <slot 176> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 347> - <slot 348>))\n" + 
				"  |  group by: <slot 351>, <slot 352>, <slot 353>, <slot 354>, <slot 355>, <slot 356>, <slot 357>, <slot 359>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 338> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 47") && 
		explainStr.contains("output slot ids: 347 348 351 352 353 354 355 356 357 359 \n" + 
				"  |  hash output slot ids: 336 162 339 340 341 342 343 344 345 335 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_bill_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 46") && 
		explainStr.contains("output slot ids: 335 336 338 339 340 341 342 343 344 345 \n" + 
				"  |  hash output slot ids: 160 161 163 164 167 155 156 157 158 159 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ws_bill_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 88> sum((`ss_ext_list_price` - `ss_ext_discount_amt`)))\n" + 
				"  |  group by: <slot 80> `c_customer_id`, <slot 81> `c_first_name`, <slot 82> `c_last_name`, <slot 83> `c_preferred_cust_flag`, <slot 84> `c_birth_country`, <slot 85> `c_login`, <slot 86> `c_email_address`, <slot 87> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 308> - <slot 309>))\n" + 
				"  |  group by: <slot 312>, <slot 313>, <slot 314>, <slot 315>, <slot 316>, <slot 317>, <slot 318>, <slot 320>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 299> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 44") && 
		explainStr.contains("output slot ids: 308 309 312 313 314 315 316 317 318 320 \n" + 
				"  |  hash output slot ids: 304 305 306 296 297 73 300 301 302 303 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 43") && 
		explainStr.contains("output slot ids: 296 297 299 300 301 302 303 304 305 306 \n" + 
				"  |  hash output slot ids: 66 67 68 69 70 71 72 74 75 78 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 22> sum((`ss_ext_list_price` - `ss_ext_discount_amt`)))\n" + 
				"  |  group by: <slot 14> `c_customer_id`, <slot 15> `c_first_name`, <slot 16> `c_last_name`, <slot 17> `c_preferred_cust_flag`, <slot 18> `c_birth_country`, <slot 19> `c_login`, <slot 20> `c_email_address`, <slot 21> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 282> - <slot 283>))\n" + 
				"  |  group by: <slot 286>, <slot 287>, <slot 288>, <slot 289>, <slot 290>, <slot 291>, <slot 292>, <slot 294>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 273> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 42") && 
		explainStr.contains("output slot ids: 282 283 286 287 288 289 290 291 292 294 \n" + 
				"  |  hash output slot ids: 274 275 276 277 278 279 7 280 270 271 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 41") && 
		explainStr.contains("output slot ids: 270 271 273 274 275 276 277 278 279 280 \n" + 
				"  |  hash output slot ids: 0 1 2 3 4 5 6 8 9 12 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") 
            
        }
    }
}