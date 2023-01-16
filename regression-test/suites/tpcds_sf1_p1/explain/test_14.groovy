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

suite("test_regression_test_tpcds_sf1_p1_q14", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  cross_items AS (
		   SELECT i_item_sk ss_item_sk
		   FROM
		     item
		   , (
		      SELECT
		        iss.i_brand_id brand_id
		      , iss.i_class_id class_id
		      , iss.i_category_id category_id
		      FROM
		        store_sales
		      , item iss
		      , date_dim d1
		      WHERE (ss_item_sk = iss.i_item_sk)
		         AND (ss_sold_date_sk = d1.d_date_sk)
		         AND (d1.d_year BETWEEN 1999 AND (1999 + 2))
		INTERSECT       SELECT
		        ics.i_brand_id
		      , ics.i_class_id
		      , ics.i_category_id
		      FROM
		        catalog_sales
		      , item ics
		      , date_dim d2
		      WHERE (cs_item_sk = ics.i_item_sk)
		         AND (cs_sold_date_sk = d2.d_date_sk)
		         AND (d2.d_year BETWEEN 1999 AND (1999 + 2))
		INTERSECT       SELECT
		        iws.i_brand_id
		      , iws.i_class_id
		      , iws.i_category_id
		      FROM
		        web_sales
		      , item iws
		      , date_dim d3
		      WHERE (ws_item_sk = iws.i_item_sk)
		         AND (ws_sold_date_sk = d3.d_date_sk)
		         AND (d3.d_year BETWEEN 1999 AND (1999 + 2))
		   )  x
		   WHERE (i_brand_id = brand_id)
		      AND (i_class_id = class_id)
		      AND (i_category_id = category_id)
		)
		, avg_sales AS (
		   SELECT avg((quantity * list_price)) average_sales
		   FROM
		     (
		      SELECT
		        ss_quantity quantity
		      , ss_list_price list_price
		      FROM
		        store_sales
		      , date_dim
		      WHERE (ss_sold_date_sk = d_date_sk)
		         AND (d_year BETWEEN 1999 AND (1999 + 2))
		UNION ALL       SELECT
		        cs_quantity quantity
		      , cs_list_price list_price
		      FROM
		        catalog_sales
		      , date_dim
		      WHERE (cs_sold_date_sk = d_date_sk)
		         AND (d_year BETWEEN 1999 AND (1999 + 2))
		UNION ALL       SELECT
		        ws_quantity quantity
		      , ws_list_price list_price
		      FROM
		        web_sales
		      , date_dim
		      WHERE (ws_sold_date_sk = d_date_sk)
		         AND (d_year BETWEEN 1999 AND (1999 + 2))
		   ) y
		)
		SELECT *
		FROM
		  (
		   SELECT
		     'store' channel
		   , i_brand_id
		   , i_class_id
		   , i_category_id
		   , sum((ss_quantity * ss_list_price)) sales
		   , count(*) number_sales
		   FROM
		     store_sales
		   , item
		   , date_dim
		   WHERE (ss_item_sk IN (
		      SELECT ss_item_sk
		      FROM
		        cross_items
		   ))
		      AND (ss_item_sk = i_item_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (d_week_seq = (
		         SELECT d_week_seq
		         FROM
		           date_dim
		         WHERE (d_year = (1999 + 1))
		            AND (d_moy = 12)
		            AND (d_dom = 11)
		      ))
		   GROUP BY i_brand_id, i_class_id, i_category_id
		   HAVING (sum((ss_quantity * ss_list_price)) > (
		         SELECT average_sales
		         FROM
		           avg_sales
		      ))
		)  this_year
		, (
		   SELECT
		     'store' channel
		   , i_brand_id
		   , i_class_id
		   , i_category_id
		   , sum((ss_quantity * ss_list_price)) sales
		   , count(*) number_sales
		   FROM
		     store_sales
		   , item
		   , date_dim
		   WHERE (ss_item_sk IN (
		      SELECT ss_item_sk
		      FROM
		        cross_items
		   ))
		      AND (ss_item_sk = i_item_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (d_week_seq = (
		         SELECT d_week_seq
		         FROM
		           date_dim
		         WHERE (d_year = 1999)
		            AND (d_moy = 12)
		            AND (d_dom = 11)
		      ))
		   GROUP BY i_brand_id, i_class_id, i_category_id
		   HAVING (sum((ss_quantity * ss_list_price)) > (
		         SELECT average_sales
		         FROM
		           avg_sales
		      ))
		)  last_year
		WHERE (this_year.i_brand_id = last_year.i_brand_id)
		   AND (this_year.i_class_id = last_year.i_class_id)
		   AND (this_year.i_category_id = last_year.i_category_id)
		ORDER BY this_year.channel ASC, this_year.i_brand_id ASC, this_year.i_class_id ASC, this_year.i_category_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 254> `this_year`.`channel` ASC, <slot 255> `this_year`.`i_brand_id` ASC, <slot 256> `this_year`.`i_class_id` ASC, <slot 257> `this_year`.`i_category_id` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 87> `i_brand_id` = <slot 214> `i_brand_id`)\n" + 
				"  |  equal join conjunct: (<slot 88> `i_class_id` = <slot 215> `i_class_id`)\n" + 
				"  |  equal join conjunct: (<slot 89> `i_category_id` = <slot 216> `i_category_id`)") && 
		explainStr.contains("vec output tuple id: 141") && 
		explainStr.contains("output slot ids: 588 589 590 591 592 593 594 595 596 597 598 599 \n" + 
				"  |  hash output slot ids: 214 87 215 88 216 89 217 90 218 91 ") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: (<slot 90> sum((`ss_quantity` * `ss_list_price`)) > <slot 118> avg((`quantity` * `list_price`)))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 90> sum((`ss_quantity` * `ss_list_price`))), count(<slot 91> count(*))\n" + 
				"  |  group by: <slot 87> `i_brand_id`, <slot 88> `i_class_id`, <slot 89> `i_category_id`") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: (<slot 217> sum((`ss_quantity` * `ss_list_price`)) > <slot 245> avg((`quantity` * `list_price`)))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 217> sum((`ss_quantity` * `ss_list_price`))), count(<slot 218> count(*))\n" + 
				"  |  group by: <slot 214> `i_brand_id`, <slot 215> `i_class_id`, <slot 216> `i_category_id`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 244> avg((`quantity` * `list_price`)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: avg((<slot 240> `ss_quantity` `cs_quantity` `ws_quantity` * <slot 241> `ss_list_price` `cs_list_price` `ws_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF031[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 140") && 
		explainStr.contains("output slot ids: 583 584 585 586 587 \n" + 
				"  |  hash output slot ids: 235 236 237 238 239 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF031[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_year` >= 1999, `d_year` <= 2001") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF030[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 139") && 
		explainStr.contains("output slot ids: 578 579 580 581 582 \n" + 
				"  |  hash output slot ids: 230 231 232 233 234 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF030[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_year` >= 1999, `d_year` <= 2001") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF029[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 138") && 
		explainStr.contains("output slot ids: 573 574 575 576 577 \n" + 
				"  |  hash output slot ids: 225 226 227 228 229 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF029[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_year` >= 1999, `d_year` <= 2001") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 564> * <slot 565>)), count(*)\n" + 
				"  |  group by: <slot 568>, <slot 569>, <slot 570>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 508> = <slot 556>") && 
		explainStr.contains("vec output tuple id: 137") && 
		explainStr.contains("output slot ids: 564 565 568 569 570 \n" + 
				"  |  hash output slot ids: 505 506 509 510 511 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 502> = `d_week_seq`)") && 
		explainStr.contains("vec output tuple id: 129") && 
		explainStr.contains("output slot ids: 505 506 508 509 510 511 \n" + 
				"  |  hash output slot ids: 496 498 499 500 501 495 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 435> = <slot 487>") && 
		explainStr.contains("vec output tuple id: 128") && 
		explainStr.contains("output slot ids: 495 496 498 499 500 501 502 \n" + 
				"  |  hash output slot ids: 436 437 439 440 441 442 443 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 430> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 120") && 
		explainStr.contains("output slot ids: 435 436 437 439 440 441 442 443 \n" + 
				"  |  hash output slot ids: 432 433 434 169 427 428 429 431 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF016[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 119") && 
		explainStr.contains("output slot ids: 427 428 429 430 431 432 433 434 \n" + 
				"  |  hash output slot ids: 208 209 210 163 211 212 206 207 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF016[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (`i_brand_id` = <slot 194> `iss`.`i_brand_id` `ics`.`i_brand_id` `iws`.`i_brand_id`)\n" + 
				"  |  equal join conjunct: (`i_class_id` = <slot 195> `iss`.`i_class_id` `ics`.`i_class_id` `iws`.`i_class_id`)\n" + 
				"  |  equal join conjunct: (`i_category_id` = <slot 196> `iss`.`i_category_id` `ics`.`i_category_id` `iws`.`i_category_id`)\n" + 
				"  |  runtime filters: RF023[in_or_bloom] <- <slot 194> `iss`.`i_brand_id` `ics`.`i_brand_id` `iws`.`i_brand_id`, RF024[in_or_bloom] <- <slot 195> `iss`.`i_class_id` `ics`.`i_class_id` `iws`.`i_class_id`, RF025[in_or_bloom] <- <slot 196> `iss`.`i_category_id` `ics`.`i_category_id` `iws`.`i_category_id`") && 
		explainStr.contains("vec output tuple id: 136") && 
		explainStr.contains("output slot ids: 556 \n" + 
				"  |  hash output slot ids: 200 ") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF023[in_or_bloom] -> `i_brand_id`, RF024[in_or_bloom] -> `i_class_id`, RF025[in_or_bloom] -> `i_category_id`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 543> = `d3`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 135") && 
		explainStr.contains("output slot ids: 548 549 550 551 552 553 554 555 \n" + 
				"  |  hash output slot ids: 544 192 545 193 546 547 542 543 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_item_sk` = `iws`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF028[in_or_bloom] <- `iws`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 134") && 
		explainStr.contains("output slot ids: 542 543 544 545 546 547 \n" + 
				"  |  hash output slot ids: 186 187 188 189 190 191 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF028[in_or_bloom] -> `ws_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d3`.`d_year` >= 1999, `d3`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 529> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 133") && 
		explainStr.contains("output slot ids: 534 535 536 537 538 539 540 541 \n" + 
				"  |  hash output slot ids: 528 529 530 531 532 533 184 185 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `ics`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF027[in_or_bloom] <- `ics`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 132") && 
		explainStr.contains("output slot ids: 528 529 530 531 532 533 \n" + 
				"  |  hash output slot ids: 178 179 180 181 182 183 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF027[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d2`.`d_year` >= 1999, `d2`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 515> = `d1`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 131") && 
		explainStr.contains("output slot ids: 520 521 522 523 524 525 526 527 \n" + 
				"  |  hash output slot ids: 176 177 514 515 516 517 518 519 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `iss`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF026[in_or_bloom] <- `iss`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 130") && 
		explainStr.contains("output slot ids: 514 515 516 517 518 519 \n" + 
				"  |  hash output slot ids: 170 171 172 173 174 175 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF026[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d1`.`d_year` >= 1999, `d1`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1999), (`d_moy` = 12), (`d_dom` = 11)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (`i_brand_id` = <slot 151> `iss`.`i_brand_id` `ics`.`i_brand_id` `iws`.`i_brand_id`)\n" + 
				"  |  equal join conjunct: (`i_class_id` = <slot 152> `iss`.`i_class_id` `ics`.`i_class_id` `iws`.`i_class_id`)\n" + 
				"  |  equal join conjunct: (`i_category_id` = <slot 153> `iss`.`i_category_id` `ics`.`i_category_id` `iws`.`i_category_id`)\n" + 
				"  |  runtime filters: RF017[in_or_bloom] <- <slot 151> `iss`.`i_brand_id` `ics`.`i_brand_id` `iws`.`i_brand_id`, RF018[in_or_bloom] <- <slot 152> `iss`.`i_class_id` `ics`.`i_class_id` `iws`.`i_class_id`, RF019[in_or_bloom] <- <slot 153> `iss`.`i_category_id` `ics`.`i_category_id` `iws`.`i_category_id`") && 
		explainStr.contains("vec output tuple id: 127") && 
		explainStr.contains("output slot ids: 487 \n" + 
				"  |  hash output slot ids: 157 ") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF017[in_or_bloom] -> `i_brand_id`, RF018[in_or_bloom] -> `i_class_id`, RF019[in_or_bloom] -> `i_category_id`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 474> = `d3`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 126") && 
		explainStr.contains("output slot ids: 479 480 481 482 483 484 485 486 \n" + 
				"  |  hash output slot ids: 149 150 473 474 475 476 477 478 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_item_sk` = `iws`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF022[in_or_bloom] <- `iws`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 125") && 
		explainStr.contains("output slot ids: 473 474 475 476 477 478 \n" + 
				"  |  hash output slot ids: 144 145 146 147 148 143 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF022[in_or_bloom] -> `ws_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d3`.`d_year` >= 1999, `d3`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 460> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 124") && 
		explainStr.contains("output slot ids: 465 466 467 468 469 470 471 472 \n" + 
				"  |  hash output slot ids: 464 459 460 461 141 462 142 463 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `ics`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF021[in_or_bloom] <- `ics`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 123") && 
		explainStr.contains("output slot ids: 459 460 461 462 463 464 \n" + 
				"  |  hash output slot ids: 135 136 137 138 139 140 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF021[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d2`.`d_year` >= 1999, `d2`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 446> = `d1`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 122") && 
		explainStr.contains("output slot ids: 451 452 453 454 455 456 457 458 \n" + 
				"  |  hash output slot ids: 448 449 450 133 134 445 446 447 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `iss`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF020[in_or_bloom] <- `iss`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 121") && 
		explainStr.contains("output slot ids: 445 446 447 448 449 450 \n" + 
				"  |  hash output slot ids: 128 129 130 131 132 127 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF020[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d1`.`d_year` >= 1999, `d1`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 117> avg((`quantity` * `list_price`)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: avg((<slot 113> `ss_quantity` `cs_quantity` `ws_quantity` * <slot 114> `ss_list_price` `cs_list_price` `ws_list_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF015[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 118") && 
		explainStr.contains("output slot ids: 422 423 424 425 426 \n" + 
				"  |  hash output slot ids: 112 108 109 110 111 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF015[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_year` >= 1999, `d_year` <= 2001") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF014[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 117") && 
		explainStr.contains("output slot ids: 417 418 419 420 421 \n" + 
				"  |  hash output slot ids: 103 104 105 106 107 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF014[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_year` >= 1999, `d_year` <= 2001") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF013[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 116") && 
		explainStr.contains("output slot ids: 412 413 414 415 416 \n" + 
				"  |  hash output slot ids: 98 99 100 101 102 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF013[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_year` >= 1999, `d_year` <= 2001") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((<slot 403> * <slot 404>)), count(*)\n" + 
				"  |  group by: <slot 407>, <slot 408>, <slot 409>") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 347> = <slot 395>") && 
		explainStr.contains("vec output tuple id: 115") && 
		explainStr.contains("output slot ids: 403 404 407 408 409 \n" + 
				"  |  hash output slot ids: 344 345 348 349 350 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 341> = `d_week_seq`)") && 
		explainStr.contains("vec output tuple id: 107") && 
		explainStr.contains("output slot ids: 344 345 347 348 349 350 \n" + 
				"  |  hash output slot ids: 337 338 339 340 334 335 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 274> = <slot 326>") && 
		explainStr.contains("vec output tuple id: 106") && 
		explainStr.contains("output slot ids: 334 335 337 338 339 340 341 \n" + 
				"  |  hash output slot ids: 275 276 278 279 280 281 282 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 269> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 98") && 
		explainStr.contains("output slot ids: 274 275 276 278 279 280 281 282 \n" + 
				"  |  hash output slot ids: 272 273 266 42 267 268 270 271 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 97") && 
		explainStr.contains("output slot ids: 266 267 268 269 270 271 272 273 \n" + 
				"  |  hash output slot ids: 80 81 82 83 36 84 85 79 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (`i_brand_id` = <slot 67> `iss`.`i_brand_id` `ics`.`i_brand_id` `iws`.`i_brand_id`)\n" + 
				"  |  equal join conjunct: (`i_class_id` = <slot 68> `iss`.`i_class_id` `ics`.`i_class_id` `iws`.`i_class_id`)\n" + 
				"  |  equal join conjunct: (`i_category_id` = <slot 69> `iss`.`i_category_id` `ics`.`i_category_id` `iws`.`i_category_id`)\n" + 
				"  |  runtime filters: RF007[in_or_bloom] <- <slot 67> `iss`.`i_brand_id` `ics`.`i_brand_id` `iws`.`i_brand_id`, RF008[in_or_bloom] <- <slot 68> `iss`.`i_class_id` `ics`.`i_class_id` `iws`.`i_class_id`, RF009[in_or_bloom] <- <slot 69> `iss`.`i_category_id` `ics`.`i_category_id` `iws`.`i_category_id`") && 
		explainStr.contains("vec output tuple id: 114") && 
		explainStr.contains("output slot ids: 395 \n" + 
				"  |  hash output slot ids: 73 ") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF007[in_or_bloom] -> `i_brand_id`, RF008[in_or_bloom] -> `i_class_id`, RF009[in_or_bloom] -> `i_category_id`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 382> = `d3`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 113") && 
		explainStr.contains("output slot ids: 387 388 389 390 391 392 393 394 \n" + 
				"  |  hash output slot ids: 384 385 65 386 66 381 382 383 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_item_sk` = `iws`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF012[in_or_bloom] <- `iws`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 112") && 
		explainStr.contains("output slot ids: 381 382 383 384 385 386 \n" + 
				"  |  hash output slot ids: 64 59 60 61 62 63 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF012[in_or_bloom] -> `ws_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d3`.`d_year` >= 1999, `d3`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 368> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 111") && 
		explainStr.contains("output slot ids: 373 374 375 376 377 378 379 380 \n" + 
				"  |  hash output slot ids: 368 369 370 371 372 57 58 367 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `ics`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF011[in_or_bloom] <- `ics`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 110") && 
		explainStr.contains("output slot ids: 367 368 369 370 371 372 \n" + 
				"  |  hash output slot ids: 51 52 53 54 55 56 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF011[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d2`.`d_year` >= 1999, `d2`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 354> = `d1`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 109") && 
		explainStr.contains("output slot ids: 359 360 361 362 363 364 365 366 \n" + 
				"  |  hash output slot ids: 353 49 354 50 355 356 357 358 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `iss`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF010[in_or_bloom] <- `iss`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 108") && 
		explainStr.contains("output slot ids: 353 354 355 356 357 358 \n" + 
				"  |  hash output slot ids: 48 43 44 45 46 47 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF010[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d1`.`d_year` >= 1999, `d1`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000), (`d_moy` = 12), (`d_dom` = 11)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (`i_brand_id` = <slot 24> `iss`.`i_brand_id` `ics`.`i_brand_id` `iws`.`i_brand_id`)\n" + 
				"  |  equal join conjunct: (`i_class_id` = <slot 25> `iss`.`i_class_id` `ics`.`i_class_id` `iws`.`i_class_id`)\n" + 
				"  |  equal join conjunct: (`i_category_id` = <slot 26> `iss`.`i_category_id` `ics`.`i_category_id` `iws`.`i_category_id`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- <slot 24> `iss`.`i_brand_id` `ics`.`i_brand_id` `iws`.`i_brand_id`, RF002[in_or_bloom] <- <slot 25> `iss`.`i_class_id` `ics`.`i_class_id` `iws`.`i_class_id`, RF003[in_or_bloom] <- <slot 26> `iss`.`i_category_id` `ics`.`i_category_id` `iws`.`i_category_id`") && 
		explainStr.contains("vec output tuple id: 105") && 
		explainStr.contains("output slot ids: 326 \n" + 
				"  |  hash output slot ids: 30 ") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `i_brand_id`, RF002[in_or_bloom] -> `i_class_id`, RF003[in_or_bloom] -> `i_category_id`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 313> = `d3`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 104") && 
		explainStr.contains("output slot ids: 318 319 320 321 322 323 324 325 \n" + 
				"  |  hash output slot ids: 22 23 312 313 314 315 316 317 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_item_sk` = `iws`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF006[in_or_bloom] <- `iws`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 103") && 
		explainStr.contains("output slot ids: 312 313 314 315 316 317 \n" + 
				"  |  hash output slot ids: 16 17 18 19 20 21 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF006[in_or_bloom] -> `ws_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d3`.`d_year` >= 1999, `d3`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 299> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 102") && 
		explainStr.contains("output slot ids: 304 305 306 307 308 309 310 311 \n" + 
				"  |  hash output slot ids: 298 299 300 301 302 14 303 15 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `ics`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `ics`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 101") && 
		explainStr.contains("output slot ids: 298 299 300 301 302 303 \n" + 
				"  |  hash output slot ids: 8 9 10 11 12 13 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF005[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d2`.`d_year` >= 1999, `d2`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 285> = `d1`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 100") && 
		explainStr.contains("output slot ids: 290 291 292 293 294 295 296 297 \n" + 
				"  |  hash output slot ids: 288 289 6 7 284 285 286 287 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `iss`.`i_item_sk`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `iss`.`i_item_sk`") && 
		explainStr.contains("vec output tuple id: 99") && 
		explainStr.contains("output slot ids: 284 285 286 287 288 289 \n" + 
				"  |  hash output slot ids: 0 1 2 3 4 5 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d1`.`d_year` >= 1999, `d1`.`d_year` <= 2001") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") 
            
        }
    }
}