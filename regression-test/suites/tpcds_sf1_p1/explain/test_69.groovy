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

suite("test_regression_test_tpcds_sf1_p1_q69", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  cd_gender
		, cd_marital_status
		, cd_education_status
		, count(*) cnt1
		, cd_purchase_estimate
		, count(*) cnt2
		, cd_credit_rating
		, count(*) cnt3
		FROM
		  customer c
		, customer_address ca
		, customer_demographics
		WHERE (c.c_current_addr_sk = ca.ca_address_sk)
		   AND (ca_state IN ('KY', 'GA', 'NM'))
		   AND (cd_demo_sk = c.c_current_cdemo_sk)
		   AND (EXISTS (
		   SELECT *
		   FROM
		     store_sales
		   , date_dim
		   WHERE (c.c_customer_sk = ss_customer_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (d_year = 2001)
		      AND (d_moy BETWEEN 4 AND (4 + 2))
		))
		   AND (NOT (EXISTS (
		   SELECT *
		   FROM
		     web_sales
		   , date_dim
		   WHERE (c.c_customer_sk = ws_bill_customer_sk)
		      AND (ws_sold_date_sk = d_date_sk)
		      AND (d_year = 2001)
		      AND (d_moy BETWEEN 4 AND (4 + 2))
		)))
		   AND (NOT (EXISTS (
		   SELECT *
		   FROM
		     catalog_sales
		   , date_dim
		   WHERE (c.c_customer_sk = cs_ship_customer_sk)
		      AND (cs_sold_date_sk = d_date_sk)
		      AND (d_year = 2001)
		      AND (d_moy BETWEEN 4 AND (4 + 2))
		)))
		GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating
		ORDER BY cd_gender ASC, cd_marital_status ASC, cd_education_status ASC, cd_purchase_estimate ASC, cd_credit_rating ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 370> <slot 364> `cd_gender` ASC, <slot 371> <slot 365> `cd_marital_status` ASC, <slot 372> <slot 366> `cd_education_status` ASC, <slot 373> <slot 367> `cd_purchase_estimate` ASC, <slot 374> <slot 368> `cd_credit_rating` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 369> count(*))\n" + 
				"  |  group by: <slot 364> `cd_gender`, <slot 365> `cd_marital_status`, <slot 366> `cd_education_status`, <slot 367> `cd_purchase_estimate`, <slot 368> `cd_credit_rating`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: <slot 433>, <slot 434>, <slot 435>, <slot 436>, <slot 437>") && 
		explainStr.contains("join op: LEFT ANTI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 423> = <slot 428>)") && 
		explainStr.contains("vec output tuple id: 21") && 
		explainStr.contains("output slot ids: 433 434 435 436 437 \n" + 
				"  |  hash output slot ids: 417 418 419 420 421 ") && 
		explainStr.contains("join op: LEFT ANTI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 407> = <slot 412>)") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 417 418 419 420 421 423 \n" + 
				"  |  hash output slot ids: 401 402 403 404 405 407 ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 391> = <slot 396>)") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 401 402 403 404 405 407 \n" + 
				"  |  hash output slot ids: 385 386 387 388 389 391 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 383> = `ca`.`ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 385 386 387 388 389 391 \n" + 
				"  |  hash output slot ids: 376 377 378 379 380 382 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cd_demo_sk` = `c`.`c_current_cdemo_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `c`.`c_current_cdemo_sk`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 376 377 378 379 380 382 383 \n" + 
				"  |  hash output slot ids: 354 355 356 357 358 103 359 ") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cd_demo_sk`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 428 \n" + 
				"  |  hash output slot ids: 229 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001), `d_moy` >= 4, `d_moy` <= 6") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 412 \n" + 
				"  |  hash output slot ids: 104 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001), `d_moy` >= 4, `d_moy` <= 6") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 396 \n" + 
				"  |  hash output slot ids: 0 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001), `d_moy` >= 4, `d_moy` <= 6") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_state` IN ('KY', 'GA', 'NM'))") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") 
            
        }
    }
}