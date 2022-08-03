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

suite("test_regression_test_tpcds_sf1_p1_q26", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_item_id
		, avg(cs_quantity) agg1
		, avg(cs_list_price) agg2
		, avg(cs_coupon_amt) agg3
		, avg(cs_sales_price) agg4
		FROM
		  catalog_sales
		, customer_demographics
		, date_dim
		, item
		, promotion
		WHERE (cs_sold_date_sk = d_date_sk)
		   AND (cs_item_sk = i_item_sk)
		   AND (cs_bill_cdemo_sk = cd_demo_sk)
		   AND (cs_promo_sk = p_promo_sk)
		   AND (cd_gender = 'M')
		   AND (cd_marital_status = 'S')
		   AND (cd_education_status = 'College')
		   AND ((p_channel_email = 'N')
		      OR (p_channel_event = 'N'))
		   AND (d_year = 2000)
		GROUP BY i_item_id
		ORDER BY i_item_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 29> <slot 24> `i_item_id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 20> avg(`cs_quantity`)), avg(<slot 21> avg(`cs_list_price`)), avg(<slot 22> avg(`cs_coupon_amt`)), avg(<slot 23> avg(`cs_sales_price`))\n" + 
				"  |  group by: <slot 19> `i_item_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(<slot 80>), avg(<slot 81>), avg(<slot 82>), avg(<slot 83>)\n" + 
				"  |  group by: <slot 90>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 71> = `p_promo_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 80 81 82 83 90 \n" + 
				"  |  hash output slot ids: 64 65 66 67 74 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 55> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 64 65 66 67 71 74 \n" + 
				"  |  hash output slot ids: 0 50 51 52 53 57 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 42> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 50 51 52 53 55 57 \n" + 
				"  |  hash output slot ids: 38 39 40 41 43 45 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cd_demo_sk` = `cs_bill_cdemo_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `cs_bill_cdemo_sk`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 38 39 40 41 42 43 45 \n" + 
				"  |  hash output slot ids: 1 2 3 4 5 7 11 ") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cd_gender` = 'M'), (`cd_marital_status` = 'S'), (`cd_education_status` = 'College')\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cd_demo_sk`") && 
		explainStr.contains("TABLE: promotion(promotion), PREAGGREGATION: ON\n" + 
				"     PREDICATES: ((`p_channel_email` = 'N') OR (`p_channel_event` = 'N'))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000)") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") 
            
        }
    }
}