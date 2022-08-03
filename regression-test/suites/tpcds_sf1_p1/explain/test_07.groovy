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

suite("test_regression_test_tpcds_sf1_p1_q07", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_item_id
		, avg(ss_quantity) agg1
		, avg(ss_list_price) agg2
		, avg(ss_coupon_amt) agg3
		, avg(ss_sales_price) agg4
		FROM
		  store_sales
		, customer_demographics
		, date_dim
		, item
		, promotion
		WHERE (ss_sold_date_sk = d_date_sk)
		   AND (ss_item_sk = i_item_sk)
		   AND (ss_cdemo_sk = cd_demo_sk)
		   AND (ss_promo_sk = p_promo_sk)
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
				"  |  output: avg(<slot 20> avg(`ss_quantity`)), avg(<slot 21> avg(`ss_list_price`)), avg(<slot 22> avg(`ss_coupon_amt`)), avg(<slot 23> avg(`ss_sales_price`))\n" + 
				"  |  group by: <slot 19> `i_item_id`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(<slot 72>), avg(<slot 73>), avg(<slot 74>), avg(<slot 75>)\n" + 
				"  |  group by: <slot 82>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 63> = `p_promo_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 72 73 74 75 82 \n" + 
				"  |  hash output slot ids: 66 56 57 58 59 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 50> = `cd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 56 57 58 59 63 66 \n" + 
				"  |  hash output slot ids: 51 54 44 45 46 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 39> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 44 45 46 47 50 51 54 \n" + 
				"  |  hash output slot ids: 0 34 35 36 37 40 41 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 8") && 
		explainStr.contains("output slot ids: 34 35 36 37 39 40 41 \n" + 
				"  |  hash output slot ids: 1 2 3 4 7 9 11 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: promotion(promotion), PREAGGREGATION: ON\n" + 
				"     PREDICATES: ((`p_channel_email` = 'N') OR (`p_channel_event` = 'N'))") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cd_gender` = 'M'), (`cd_marital_status` = 'S'), (`cd_education_status` = 'College')") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000)") 
            
        }
    }
}