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

suite("test_regression_test_tpcds_sf1_p1_q72", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_item_desc
		, w_warehouse_name
		, d1.d_week_seq
		, sum((CASE WHEN (p_promo_sk IS NULL) THEN 1 ELSE 0 END)) no_promo
		, sum((CASE WHEN (p_promo_sk IS NOT NULL) THEN 1 ELSE 0 END)) promo
		, count(*) total_cnt
		FROM
		  catalog_sales
		INNER JOIN inventory ON (cs_item_sk = inv_item_sk)
		INNER JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk)
		INNER JOIN item ON (i_item_sk = cs_item_sk)
		INNER JOIN customer_demographics ON (cs_bill_cdemo_sk = cd_demo_sk)
		INNER JOIN household_demographics ON (cs_bill_hdemo_sk = hd_demo_sk)
		INNER JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)
		INNER JOIN date_dim d2 ON (inv_date_sk = d2.d_date_sk)
		INNER JOIN date_dim d3 ON (cs_ship_date_sk = d3.d_date_sk)
		LEFT JOIN promotion ON (cs_promo_sk = p_promo_sk)
		LEFT JOIN catalog_returns ON (cr_item_sk = cs_item_sk)
		   AND (cr_order_number = cs_order_number)
		WHERE (d1.d_week_seq = d2.d_week_seq)
		   AND (inv_quantity_on_hand < cs_quantity)
		   AND (d3.d_date > (d1.d_date + INTERVAL  '5' DAY))
		   AND (hd_buy_potential = '>10000')
		   AND (d1.d_year = 1999)
		   AND (cd_marital_status = 'D')
		GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq
		ORDER BY total_cnt DESC, i_item_desc ASC, w_warehouse_name ASC, d1.d_week_seq ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 37> <slot 36> count(*) DESC, <slot 38> <slot 31> `i_item_desc` ASC, <slot 39> <slot 32> `w_warehouse_name` ASC, <slot 40> <slot 33> `d1`.`d_week_seq` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 34> sum((CASE WHEN (`p_promo_sk` IS NULL) THEN 1 ELSE 0 END))), sum(<slot 35> sum((CASE WHEN (`p_promo_sk` IS NOT NULL) THEN 1 ELSE 0 END))), count(<slot 36> count(*))\n" + 
				"  |  group by: <slot 31> `i_item_desc`, <slot 32> `w_warehouse_name`, <slot 33> `d1`.`d_week_seq`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (<slot 258> IS NULL) THEN 1 ELSE 0 END)), sum((CASE WHEN (<slot 258> IS NOT NULL) THEN 1 ELSE 0 END)), count(*)\n" + 
				"  |  group by: <slot 245>, <slot 243>, <slot 251>") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 201> = `cr_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 207> = `cr_order_number`)") && 
		explainStr.contains("vec output tuple id: 22") && 
		explainStr.contains("output slot ids: 243 245 251 258 \n" + 
				"  |  hash output slot ids: 229 214 216 222 ") && 
		explainStr.contains("join op: LEFT OUTER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 178> = `p_promo_sk`)") && 
		explainStr.contains("vec output tuple id: 21") && 
		explainStr.contains("output slot ids: 201 207 214 216 222 229 \n" + 
				"  |  hash output slot ids: 16 194 179 186 188 173 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 151> = `d3`.`d_date_sk`)\n" + 
				"  |  other predicates: (<slot 350> > (<slot 348> + INTERVAL 5 DAY))") && 
		explainStr.contains("other predicates: (<slot 350> > (<slot 348> + INTERVAL 5 DAY))") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 173 178 179 186 188 194 \n" + 
				"  |  hash output slot ids: 160 162 147 152 168 153 169 26 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 133> = `d2`.`d_date_sk`)\n" + 
				"  |  equal join conjunct: (<slot 144> = `d2`.`d_week_seq`)") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 147 151 152 153 160 162 168 169 \n" + 
				"  |  hash output slot ids: 128 144 129 145 136 138 123 127 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 106> = `d1`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 123 127 128 129 133 136 138 144 145 \n" + 
				"  |  hash output slot ids: 113 116 118 22 103 107 27 108 109 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 87> = `hd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 103 106 107 108 109 113 116 118 \n" + 
				"  |  hash output slot ids: 98 100 85 88 89 90 91 95 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 70> = `cd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 85 87 88 89 90 91 95 98 100 \n" + 
				"  |  hash output slot ids: 82 84 69 71 72 73 74 75 79 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 55> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 69 70 71 72 73 74 75 79 82 84 \n" + 
				"  |  hash output slot ids: 65 68 20 55 56 57 58 59 60 61 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 52> = `w_warehouse_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 55 56 57 58 59 60 61 65 68 \n" + 
				"  |  hash output slot ids: 48 49 53 21 43 44 45 46 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `inv_item_sk`)\n" + 
				"  |  other predicates: (<slot 272> < <slot 268>)") && 
		explainStr.contains("other predicates: (<slot 272> < <slot 268>)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `inv_item_sk`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 43 44 45 46 47 48 49 52 53 \n" + 
				"  |  hash output slot ids: 0 19 3 5 7 24 9 25 11 13 15 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("TABLE: catalog_returns(catalog_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: promotion(promotion), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d1`.`d_year` = 1999)") && 
		explainStr.contains("TABLE: household_demographics(household_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`hd_buy_potential` = '>10000')") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cd_marital_status` = 'D')") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: warehouse(warehouse), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: inventory(inventory), PREAGGREGATION: ON") 
            
        }
    }
}