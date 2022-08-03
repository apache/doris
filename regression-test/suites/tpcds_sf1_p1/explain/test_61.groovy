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

suite("test_regression_test_tpcds_sf1_p1_q61", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  promotions
		, total
		, ((CAST(promotions AS DECIMAL(15,4)) / CAST(total AS DECIMAL(15,4))) * 100)
		FROM
		  (
		   SELECT sum(ss_ext_sales_price) promotions
		   FROM
		     store_sales
		   , store
		   , promotion
		   , date_dim
		   , customer
		   , customer_address
		   , item
		   WHERE (ss_sold_date_sk = d_date_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (ss_promo_sk = p_promo_sk)
		      AND (ss_customer_sk = c_customer_sk)
		      AND (ca_address_sk = c_current_addr_sk)
		      AND (ss_item_sk = i_item_sk)
		      AND (ca_gmt_offset = -5)
		      AND (i_category = 'Jewelry')
		      AND ((p_channel_dmail = 'Y')
		         OR (p_channel_email = 'Y')
		         OR (p_channel_tv = 'Y'))
		      AND (s_gmt_offset = -5)
		      AND (d_year = 1998)
		      AND (d_moy = 11)
		)  promotional_sales
		, (
		   SELECT sum(ss_ext_sales_price) total
		   FROM
		     store_sales
		   , store
		   , date_dim
		   , customer
		   , customer_address
		   , item
		   WHERE (ss_sold_date_sk = d_date_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (ss_customer_sk = c_customer_sk)
		      AND (ca_address_sk = c_current_addr_sk)
		      AND (ss_item_sk = i_item_sk)
		      AND (ca_gmt_offset = -5)
		      AND (i_category = 'Jewelry')
		      AND (s_gmt_offset = -5)
		      AND (d_year = 1998)
		      AND (d_moy = 11)
		)  all_sales
		ORDER BY promotions ASC, total ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 43> `promotions` ASC, <slot 44> `total` ASC") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates is NULL.  |  cardinality=1") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 21> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 40> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(<slot 181>)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 178> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 30") && 
		explainStr.contains("output slot ids: 181 \n" + 
				"  |  hash output slot ids: 167 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 159> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 29") && 
		explainStr.contains("output slot ids: 167 178 \n" + 
				"  |  hash output slot ids: 166 155 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 148> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 28") && 
		explainStr.contains("output slot ids: 155 159 166 \n" + 
				"  |  hash output slot ids: 32 145 149 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 139> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 27") && 
		explainStr.contains("output slot ids: 145 148 149 \n" + 
				"  |  hash output slot ids: 137 140 141 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 26") && 
		explainStr.contains("output slot ids: 137 139 140 141 \n" + 
				"  |  hash output slot ids: 33 24 27 29 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_gmt_offset` = -5)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` = 'Jewelry')") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`s_gmt_offset` = -5)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998), (`d_moy` = 11)") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(<slot 116>)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 113> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 25") && 
		explainStr.contains("output slot ids: 116 \n" + 
				"  |  hash output slot ids: 97 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 85> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 24") && 
		explainStr.contains("output slot ids: 97 113 \n" + 
				"  |  hash output slot ids: 80 96 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 69> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 23") && 
		explainStr.contains("output slot ids: 80 85 96 \n" + 
				"  |  hash output slot ids: 65 70 10 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 57> = `p_promo_sk`)") && 
		explainStr.contains("vec output tuple id: 22") && 
		explainStr.contains("output slot ids: 65 69 70 \n" + 
				"  |  hash output slot ids: 54 58 59 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 47> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 21") && 
		explainStr.contains("output slot ids: 54 57 58 59 \n" + 
				"  |  hash output slot ids: 48 49 50 45 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 45 47 48 49 50 \n" + 
				"  |  hash output slot ids: 0 3 5 7 11 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_gmt_offset` = -5)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` = 'Jewelry')") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: promotion(promotion), PREAGGREGATION: ON\n" + 
				"     PREDICATES: ((`p_channel_dmail` = 'Y') OR (`p_channel_email` = 'Y') OR (`p_channel_tv` = 'Y'))") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`s_gmt_offset` = -5)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998), (`d_moy` = 11)") 
            
        }
    }
}