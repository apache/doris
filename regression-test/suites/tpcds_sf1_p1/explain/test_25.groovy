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

suite("test_regression_test_tpcds_sf1_p1_q25", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_item_id
		, i_item_desc
		, s_store_id
		, s_store_name
		, sum(ss_net_profit) store_sales_profit
		, sum(sr_net_loss) store_returns_loss
		, sum(cs_net_profit) catalog_sales_profit
		FROM
		  store_sales
		, store_returns
		, catalog_sales
		, date_dim d1
		, date_dim d2
		, date_dim d3
		, store
		, item
		WHERE (d1.d_moy = 4)
		   AND (d1.d_year = 2001)
		   AND (d1.d_date_sk = ss_sold_date_sk)
		   AND (i_item_sk = ss_item_sk)
		   AND (s_store_sk = ss_store_sk)
		   AND (ss_customer_sk = sr_customer_sk)
		   AND (ss_item_sk = sr_item_sk)
		   AND (ss_ticket_number = sr_ticket_number)
		   AND (sr_returned_date_sk = d2.d_date_sk)
		   AND (d2.d_moy BETWEEN 4 AND 10)
		   AND (d2.d_year = 2001)
		   AND (sr_customer_sk = cs_bill_customer_sk)
		   AND (sr_item_sk = cs_item_sk)
		   AND (cs_sold_date_sk = d3.d_date_sk)
		   AND (d3.d_moy BETWEEN 4 AND 10)
		   AND (d3.d_year = 2001)
		GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
		ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 37> <slot 30> `i_item_id` ASC, <slot 38> <slot 31> `i_item_desc` ASC, <slot 39> <slot 32> `s_store_id` ASC, <slot 40> <slot 33> `s_store_name` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 34> sum(`ss_net_profit`)), sum(<slot 35> sum(`sr_net_loss`)), sum(<slot 36> sum(`cs_net_profit`))\n" + 
				"  |  group by: <slot 30> `i_item_id`, <slot 31> `i_item_desc`, <slot 32> `s_store_id`, <slot 33> `s_store_name`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 150>), sum(<slot 165>), sum(<slot 173>)\n" + 
				"  |  group by: <slot 159>, <slot 160>, <slot 162>, <slot 163>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 149> = `d3`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 150 159 160 162 163 165 173 \n" + 
				"  |  hash output slot ids: 146 132 133 135 136 138 123 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 116> = `cs_bill_customer_sk`)\n" + 
				"  |  equal join conjunct: (<slot 117> = `cs_item_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 123 132 133 135 136 138 146 149 \n" + 
				"  |  hash output slot ids: 112 113 115 100 6 26 109 110 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 99> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 100 109 110 112 113 115 116 117 \n" + 
				"  |  hash output slot ids: 80 96 97 89 90 92 93 95 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 69> = `sr_customer_sk`)\n" + 
				"  |  equal join conjunct: (<slot 67> = `sr_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 70> = `sr_ticket_number`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 80 89 90 92 93 95 96 97 99 \n" + 
				"  |  hash output slot ids: 16 65 17 20 5 74 75 77 78 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 56> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 65 67 69 70 74 75 77 78 \n" + 
				"  |  hash output slot ids: 2 3 53 55 57 58 62 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 46> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 53 55 56 57 58 62 63 \n" + 
				"  |  hash output slot ids: 48 0 49 1 44 46 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d1`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d1`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 44 46 47 48 49 \n" + 
				"  |  hash output slot ids: 18 4 12 14 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d3`.`d_moy` >= 4, `d3`.`d_moy` <= 10, (`d3`.`d_year` = 2001)") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d2`.`d_moy` >= 4, `d2`.`d_moy` <= 10, (`d2`.`d_year` = 2001)") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d1`.`d_moy` = 4), (`d1`.`d_year` = 2001)") 
            
        }
    }
}