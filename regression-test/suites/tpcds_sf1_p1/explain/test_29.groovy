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

suite("test_regression_test_tpcds_sf1_p1_q29", "tpch_sf1") {
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
		, sum(ss_quantity) store_sales_quantity
		, sum(sr_return_quantity) store_returns_quantity
		, sum(cs_quantity) catalog_sales_quantity
		FROM
		  store_sales
		, store_returns
		, catalog_sales
		, date_dim d1
		, date_dim d2
		, date_dim d3
		, store
		, item
		WHERE (d1.d_moy = 9)
		   AND (d1.d_year = 1999)
		   AND (d1.d_date_sk = ss_sold_date_sk)
		   AND (i_item_sk = ss_item_sk)
		   AND (s_store_sk = ss_store_sk)
		   AND (ss_customer_sk = sr_customer_sk)
		   AND (ss_item_sk = sr_item_sk)
		   AND (ss_ticket_number = sr_ticket_number)
		   AND (sr_returned_date_sk = d2.d_date_sk)
		   AND (d2.d_moy BETWEEN 9 AND (9 + 3))
		   AND (d2.d_year = 1999)
		   AND (sr_customer_sk = cs_bill_customer_sk)
		   AND (sr_item_sk = cs_item_sk)
		   AND (cs_sold_date_sk = d3.d_date_sk)
		   AND (d3.d_year IN (1999, (1999 + 1), (1999 + 2)))
		GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name
		ORDER BY i_item_id ASC, i_item_desc ASC, s_store_id ASC, s_store_name ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 36> <slot 29> `i_item_id` ASC, <slot 37> <slot 30> `i_item_desc` ASC, <slot 38> <slot 31> `s_store_id` ASC, <slot 39> <slot 32> `s_store_name` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 33> sum(`ss_quantity`)), sum(<slot 34> sum(`sr_return_quantity`)), sum(<slot 35> sum(`cs_quantity`))\n" + 
				"  |  group by: <slot 29> `i_item_id`, <slot 30> `i_item_desc`, <slot 31> `s_store_id`, <slot 32> `s_store_name`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 149>), sum(<slot 164>), sum(<slot 172>)\n" + 
				"  |  group by: <slot 158>, <slot 159>, <slot 161>, <slot 162>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 148> = `d3`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 149 158 159 161 162 164 172 \n" + 
				"  |  hash output slot ids: 145 131 132 134 135 137 122 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 115> = `cs_bill_customer_sk`)\n" + 
				"  |  equal join conjunct: (<slot 116> = `cs_item_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 122 131 132 134 135 137 145 148 \n" + 
				"  |  hash output slot ids: 112 114 99 6 26 108 109 111 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 98> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 99 108 109 111 112 114 115 116 \n" + 
				"  |  hash output slot ids: 96 88 89 91 92 94 79 95 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 68> = `sr_customer_sk`)\n" + 
				"  |  equal join conjunct: (<slot 66> = `sr_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 69> = `sr_ticket_number`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 79 88 89 91 92 94 95 96 98 \n" + 
				"  |  hash output slot ids: 64 16 17 20 5 73 74 76 77 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 55> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 64 66 68 69 73 74 76 77 \n" + 
				"  |  hash output slot ids: 2 3 52 54 56 57 61 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 45> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 52 54 55 56 57 61 62 \n" + 
				"  |  hash output slot ids: 48 0 1 43 45 46 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d1`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d1`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 43 45 46 47 48 \n" + 
				"  |  hash output slot ids: 18 4 12 14 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d3`.`d_year` IN (1999, 2000, 2001))") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d2`.`d_moy` >= 9, `d2`.`d_moy` <= 12, (`d2`.`d_year` = 1999)") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d1`.`d_moy` = 9), (`d1`.`d_year` = 1999)") 
            
        }
    }
}