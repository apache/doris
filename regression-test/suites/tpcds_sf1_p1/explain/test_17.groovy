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

suite("test_regression_test_tpcds_sf1_p1_q17", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_item_id
		, i_item_desc
		, s_state
		, count(ss_quantity) store_sales_quantitycount
		, avg(ss_quantity) store_sales_quantityave
		, stddev_samp(ss_quantity) store_sales_quantitystdev
		, (stddev_samp(ss_quantity) / avg(ss_quantity)) store_sales_quantitycov
		, count(sr_return_quantity) store_returns_quantitycount
		, avg(sr_return_quantity) store_returns_quantityave
		, stddev_samp(sr_return_quantity) store_returns_quantitystdev
		, (stddev_samp(sr_return_quantity) / avg(sr_return_quantity)) store_returns_quantitycov
		, count(cs_quantity) catalog_sales_quantitycount
		, avg(cs_quantity) catalog_sales_quantityave
		, stddev_samp(cs_quantity) catalog_sales_quantitystdev
		, (stddev_samp(cs_quantity) / avg(cs_quantity)) catalog_sales_quantitycov
		FROM
		  store_sales
		, store_returns
		, catalog_sales
		, date_dim d1
		, date_dim d2
		, date_dim d3
		, store
		, item
		WHERE (d1.d_quarter_name = '2001Q1')
		   AND (d1.d_date_sk = ss_sold_date_sk)
		   AND (i_item_sk = ss_item_sk)
		   AND (s_store_sk = ss_store_sk)
		   AND (ss_customer_sk = sr_customer_sk)
		   AND (ss_item_sk = sr_item_sk)
		   AND (ss_ticket_number = sr_ticket_number)
		   AND (sr_returned_date_sk = d2.d_date_sk)
		   AND (d2.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
		   AND (sr_customer_sk = cs_bill_customer_sk)
		   AND (sr_item_sk = cs_item_sk)
		   AND (cs_sold_date_sk = d3.d_date_sk)
		   AND (d3.d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3'))
		GROUP BY i_item_id, i_item_desc, s_state
		ORDER BY i_item_id ASC, i_item_desc ASC, s_state ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 50> <slot 38> `i_item_id` ASC, <slot 51> <slot 39> `i_item_desc` ASC, <slot 52> <slot 40> `s_state` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 29> count(`ss_quantity`)), avg(<slot 30> avg(`ss_quantity`)), stddev_samp(<slot 31> stddev_samp(`ss_quantity`)), count(<slot 32> count(`sr_return_quantity`)), avg(<slot 33> avg(`sr_return_quantity`)), stddev_samp(<slot 34> stddev_samp(`sr_return_quantity`)), count(<slot 35> count(`cs_quantity`)), avg(<slot 36> avg(`cs_quantity`)), stddev_samp(<slot 37> stddev_samp(`cs_quantity`))\n" + 
				"  |  group by: <slot 26> `i_item_id`, <slot 27> `i_item_desc`, <slot 28> `s_state`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(<slot 156>), avg(<slot 156>), stddev_samp(<slot 156>), count(<slot 169>), avg(<slot 169>), stddev_samp(<slot 169>), count(<slot 176>), avg(<slot 176>), stddev_samp(<slot 176>)\n" + 
				"  |  group by: <slot 164>, <slot 165>, <slot 167>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 155> = `d3`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 156 164 165 167 169 176 \n" + 
				"  |  hash output slot ids: 145 132 152 140 141 143 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 126> = `cs_bill_customer_sk`)\n" + 
				"  |  equal join conjunct: (<slot 127> = `cs_item_sk`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 132 140 141 143 145 152 155 \n" + 
				"  |  hash output slot ids: 112 5 23 120 121 123 125 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 111> = `d2`.`d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 112 120 121 123 125 126 127 \n" + 
				"  |  hash output slot ids: 102 103 105 107 108 109 94 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 85> = `sr_customer_sk`)\n" + 
				"  |  equal join conjunct: (<slot 83> = `sr_item_sk`)\n" + 
				"  |  equal join conjunct: (<slot 86> = `sr_ticket_number`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 94 102 103 105 107 108 109 111 \n" + 
				"  |  hash output slot ids: 81 18 4 89 90 92 14 15 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 73> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 81 83 85 86 89 90 92 \n" + 
				"  |  hash output slot ids: 2 70 72 74 75 78 79 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 64> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 70 72 73 74 75 78 79 \n" + 
				"  |  hash output slot ids: 64 0 65 1 66 67 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d1`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d1`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 62 64 65 66 67 \n" + 
				"  |  hash output slot ids: 16 3 10 12 13 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d3`.`d_quarter_name` IN ('2001Q1', '2001Q2', '2001Q3'))") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d2`.`d_quarter_name` IN ('2001Q1', '2001Q2', '2001Q3'))") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d1`.`d_quarter_name` = '2001Q1')") 
            
        }
    }
}