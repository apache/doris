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

suite("test_regression_test_tpcds_sf1_p1_q54", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  my_customers AS (
		   SELECT DISTINCT
		     c_customer_sk
		   , c_current_addr_sk
		   FROM
		     (
		      SELECT
		        cs_sold_date_sk sold_date_sk
		      , cs_bill_customer_sk customer_sk
		      , cs_item_sk item_sk
		      FROM
		        catalog_sales
		UNION ALL       SELECT
		        ws_sold_date_sk sold_date_sk
		      , ws_bill_customer_sk customer_sk
		      , ws_item_sk item_sk
		      FROM
		        web_sales
		   )  cs_or_ws_sales
		   , item
		   , date_dim
		   , customer
		   WHERE (sold_date_sk = d_date_sk)
		      AND (item_sk = i_item_sk)
		      AND (i_category = 'Women')
		      AND (i_class = 'maternity')
		      AND (c_customer_sk = cs_or_ws_sales.customer_sk)
		      AND (d_moy = 12)
		      AND (d_year = 1998)
		)
		, my_revenue AS (
		   SELECT
		     c_customer_sk
		   , sum(ss_ext_sales_price) revenue
		   FROM
		     my_customers
		   , store_sales
		   , customer_address
		   , store
		   , date_dim
		   WHERE (c_current_addr_sk = ca_address_sk)
		      AND (ca_county = s_county)
		      AND (ca_state = s_state)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (c_customer_sk = ss_customer_sk)
		      AND (d_month_seq BETWEEN (
		      SELECT DISTINCT (d_month_seq + 1)
		      FROM
		        date_dim
		      WHERE (d_year = 1998)
		         AND (d_moy = 12)
		   ) AND (
		      SELECT DISTINCT (d_month_seq + 3)
		      FROM
		        date_dim
		      WHERE (d_year = 1998)
		         AND (d_moy = 12)
		   ))
		   GROUP BY c_customer_sk
		)
		, segments AS (
		   SELECT CAST((revenue / 50) AS INTEGER) segment
		   FROM
		     my_revenue
		)
		SELECT
		  segment
		, count(*) num_customers
		, (segment * 50) segment_base
		FROM
		  segments
		GROUP BY segment
		ORDER BY segment ASC, num_customers ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 51> <slot 49> `segment` ASC, <slot 52> <slot 50> count(*) ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 50> count(*))\n" + 
				"  |  group by: <slot 49> `segment`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: CAST((<slot 45> sum(`ss_ext_sales_price`) / 50) AS INT)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 45> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 44> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 99>)\n" + 
				"  |  group by: <slot 104>") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: <slot 103> <= <slot 32> (`d_month_seq` + 3)") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: <slot 103> >= <slot 27> (`d_month_seq` + 1)") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 97> = `s_county`)\n" + 
				"  |  equal join conjunct: (<slot 98> = `s_state`)") && 
		explainStr.contains("vec output tuple id: 30") && 
		explainStr.contains("output slot ids: 99 100 101 102 103 104 105 106 107 108 109 110 \n" + 
				"  |  hash output slot ids: 96 97 98 37 39 89 90 91 92 93 94 95 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 88> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 29") && 
		explainStr.contains("output slot ids: 89 90 91 92 93 94 95 96 97 98 \n" + 
				"  |  hash output slot ids: 82 83 35 84 36 85 86 38 87 88 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 55> = <slot 20> `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 28") && 
		explainStr.contains("output slot ids: 82 83 84 85 86 87 88 \n" + 
				"  |  hash output slot ids: 20 53 21 54 55 56 57 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 24") && 
		explainStr.contains("output slot ids: 53 54 55 56 57 \n" + 
				"  |  hash output slot ids: 34 40 41 42 43 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 32> (`d_month_seq` + 3)") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: (`d_month_seq` + 3)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998), (`d_moy` = 12)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 27> (`d_month_seq` + 1)") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: (`d_month_seq` + 1)") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998), (`d_moy` = 12)") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 20> `c_customer_sk`, <slot 21> `c_current_addr_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 71>, <slot 72>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 67> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 27") && 
		explainStr.contains("output slot ids: 71 72 \n" + 
				"  |  hash output slot ids: 64 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 60> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 26") && 
		explainStr.contains("output slot ids: 63 64 67 \n" + 
				"  |  hash output slot ids: 58 59 62 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (`c_customer_sk` = <slot 7> `cs_bill_customer_sk` `ws_bill_customer_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- <slot 7> `cs_bill_customer_sk` `ws_bill_customer_sk`") && 
		explainStr.contains("vec output tuple id: 25") && 
		explainStr.contains("output slot ids: 58 59 60 62 \n" + 
				"  |  hash output slot ids: 6 8 12 13 ") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `c_customer_sk`") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_category` = 'Women'), (`i_class` = 'maternity')") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_moy` = 12), (`d_year` = 1998)") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") 
            
        }
    }
}