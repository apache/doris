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

suite("test_regression_test_tpcds_sf1_p1_q38", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT count(*)
		FROM
		  (
		   SELECT DISTINCT
		     c_last_name
		   , c_first_name
		   , d_date
		   FROM
		     store_sales
		   , date_dim
		   , customer
		   WHERE (store_sales.ss_sold_date_sk = date_dim.d_date_sk)
		      AND (store_sales.ss_customer_sk = customer.c_customer_sk)
		      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
		INTERSECT    SELECT DISTINCT
		     c_last_name
		   , c_first_name
		   , d_date
		   FROM
		     catalog_sales
		   , date_dim
		   , customer
		   WHERE (catalog_sales.cs_sold_date_sk = date_dim.d_date_sk)
		      AND (catalog_sales.cs_bill_customer_sk = customer.c_customer_sk)
		      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
		INTERSECT    SELECT DISTINCT
		     c_last_name
		   , c_first_name
		   , d_date
		   FROM
		     web_sales
		   , date_dim
		   , customer
		   WHERE (web_sales.ws_sold_date_sk = date_dim.d_date_sk)
		      AND (web_sales.ws_bill_customer_sk = customer.c_customer_sk)
		      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
		)  hot_cust
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: count(<slot 39> count(*))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: count(*)\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 30> `c_last_name`, <slot 31> `c_first_name`, <slot 32> `d_date`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 76>, <slot 77>, <slot 73>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 67> = `customer`.`c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 73 76 77 \n" + 
				"  |  hash output slot ids: 68 22 23 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`web_sales`.`ws_sold_date_sk` = `date_dim`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `date_dim`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 67 68 \n" + 
				"  |  hash output slot ids: 24 27 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `web_sales`.`ws_sold_date_sk`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 19> `c_last_name`, <slot 20> `c_first_name`, <slot 21> `d_date`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 63>, <slot 64>, <slot 60>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 54> = `customer`.`c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 60 63 64 \n" + 
				"  |  hash output slot ids: 55 11 12 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`catalog_sales`.`cs_sold_date_sk` = `date_dim`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `date_dim`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 54 55 \n" + 
				"  |  hash output slot ids: 16 13 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `catalog_sales`.`cs_sold_date_sk`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 8> `c_last_name`, <slot 9> `c_first_name`, <slot 10> `d_date`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 50>, <slot 51>, <slot 47>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 41> = `customer`.`c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 47 50 51 \n" + 
				"  |  hash output slot ids: 0 1 42 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`store_sales`.`ss_sold_date_sk` = `date_dim`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `date_dim`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 41 42 \n" + 
				"  |  hash output slot ids: 2 5 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `store_sales`.`ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") 
            
        }
    }
}