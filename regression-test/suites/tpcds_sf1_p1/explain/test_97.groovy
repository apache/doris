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

suite("test_regression_test_tpcds_sf1_p1_q97", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  ssci AS (
		   SELECT
		     ss_customer_sk customer_sk
		   , ss_item_sk item_sk
		   FROM
		     store_sales
		   , date_dim
		   WHERE (ss_sold_date_sk = d_date_sk)
		      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
		   GROUP BY ss_customer_sk, ss_item_sk
		)
		, csci AS (
		   SELECT
		     cs_bill_customer_sk customer_sk
		   , cs_item_sk item_sk
		   FROM
		     catalog_sales
		   , date_dim
		   WHERE (cs_sold_date_sk = d_date_sk)
		      AND (d_month_seq BETWEEN 1200 AND (1200 + 11))
		   GROUP BY cs_bill_customer_sk, cs_item_sk
		)
		SELECT
		  sum((CASE WHEN (ssci.customer_sk IS NOT NULL)
		   AND (csci.customer_sk IS NULL) THEN 1 ELSE 0 END)) store_only
		, sum((CASE WHEN (ssci.customer_sk IS NULL)
		   AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END)) catalog_only
		, sum((CASE WHEN (ssci.customer_sk IS NOT NULL)
		   AND (csci.customer_sk IS NOT NULL) THEN 1 ELSE 0 END)) store_and_catalog
		FROM
		  ssci
		FULL JOIN csci ON (ssci.customer_sk = csci.customer_sk)
		   AND (ssci.item_sk = csci.item_sk)
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 18> sum((CASE WHEN (`ssci`.`customer_sk` IS NOT NULL) AND (`csci`.`customer_sk` IS NULL) THEN 1 ELSE 0 END))), sum(<slot 19> sum((CASE WHEN (`ssci`.`customer_sk` IS NULL) AND (`csci`.`customer_sk` IS NOT NULL) THEN 1 ELSE 0 END))), sum(<slot 20> sum((CASE WHEN (`ssci`.`customer_sk` IS NOT NULL) AND (`csci`.`customer_sk` IS NOT NULL) THEN 1 ELSE 0 END)))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum((CASE WHEN (<slot 34> IS NOT NULL) AND (<slot 36> IS NULL) THEN 1 ELSE 0 END)), sum((CASE WHEN (<slot 34> IS NULL) AND (<slot 36> IS NOT NULL) THEN 1 ELSE 0 END)), sum((CASE WHEN (<slot 34> IS NOT NULL) AND (<slot 36> IS NOT NULL) THEN 1 ELSE 0 END))\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: FULL OUTER JOIN(PARTITIONED)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 5> `ss_customer_sk` = <slot 14> `cs_bill_customer_sk`)\n" + 
				"  |  equal join conjunct: (<slot 6> `ss_item_sk` = <slot 15> `cs_item_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 34 36 \n" + 
				"  |  hash output slot ids: 5 14 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 14> `cs_bill_customer_sk`, <slot 15> `cs_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 29>, <slot 30>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 29 30 \n" + 
				"  |  hash output slot ids: 9 10 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  group by: <slot 5> `ss_customer_sk`, <slot 6> `ss_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  group by: <slot 24>, <slot 25>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 24 25 \n" + 
				"  |  hash output slot ids: 0 1 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") 
            
        }
    }
}