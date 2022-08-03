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

suite("test_regression_test_tpcds_sf1_p1_q32", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT sum(cs_ext_discount_amt) 'excess discount amount'
		FROM
		  catalog_sales
		, item
		, date_dim
		WHERE (i_manufact_id = 977)
		   AND (i_item_sk = cs_item_sk)
		   AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
		   AND (d_date_sk = cs_sold_date_sk)
		   AND (cs_ext_discount_amt > (
		      SELECT (CAST('1.3' AS DECIMAL) * avg(cs_ext_discount_amt))
		      FROM
		        catalog_sales
		      , date_dim
		      WHERE (cs_item_sk = i_item_sk)
		         AND (d_date BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
		         AND (d_date_sk = cs_sold_date_sk)
		   ))
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 16> sum(`cs_ext_discount_amt`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(<slot 35>)\n" + 
				"  |  group by: ") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 26> = <slot 5> `cs_item_sk`)\n" + 
				"  |  other join predicates: (<slot 57> > (1.3 * <slot 60>))") && 
		explainStr.contains("other join predicates: (<slot 57> > (1.3 * <slot 60>))") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 35 \n" + 
				"  |  hash output slot ids: 6 23 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 20> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 10") && 
		explainStr.contains("output slot ids: 23 26 \n" + 
				"  |  hash output slot ids: 18 21 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_item_sk` = `i_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `i_item_sk`") && 
		explainStr.contains("vec output tuple id: 9") && 
		explainStr.contains("output slot ids: 18 20 21 \n" + 
				"  |  hash output slot ids: 9 10 15 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cs_item_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 6> avg(`cs_ext_discount_amt`))\n" + 
				"  |  group by: <slot 5> `cs_item_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(<slot 30>)\n" + 
				"  |  group by: <slot 31>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 30 31 \n" + 
				"  |  hash output slot ids: 0 1 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cs_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-01-27 00:00:00', `d_date` <= '2000-04-26 00:00:00'") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_date` >= '2000-01-27 00:00:00', `d_date` <= '2000-04-26 00:00:00'") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_manufact_id` = 977)") 
            
        }
    }
}