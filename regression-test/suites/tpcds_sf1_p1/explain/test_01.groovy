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

suite("test_regression_test_tpcds_sf1_p1_q01", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  customer_total_return AS (
		   SELECT
		     sr_customer_sk ctr_customer_sk
		   , sr_store_sk ctr_store_sk
		   , sum(sr_return_amt) ctr_total_return
		   FROM
		     store_returns
		   , date_dim
		   WHERE (sr_returned_date_sk = d_date_sk)
		      AND (d_year = 2000)
		   GROUP BY sr_customer_sk, sr_store_sk
		)
		SELECT c_customer_id
		FROM
		  customer_total_return ctr1
		, store
		, customer
		WHERE (ctr1.ctr_total_return > (
		      SELECT (avg(ctr_total_return) * CAST('1.2' AS DECIMAL))
		      FROM
		        customer_total_return ctr2
		      WHERE (ctr1.ctr_store_sk = ctr2.ctr_store_sk)
		   ))
		   AND (s_store_sk = ctr1.ctr_store_sk)
		   AND (s_state = 'TN')
		   AND (ctr1.ctr_customer_sk = c_customer_sk)
		ORDER BY c_customer_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 32> `c_customer_id` ASC") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 47> = <slot 24> `ctr2`.`ctr_store_sk`)\n" + 
				"  |  other join predicates: (<slot 88> > (<slot 90> * 1.2))") && 
		explainStr.contains("other join predicates: (<slot 88> > (<slot 90> * 1.2))") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 57 \n" + 
				"  |  hash output slot ids: 48 25 44 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 42> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 44 47 48 \n" + 
				"  |  hash output slot ids: 39 42 43 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (`c_customer_sk` = <slot 6> `sr_customer_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 6> `sr_customer_sk`") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 39 42 43 \n" + 
				"  |  hash output slot ids: 7 8 28 ") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `c_customer_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 25> avg(`ctr_total_return`))\n" + 
				"  |  group by: <slot 24> `ctr2`.`ctr_store_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(<slot 20> sum(`sr_return_amt`))\n" + 
				"  |  group by: <slot 19> `sr_store_sk`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 20> sum(`sr_return_amt`))\n" + 
				"  |  group by: <slot 18> `sr_customer_sk`, <slot 19> `sr_store_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 53>)\n" + 
				"  |  group by: <slot 51>, <slot 52>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`sr_returned_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 51 52 53 \n" + 
				"  |  hash output slot ids: 12 13 14 ") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `sr_returned_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000)") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`s_state` = 'TN')") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 8> sum(`sr_return_amt`))\n" + 
				"  |  group by: <slot 6> `sr_customer_sk`, <slot 7> `sr_store_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 35>)\n" + 
				"  |  group by: <slot 33>, <slot 34>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`sr_returned_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 33 34 35 \n" + 
				"  |  hash output slot ids: 0 1 2 ") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `sr_returned_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2000)") 
            
        }
    }
}