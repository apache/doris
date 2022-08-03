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

suite("test_regression_test_tpcds_sf1_p1_q70", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  sum(ss_net_profit) total_sum
		, s_state
		, s_county
		, (GROUPING (s_state) + GROUPING (s_county)) lochierarchy
		, rank() OVER (PARTITION BY (GROUPING (s_state) + GROUPING (s_county)), (CASE WHEN (GROUPING (s_county) = 0) THEN s_state END) ORDER BY sum(ss_net_profit) DESC) rank_within_parent
		FROM
		  store_sales
		, date_dim d1
		, store
		WHERE (d1.d_month_seq BETWEEN 1200 AND (1200 + 11))
		   AND (d1.d_date_sk = ss_sold_date_sk)
		   AND (s_store_sk = ss_store_sk)
		   AND (s_state IN (
		   SELECT s_state
		   FROM
		     (
		      SELECT
		        s_state s_state
		      , rank() OVER (PARTITION BY s_state ORDER BY sum(ss_net_profit) DESC) ranking
		      FROM
		        store_sales
		      , store
		      , date_dim
		      WHERE (d_month_seq BETWEEN 1200 AND (1200 + 11))
		         AND (d_date_sk = ss_sold_date_sk)
		         AND (s_store_sk = ss_store_sk)
		      GROUP BY s_state
		   )  tmp1
		   WHERE (ranking <= 5)
		))
		GROUP BY ROLLUP (s_state, s_county)
		ORDER BY lochierarchy DESC, (CASE WHEN (lochierarchy = 0) THEN s_state END) ASC, rank_within_parent ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 39> (grouping(<slot 34> `GROUPING_PREFIX_`s_state``) + grouping(<slot 35> `GROUPING_PREFIX_`s_county``)) DESC, <slot 40> (CASE WHEN ((grouping(<slot 34> `GROUPING_PREFIX_`s_state``) + grouping(<slot 35> `GROUPING_PREFIX_`s_county``)) = 0) THEN <slot 31> `s_state` END) ASC, <slot 41> <slot 38> rank() OVER (PARTITION BY (grouping(`s_state`) + grouping(`s_county`)), (CASE WHEN (grouping(`s_county`) = 0) THEN `s_state` END) ORDER BY sum(`ss_net_profit`) DESC NULLS LAST) ASC") && 
		explainStr.contains("order by: <slot 95> <slot 36> sum(`ss_net_profit`) DESC NULLS LAST") && 
		explainStr.contains("order by: <slot 96> (grouping(<slot 34> `GROUPING_PREFIX_`s_state``) + grouping(<slot 35> `GROUPING_PREFIX_`s_county``)) ASC, <slot 97> (CASE WHEN (grouping(<slot 35> `GROUPING_PREFIX_`s_county``) = 0) THEN <slot 31> `s_state` END) ASC, <slot 95> <slot 36> sum(`ss_net_profit`) DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 36> sum(`ss_net_profit`))\n" + 
				"  |  group by: <slot 31> `s_state`, <slot 32> `s_county`, <slot 33> `GROUPING_ID`, <slot 34> `GROUPING_PREFIX_`s_state``, <slot 35> `GROUPING_PREFIX_`s_county``") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 27> `ss_net_profit`)\n" + 
				"  |  group by: <slot 25> `s_state`, <slot 26> `s_county`, <slot 28> `GROUPING_ID`, <slot 29> `GROUPING_PREFIX_`s_state``, <slot 30> `GROUPING_PREFIX_`s_county``") && 
		explainStr.contains("output slots: ``s_state``, ``s_county``, ``ss_net_profit``, ``GROUPING_ID``, ``GROUPING_PREFIX_`s_state```, ``GROUPING_PREFIX_`s_county```") && 
		explainStr.contains("join op: LEFT SEMI JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: <slot 55> = <slot 72> <slot 7> `s_state`") && 
		explainStr.contains("vec output tuple id: 25") && 
		explainStr.contains("output slot ids: 80 81 82 83 84 85 86 87 \n" + 
				"  |  hash output slot ids: 50 51 52 53 54 55 56 57 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 47> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 50 51 52 53 54 55 56 57 \n" + 
				"  |  hash output slot ids: 48 16 49 23 45 46 14 47 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d1`.`d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d1`.`d_date_sk`") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 45 46 47 48 49 \n" + 
				"  |  hash output slot ids: 20 21 22 24 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("predicates: (<slot 70> <= 5)") && 
		explainStr.contains("order by: <slot 73> <slot 8> sum(`ss_net_profit`) DESC NULLS LAST") && 
		explainStr.contains("order by: <slot 72> <slot 7> `s_state` ASC, <slot 73> <slot 8> sum(`ss_net_profit`) DESC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 8> sum(`ss_net_profit`))\n" + 
				"  |  group by: <slot 7> `s_state`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 63>)\n" + 
				"  |  group by: <slot 68>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 60> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 63 68 \n" + 
				"  |  hash output slot ids: 0 58 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 58 60 \n" + 
				"  |  hash output slot ids: 1 6 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1200, `d_month_seq` <= 1211") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d1`.`d_month_seq` >= 1200, `d1`.`d_month_seq` <= 1211") 
            
        }
    }
}