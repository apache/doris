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

suite("test_regression_test_tpcds_sf1_p1_q59", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  wss AS (
		   SELECT
		     d_week_seq
		   , ss_store_sk
		   , sum((CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE null END)) sun_sales
		   , sum((CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE null END)) mon_sales
		   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE null END)) tue_sales
		   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE null END)) wed_sales
		   , sum((CASE WHEN (d_day_name = 'Thursday ') THEN ss_sales_price ELSE null END)) thu_sales
		   , sum((CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE null END)) fri_sales
		   , sum((CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE null END)) sat_sales
		   FROM
		     store_sales
		   , date_dim
		   WHERE (d_date_sk = ss_sold_date_sk)
		   GROUP BY d_week_seq, ss_store_sk
		)
		SELECT
		  s_store_name1
		, s_store_id1
		, d_week_seq1
		, (sun_sales1 / sun_sales2)
		, (mon_sales1 / mon_sales2)
		, (tue_sales1 / tue_sales2)
		, (wed_sales1 / wed_sales2)
		, (thu_sales1 / thu_sales2)
		, (fri_sales1 / fri_sales2)
		, (sat_sales1 / sat_sales2)
		FROM
		  (
		   SELECT
		     s_store_name s_store_name1
		   , wss.d_week_seq d_week_seq1
		   , s_store_id s_store_id1
		   , sun_sales sun_sales1
		   , mon_sales mon_sales1
		   , tue_sales tue_sales1
		   , wed_sales wed_sales1
		   , thu_sales thu_sales1
		   , fri_sales fri_sales1
		   , sat_sales sat_sales1
		   FROM
		     wss
		   , store
		   , date_dim d
		   WHERE (d.d_week_seq = wss.d_week_seq)
		      AND (ss_store_sk = s_store_sk)
		      AND (d_month_seq BETWEEN 1212 AND (1212 + 11))
		)  y
		, (
		   SELECT
		     s_store_name s_store_name2
		   , wss.d_week_seq d_week_seq2
		   , s_store_id s_store_id2
		   , sun_sales sun_sales2
		   , mon_sales mon_sales2
		   , tue_sales tue_sales2
		   , wed_sales wed_sales2
		   , thu_sales thu_sales2
		   , fri_sales fri_sales2
		   , sat_sales sat_sales2
		   FROM
		     wss
		   , store
		   , date_dim d
		   WHERE (d.d_week_seq = wss.d_week_seq)
		      AND (ss_store_sk = s_store_sk)
		      AND (d_month_seq BETWEEN (1212 + 12) AND (1212 + 23))
		)  x
		WHERE (s_store_id1 = s_store_id2)
		   AND (d_week_seq1 = (d_week_seq2 - 52))
		ORDER BY s_store_name1 ASC, s_store_id1 ASC, d_week_seq1 ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 78> `s_store_name1` ASC, <slot 79> `s_store_id1` ASC, <slot 80> `d_week_seq1` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 124> = <slot 154>)\n" + 
				"  |  equal join conjunct: (<slot 114> = (<slot 145> - 52))") && 
		explainStr.contains("vec output tuple id: 21") && 
		explainStr.contains("output slot ids: 158 160 161 162 163 164 165 166 167 168 174 175 176 177 178 179 180 \n" + 
				"  |  hash output slot ids: 114 147 116 148 117 149 118 150 119 151 120 152 121 153 122 123 124 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 104> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 114 116 117 118 119 120 121 122 123 124 \n" + 
				"  |  hash output slot ids: 103 24 105 25 106 107 108 109 110 111 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (`d`.`d_week_seq` = <slot 6> `d_week_seq`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- <slot 6> `d_week_seq`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 103 104 105 106 107 108 109 110 111 \n" + 
				"  |  hash output slot ids: 6 7 8 9 10 11 12 13 14 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1212, `d_month_seq` <= 1223\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `d`.`d_week_seq`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 135> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 145 147 148 149 150 151 152 153 154 \n" + 
				"  |  hash output slot ids: 64 134 136 137 138 139 140 141 142 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (`d`.`d_week_seq` = <slot 45> `d_week_seq`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- <slot 45> `d_week_seq`") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 134 135 136 137 138 139 140 141 142 \n" + 
				"  |  hash output slot ids: 48 49 50 51 52 53 45 46 47 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `d_month_seq` >= 1224, `d_month_seq` <= 1235\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `d`.`d_week_seq`") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 47> sum((CASE WHEN (`d_day_name` = 'Sunday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 48> sum((CASE WHEN (`d_day_name` = 'Monday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 49> sum((CASE WHEN (`d_day_name` = 'Tuesday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 50> sum((CASE WHEN (`d_day_name` = 'Wednesday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 51> sum((CASE WHEN (`d_day_name` = 'Thursday ') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 52> sum((CASE WHEN (`d_day_name` = 'Friday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 53> sum((CASE WHEN (`d_day_name` = 'Saturday') THEN `ss_sales_price` ELSE NULL END)))\n" + 
				"  |  group by: <slot 45> `d_week_seq`, <slot 46> `ss_store_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (<slot 130> = 'Sunday') THEN <slot 127> ELSE NULL END)), sum((CASE WHEN (<slot 130> = 'Monday') THEN <slot 127> ELSE NULL END)), sum((CASE WHEN (<slot 130> = 'Tuesday') THEN <slot 127> ELSE NULL END)), sum((CASE WHEN (<slot 130> = 'Wednesday') THEN <slot 127> ELSE NULL END)), sum((CASE WHEN (<slot 130> = 'Thursday ') THEN <slot 127> ELSE NULL END)), sum((CASE WHEN (<slot 130> = 'Friday') THEN <slot 127> ELSE NULL END)), sum((CASE WHEN (<slot 130> = 'Saturday') THEN <slot 127> ELSE NULL END))\n" + 
				"  |  group by: <slot 129>, <slot 126>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 126 127 129 130 \n" + 
				"  |  hash output slot ids: 39 40 41 42 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 8> sum((CASE WHEN (`d_day_name` = 'Sunday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 9> sum((CASE WHEN (`d_day_name` = 'Monday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 10> sum((CASE WHEN (`d_day_name` = 'Tuesday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 11> sum((CASE WHEN (`d_day_name` = 'Wednesday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 12> sum((CASE WHEN (`d_day_name` = 'Thursday ') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 13> sum((CASE WHEN (`d_day_name` = 'Friday') THEN `ss_sales_price` ELSE NULL END))), sum(<slot 14> sum((CASE WHEN (`d_day_name` = 'Saturday') THEN `ss_sales_price` ELSE NULL END)))\n" + 
				"  |  group by: <slot 6> `d_week_seq`, <slot 7> `ss_store_sk`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (<slot 99> = 'Sunday') THEN <slot 96> ELSE NULL END)), sum((CASE WHEN (<slot 99> = 'Monday') THEN <slot 96> ELSE NULL END)), sum((CASE WHEN (<slot 99> = 'Tuesday') THEN <slot 96> ELSE NULL END)), sum((CASE WHEN (<slot 99> = 'Wednesday') THEN <slot 96> ELSE NULL END)), sum((CASE WHEN (<slot 99> = 'Thursday ') THEN <slot 96> ELSE NULL END)), sum((CASE WHEN (<slot 99> = 'Friday') THEN <slot 96> ELSE NULL END)), sum((CASE WHEN (<slot 99> = 'Saturday') THEN <slot 96> ELSE NULL END))\n" + 
				"  |  group by: <slot 98>, <slot 95>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 95 96 98 99 \n" + 
				"  |  hash output slot ids: 0 1 2 3 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") 
            
        }
    }
}