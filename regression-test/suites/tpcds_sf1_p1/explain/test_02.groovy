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

suite("test_regression_test_tpcds_sf1_p1_q02", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  wscs AS (
		   SELECT
		     sold_date_sk
		   , sales_price
		   FROM
		     (
		      SELECT
		        ws_sold_date_sk sold_date_sk
		      , ws_ext_sales_price sales_price
		      FROM
		        web_sales
		   ) x
		UNION ALL (
		      SELECT
		        cs_sold_date_sk sold_date_sk
		      , cs_ext_sales_price sales_price
		      FROM
		        catalog_sales
		   ) )
		, wswscs AS (
		   SELECT
		     d_week_seq
		   , sum((CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE null END)) sun_sales
		   , sum((CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE null END)) mon_sales
		   , sum((CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE null END)) tue_sales
		   , sum((CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE null END)) wed_sales
		   , sum((CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE null END)) thu_sales
		   , sum((CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE null END)) fri_sales
		   , sum((CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE null END)) sat_sales
		   FROM
		     wscs
		   , date_dim
		   WHERE (d_date_sk = sold_date_sk)
		   GROUP BY d_week_seq
		)
		SELECT
		  d_week_seq1
		, round((sun_sales1 / sun_sales2), 2)
		, round((mon_sales1 / mon_sales2), 2)
		, round((tue_sales1 / tue_sales2), 2)
		, round((wed_sales1 / wed_sales2), 2)
		, round((thu_sales1 / thu_sales2), 2)
		, round((fri_sales1 / fri_sales2), 2)
		, round((sat_sales1 / sat_sales2), 2)
		FROM
		  (
		   SELECT
		     wswscs.d_week_seq d_week_seq1
		   , sun_sales sun_sales1
		   , mon_sales mon_sales1
		   , tue_sales tue_sales1
		   , wed_sales wed_sales1
		   , thu_sales thu_sales1
		   , fri_sales fri_sales1
		   , sat_sales sat_sales1
		   FROM
		     wswscs
		   , date_dim
		   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
		      AND (d_year = 2001)
		)  y
		, (
		   SELECT
		     wswscs.d_week_seq d_week_seq2
		   , sun_sales sun_sales2
		   , mon_sales mon_sales2
		   , tue_sales tue_sales2
		   , wed_sales wed_sales2
		   , thu_sales thu_sales2
		   , fri_sales fri_sales2
		   , sat_sales sat_sales2
		   FROM
		     wswscs
		   , date_dim
		   WHERE (date_dim.d_week_seq = wswscs.d_week_seq)
		      AND (d_year = (2001 + 1))
		)  z
		WHERE (d_week_seq1 = (d_week_seq2 - 53))
		ORDER BY d_week_seq1 ASC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 78> `d_week_seq1` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 98> = (<slot 113> - 53))") && 
		explainStr.contains("vec output tuple id: 25") && 
		explainStr.contains("output slot ids: 123 124 125 126 127 128 129 130 134 135 136 137 138 139 140 \n" + 
				"  |  hash output slot ids: 98 99 100 101 102 103 104 105 114 115 116 117 118 119 120 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 13> `d_week_seq` = `date_dim`.`d_week_seq`)") && 
		explainStr.contains("vec output tuple id: 22") && 
		explainStr.contains("output slot ids: 98 99 100 101 102 103 104 105 \n" + 
				"  |  hash output slot ids: 16 17 18 19 20 13 14 15 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 14> sum((CASE WHEN (`d_day_name` = 'Sunday') THEN `sales_price` ELSE NULL END))), sum(<slot 15> sum((CASE WHEN (`d_day_name` = 'Monday') THEN `sales_price` ELSE NULL END))), sum(<slot 16> sum((CASE WHEN (`d_day_name` = 'Tuesday') THEN `sales_price` ELSE NULL END))), sum(<slot 17> sum((CASE WHEN (`d_day_name` = 'Wednesday') THEN `sales_price` ELSE NULL END))), sum(<slot 18> sum((CASE WHEN (`d_day_name` = 'Thursday') THEN `sales_price` ELSE NULL END))), sum(<slot 19> sum((CASE WHEN (`d_day_name` = 'Friday') THEN `sales_price` ELSE NULL END))), sum(<slot 20> sum((CASE WHEN (`d_day_name` = 'Saturday') THEN `sales_price` ELSE NULL END)))\n" + 
				"  |  group by: <slot 13> `d_week_seq`") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 52> `d_week_seq` = `date_dim`.`d_week_seq`)") && 
		explainStr.contains("vec output tuple id: 24") && 
		explainStr.contains("output slot ids: 113 114 115 116 117 118 119 120 \n" + 
				"  |  hash output slot ids: 52 53 54 55 56 57 58 59 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 53> sum((CASE WHEN (`d_day_name` = 'Sunday') THEN `sales_price` ELSE NULL END))), sum(<slot 54> sum((CASE WHEN (`d_day_name` = 'Monday') THEN `sales_price` ELSE NULL END))), sum(<slot 55> sum((CASE WHEN (`d_day_name` = 'Tuesday') THEN `sales_price` ELSE NULL END))), sum(<slot 56> sum((CASE WHEN (`d_day_name` = 'Wednesday') THEN `sales_price` ELSE NULL END))), sum(<slot 57> sum((CASE WHEN (`d_day_name` = 'Thursday') THEN `sales_price` ELSE NULL END))), sum(<slot 58> sum((CASE WHEN (`d_day_name` = 'Friday') THEN `sales_price` ELSE NULL END))), sum(<slot 59> sum((CASE WHEN (`d_day_name` = 'Saturday') THEN `sales_price` ELSE NULL END)))\n" + 
				"  |  group by: <slot 52> `d_week_seq`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2002)") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (<slot 111> = 'Sunday') THEN <slot 109> ELSE NULL END)), sum((CASE WHEN (<slot 111> = 'Monday') THEN <slot 109> ELSE NULL END)), sum((CASE WHEN (<slot 111> = 'Tuesday') THEN <slot 109> ELSE NULL END)), sum((CASE WHEN (<slot 111> = 'Wednesday') THEN <slot 109> ELSE NULL END)), sum((CASE WHEN (<slot 111> = 'Thursday') THEN <slot 109> ELSE NULL END)), sum((CASE WHEN (<slot 111> = 'Friday') THEN <slot 109> ELSE NULL END)), sum((CASE WHEN (<slot 111> = 'Saturday') THEN <slot 109> ELSE NULL END))\n" + 
				"  |  group by: <slot 110>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (<slot 45> `sold_date_sk` `cs_sold_date_sk` = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 23") && 
		explainStr.contains("output slot ids: 109 110 111 \n" + 
				"  |  hash output slot ids: 49 50 46 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (<slot 96> = 'Sunday') THEN <slot 94> ELSE NULL END)), sum((CASE WHEN (<slot 96> = 'Monday') THEN <slot 94> ELSE NULL END)), sum((CASE WHEN (<slot 96> = 'Tuesday') THEN <slot 94> ELSE NULL END)), sum((CASE WHEN (<slot 96> = 'Wednesday') THEN <slot 94> ELSE NULL END)), sum((CASE WHEN (<slot 96> = 'Thursday') THEN <slot 94> ELSE NULL END)), sum((CASE WHEN (<slot 96> = 'Friday') THEN <slot 94> ELSE NULL END)), sum((CASE WHEN (<slot 96> = 'Saturday') THEN <slot 94> ELSE NULL END))\n" + 
				"  |  group by: <slot 95>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (<slot 6> `sold_date_sk` `cs_sold_date_sk` = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 21") && 
		explainStr.contains("output slot ids: 94 95 96 \n" + 
				"  |  hash output slot ids: 7 10 11 ") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON") 
            
        }
    }
}