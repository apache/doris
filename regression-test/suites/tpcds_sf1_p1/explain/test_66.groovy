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

suite("test_regression_test_tpcds_sf1_p1_q66", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  w_warehouse_name
		, w_warehouse_sq_ft
		, w_city
		, w_county
		, w_state
		, w_country
		, ship_carriers
		, year
		, sum(jan_sales) jan_sales
		, sum(feb_sales) feb_sales
		, sum(mar_sales) mar_sales
		, sum(apr_sales) apr_sales
		, sum(may_sales) may_sales
		, sum(jun_sales) jun_sales
		, sum(jul_sales) jul_sales
		, sum(aug_sales) aug_sales
		, sum(sep_sales) sep_sales
		, sum(oct_sales) oct_sales
		, sum(nov_sales) nov_sales
		, sum(dec_sales) dec_sales
		, sum((jan_sales / w_warehouse_sq_ft)) jan_sales_per_sq_foot
		, sum((feb_sales / w_warehouse_sq_ft)) feb_sales_per_sq_foot
		, sum((mar_sales / w_warehouse_sq_ft)) mar_sales_per_sq_foot
		, sum((apr_sales / w_warehouse_sq_ft)) apr_sales_per_sq_foot
		, sum((may_sales / w_warehouse_sq_ft)) may_sales_per_sq_foot
		, sum((jun_sales / w_warehouse_sq_ft)) jun_sales_per_sq_foot
		, sum((jul_sales / w_warehouse_sq_ft)) jul_sales_per_sq_foot
		, sum((aug_sales / w_warehouse_sq_ft)) aug_sales_per_sq_foot
		, sum((sep_sales / w_warehouse_sq_ft)) sep_sales_per_sq_foot
		, sum((oct_sales / w_warehouse_sq_ft)) oct_sales_per_sq_foot
		, sum((nov_sales / w_warehouse_sq_ft)) nov_sales_per_sq_foot
		, sum((dec_sales / w_warehouse_sq_ft)) dec_sales_per_sq_foot
		, sum(jan_net) jan_net
		, sum(feb_net) feb_net
		, sum(mar_net) mar_net
		, sum(apr_net) apr_net
		, sum(may_net) may_net
		, sum(jun_net) jun_net
		, sum(jul_net) jul_net
		, sum(aug_net) aug_net
		, sum(sep_net) sep_net
		, sum(oct_net) oct_net
		, sum(nov_net) nov_net
		, sum(dec_net) dec_net
		FROM
		(
		      SELECT
		        w_warehouse_name
		      , w_warehouse_sq_ft
		      , w_city
		      , w_county
		      , w_state
		      , w_country
		      , concat(concat('DHL', ','), 'BARIAN') ship_carriers
		      , d_year YEAR
		      , sum((CASE WHEN (d_moy = 1) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) jan_sales
		      , sum((CASE WHEN (d_moy = 2) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) feb_sales
		      , sum((CASE WHEN (d_moy = 3) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) mar_sales
		      , sum((CASE WHEN (d_moy = 4) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) apr_sales
		      , sum((CASE WHEN (d_moy = 5) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) may_sales
		      , sum((CASE WHEN (d_moy = 6) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) jun_sales
		      , sum((CASE WHEN (d_moy = 7) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) jul_sales
		      , sum((CASE WHEN (d_moy = 8) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) aug_sales
		      , sum((CASE WHEN (d_moy = 9) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) sep_sales
		      , sum((CASE WHEN (d_moy = 10) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) oct_sales
		      , sum((CASE WHEN (d_moy = 11) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) nov_sales
		      , sum((CASE WHEN (d_moy = 12) THEN (ws_ext_sales_price * ws_quantity) ELSE 0 END)) dec_sales
		      , sum((CASE WHEN (d_moy = 1) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) jan_net
		      , sum((CASE WHEN (d_moy = 2) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) feb_net
		      , sum((CASE WHEN (d_moy = 3) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) mar_net
		      , sum((CASE WHEN (d_moy = 4) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) apr_net
		      , sum((CASE WHEN (d_moy = 5) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) may_net
		      , sum((CASE WHEN (d_moy = 6) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) jun_net
		      , sum((CASE WHEN (d_moy = 7) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) jul_net
		      , sum((CASE WHEN (d_moy = 8) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) aug_net
		      , sum((CASE WHEN (d_moy = 9) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) sep_net
		      , sum((CASE WHEN (d_moy = 10) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) oct_net
		      , sum((CASE WHEN (d_moy = 11) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) nov_net
		      , sum((CASE WHEN (d_moy = 12) THEN (ws_net_paid * ws_quantity) ELSE 0 END)) dec_net
		      FROM
		        web_sales
		      , warehouse
		      , date_dim
		      , time_dim
		      , ship_mode
		      WHERE (ws_warehouse_sk = w_warehouse_sk)
		         AND (ws_sold_date_sk = d_date_sk)
		         AND (ws_sold_time_sk = t_time_sk)
		         AND (ws_ship_mode_sk = sm_ship_mode_sk)
		         AND (d_year = 2001)
		         AND (t_time BETWEEN 30838 AND (30838 + 28800))
		         AND (sm_carrier IN ('DHL'      , 'BARIAN'))
		      GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
		   UNION ALL
		      SELECT
		        w_warehouse_name
		      , w_warehouse_sq_ft
		      , w_city
		      , w_county
		      , w_state
		      , w_country
		      , concat(concat('DHL', ','), 'BARIAN') ship_carriers
		      , d_year YEAR
		      , sum((CASE WHEN (d_moy = 1) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) jan_sales
		      , sum((CASE WHEN (d_moy = 2) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) feb_sales
		      , sum((CASE WHEN (d_moy = 3) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) mar_sales
		      , sum((CASE WHEN (d_moy = 4) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) apr_sales
		      , sum((CASE WHEN (d_moy = 5) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) may_sales
		      , sum((CASE WHEN (d_moy = 6) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) jun_sales
		      , sum((CASE WHEN (d_moy = 7) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) jul_sales
		      , sum((CASE WHEN (d_moy = 8) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) aug_sales
		      , sum((CASE WHEN (d_moy = 9) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) sep_sales
		      , sum((CASE WHEN (d_moy = 10) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) oct_sales
		      , sum((CASE WHEN (d_moy = 11) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) nov_sales
		      , sum((CASE WHEN (d_moy = 12) THEN (cs_sales_price * cs_quantity) ELSE 0 END)) dec_sales
		      , sum((CASE WHEN (d_moy = 1) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) jan_net
		      , sum((CASE WHEN (d_moy = 2) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) feb_net
		      , sum((CASE WHEN (d_moy = 3) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) mar_net
		      , sum((CASE WHEN (d_moy = 4) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) apr_net
		      , sum((CASE WHEN (d_moy = 5) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) may_net
		      , sum((CASE WHEN (d_moy = 6) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) jun_net
		      , sum((CASE WHEN (d_moy = 7) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) jul_net
		      , sum((CASE WHEN (d_moy = 8) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) aug_net
		      , sum((CASE WHEN (d_moy = 9) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) sep_net
		      , sum((CASE WHEN (d_moy = 10) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) oct_net
		      , sum((CASE WHEN (d_moy = 11) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) nov_net
		      , sum((CASE WHEN (d_moy = 12) THEN (cs_net_paid_inc_tax * cs_quantity) ELSE 0 END)) dec_net
		      FROM
		        catalog_sales
		      , warehouse
		      , date_dim
		      , time_dim
		      , ship_mode
		      WHERE (cs_warehouse_sk = w_warehouse_sk)
		         AND (cs_sold_date_sk = d_date_sk)
		         AND (cs_sold_time_sk = t_time_sk)
		         AND (cs_ship_mode_sk = sm_ship_mode_sk)
		         AND (d_year = 2001)
		         AND (t_time BETWEEN 30838 AND (30838 + 28800))
		         AND (sm_carrier IN ('DHL'      , 'BARIAN'))
		      GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year
		   )  x
		GROUP BY w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, ship_carriers, year
		ORDER BY w_warehouse_name ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 212> <slot 168> `w_warehouse_name` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 176> sum(`jan_sales`)), sum(<slot 177> sum(`feb_sales`)), sum(<slot 178> sum(`mar_sales`)), sum(<slot 179> sum(`apr_sales`)), sum(<slot 180> sum(`may_sales`)), sum(<slot 181> sum(`jun_sales`)), sum(<slot 182> sum(`jul_sales`)), sum(<slot 183> sum(`aug_sales`)), sum(<slot 184> sum(`sep_sales`)), sum(<slot 185> sum(`oct_sales`)), sum(<slot 186> sum(`nov_sales`)), sum(<slot 187> sum(`dec_sales`)), sum(<slot 188> sum((`jan_sales` / `w_warehouse_sq_ft`))), sum(<slot 189> sum((`feb_sales` / `w_warehouse_sq_ft`))), sum(<slot 190> sum((`mar_sales` / `w_warehouse_sq_ft`))), sum(<slot 191> sum((`apr_sales` / `w_warehouse_sq_ft`))), sum(<slot 192> sum((`may_sales` / `w_warehouse_sq_ft`))), sum(<slot 193> sum((`jun_sales` / `w_warehouse_sq_ft`))), sum(<slot 194> sum((`jul_sales` / `w_warehouse_sq_ft`))), sum(<slot 195> sum((`aug_sales` / `w_warehouse_sq_ft`))), sum(<slot 196> sum((`sep_sales` / `w_warehouse_sq_ft`))), sum(<slot 197> sum((`oct_sales` / `w_warehouse_sq_ft`))), sum(<slot 198> sum((`nov_sales` / `w_warehouse_sq_ft`))), sum(<slot 199> sum((`dec_sales` / `w_warehouse_sq_ft`))), sum(<slot 200> sum(`jan_net`)), sum(<slot 201> sum(`feb_net`)), sum(<slot 202> sum(`mar_net`)), sum(<slot 203> sum(`apr_net`)), sum(<slot 204> sum(`may_net`)), sum(<slot 205> sum(`jun_net`)), sum(<slot 206> sum(`jul_net`)), sum(<slot 207> sum(`aug_net`)), sum(<slot 208> sum(`sep_net`)), sum(<slot 209> sum(`oct_net`)), sum(<slot 210> sum(`nov_net`)), sum(<slot 211> sum(`dec_net`))\n" + 
				"  |  group by: <slot 168> `w_warehouse_name`, <slot 169> `w_warehouse_sq_ft`, <slot 170> `w_city`, <slot 171> `w_county`, <slot 172> `w_state`, <slot 173> `w_country`, <slot 174> `ship_carriers`, <slot 175> `year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 112> <slot 28> sum((CASE WHEN (`d_moy` = 1) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 80> sum((CASE WHEN (`d_moy` = 1) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 113> <slot 29> sum((CASE WHEN (`d_moy` = 2) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 81> sum((CASE WHEN (`d_moy` = 2) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 114> <slot 30> sum((CASE WHEN (`d_moy` = 3) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 82> sum((CASE WHEN (`d_moy` = 3) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 115> <slot 31> sum((CASE WHEN (`d_moy` = 4) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 83> sum((CASE WHEN (`d_moy` = 4) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 116> <slot 32> sum((CASE WHEN (`d_moy` = 5) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 84> sum((CASE WHEN (`d_moy` = 5) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 117> <slot 33> sum((CASE WHEN (`d_moy` = 6) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 85> sum((CASE WHEN (`d_moy` = 6) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 118> <slot 34> sum((CASE WHEN (`d_moy` = 7) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 86> sum((CASE WHEN (`d_moy` = 7) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 119> <slot 35> sum((CASE WHEN (`d_moy` = 8) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 87> sum((CASE WHEN (`d_moy` = 8) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 120> <slot 36> sum((CASE WHEN (`d_moy` = 9) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 88> sum((CASE WHEN (`d_moy` = 9) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 121> <slot 37> sum((CASE WHEN (`d_moy` = 10) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 89> sum((CASE WHEN (`d_moy` = 10) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 122> <slot 38> sum((CASE WHEN (`d_moy` = 11) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 90> sum((CASE WHEN (`d_moy` = 11) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 123> <slot 39> sum((CASE WHEN (`d_moy` = 12) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 91> sum((CASE WHEN (`d_moy` = 12) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum((<slot 112> <slot 28> sum((CASE WHEN (`d_moy` = 1) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 80> sum((CASE WHEN (`d_moy` = 1) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 113> <slot 29> sum((CASE WHEN (`d_moy` = 2) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 81> sum((CASE WHEN (`d_moy` = 2) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 114> <slot 30> sum((CASE WHEN (`d_moy` = 3) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 82> sum((CASE WHEN (`d_moy` = 3) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 115> <slot 31> sum((CASE WHEN (`d_moy` = 4) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 83> sum((CASE WHEN (`d_moy` = 4) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 116> <slot 32> sum((CASE WHEN (`d_moy` = 5) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 84> sum((CASE WHEN (`d_moy` = 5) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 117> <slot 33> sum((CASE WHEN (`d_moy` = 6) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 85> sum((CASE WHEN (`d_moy` = 6) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 118> <slot 34> sum((CASE WHEN (`d_moy` = 7) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 86> sum((CASE WHEN (`d_moy` = 7) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 119> <slot 35> sum((CASE WHEN (`d_moy` = 8) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 87> sum((CASE WHEN (`d_moy` = 8) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 120> <slot 36> sum((CASE WHEN (`d_moy` = 9) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 88> sum((CASE WHEN (`d_moy` = 9) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 121> <slot 37> sum((CASE WHEN (`d_moy` = 10) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 89> sum((CASE WHEN (`d_moy` = 10) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 122> <slot 38> sum((CASE WHEN (`d_moy` = 11) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 90> sum((CASE WHEN (`d_moy` = 11) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum((<slot 123> <slot 39> sum((CASE WHEN (`d_moy` = 12) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END)) <slot 91> sum((CASE WHEN (`d_moy` = 12) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END)) / <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`)), sum(<slot 124> <slot 40> sum((CASE WHEN (`d_moy` = 1) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 92> sum((CASE WHEN (`d_moy` = 1) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 125> <slot 41> sum((CASE WHEN (`d_moy` = 2) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 93> sum((CASE WHEN (`d_moy` = 2) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 126> <slot 42> sum((CASE WHEN (`d_moy` = 3) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 94> sum((CASE WHEN (`d_moy` = 3) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 127> <slot 43> sum((CASE WHEN (`d_moy` = 4) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 95> sum((CASE WHEN (`d_moy` = 4) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 128> <slot 44> sum((CASE WHEN (`d_moy` = 5) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 96> sum((CASE WHEN (`d_moy` = 5) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 129> <slot 45> sum((CASE WHEN (`d_moy` = 6) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 97> sum((CASE WHEN (`d_moy` = 6) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 130> <slot 46> sum((CASE WHEN (`d_moy` = 7) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 98> sum((CASE WHEN (`d_moy` = 7) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 131> <slot 47> sum((CASE WHEN (`d_moy` = 8) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 99> sum((CASE WHEN (`d_moy` = 8) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 132> <slot 48> sum((CASE WHEN (`d_moy` = 9) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 100> sum((CASE WHEN (`d_moy` = 9) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 133> <slot 49> sum((CASE WHEN (`d_moy` = 10) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 101> sum((CASE WHEN (`d_moy` = 10) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 134> <slot 50> sum((CASE WHEN (`d_moy` = 11) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 102> sum((CASE WHEN (`d_moy` = 11) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 135> <slot 51> sum((CASE WHEN (`d_moy` = 12) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)) <slot 103> sum((CASE WHEN (`d_moy` = 12) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END)))\n" + 
				"  |  group by: <slot 104> <slot 21> `w_warehouse_name` <slot 73> `w_warehouse_name`, <slot 105> <slot 22> `w_warehouse_sq_ft` <slot 74> `w_warehouse_sq_ft`, <slot 106> <slot 23> `w_city` <slot 75> `w_city`, <slot 107> <slot 24> `w_county` <slot 76> `w_county`, <slot 108> <slot 25> `w_state` <slot 77> `w_state`, <slot 109> <slot 26> `w_country` <slot 78> `w_country`, <slot 110> 'DHL,BARIAN' 'DHL,BARIAN', <slot 111> <slot 27> `d_year` <slot 79> `d_year`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 80> sum((CASE WHEN (`d_moy` = 1) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 81> sum((CASE WHEN (`d_moy` = 2) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 82> sum((CASE WHEN (`d_moy` = 3) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 83> sum((CASE WHEN (`d_moy` = 4) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 84> sum((CASE WHEN (`d_moy` = 5) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 85> sum((CASE WHEN (`d_moy` = 6) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 86> sum((CASE WHEN (`d_moy` = 7) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 87> sum((CASE WHEN (`d_moy` = 8) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 88> sum((CASE WHEN (`d_moy` = 9) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 89> sum((CASE WHEN (`d_moy` = 10) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 90> sum((CASE WHEN (`d_moy` = 11) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 91> sum((CASE WHEN (`d_moy` = 12) THEN (`cs_sales_price` * `cs_quantity`) ELSE 0 END))), sum(<slot 92> sum((CASE WHEN (`d_moy` = 1) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 93> sum((CASE WHEN (`d_moy` = 2) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 94> sum((CASE WHEN (`d_moy` = 3) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 95> sum((CASE WHEN (`d_moy` = 4) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 96> sum((CASE WHEN (`d_moy` = 5) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 97> sum((CASE WHEN (`d_moy` = 6) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 98> sum((CASE WHEN (`d_moy` = 7) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 99> sum((CASE WHEN (`d_moy` = 8) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 100> sum((CASE WHEN (`d_moy` = 9) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 101> sum((CASE WHEN (`d_moy` = 10) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 102> sum((CASE WHEN (`d_moy` = 11) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END))), sum(<slot 103> sum((CASE WHEN (`d_moy` = 12) THEN (`cs_net_paid_inc_tax` * `cs_quantity`) ELSE 0 END)))\n" + 
				"  |  group by: <slot 73> `w_warehouse_name`, <slot 74> `w_warehouse_sq_ft`, <slot 75> `w_city`, <slot 76> `w_county`, <slot 77> `w_state`, <slot 78> `w_country`, <slot 79> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (<slot 392> = 1) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 2) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 3) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 4) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 5) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 6) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 7) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 8) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 9) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 10) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 11) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 12) THEN (<slot 377> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 1) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 2) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 3) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 4) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 5) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 6) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 7) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 8) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 9) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 10) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 11) THEN (<slot 379> * <slot 378>) ELSE 0 END)), sum((CASE WHEN (<slot 392> = 12) THEN (<slot 379> * <slot 378>) ELSE 0 END))\n" + 
				"  |  group by: <slot 384>, <slot 385>, <slot 386>, <slot 387>, <slot 388>, <slot 389>, <slot 391>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 364> = `sm_ship_mode_sk`)") && 
		explainStr.contains("vec output tuple id: 23") && 
		explainStr.contains("output slot ids: 377 378 379 384 385 386 387 388 389 391 392 \n" + 
				"  |  hash output slot ids: 368 369 370 372 373 358 359 360 365 366 367 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 346> = `t_time_sk`)") && 
		explainStr.contains("vec output tuple id: 22") && 
		explainStr.contains("output slot ids: 358 359 360 364 365 366 367 368 369 370 372 373 \n" + 
				"  |  hash output slot ids: 352 353 355 356 341 342 343 347 348 349 350 351 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 331> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 21") && 
		explainStr.contains("output slot ids: 341 342 343 346 347 348 349 350 351 352 353 355 356 \n" + 
				"  |  hash output slot ids: 327 328 329 332 333 334 335 336 337 338 339 58 59 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cs_warehouse_sk` = `w_warehouse_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `w_warehouse_sk`") && 
		explainStr.contains("vec output tuple id: 20") && 
		explainStr.contains("output slot ids: 327 328 329 331 332 333 334 335 336 337 338 339 \n" + 
				"  |  hash output slot ids: 65 67 52 69 53 54 55 56 57 60 61 62 ") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `cs_warehouse_sk`") && 
		explainStr.contains("TABLE: ship_mode(ship_mode), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`sm_carrier` IN ('DHL', 'BARIAN'))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `t_time` >= 30838, `t_time` <= 59638") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: warehouse(warehouse), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 28> sum((CASE WHEN (`d_moy` = 1) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 29> sum((CASE WHEN (`d_moy` = 2) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 30> sum((CASE WHEN (`d_moy` = 3) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 31> sum((CASE WHEN (`d_moy` = 4) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 32> sum((CASE WHEN (`d_moy` = 5) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 33> sum((CASE WHEN (`d_moy` = 6) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 34> sum((CASE WHEN (`d_moy` = 7) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 35> sum((CASE WHEN (`d_moy` = 8) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 36> sum((CASE WHEN (`d_moy` = 9) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 37> sum((CASE WHEN (`d_moy` = 10) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 38> sum((CASE WHEN (`d_moy` = 11) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 39> sum((CASE WHEN (`d_moy` = 12) THEN (`ws_ext_sales_price` * `ws_quantity`) ELSE 0 END))), sum(<slot 40> sum((CASE WHEN (`d_moy` = 1) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 41> sum((CASE WHEN (`d_moy` = 2) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 42> sum((CASE WHEN (`d_moy` = 3) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 43> sum((CASE WHEN (`d_moy` = 4) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 44> sum((CASE WHEN (`d_moy` = 5) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 45> sum((CASE WHEN (`d_moy` = 6) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 46> sum((CASE WHEN (`d_moy` = 7) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 47> sum((CASE WHEN (`d_moy` = 8) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 48> sum((CASE WHEN (`d_moy` = 9) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 49> sum((CASE WHEN (`d_moy` = 10) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 50> sum((CASE WHEN (`d_moy` = 11) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END))), sum(<slot 51> sum((CASE WHEN (`d_moy` = 12) THEN (`ws_net_paid` * `ws_quantity`) ELSE 0 END)))\n" + 
				"  |  group by: <slot 21> `w_warehouse_name`, <slot 22> `w_warehouse_sq_ft`, <slot 23> `w_city`, <slot 24> `w_county`, <slot 25> `w_state`, <slot 26> `w_country`, <slot 27> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum((CASE WHEN (<slot 321> = 1) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 2) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 3) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 4) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 5) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 6) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 7) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 8) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 9) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 10) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 11) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 12) THEN (<slot 306> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 1) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 2) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 3) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 4) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 5) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 6) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 7) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 8) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 9) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 10) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 11) THEN (<slot 308> * <slot 307>) ELSE 0 END)), sum((CASE WHEN (<slot 321> = 12) THEN (<slot 308> * <slot 307>) ELSE 0 END))\n" + 
				"  |  group by: <slot 313>, <slot 314>, <slot 315>, <slot 316>, <slot 317>, <slot 318>, <slot 320>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 293> = `sm_ship_mode_sk`)") && 
		explainStr.contains("vec output tuple id: 19") && 
		explainStr.contains("output slot ids: 306 307 308 313 314 315 316 317 318 320 321 \n" + 
				"  |  hash output slot ids: 288 289 294 295 296 297 298 299 301 302 287 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 275> = `t_time_sk`)") && 
		explainStr.contains("vec output tuple id: 18") && 
		explainStr.contains("output slot ids: 287 288 289 293 294 295 296 297 298 299 301 302 \n" + 
				"  |  hash output slot ids: 272 276 277 278 279 280 281 282 284 285 270 271 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 260> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 17") && 
		explainStr.contains("output slot ids: 270 271 272 275 276 277 278 279 280 281 282 284 285 \n" + 
				"  |  hash output slot ids: 256 257 258 261 262 6 263 7 264 265 266 267 268 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_warehouse_sk` = `w_warehouse_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `w_warehouse_sk`") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 256 257 258 260 261 262 263 264 265 266 267 268 \n" + 
				"  |  hash output slot ids: 0 17 1 2 3 4 5 8 9 10 13 15 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ws_warehouse_sk`") && 
		explainStr.contains("TABLE: ship_mode(ship_mode), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`sm_carrier` IN ('DHL', 'BARIAN'))") && 
		explainStr.contains("TABLE: time_dim(time_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `t_time` >= 30838, `t_time` <= 59638") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 2001)") && 
		explainStr.contains("TABLE: warehouse(warehouse), PREAGGREGATION: ON") 
            
        }
    }
}