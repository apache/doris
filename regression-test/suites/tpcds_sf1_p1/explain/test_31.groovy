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

suite("test_regression_test_tpcds_sf1_p1_q31", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  ss AS (
		   SELECT
		     ca_county
		   , d_qoy
		   , d_year
		   , sum(ss_ext_sales_price) store_sales
		   FROM
		     store_sales
		   , date_dim
		   , customer_address
		   WHERE (ss_sold_date_sk = d_date_sk)
		      AND (ss_addr_sk = ca_address_sk)
		   GROUP BY ca_county, d_qoy, d_year
		)
		, ws AS (
		   SELECT
		     ca_county
		   , d_qoy
		   , d_year
		   , sum(ws_ext_sales_price) web_sales
		   FROM
		     web_sales
		   , date_dim
		   , customer_address
		   WHERE (ws_sold_date_sk = d_date_sk)
		      AND (ws_bill_addr_sk = ca_address_sk)
		   GROUP BY ca_county, d_qoy, d_year
		)
		SELECT
		  ss1.ca_county
		, ss1.d_year
		, (ws2.web_sales / ws1.web_sales) web_q1_q2_increase
		, (ss2.store_sales / ss1.store_sales) store_q1_q2_increase
		, (ws3.web_sales / ws2.web_sales) web_q2_q3_increase
		, (ss3.store_sales / ss2.store_sales) store_q2_q3_increase
		FROM
		  ss ss1
		, ss ss2
		, ss ss3
		, ws ws1
		, ws ws2
		, ws ws3
		WHERE (ss1.d_qoy = 1)
		   AND (ss1.d_year = 2000)
		   AND (ss1.ca_county = ss2.ca_county)
		   AND (ss2.d_qoy = 2)
		   AND (ss2.d_year = 2000)
		   AND (ss2.ca_county = ss3.ca_county)
		   AND (ss3.d_qoy = 3)
		   AND (ss3.d_year = 2000)
		   AND (ss1.ca_county = ws1.ca_county)
		   AND (ws1.d_qoy = 1)
		   AND (ws1.d_year = 2000)
		   AND (ws1.ca_county = ws2.ca_county)
		   AND (ws2.d_qoy = 2)
		   AND (ws2.d_year = 2000)
		   AND (ws1.ca_county = ws3.ca_county)
		   AND (ws3.d_qoy = 3)
		   AND (ws3.d_year = 2000)
		   AND ((CASE WHEN (ws1.web_sales > 0) THEN (CAST(ws2.web_sales AS DECIMAL(27,3)) / ws1.web_sales) ELSE null END) > (CASE WHEN (ss1.store_sales > 0) THEN (CAST(ss2.store_sales AS DECIMAL(27,3)) / ss1.store_sales) ELSE null END))
		   AND ((CASE WHEN (ws2.web_sales > 0) THEN (CAST(ws3.web_sales AS DECIMAL(27,3)) / ws2.web_sales) ELSE null END) > (CASE WHEN (ss2.store_sales > 0) THEN (CAST(ss3.store_sales AS DECIMAL(27,3)) / ss2.store_sales) ELSE null END))
		ORDER BY ss1.ca_county ASC

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 96> `ss1`.`ca_county` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 218> = <slot 88> `ca_county`)\n" + 
				"  |  other predicates: ((CASE WHEN (<slot 386> > 0) THEN (CAST(<slot 390> AS DECIMAL(27,3)) / <slot 386>) ELSE NULL END) > (CASE WHEN (<slot 382> > 0) THEN (CAST(<slot 385> AS DECIMAL(27,3)) / <slot 382>) ELSE NULL END))") && 
		explainStr.contains("other predicates: ((CASE WHEN (<slot 386> > 0) THEN (CAST(<slot 390> AS DECIMAL(27,3)) / <slot 386>) ELSE NULL END) > (CASE WHEN (<slot 382> > 0) THEN (CAST(<slot 385> AS DECIMAL(27,3)) / <slot 382>) ELSE NULL END))") && 
		explainStr.contains("vec output tuple id: 47") && 
		explainStr.contains("output slot ids: 244 246 247 251 255 259 263 267 \n" + 
				"  |  hash output slot ids: 225 210 212 213 229 217 91 221 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 188> = <slot 72> `ca_county`)\n" + 
				"  |  other predicates: ((CASE WHEN (<slot 361> > 0) THEN (CAST(<slot 366> AS DECIMAL(27,3)) / <slot 361>) ELSE NULL END) > (CASE WHEN (<slot 358> > 0) THEN (CAST(<slot 359> AS DECIMAL(27,3)) / <slot 358>) ELSE NULL END))") && 
		explainStr.contains("other predicates: ((CASE WHEN (<slot 361> > 0) THEN (CAST(<slot 366> AS DECIMAL(27,3)) / <slot 361>) ELSE NULL END) > (CASE WHEN (<slot 358> > 0) THEN (CAST(<slot 359> AS DECIMAL(27,3)) / <slot 358>) ELSE NULL END))") && 
		explainStr.contains("vec output tuple id: 44") && 
		explainStr.contains("output slot ids: 210 212 213 217 218 221 225 229 \n" + 
				"  |  hash output slot ids: 195 180 182 183 187 75 188 191 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 158> = <slot 40> `ca_county`)") && 
		explainStr.contains("vec output tuple id: 41") && 
		explainStr.contains("output slot ids: 180 182 183 187 188 191 195 \n" + 
				"  |  hash output slot ids: 161 162 165 154 43 156 157 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 132> = <slot 56> `ca_county`)") && 
		explainStr.contains("vec output tuple id: 38") && 
		explainStr.contains("output slot ids: 154 156 157 158 161 162 165 \n" + 
				"  |  hash output slot ids: 132 134 135 136 56 139 59 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 8> `ca_county` = <slot 24> `ca_county`)") && 
		explainStr.contains("vec output tuple id: 35") && 
		explainStr.contains("output slot ids: 132 134 135 136 139 \n" + 
				"  |  hash output slot ids: 8 24 10 11 27 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 11> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 8> `ca_county`, <slot 9> `d_qoy`, <slot 10> `d_year`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 91> sum(`ws_ext_sales_price`))\n" + 
				"  |  group by: <slot 88> `ca_county`, <slot 89> `d_qoy`, <slot 90> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 236>)\n" + 
				"  |  group by: <slot 242>, <slot 239>, <slot 240>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 232> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 46") && 
		explainStr.contains("output slot ids: 236 239 240 242 \n" + 
				"  |  hash output slot ids: 80 230 233 234 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF005[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 45") && 
		explainStr.contains("output slot ids: 230 232 233 234 \n" + 
				"  |  hash output slot ids: 81 82 83 86 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF005[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_qoy` = 3), (`d_year` = 2000)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 75> sum(`ws_ext_sales_price`))\n" + 
				"  |  group by: <slot 72> `ca_county`, <slot 73> `d_qoy`, <slot 74> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 202>)\n" + 
				"  |  group by: <slot 208>, <slot 205>, <slot 206>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 198> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 43") && 
		explainStr.contains("output slot ids: 202 205 206 208 \n" + 
				"  |  hash output slot ids: 64 196 199 200 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF004[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 42") && 
		explainStr.contains("output slot ids: 196 198 199 200 \n" + 
				"  |  hash output slot ids: 65 66 67 70 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF004[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_qoy` = 2), (`d_year` = 2000)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 43> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 40> `ca_county`, <slot 41> `d_qoy`, <slot 42> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 172>)\n" + 
				"  |  group by: <slot 178>, <slot 175>, <slot 176>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 168> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 40") && 
		explainStr.contains("output slot ids: 172 175 176 178 \n" + 
				"  |  hash output slot ids: 32 166 169 170 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 39") && 
		explainStr.contains("output slot ids: 166 168 169 170 \n" + 
				"  |  hash output slot ids: 33 34 35 38 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_qoy` = 3), (`d_year` = 2000)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 59> sum(`ws_ext_sales_price`))\n" + 
				"  |  group by: <slot 56> `ca_county`, <slot 57> `d_qoy`, <slot 58> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 146>)\n" + 
				"  |  group by: <slot 152>, <slot 149>, <slot 150>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 142> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 37") && 
		explainStr.contains("output slot ids: 146 149 150 152 \n" + 
				"  |  hash output slot ids: 144 48 140 143 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 36") && 
		explainStr.contains("output slot ids: 140 142 143 144 \n" + 
				"  |  hash output slot ids: 49 50 51 54 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ws_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_qoy` = 1), (`d_year` = 2000)") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 27> sum(`ss_ext_sales_price`))\n" + 
				"  |  group by: <slot 24> `ca_county`, <slot 25> `d_qoy`, <slot 26> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 124>)\n" + 
				"  |  group by: <slot 130>, <slot 127>, <slot 128>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 120> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 34") && 
		explainStr.contains("output slot ids: 124 127 128 130 \n" + 
				"  |  hash output slot ids: 16 118 121 122 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 33") && 
		explainStr.contains("output slot ids: 118 120 121 122 \n" + 
				"  |  hash output slot ids: 17 18 19 22 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_qoy` = 2), (`d_year` = 2000)") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 110>)\n" + 
				"  |  group by: <slot 116>, <slot 113>, <slot 114>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 106> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 32") && 
		explainStr.contains("output slot ids: 110 113 114 116 \n" + 
				"  |  hash output slot ids: 0 104 107 108 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_sold_date_sk` = `d_date_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `d_date_sk`") && 
		explainStr.contains("vec output tuple id: 31") && 
		explainStr.contains("output slot ids: 104 106 107 108 \n" + 
				"  |  hash output slot ids: 1 2 3 6 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_sold_date_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_qoy` = 1), (`d_year` = 2000)") 
            
        }
    }
}