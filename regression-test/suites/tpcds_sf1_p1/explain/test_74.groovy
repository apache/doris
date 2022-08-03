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

suite("test_regression_test_tpcds_sf1_p1_q74", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  year_total AS (
		   SELECT
		     c_customer_id customer_id
		   , c_first_name customer_first_name
		   , c_last_name customer_last_name
		   , d_year YEAR
		   , sum(ss_net_paid) year_total
		   , 's' sale_type
		   FROM
		     customer
		   , store_sales
		   , date_dim
		   WHERE (c_customer_sk = ss_customer_sk)
		      AND (ss_sold_date_sk = d_date_sk)
		      AND (d_year IN (2001   , (2001 + 1)))
		   GROUP BY c_customer_id, c_first_name, c_last_name, d_year
		UNION ALL    SELECT
		     c_customer_id customer_id
		   , c_first_name customer_first_name
		   , c_last_name customer_last_name
		   , d_year YEAR
		   , sum(ws_net_paid) year_total
		   , 'w' sale_type
		   FROM
		     customer
		   , web_sales
		   , date_dim
		   WHERE (c_customer_sk = ws_bill_customer_sk)
		      AND (ws_sold_date_sk = d_date_sk)
		      AND (d_year IN (2001   , (2001 + 1)))
		   GROUP BY c_customer_id, c_first_name, c_last_name, d_year
		)
		SELECT
		  t_s_secyear.customer_id
		, t_s_secyear.customer_first_name
		, t_s_secyear.customer_last_name
		FROM
		  year_total t_s_firstyear
		, year_total t_s_secyear
		, year_total t_w_firstyear
		, year_total t_w_secyear
		WHERE (t_s_secyear.customer_id = t_s_firstyear.customer_id)
		   AND (t_s_firstyear.customer_id = t_w_secyear.customer_id)
		   AND (t_s_firstyear.customer_id = t_w_firstyear.customer_id)
		   AND (t_s_firstyear.sale_type = 's')
		   AND (t_w_firstyear.sale_type = 'w')
		   AND (t_s_secyear.sale_type = 's')
		   AND (t_w_secyear.sale_type = 'w')
		   AND (t_s_firstyear.year = 2001)
		   AND (t_s_secyear.year = (2001 + 1))
		   AND (t_w_firstyear.year = 2001)
		   AND (t_w_secyear.year = (2001 + 1))
		   AND (t_s_firstyear.year_total > 0)
		   AND (t_w_firstyear.year_total > 0)
		   AND ((CASE WHEN (t_w_firstyear.year_total > 0) THEN (t_w_secyear.year_total / t_w_firstyear.year_total) ELSE null END) > (CASE WHEN (t_s_firstyear.year_total > 0) THEN (t_s_secyear.year_total / t_s_firstyear.year_total) ELSE null END))
		ORDER BY 1 ASC, 1 ASC, 1 ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 160> `t_s_secyear`.`customer_id` ASC, <slot 160> `t_s_secyear`.`customer_id` ASC, <slot 160> `t_s_secyear`.`customer_id` ASC") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 223> = <slot 148> <slot 129> `c_customer_id` <slot 143> `c_customer_id`)\n" + 
				"  |  other predicates: ((CASE WHEN (<slot 356> > 0) THEN (<slot 353> / <slot 356>) ELSE NULL END) > (CASE WHEN (<slot 348> > 0) THEN (<slot 352> / <slot 348>) ELSE NULL END))") && 
		explainStr.contains("other predicates: ((CASE WHEN (<slot 356> > 0) THEN (<slot 353> / <slot 356>) ELSE NULL END) > (CASE WHEN (<slot 348> > 0) THEN (<slot 352> / <slot 348>) ELSE NULL END))") && 
		explainStr.contains("vec output tuple id: 51") && 
		explainStr.contains("output slot ids: 257 258 259 \n" + 
				"  |  hash output slot ids: 225 227 228 229 231 152 235 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 197> = <slot 108> <slot 89> `c_customer_id` <slot 103> `c_customer_id`)") && 
		explainStr.contains("vec output tuple id: 48") && 
		explainStr.contains("output slot ids: 223 225 227 228 229 231 235 \n" + 
				"  |  hash output slot ids: 112 197 199 201 202 203 205 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Src column hash been transformed by expr]\n" + 
				"  |  equal join conjunct: (<slot 28> <slot 9> `c_customer_id` <slot 23> `c_customer_id` = <slot 68> <slot 49> `c_customer_id` <slot 63> `c_customer_id`)") && 
		explainStr.contains("vec output tuple id: 45") && 
		explainStr.contains("output slot ids: 197 199 201 202 203 205 \n" + 
				"  |  hash output slot ids: 32 68 69 70 72 28 ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 147> sum(`ws_net_paid`))\n" + 
				"  |  group by: <slot 143> `c_customer_id`, <slot 144> `c_first_name`, <slot 145> `c_last_name`, <slot 146> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 244>)\n" + 
				"  |  group by: <slot 247>, <slot 248>, <slot 249>, <slot 251>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 239> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 50") && 
		explainStr.contains("output slot ids: 244 247 248 249 251 \n" + 
				"  |  hash output slot ids: 240 241 242 137 237 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_bill_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF003[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 49") && 
		explainStr.contains("output slot ids: 237 239 240 241 242 \n" + 
				"  |  hash output slot ids: 134 135 136 138 141 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF003[in_or_bloom] -> `ws_bill_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2001, 2002)), (`d_year` = 2001)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 107> sum(`ws_net_paid`))\n" + 
				"  |  group by: <slot 103> `c_customer_id`, <slot 104> `c_first_name`, <slot 105> `c_last_name`, <slot 106> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 214>)\n" + 
				"  |  group by: <slot 217>, <slot 218>, <slot 219>, <slot 221>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 209> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 47") && 
		explainStr.contains("output slot ids: 214 217 218 219 221 \n" + 
				"  |  hash output slot ids: 97 210 211 212 207 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ws_bill_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 46") && 
		explainStr.contains("output slot ids: 207 209 210 211 212 \n" + 
				"  |  hash output slot ids: 96 98 101 94 95 ") && 
		explainStr.contains("TABLE: web_sales(web_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ws_bill_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2001, 2002)), (`d_year` = 2002)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 53> sum(`ss_net_paid`))\n" + 
				"  |  group by: <slot 49> `c_customer_id`, <slot 50> `c_first_name`, <slot 51> `c_last_name`, <slot 52> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 188>)\n" + 
				"  |  group by: <slot 191>, <slot 192>, <slot 193>, <slot 195>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 183> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 44") && 
		explainStr.contains("output slot ids: 188 191 192 193 195 \n" + 
				"  |  hash output slot ids: 181 184 185 186 43 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF001[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 43") && 
		explainStr.contains("output slot ids: 181 183 184 185 186 \n" + 
				"  |  hash output slot ids: 40 41 42 44 47 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF001[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2001, 2002)), (`d_year` = 2002)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 13> sum(`ss_net_paid`))\n" + 
				"  |  group by: <slot 9> `c_customer_id`, <slot 10> `c_first_name`, <slot 11> `c_last_name`, <slot 12> `d_year`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 172>)\n" + 
				"  |  group by: <slot 175>, <slot 176>, <slot 177>, <slot 179>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 167> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 42") && 
		explainStr.contains("output slot ids: 172 175 176 177 179 \n" + 
				"  |  hash output slot ids: 3 165 168 169 170 ") && 
		explainStr.contains("join op: INNER JOIN(BUCKET_SHUFFLE)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_customer_sk` = `c_customer_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `c_customer_sk`") && 
		explainStr.contains("vec output tuple id: 41") && 
		explainStr.contains("output slot ids: 165 167 168 169 170 \n" + 
				"  |  hash output slot ids: 0 1 2 4 7 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_customer_sk`") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` IN (2001, 2002)), (`d_year` = 2001)") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") 
            
        }
    }
}