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

suite("test_regression_test_tpcds_sf1_p1_q24", "regression_test_tpcds_sf1_p1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		WITH
		  ssales AS (
		   SELECT
		     c_last_name
		   , c_first_name
		   , s_store_name
		   , ca_state
		   , s_state
		   , i_color
		   , i_current_price
		   , i_manager_id
		   , i_units
		   , i_size
		   , sum(ss_net_paid) netpaid
		   FROM
		     store_sales
		   , store_returns
		   , store
		   , item
		   , customer
		   , customer_address
		   WHERE (ss_ticket_number = sr_ticket_number)
		      AND (ss_item_sk = sr_item_sk)
		      AND (ss_customer_sk = c_customer_sk)
		      AND (ss_item_sk = i_item_sk)
		      AND (ss_store_sk = s_store_sk)
		      AND (c_birth_country = upper(ca_country))
		      AND (s_zip = ca_zip)
		      AND (s_market_id = 8)
		   GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size
		)
		SELECT
		  c_last_name
		, c_first_name
		, s_store_name
		, sum(netpaid) paid
		FROM
		  ssales
		WHERE (i_color = 'pale')
		GROUP BY c_last_name, c_first_name, s_store_name
		HAVING (sum(netpaid) > (
		      SELECT (CAST('0.05' AS DECIMAL) * avg(netpaid))
		      FROM
		        ssales
		   ))
		ORDER BY c_last_name, c_first_name, s_store_name

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 105> `\$a\$1`.`\$c\$1` ASC, <slot 106> `\$a\$1`.`\$c\$2` ASC, <slot 107> `\$a\$1`.`\$c\$3` ASC") && 
		explainStr.contains("cross join:\n" + 
				"  |  predicates: (<slot 50> sum(`netpaid`) > (0.05 * <slot 103> avg(`netpaid`)))") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 50> sum(`netpaid`))\n" + 
				"  |  group by: <slot 47> `c_last_name`, <slot 48> `c_first_name`, <slot 49> `s_store_name`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 102> avg(`netpaid`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: avg(<slot 90> sum(`ss_net_paid`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 90> sum(`ss_net_paid`))\n" + 
				"  |  group by: <slot 80> `c_last_name`, <slot 81> `c_first_name`, <slot 82> `s_store_name`, <slot 83> `ca_state`, <slot 84> `s_state`, <slot 85> `i_color`, <slot 86> `i_current_price`, <slot 87> `i_manager_id`, <slot 88> `i_units`, <slot 89> `i_size`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 248>)\n" + 
				"  |  group by: <slot 255>, <slot 256>, <slot 265>, <slot 270>, <slot 266>, <slot 259>, <slot 260>, <slot 261>, <slot 262>, <slot 263>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 236> = upper(`ca_country`))\n" + 
				"  |  equal join conjunct: (<slot 246> = `ca_zip`)") && 
		explainStr.contains("vec output tuple id: 31") && 
		explainStr.contains("output slot ids: 248 255 256 259 260 261 262 263 265 266 270 \n" + 
				"  |  hash output slot ids: 240 241 226 243 244 233 234 58 237 238 239 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 213> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 30") && 
		explainStr.contains("output slot ids: 226 233 234 236 237 238 239 240 241 243 244 246 \n" + 
				"  |  hash output slot ids: 224 209 216 217 57 219 59 220 221 77 222 223 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 200> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 29") && 
		explainStr.contains("output slot ids: 209 213 216 217 219 220 221 222 223 224 \n" + 
				"  |  hash output slot ids: 208 64 198 202 60 205 61 206 62 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 194> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 28") && 
		explainStr.contains("output slot ids: 198 200 202 205 206 208 \n" + 
				"  |  hash output slot ids: 193 195 55 56 75 191 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_ticket_number` = `sr_ticket_number`)\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `sr_item_sk`)\n" + 
				"  |  runtime filters: RF002[in_or_bloom] <- `sr_ticket_number`, RF003[in_or_bloom] <- `sr_item_sk`") && 
		explainStr.contains("vec output tuple id: 27") && 
		explainStr.contains("output slot ids: 191 193 194 195 \n" + 
				"  |  hash output slot ids: 65 68 70 73 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF002[in_or_bloom] -> `ss_ticket_number`, RF003[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`s_market_id` = 8)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 35> sum(`ss_net_paid`))\n" + 
				"  |  group by: <slot 25> `c_last_name`, <slot 26> `c_first_name`, <slot 27> `s_store_name`") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 35> sum(`ss_net_paid`))\n" + 
				"  |  group by: <slot 25> `c_last_name`, <slot 26> `c_first_name`, <slot 27> `s_store_name`, <slot 28> `ca_state`, <slot 29> `s_state`, <slot 30> `i_color`, <slot 31> `i_current_price`, <slot 32> `i_manager_id`, <slot 33> `i_units`, <slot 34> `i_size`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(<slot 166>)\n" + 
				"  |  group by: <slot 173>, <slot 174>, <slot 183>, <slot 188>, <slot 184>, <slot 177>, <slot 178>, <slot 179>, <slot 180>, <slot 181>") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 154> = upper(`ca_country`))\n" + 
				"  |  equal join conjunct: (<slot 164> = `ca_zip`)") && 
		explainStr.contains("vec output tuple id: 26") && 
		explainStr.contains("output slot ids: 166 173 174 177 178 179 180 181 183 184 188 \n" + 
				"  |  hash output slot ids: 144 161 162 3 151 152 155 156 157 158 159 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 131> = `s_store_sk`)") && 
		explainStr.contains("vec output tuple id: 25") && 
		explainStr.contains("output slot ids: 144 151 152 154 155 156 157 158 159 161 162 164 \n" + 
				"  |  hash output slot ids: 2 4 134 22 135 137 138 139 140 141 142 127 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 118> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 24") && 
		explainStr.contains("output slot ids: 127 131 134 135 137 138 139 140 141 142 \n" + 
				"  |  hash output slot ids: 116 5 6 7 120 8 9 123 124 126 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 112> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 23") && 
		explainStr.contains("output slot ids: 116 118 120 123 124 126 \n" + 
				"  |  hash output slot ids: 0 113 1 20 109 111 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`ss_ticket_number` = `sr_ticket_number`)\n" + 
				"  |  equal join conjunct: (`ss_item_sk` = `sr_item_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `sr_ticket_number`, RF001[in_or_bloom] <- `sr_item_sk`") && 
		explainStr.contains("vec output tuple id: 22") && 
		explainStr.contains("output slot ids: 109 111 112 113 \n" + 
				"  |  hash output slot ids: 18 10 13 15 ") && 
		explainStr.contains("TABLE: store_sales(store_sales), PREAGGREGATION: ON\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `ss_ticket_number`, RF001[in_or_bloom] -> `ss_item_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store(store), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`s_market_id` = 8)") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`i_color` = 'pale')") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: store_returns(store_returns), PREAGGREGATION: ON") 
            
        }
    }
}