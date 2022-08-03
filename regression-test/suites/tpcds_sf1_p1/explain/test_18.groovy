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

suite("test_regression_test_tpcds_sf1_p1_q18", "tpch_sf1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  i_item_id
		, ca_country
		, ca_state
		, ca_county
		, avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1
		, avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2
		, avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3
		, avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4
		, avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5
		, avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6
		, avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
		FROM
		  catalog_sales
		, customer_demographics cd1
		, customer_demographics cd2
		, customer
		, customer_address
		, date_dim
		, item
		WHERE (cs_sold_date_sk = d_date_sk)
		   AND (cs_item_sk = i_item_sk)
		   AND (cs_bill_cdemo_sk = cd1.cd_demo_sk)
		   AND (cs_bill_customer_sk = c_customer_sk)
		   AND (cd1.cd_gender = 'F')
		   AND (cd1.cd_education_status = 'Unknown')
		   AND (c_current_cdemo_sk = cd2.cd_demo_sk)
		   AND (c_current_addr_sk = ca_address_sk)
		   AND (c_birth_month IN (1, 6, 8, 9, 12, 2))
		   AND (d_year = 1998)
		   AND (ca_state IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))
		GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county)
		ORDER BY ca_country ASC, ca_state ASC, ca_county ASC, i_item_id ASC
		LIMIT 100

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 52> <slot 41> `ca_country` ASC, <slot 53> <slot 42> `ca_state` ASC, <slot 54> <slot 43> `ca_county` ASC, <slot 55> <slot 40> `i_item_id` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: avg(<slot 45> avg(CAST(`cs_quantity` AS DECIMAL(12,2)))), avg(<slot 46> avg(CAST(`cs_list_price` AS DECIMAL(12,2)))), avg(<slot 47> avg(CAST(`cs_coupon_amt` AS DECIMAL(12,2)))), avg(<slot 48> avg(CAST(`cs_sales_price` AS DECIMAL(12,2)))), avg(<slot 49> avg(CAST(`cs_net_profit` AS DECIMAL(12,2)))), avg(<slot 50> avg(CAST(`c_birth_year` AS DECIMAL(12,2)))), avg(<slot 51> avg(CAST(`cd1`.`cd_dep_count` AS DECIMAL(12,2))))\n" + 
				"  |  group by: <slot 40> `i_item_id`, <slot 41> `ca_country`, <slot 42> `ca_state`, <slot 43> `ca_county`, <slot 44> `GROUPING_ID`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: avg(CAST(<slot 32> `cs_quantity` AS DECIMAL(12,2))), avg(CAST(<slot 33> `cs_list_price` AS DECIMAL(12,2))), avg(CAST(<slot 34> `cs_coupon_amt` AS DECIMAL(12,2))), avg(CAST(<slot 35> `cs_sales_price` AS DECIMAL(12,2))), avg(CAST(<slot 36> `cs_net_profit` AS DECIMAL(12,2))), avg(CAST(<slot 37> `c_birth_year` AS DECIMAL(12,2))), avg(CAST(<slot 38> `cd1`.`cd_dep_count` AS DECIMAL(12,2)))\n" + 
				"  |  group by: <slot 28> `i_item_id`, <slot 29> `ca_country`, <slot 30> `ca_state`, <slot 31> `ca_county`, <slot 39> `GROUPING_ID`") && 
		explainStr.contains("output slots: ``i_item_id``, ``ca_country``, ``ca_state``, ``ca_county``, ``cs_quantity``, ``cs_list_price``, ``cs_coupon_amt``, ``cs_sales_price``, ``cs_net_profit``, ``c_birth_year``, ``cd1`.`cd_dep_count``, ``GROUPING_ID``") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 150> = `ca_address_sk`)") && 
		explainStr.contains("vec output tuple id: 16") && 
		explainStr.contains("output slot ids: 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 \n" + 
				"  |  hash output slot ids: 1 130 2 131 3 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 25 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 127> = `cd2`.`cd_demo_sk`)") && 
		explainStr.contains("vec output tuple id: 15") && 
		explainStr.contains("output slot ids: 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 \n" + 
				"  |  hash output slot ids: 128 129 108 109 110 111 112 113 114 115 116 117 118 119 23 120 121 122 123 124 125 126 127 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 103> = `c_customer_sk`)") && 
		explainStr.contains("vec output tuple id: 14") && 
		explainStr.contains("output slot ids: 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 \n" + 
				"  |  hash output slot ids: 96 97 98 99 100 101 102 103 104 105 9 106 107 19 22 24 26 91 92 93 94 95 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 86> = `i_item_sk`)") && 
		explainStr.contains("vec output tuple id: 13") && 
		explainStr.contains("output slot ids: 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 \n" + 
				"  |  hash output slot ids: 0 76 77 78 79 15 80 81 82 83 84 85 86 87 88 89 90 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[The src data has been redistributed]\n" + 
				"  |  equal join conjunct: (<slot 72> = `d_date_sk`)") && 
		explainStr.contains("vec output tuple id: 12") && 
		explainStr.contains("output slot ids: 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 \n" + 
				"  |  hash output slot ids: 64 65 66 67 68 69 70 71 72 73 74 75 13 27 63 ") && 
		explainStr.contains("join op: INNER JOIN(BROADCAST)[Tables are not in the same group]\n" + 
				"  |  equal join conjunct: (`cd1`.`cd_demo_sk` = `cs_bill_cdemo_sk`)\n" + 
				"  |  runtime filters: RF000[in_or_bloom] <- `cs_bill_cdemo_sk`") && 
		explainStr.contains("vec output tuple id: 11") && 
		explainStr.contains("output slot ids: 63 64 65 66 67 68 69 70 71 72 73 74 75 \n" + 
				"  |  hash output slot ids: 4 5 6 7 8 10 12 14 16 17 18 20 21 ") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`cd1`.`cd_gender` = 'F'), (`cd1`.`cd_education_status` = 'Unknown')\n" + 
				"     runtime filters: RF000[in_or_bloom] -> `cd1`.`cd_demo_sk`") && 
		explainStr.contains("TABLE: customer_address(customer_address), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`ca_state` IN ('MS', 'IN', 'ND', 'OK', 'NM', 'VA', 'MS'))") && 
		explainStr.contains("TABLE: customer_demographics(customer_demographics), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: customer(customer), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`c_birth_month` IN (1, 6, 8, 9, 12, 2))") && 
		explainStr.contains("TABLE: item(item), PREAGGREGATION: ON") && 
		explainStr.contains("TABLE: date_dim(date_dim), PREAGGREGATION: ON\n" + 
				"     PREDICATES: (`d_year` = 1998)") && 
		explainStr.contains("TABLE: catalog_sales(catalog_sales), PREAGGREGATION: ON") 
            
        }
    }
}