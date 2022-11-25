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

suite("test_explain_tpch_sf_1_q1") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		SELECT
		  l_returnflag,
		  l_linestatus,
		  sum(l_quantity)                                       AS sum_qty,
		  sum(l_extendedprice)                                  AS sum_base_price,
		  sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
		  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
		  avg(l_quantity)                                       AS avg_qty,
		  avg(l_extendedprice)                                  AS avg_price,
		  avg(l_discount)                                       AS avg_disc,
		  count(*)                                              AS count_order
		FROM
		  lineitem
		WHERE
		  l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
		GROUP BY
		l_returnflag,
		l_linestatus
		ORDER BY
		l_returnflag,
		l_linestatus

            """
        check {
            explainStr ->
		explainStr.contains("VTOP-N\n" + 
				"  |  order by: <slot 17> <slot 7> `l_returnflag` ASC, <slot 18> <slot 8> `l_linestatus` ASC") && 
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 9> sum(`l_quantity`)), sum(<slot 10> sum(`l_extendedprice`)), sum(<slot 11> sum(`l_extendedprice` * (1 - `l_discount`))), sum(<slot 12> sum(`l_extendedprice` * (1 - `l_discount`) * (1 + `l_tax`))), avg(<slot 13> avg(`l_quantity`)), avg(<slot 14> avg(`l_extendedprice`)), avg(<slot 15> avg(`l_discount`)), count(<slot 16> count(*))\n" + 
				"  |  group by: <slot 7> `l_returnflag`, <slot 8> `l_linestatus`") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  STREAMING\n" + 
				"  |  output: sum(`l_quantity`), sum(`l_extendedprice`), sum(`l_extendedprice` * (1 - `l_discount`)), sum(`l_extendedprice` * (1 - `l_discount`) * (1 + `l_tax`)), avg(`l_quantity`), avg(`l_extendedprice`), avg(`l_discount`), count(*)\n" + 
				"  |  group by: `l_returnflag`, `l_linestatus`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipdate` <= '1998-09-02 00:00:00'") 
            
        }
    }
}