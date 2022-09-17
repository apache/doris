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

suite("test_explain_tpch_sf_1_q01", "tpch_sf1") {
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
		explainStr.contains("VAGGREGATE (update finalize)\n" + 
				"  |  output: sum(`l_quantity`), sum(`l_extendedprice`), sum(`l_extendedprice` * (1 - `l_discount`)), sum(`l_extendedprice` * (1 - `l_discount`) * (1 + `l_tax`)), avg(`l_quantity`), avg(`l_extendedprice`), avg(`l_discount`), count(*)\n" + 
				"  |  group by: `l_returnflag`, `l_linestatus`") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1.lineitem(lineitem), PREAGGREGATION: OFF. Reason: __DORIS_DELETE_SIGN__ is used as conjuncts. conjunct on `L_RETURNFLAG` which is StorageEngine value column\n" + 
				"     PREDICATES: `l_shipdate` <= '1998-09-02 00:00:00', `default_cluster:regression_test_tpch_sf1_p1.lineitem`.`__DORIS_DELETE_SIGN__` = 0") 
            
        }
    }
}