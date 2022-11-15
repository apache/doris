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

suite("test_explain_tpch_sf_1_q6") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    explain {
            sql """
		
		SELECT sum(l_extendedprice * l_discount) AS revenue
		FROM
		  lineitem
		WHERE
		  l_shipdate >= DATE '1994-01-01'
		  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
		AND l_discount BETWEEN 0.06 - 0.01 AND .06 + 0.01
		AND l_quantity < 24
		

            """
        check {
            explainStr ->
		explainStr.contains("VAGGREGATE (merge finalize)\n" + 
				"  |  output: sum(<slot 4> sum(`l_extendedprice` * `l_discount`))\n" + 
				"  |  group by: ") && 
		explainStr.contains("VAGGREGATE (update serialize)\n" + 
				"  |  output: sum(`l_extendedprice` * `l_discount`)\n" + 
				"  |  group by: ") && 
		explainStr.contains("TABLE: default_cluster:regression_test_tpch_sf1_p1_tpch_sf1.lineitem(lineitem), PREAGGREGATION: ON\n" + 
				"     PREDICATES: `l_shipdate` >= '1994-01-01 00:00:00', `l_shipdate` < '1995-01-01 00:00:00', `l_discount` >= 0.05, `l_discount` <= 0.07, `l_quantity` < 24") 
            
        }
    }
}