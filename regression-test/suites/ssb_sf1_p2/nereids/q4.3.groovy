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

suite("ssb_sf1_q4_3_nereids") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    sql 'set enable_nereids_planner=true'

    sql 'set exec_mem_limit=2147483648*16'

    test {
        // sql(new File(context.file.parentFile, "../sql/q4.3.sql").text)
        sql """SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/
        d_year, s_city, p_brand,
        SUM(lo_revenue - lo_supplycost) AS PROFIT
        FROM date, customer, supplier, part, lineorder
        WHERE lo_custkey = c_custkey
        AND lo_suppkey = s_suppkey
        AND lo_partkey = p_partkey
        AND lo_orderdate = d_datekey
        AND s_nation = 'UNITED STATES'
        AND (d_year = 1997 OR d_year = 1998)
        AND p_category = 'MFGR#14'
        GROUP BY d_year, s_city, p_brand
        ORDER BY d_year, s_city, p_brand;"""

        resultFile("../sql/q4.3.out", "q4.3")
    }
}
