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

suite("ssb_sf1_q1_2_nereids") {
    String realDb = context.config.getDbNameByFile(context.file)
    // get parent directory's group
    realDb = realDb.substring(0, realDb.lastIndexOf("_"))

    sql "use ${realDb}"

    sql 'set enable_nereids_planner=true'
    // nereids need vectorized

    sql 'set exec_mem_limit=2147483648*2'

    test {
        // sql(new File(context.file.parentFile, "../sql/q1.2.sql").text)
        sql """
        SELECT /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/  
        SUM(lo_extendedprice*lo_discount) AS
        REVENUE
        FROM  lineorder, date
        WHERE  lo_orderdate = d_datekey
        AND d_yearmonth = 'Jan1994'
        AND lo_discount BETWEEN 4 AND 6
        AND lo_quantity BETWEEN 26 AND 35;"""

        resultFile("../sql/q1.2.out", "q1.2")
    }
}
