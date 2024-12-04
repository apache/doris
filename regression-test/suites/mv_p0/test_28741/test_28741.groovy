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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_28741") {

    sql """ DROP TABLE IF EXISTS test; """

    sql """
            CREATE TABLE test (
            a tinyint(4) NULL DEFAULT "0" ,
            b int(11) DEFAULT "0" ,
            c int(11) DEFAULT "0" ,
            t DATETIMEV2 COMMENT "时间",
            d bigint(20) SUM NULL DEFAULT "0",
            e bigint(20) SUM NULL DEFAULT "0"
            )
            AGGREGATE KEY(a, b, c, t)
            DISTRIBUTED BY HASH(a) BUCKETS AUTO
            PROPERTIES
            (
            "replication_num" = "1"
            );
        """

    createMV ("CREATE MATERIALIZED VIEW mv_test AS SELECT a,b,t,SUM(d) FROM test GROUP BY 1,2,3")

    sql "INSERT INTO test(a,b,c,t,d,e) VALUES (1,2,3,'2023-12-19 18:21:00', 56, 78)"

    sql """
    ALTER TABLE test ADD COLUMN a1 INT KEY AFTER a, ADD COLUMN b1 VARCHAR(1024) KEY AFTER b, ADD COLUMN d1 BIGINT SUM AFTER d;
    """

    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    int max_try_time = 100
    while (max_try_time--){
        String result = getJobState("test")
        if (result == "FINISHED") {
            sleep(1000)
            break
        } else {
            sleep(1000)
            assertTrue(max_try_time>1)
        }
    }

    sql "INSERT INTO test(a,a1,b,b1,c,t,d,d1,e) VALUES (1,1,2,'-',3,'2023-12-20 17:21:00', 56, 78, 89)"

    sql """analyze table test with sync;"""
    sql """set enable_stats=false;"""

    mv_rewrite_fail("select b1 from test where t >= '2023-12-20 17:21:00'", "mv_test")

    sql """set enable_stats=true;"""
    sql """alter table test modify column a set stats ('row_count'='2');"""
    mv_rewrite_fail("select b1 from test where t >= '2023-12-20 17:21:00'", "mv_test")
}
