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

suite("test_materialized_view_common_expr_push_down") {
    def baseTable = "test_base_tbl"
    def mvTable = "test_mv"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }

    sql """set enable_common_expr_pushdown = true"""

    sql "DROP TABLE IF EXISTS ${baseTable}"
    sql """
            CREATE TABLE `${baseTable}` (
                `companyId` bigint NULL,
                `jobId` bigint NULL,
                `province` text NULL,
                `vCallerId` bigint NULL DEFAULT "0",
                `aCallerId` bigint NULL DEFAULT "-1"
            ) ENGINE=OLAP
            DUPLICATE KEY(`companyId`)
            DISTRIBUTED BY HASH(`companyId`) BUCKETS 8
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
    sql """
            CREATE materialized VIEW ${mvTable} AS SELECT
                companyId as company_id,
                jobId as job_id,
                province as p,
                vCallerId as v_caller_id,
                aCallerId as a_caller_id
            FROM ${baseTable}
            GROUP BY company_id, job_id, p, v_caller_id, a_caller_id;
    """
    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(baseTable)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    sql "insert into ${baseTable} values(100,100,'北京',3,3)"
    sql "insert into ${baseTable} values(100,100,'广东',3,3)"

    qt_sql """ select company_id from ${baseTable} index ${mvTable} where v_caller_id = 3 and if(`a_caller_id` is null,-1, `a_caller_id`) = 3 """

    sql "DROP TABLE ${baseTable} FORCE;"
}
