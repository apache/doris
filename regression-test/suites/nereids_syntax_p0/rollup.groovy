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


suite("rollup") {
    sql """
        DROP TABLE IF EXISTS `rollup_t1`
    """
    sql """
        CREATE TABLE IF NOT EXISTS `rollup_t1` (
          `k1` int(11) NULL,
          `k2` int(11) NULL,
          `k3` int(11) NULL,
          `v1` int(11) SUM NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`, `k3`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql "ALTER TABLE rollup_t1 ADD ROLLUP r1(k2, v1)"


    def getJobRollupState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE ROLLUP WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }

    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobRollupState("rollup_t1")
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
    Thread.sleep(2000)

    sql "insert into rollup_t1 values(1, 2, 3, 4)"
    sql "insert into rollup_t1 values(1, 2, 3, 2)"
    sql "insert into rollup_t1 values(2, 3, 4, 1)"
    sql "insert into rollup_t1 values(2, 3, 4, 3)"

    sql "set enable_vectorized_engine=true"

    sql "set enable_nereids_planner=true"

    sql "SET enable_fallback_to_original_planner=false"

    explain {
        sql("select k2, sum(v1) from rollup_t1 group by k2")
        contains("rollup_t1(r1)")
        contains("PREAGGREGATION: ON")
    }

    explain {
        sql("select k1, sum(v1) from rollup_t1 group by k1")
        contains("rollup_t1(rollup_t1)")
        contains("PREAGGREGATION: ON")
    }

    order_qt_rollup1 "select k2, sum(v1) from rollup_t1 group by k2"

    order_qt_rollup2 "select k1, sum(v1) from rollup_t1 group by k1"

    explain {
        sql("select k1, max(v1) from rollup_t1 group by k1")
        contains("rollup_t1(rollup_t1)")
        contains("PREAGGREGATION: OFF")
        contains("Aggregate operator don't match, aggregate function: max(v1), column aggregate type: SUM")
    }
}
