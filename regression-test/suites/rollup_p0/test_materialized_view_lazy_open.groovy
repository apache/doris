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

suite("test_materialized_view_lazy_open", "rollup") {

    // because nereids cannot support rollup correctly forbid it temporary
    sql """set enable_nereids_planner=false"""

    def tbName1 = "test_materialized_view_lazy_open"
    def tbName2 = "test_materialized_view_lazy_open_dynamic_partition"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }

    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1}(
                k1 DATE,
                k2 DECIMAL(10, 2),
                k3 CHAR(10),
                k4 INT NOT NULL
            )
            DUPLICATE KEY(k1, k2)
            PARTITION BY RANGE(k1)
            (
               PARTITION p1 VALUES LESS THAN ("2000-01-01"),
               PARTITION p2 VALUES LESS THAN ("2010-01-01"),
               PARTITION p3 VALUES LESS THAN ("2020-01-01")
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 32 
            properties("replication_num" = "1");
        """

    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName2}(
                k1 DATE,
                k2 DECIMAL(10, 2),
                k3 CHAR(10),
                k4 INT NOT NULL
            )
            PARTITION BY RANGE(k1)
            (
               PARTITION p1 VALUES LESS THAN ("2000-01-01"),
               PARTITION p2 VALUES LESS THAN ("2010-01-01"),
               PARTITION p3 VALUES LESS THAN ("2020-01-01")
            )
            DISTRIBUTED BY HASH(k1)
            PROPERTIES
            (
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "DAY",
                "dynamic_partition.start" = "-2147483648",
                "dynamic_partition.end" = "3",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "32",
                "replication_num"="1"
            );
        """
    
    sql "CREATE materialized VIEW test_lazy_open AS SELECT k1 FROM ${tbName1} GROUP BY k1;"
    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "FINISHED") {
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

    sql "CREATE materialized VIEW test_lazy_open_dynamic_partition AS SELECT k1 FROM ${tbName2} GROUP BY k1;"
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName2)
        if (res == "FINISHED") {
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

    sql "insert into ${tbName1} values('2000-05-20', 1.5, 'test', 1);"
    sql "insert into ${tbName1} values('2010-05-20', 1.5, 'test', 1);"
    sql "insert into ${tbName2} values('2000-05-20', 1.5, 'test', 1);"
    sql "insert into ${tbName2} values('2010-05-20', 1.5, 'test', 1);"

    sql "DROP TABLE ${tbName1} FORCE;"
    sql "DROP TABLE ${tbName2} FORCE;"

}