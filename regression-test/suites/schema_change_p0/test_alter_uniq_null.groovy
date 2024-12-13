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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("test_alter_uniq_null") {
    def tableName = "test_alter_uniq_null_tbl"

    def getJobState = { tableName1 ->
        def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName1}' ORDER BY createtime DESC LIMIT 1 """
        println jobStateResult
        return jobStateResult[0][9]
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName} (
        `k1` VARCHAR(30) NOT NULL,
        `k2` VARCHAR(24) NOT NULL,
        `v1` VARCHAR(6) NULL,
        `v2` INT NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k2`) BUCKETS 1
        PROPERTIES (
        "light_schema_change" = "true",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728",
        "enable_unique_key_merge_on_write" = "false",
        "replication_num" = "1"
        );
    """

    sql """alter table ${tableName} modify column `v2` INT NULL"""
    sleep(10)
    def max_try_num = 1000
    while (max_try_num--) {
        String res = getJobState(tableName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            break
        } else {
          int val = 100000 + max_try_num
          sql """ insert into ${tableName} values ("${val}", "client", "3", 100), ("${val}", "client", "4", 200)"""
          sleep(10)
          if (max_try_num < 1) {
              println "test timeout," + "state:" + res
              assertEquals("FINISHED",res)
          }
        }
    }
}
