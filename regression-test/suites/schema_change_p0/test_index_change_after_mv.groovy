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

import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_index_change_after_mv") {
    def tableName = "test_index_change_after_mv"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
                name varchar(50),
                age int NOT NULL,
                grade int NOT NULL,
                studentInfo char(100),
                tearchComment string,
            )
            DUPLICATE KEY(`name`)
            DISTRIBUTED BY HASH(`name`) BUCKETS 10
            properties("replication_num" = "1");
    """

    def mvName = """${tableName}_1"""

    sql """ INSERT INTO ${tableName} VALUES("steve", 0, 2, "xxx", "xxx") """

    sql """ 
        CREATE MATERIALIZED VIEW ${mvName} AS SELECT name as a1, age as a2, grade as a3 FROM ${tableName} WHERE name = "steve" AND age = 0
    """

    def getJobState = { tblName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tblName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }

    def max_try_secs = 60
    Awaitility.await().atMost(max_try_secs, SECONDS).pollInterval(2, SECONDS).until{
        String res = getJobState(tableName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            return true
        }
        return false
    }

    sql """
        CREATE INDEX ${tableName}_idx ON ${tableName}(age) USING INVERTED COMMENT 'inverted index age_idx_1'
    """

    waitForSchemaChangeDone({
        sql " SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 "
    })

    qt_sql """
        SHOW CREATE MATERIALIZED VIEW ${mvName} on ${tableName}
    """
}
