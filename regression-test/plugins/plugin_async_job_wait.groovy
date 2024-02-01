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

import org.apache.doris.regression.suite.Suite

// Usage: getLoadFinalState("label")
// Usage: assertEquals("FINISHED"， getLoadFinalState("label1"))
Suite.metaClass.getLoadFinalState = { String label /* param */ ->
    Suite suite = delegate as Suite
    String showLoadSql = "SHOW LOAD WHERE Label='${label}' ORDER BY CreateTime DESC LIMIT 1"
    int max_try_time = 300
    String jobState = ""
    // wait job finished/cancelled
    while(max_try_time--){
        def loadJob = sql showLoadSql
        jobState = loadJob[0][2]
        if (jobState == "FINISHED" || jobState == "CANCELLED") {
            sleep(3000)
            logger.info("load job: label=${label} is ${jobState}, msg: ${loadJob[0][7]}")
            return jobState
        } else {
            sleep(1000)
        }
    }
    logger.info("load job: label=${label} wait for 300s, status is ${jobState}... return")
    return jobState
}
logger.info("Added 'getLoadFinalState' function to Suite")


Suite.metaClass.getAlterColumnFinalState = { String tableName /* param */ ->
    Suite suite = delegate as Suite
    String showAlterColumnSql = "SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1"
    int max_try_time = 300
    String jobState = ""
    while(max_try_time--){
        def alterJob = sql showAlterColumnSql
        jobState = alterJob[0][9]
        if (jobState == "FINISHED" || jobState == "CANCELLED") {
            sleep(3000)
            logger.info("alter table ${tableName} column job is ${jobState}, msg: ${alterJob[0][10]} ")
            return jobState
        } else {
            sleep(1000)
        }
    }
    logger.info("alter table ${tableName} column job wait for 300s, status is ${jobState} ... return")
    return jobState
}
logger.info("Added 'getAlterColumnFinalState' function to Suite")


Suite.metaClass.getAlterRollupFinalState = { String tableName /* param */ ->
    Suite suite = delegate as Suite
    String showAlterRollupSql = "SHOW ALTER TABLE ROLLUP WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1"
    int max_try_time = 300
    String jobState = ""
    while(max_try_time--){
        def alterJob = sql showAlterRollupSql
        jobState = alterJob[0][8]
        if (jobState == "FINISHED" || jobState == "CANCELLED") {
            sleep(3000)
            logger.info("alter table ${tableName} rollup job is ${jobState}, msg: ${alterJob[0][9]} ")
            return jobState
        } else {
            sleep(1000)
        }
    }
    logger.info("alter table ${tableName} rollup job wait for 300s, status is ${jobState}... return")
    return jobState
}
logger.info("Added 'getAlterRollupFinalState' function to Suite")
