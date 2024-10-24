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

suite("test_task_mtmv") {
    def tableName = "t_test_task_mtmv_user"
    def mvName = "multi_mv_test_task_mtmv"
    def dbName = "regression_test_mtmv_p0"
    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists `${tableName}`"""
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            event_day DATE,
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10 
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT * FROM ${tableName};
    """
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
    """
    def jobName = getJobName(dbName, mvName);
    waitingMTMVTaskFinished(jobName)
    def taskIdArr = sql """ select TaskId from tasks('type'='mv') where MvName = '${mvName}';"""
    def taskId = taskIdArr.get(0).get(0);
    logger.info("taskId: " + taskId.toString())
     try {
        sql """
                cancel MATERIALIZED VIEW TASK ${taskId} on ${mvName};
            """
        } catch (Exception e) {
           log.info("cancel error msg: " + e.getMessage())
           assertTrue(e.getMessage().contains("no running task"));
        }
    sql """drop materialized view if exists ${mvName};"""

}
