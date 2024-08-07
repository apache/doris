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

suite("test_env_db_dropped_mtmv") {
    def tableName = "t_test_env_db_dropped_mtmv_user"
    def mvName = "test_env_db_dropped_mtmv"
    def dbName1 = "test_env_db_dropped_mtmv_db1"
    def dbName2 = "test_env_db_dropped_mtmv_db2"
    sql """drop database if exists `${dbName1}`"""
    sql """drop database if exists `${dbName2}`"""
    sql """create database `${dbName1}`"""
    sql """create database `${dbName2}`"""
    sql """use `${dbName1}`"""

    sql """
        CREATE TABLE IF NOT EXISTS ${dbName2}.${tableName} (
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
        CREATE MATERIALIZED VIEW ${dbName2}.${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT * FROM ${dbName2}.${tableName};
    """
    def jobName = getJobName(dbName2, mvName);

    // table and mv in db2, so drop db1, should success
    sql """drop database if exists `${dbName1}`"""
    sql """
            REFRESH MATERIALIZED VIEW ${dbName2}.${mvName} AUTO;
        """
    waitingMTMVTaskFinished(jobName)

    sql """
        DROP MATERIALIZED VIEW ${dbName2}.${mvName};
    """

    sql """create database `${dbName1}`"""
    sql """use `${dbName1}`"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
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
        CREATE MATERIALIZED VIEW ${dbName2}.${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT * FROM ${tableName};
    """
    jobName = getJobName(dbName2, mvName);
    sql """drop database if exists `${dbName1}`"""
    // table in db1, has been dropped, will error
    sql """
            REFRESH MATERIALIZED VIEW ${dbName2}.${mvName} AUTO;
        """
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
    def msg = sql """select ErrorMsg from tasks('type'='mv') where JobName = '${jobName}' order by CreateTime DESC limit 1"""
    logger.info(msg.toString())
    assertTrue(msg.toString().contains("does not exist"))
}
