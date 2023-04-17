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

suite("test_create_both_mtmv") {
    def tableName = "t_test_create_both_mtmv_user"
    def tableNamePv = "t_test_create_both_mtmv_user_pv"
    def mvName = "multi_mv_test_create_both_mtmv"

    sql """drop table if exists `${tableName}`"""
    sql """drop table if exists `${tableNamePv}`"""

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
        INSERT INTO ${tableName} VALUES("2022-10-26",1,"clz"),("2022-10-28",2,"zhangsang"),("2022-10-29",3,"lisi");
    """
    sql """
        create table IF NOT EXISTS ${tableNamePv}(
            event_day DATE,
            id BIGINT,
            pv BIGINT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10 
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO ${tableNamePv} VALUES("2022-10-26",1,200),("2022-10-28",2,200),("2022-10-28",3,300);
    """

    sql """drop materialized view if exists ${mvName}""" 

    // test only one job created when build IMMEDIATE and start time is before now.
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE
        start with "2022-11-03 00:00:00" next 1 DAY
        KEY(username)   
        DISTRIBUTED BY HASH (username)  buckets 1
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """
    // wait task to be finished to avoid task leak in suite.
    waitingMTMVTaskFinished(mvName)

    def show_job_result = sql "SHOW MTMV JOB ON ${mvName}"
    assertEquals 1, show_job_result.size(), show_job_result.toString()

    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """
}
