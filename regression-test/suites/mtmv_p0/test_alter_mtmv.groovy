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

suite("test_alter_mtmv") {
    def tableName = "t_test_alter_mtmv_user"
    def tableNamePv = "t_test_alter_mtmv_pv"
    def mvName = "multi_mv_test_alter_mtmv"

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

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE
        KEY(username)   
        DISTRIBUTED BY HASH (username)  buckets 1
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
    """

    // waiting the task to be finished.
    waitingMTMVTaskFinished(mvName)

    // test alter mtmv
    sql """
        alter MATERIALIZED VIEW ${mvName} REFRESH COMPLETE start with "2022-11-03 00:00:00" next 2 DAY
    """
    show_job_meta = sql_meta "SHOW MTMV JOB ON ${mvName}"
    def scheduleIndex = show_job_meta.indexOf(['Schedule', 'CHAR'])

    show_job_result = sql "SHOW MTMV JOB ON ${mvName}"
    assertEquals 1, show_job_result.size(), show_job_result.toString()

    assertEquals 'START 2022-11-03T00:00 EVERY(2 DAYS)', show_job_result.last().get(scheduleIndex).toString(), show_job_result.last().toString()

    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """
}
