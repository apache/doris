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

suite("test_create_mtmv") {
    def tableName = "t_user"
    def tableNamePv = "t_user_pv"
    def mvName = "multi_mv"
    sql """
        ADMIN SET FRONTEND CONFIG("enable_mtmv_scheduler_framework"="true");
        """

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

    def show_task_meta = sql_meta "SHOW MTMV TASK ON ${mvName}"
    def index = show_task_meta.indexOf(['State', 'CHAR'])
    def query = "SHOW MTMV TASK ON ${mvName}"
    def show_task_result
    def state = "PENDING"
    do {
        show_task_result = sql "${query}"
        if (!show_task_result.isEmpty()) {
            state = show_task_result.last().get(index)
        }
        println "The state of ${query} is ${state}"
        Thread.sleep(1000);
    } while (state.equals('PENDING') || state.equals('RUNNING'))

    assertEquals 'SUCCESS', state, show_task_result.last().toString()
    order_qt_select "SELECT * FROM ${mvName}"

    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

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
    state = "PENDING"
    do {
        show_task_result = sql "${query}"
        if (!show_task_result.isEmpty()) {
            state = show_task_result.last().get(index)
        }
        println "The state of ${query} is ${state}"
        Thread.sleep(1000);
    } while (state.equals('PENDING') || state.equals('RUNNING'))

    def show_job_result = sql "SHOW MTMV JOB ON ${mvName}"
    assertEquals 1, show_job_result.size()

    // test REFRESH make sure only define one mv and already run a task.
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE
    """
    state = "PENDING"
    do {
        show_task_result = sql "${query}"
        if (!show_task_result.isEmpty()) {
            state = show_task_result.last().get(index)
        }
        println "The state of ${query} is ${state}"
        Thread.sleep(1000);
    } while (state.equals('PENDING') || state.equals('RUNNING'))

    assertEquals 'SUCCESS', state, show_task_result.last().toString()
    assertEquals 2, show_task_result.size()

    // test alter mtmv
    sql """
        alter MATERIALIZED VIEW ${mvName} REFRESH COMPLETE start with "2022-11-03 00:00:00" next 2 DAY
    """
    show_job_meta = sql_meta "SHOW MTMV JOB ON ${mvName}"
    def scheduleIndex = show_job_meta.indexOf(['Schedule', 'CHAR'])

    show_job_result = sql "SHOW MTMV JOB ON ${mvName}"
    assertEquals 1, show_job_result.size()

    assertEquals 'START 2022-11-03T00:00 EVERY(2 DAYS)', show_job_result.last().get(scheduleIndex).toString(), show_job_result.last().toString()

    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """
}

