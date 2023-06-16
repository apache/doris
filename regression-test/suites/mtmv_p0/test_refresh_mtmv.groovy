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

suite("test_refresh_mtmv") {
    def tableName = "t_test_refresh_mtmv_user"
    def tableNamePv = "t_test_refresh_mtmv_user_pv"
    def mvName = "multi_mv_test_refresh_mtmv"
    def mvNameDemand = "multi_mv_test_refresh_demand_mtmv"

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

    order_qt_select "SELECT * FROM ${mvName}"

    // test REFRESH make sure only define one mv and already run a task.
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE
    """
    waitingMTMVTaskFinished(mvName)

    def show_task_result = sql "SHOW MTMV TASK ON ${mvName}"
    assertEquals 2, show_task_result.size(), show_task_result.toString()

    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """

    sql """drop materialized view if exists ${mvNameDemand}"""

    sql """
            CREATE MATERIALIZED VIEW ${mvNameDemand}
            BUILD DEFERRED REFRESH COMPLETE ON DEMAND
            KEY(username)
            DISTRIBUTED BY HASH (username)  buckets 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT ${tableName}.username, ${tableNamePv}.pv FROM ${tableName}, ${tableNamePv} WHERE ${tableName}.id=${tableNamePv}.id;
        """

    sql """
        REFRESH MATERIALIZED VIEW ${mvNameDemand} COMPLETE
    """
    waitingMTMVTaskFinished(mvName)

    show_task_result = sql "SHOW MTMV TASK ON ${mvNameDemand}"
    assertEquals 1, show_task_result.size(), show_task_result.toString()

    sql """
        DROP MATERIALIZED VIEW ${mvNameDemand}
    """
}
