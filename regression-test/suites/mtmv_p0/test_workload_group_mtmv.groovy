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

import org.junit.Assert;

suite("test_workload_group_mtmv") {
    def tableName = "t_test_workload_group_mtmv_user"
    def mvName = "multi_mv_test_workload_group_mtmv"
    def dbName = "regression_test_mtmv_p0"
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
    sql """ insert into ${tableName} values('2020-10-01',1,"a");"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1','workload_group' = 'g1')
        AS 
        SELECT * FROM ${tableName};
    """
    order_qt_create "select MvProperties from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
            alter MATERIALIZED VIEW ${mvName} set ('workload_group'='mv_test_not_exist_group');
        """
    order_qt_alter "select MvProperties from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
            refresh MATERIALIZED VIEW ${mvName} AUTO;
        """
    def jobName = getJobName(dbName, mvName);
    logger.info(jobName)
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
    def errors = sql """select ErrorMsg from tasks('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"""
    logger.info("errors: " + errors.toString())
    assertTrue(errors.toString().contains("mv_test_not_exist_group"))
    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """
}
