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

suite("test_multi_level_mtmv") {
    def tableName = "t_test_multi_level_user"
    def mv1 = "multi_level_mtmv1"
    def mv2 = "multi_level_mtmv2"
    def dbName = "regression_test_mtmv_p0"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mv1};"""
    sql """drop materialized view if exists ${mv2};"""

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            k1 int,
            k2 int
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        INSERT INTO ${tableName} VALUES(1,1);
    """

    sql """
        CREATE MATERIALIZED VIEW ${mv1}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT * FROM ${tableName};
    """
    def jobName1 = getJobName("regression_test_mtmv_p0", mv1);
     sql """
        REFRESH MATERIALIZED VIEW ${mv1} AUTO
    """
    waitingMTMVTaskFinished(jobName1)
    order_qt_mv1 "select * from ${mv1}"

    sql """
        CREATE MATERIALIZED VIEW ${mv2}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT * FROM ${mv1};
    """
    def jobName2 = getJobName("regression_test_mtmv_p0", mv2);
     sql """
        REFRESH MATERIALIZED VIEW ${mv2} AUTO
    """
    waitingMTMVTaskFinished(jobName2)
    order_qt_mv2 "select * from ${mv2}"

    // drop table
    sql """
        drop table ${tableName}
    """
    order_qt_status1 "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mv1}'"
    order_qt_status2 "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mv2}'"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mv1};"""
    sql """drop materialized view if exists ${mv2};"""
}
