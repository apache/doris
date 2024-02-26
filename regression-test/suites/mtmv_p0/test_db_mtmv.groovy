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

suite("test_db_mtmv") {
    def tableName = "t_test_db_mtmv_user"
    def mvName = "multi_mv_test_db_mtmv"
    def dbName = "regression_test_mtmv_db"
    sql """drop database if exists `${dbName}`"""
    sql """create database `${dbName}`"""
    sql """use `${dbName}`"""

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
    def jobName = getJobName(dbName, mvName);
    order_qt_count_init "select count(*)  from jobs('type'='mv') where Name='${jobName}'"
     sql """
        drop database `${dbName}`
    """
    order_qt_count_dropped "select count(*)  from jobs('type'='mv') where Name='${jobName}'"
}
