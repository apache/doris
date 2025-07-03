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

suite("test_column_type_mtmv","mtmv") {
    String suiteName = "test_column_type_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName} (
          `user_id` int NOT NULL,
          class_id int,
          `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
        sql """insert into ${tableName} values(1,1,1);"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') AS
        SELECT user_id,class_id,sum(age) from ${tableName} group by user_id,class_id;
        """

    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_normal "select Name,State,RefreshState  from mv_infos('database'='regression_test_mtmv_p0') where Name='${mvName}'"
    sql """alter table ${tableName} modify column class_id bigint;"""
    assertEquals("FINISHED", getAlterColumnFinalState("${tableName}"))
    order_qt_after_schema_change "select Name,State,RefreshState  from mv_infos('database'='regression_test_mtmv_p0') where Name='${mvName}'"
    sql """
         REFRESH MATERIALIZED VIEW ${mvName} auto;
     """
    def jobName = getJobName("regression_test_mtmv_p0", mvName);
    waitingMTMVTaskFinishedNotNeedSuccess(jobName)
    // can not be rewrite
    order_qt_after_refresh "select Name,State,RefreshState  from mv_infos('database'='regression_test_mtmv_p0') where Name='${mvName}'"
}
