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

suite("test_pause_mtmv") {
    def tableName = "t_test_pause_mtmv_user"
    def mvName = "multi_mv_test_pause_mtmv"
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

    sql """drop materialized view if exists ${mvName};"""

    // IMMEDIATE MANUAL
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT * FROM ${tableName};
    """
    def jobName = getJobName("regression_test_mtmv_p0", mvName);
    order_qt_status_init "select Status  from jobs('type'='mv') where Name='${jobName}'"
     sql """
        PAUSE MATERIALIZED VIEW JOB ON ${mvName}
    """
    order_qt_status_pause "select Status  from jobs('type'='mv') where Name='${jobName}'"
     sql """
        RESUME MATERIALIZED VIEW JOB ON ${mvName}
    """
    order_qt_status_resume "select Status  from jobs('type'='mv') where Name='${jobName}'"
    sql """
        DROP MATERIALIZED VIEW ${mvName}
    """
}
