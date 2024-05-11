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

suite("test_commit_mtmv") {
    def tableName = "test_commit_mtmv_table"
    def mvName1 = "test_commit_mtmv1"
    def mvName2 = "test_commit_mtmv2"
    def dbName = "regression_test_mtmv_p0"
    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName2};"""
    sql """drop table if exists `${tableName}`"""
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            k1 int,
            k2 int
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName1}
        BUILD DEFERRED REFRESH COMPLETE ON COMMIT
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT * FROM ${tableName};
    """
     sql """
         CREATE MATERIALIZED VIEW ${mvName2}
         BUILD DEFERRED REFRESH COMPLETE ON COMMIT
         DISTRIBUTED BY RANDOM BUCKETS 2
         PROPERTIES ('replication_num' = '1')
         AS
         SELECT * FROM ${mvName1};
     """
      sql """
         insert into ${tableName} values(1,1);
     """
    def jobName1 = getJobName(dbName, mvName1);
    waitingMTMVTaskFinished(jobName1)
    order_qt_mv1 "SELECT * FROM ${mvName1}"

    def jobName2 = getJobName(dbName, mvName2);
    waitingMTMVTaskFinished(jobName2)
    order_qt_mv2 "SELECT * FROM ${mvName2}"

}
