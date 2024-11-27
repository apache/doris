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
        PARTITION BY LIST(`k1`)
        (
            PARTITION `p1` VALUES IN ('1'),
            PARTITION `p2` VALUES IN ('2')
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
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT * FROM ${tableName};
    """
     sql """
        REFRESH MATERIALIZED VIEW ${mv1} AUTO
    """
    waitingMTMVTaskFinishedByMvName(mv1)
    order_qt_mv1 "select * from ${mv1}"

    sql """
        CREATE MATERIALIZED VIEW ${mv2}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT * FROM ${mv1};
    """
     sql """
        REFRESH MATERIALIZED VIEW ${mv2} AUTO
    """
    waitingMTMVTaskFinishedByMvName(mv2)
    order_qt_mv2 "select * from ${mv2}"

    sql """
            INSERT INTO ${tableName} VALUES(2,2);
        """
    sql """
           REFRESH MATERIALIZED VIEW ${mv1} AUTO
       """
    waitingMTMVTaskFinishedByMvName(mv1)
    order_qt_mv1_should_one_partition "select NeedRefreshPartitions from tasks('type'='mv') where MvName = '${mv1}' order by CreateTime desc limit 1"
    sql """
           REFRESH MATERIALIZED VIEW ${mv2} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mv2)
    order_qt_mv2_should_one_partition "select NeedRefreshPartitions from tasks('type'='mv') where MvName = '${mv2}' order by CreateTime desc limit 1"

    // insert into p2 again, check partition version if change
    sql """
            INSERT INTO ${tableName} VALUES(2,3);
        """
    sql """
           REFRESH MATERIALIZED VIEW ${mv1} AUTO
       """
    waitingMTMVTaskFinishedByMvName(mv1)
    order_qt_mv1_should_one_partition_again "select NeedRefreshPartitions from tasks('type'='mv') where MvName = '${mv1}' order by CreateTime desc limit 1"
    sql """
           REFRESH MATERIALIZED VIEW ${mv2} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mv2)
    order_qt_mv2_should_one_partition_again "select NeedRefreshPartitions from tasks('type'='mv') where MvName = '${mv2}' order by CreateTime desc limit 1"
    order_qt_mv2_again "select * from ${mv2}"

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
