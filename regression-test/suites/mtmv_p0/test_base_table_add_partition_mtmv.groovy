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

suite("test_base_table_add_partition_mtmv", "mtmv") {
    def tableName = "test_base_table_add_partition_mtmv_table"
    def mvName = "test_base_table_add_partition_mtmv_mv"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName}"""

    sql """
        CREATE TABLE `${tableName}` (
            k1 int,
            k2 int
        )
        PARTITION BY LIST(`k1`)
        (
            PARTITION `p1` VALUES IN ('1'),
            PARTITION `p2` VALUES IN ('2')
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """insert into ${tableName} values(1,1);"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(k1)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT * FROM ${tableName};
        """
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_baseline "select * from ${mvName}"

    // A new base partition makes the refresh add the matching MV partition, which has no snapshot yet.
    // Only that partition may be refreshed: reading it as a lost refresh baseline would rebuild every
    // partition of the MV, including the partitions that are already in sync.
    sql """alter table ${tableName} add partition p3 values in ('3');"""
    sql """insert into ${tableName} values(3,3);"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_need_refresh_partitions "select NeedRefreshPartitions from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1"
    order_qt_refresh_mode "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1"
    order_qt_after_add_partition "select * from ${mvName}"

    // The added partition is in sync now, so a refresh without base table changes has nothing to do.
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_mode_no_change "select RefreshMode from tasks('type'='mv') where MvName='${mvName}' order by CreateTime desc limit 1"

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists `${tableName}`"""
}
