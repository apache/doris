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

suite("test_base_recreate_col_mtmv","mtmv") {
    String dbName = context.config.getDbNameByFile(context.file)
    String suiteName = "test_base_recreate_col_mtmv"
    String tableName1 = "${suiteName}_table1"
    String mvName1 = "${suiteName}_mv1"
    sql """drop table if exists `${tableName1}`"""
    sql """drop materialized view if exists ${mvName1};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 INT,
            k2 varchar(32),
            k3 varchar(32)
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
            INSERT INTO ${tableName1} VALUES(1,"a","a2");
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName1}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY hash(k1) BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName1};
        """
    sql """
            REFRESH MATERIALIZED VIEW ${mvName1} auto
        """
    waitingMTMVTaskFinishedByMvName(mvName1)

    // drop column
    sql """
        alter table ${tableName1} drop COLUMN k3;
        """
    assertEquals("FINISHED", getAlterColumnFinalState("${tableName1}"))
    // recreate column
    sql """
        alter table ${tableName1} add COLUMN k3 varchar(32);
        """
    assertEquals("FINISHED", getAlterColumnFinalState("${tableName1}"))

    order_qt_recreate_col_t1_mv1 "select Name,State,RefreshState,SyncWithBaseTables  from mv_infos('database'='${dbName}') where Name='${mvName1}'"

    sql """
            REFRESH MATERIALIZED VIEW ${mvName1} auto
        """
    waitingMTMVTaskFinishedByMvName(mvName1)
    order_qt_refresh_mv1 "select Name,State,RefreshState,SyncWithBaseTables  from mv_infos('database'='${dbName}') where Name='${mvName1}'"
}
