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

suite("test_base_replace_on_commit_mtmv","mtmv") {
    String dbName = context.config.getDbNameByFile(context.file)
    String suiteName = "test_base_replace_on_commit_mtmv"
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String mvName = "${suiteName}_mv"

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 INT,
            k2 INT
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
            INSERT INTO ${tableName1} VALUES(1,1);
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 INT,
            k2 INT
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
            INSERT INTO ${tableName2} VALUES(10,10);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON COMMIT
        DISTRIBUTED BY hash(k1) BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName1};
        """
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_select_init "select * from ${mvName}"

    // replace
    sql """
        ALTER TABLE ${tableName1} REPLACE WITH TABLE ${tableName2} PROPERTIES('swap' = 'true');
        """
    order_qt_replace "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"

    sql """
            INSERT INTO ${tableName1} VALUES(2,2);
        """

    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_replace_normal "select Name,State,RefreshState  from mv_infos('database'='${dbName}') where Name='${mvName}'"
    order_qt_select_replace "select * from ${mvName}"

    // t2 should not trigger refresh
    order_qt_before_trigger "select count(*)  from tasks('type'='mv') where MvName='${mvName}'"
    sql """
             INSERT INTO ${tableName2} VALUES(20,20);
         """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_after_trigger "select count(*)  from tasks('type'='mv') where MvName='${mvName}'"
}
