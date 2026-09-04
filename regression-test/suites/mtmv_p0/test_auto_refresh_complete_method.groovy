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

suite("test_auto_refresh_complete_method", "mtmv") {
    // Views are not MTMVRelatedTableIf, so the partition-sync check treats them as always
    // synchronous — the same blind spot as JDBC/external base tables, reproduced on internal objects.
    def tableName = "auto_cm_base"
    def viewName = "auto_cm_view"
    def mvName = "auto_cm_mv"
    def dbName = "regression_test_mtmv_p0"

    // Read the MV storage itself, otherwise transparent rewrite answers from the fresh base table
    // and hides exactly the staleness this suite asserts on.
    sql """SET enable_materialized_view_rewrite = false"""

    sql """drop table if exists `${tableName}`"""
    sql """drop view if exists `${viewName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            id INT,
            value INT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """INSERT INTO ${tableName} VALUES(1,10),(2,20);"""
    sql """CREATE VIEW `${viewName}` AS SELECT id, value FROM `${tableName}`;"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS
        SELECT * FROM ${viewName};
        """

    // First AUTO: the deferred MV was never refreshed and has no snapshot baseline,
    // so it must be refreshed even though the sync check sees no base-table change.
    sql """REFRESH MATERIALIZED VIEW ${dbName}.${mvName} AUTO"""
    def jobName = getJobName(dbName, mvName)
    waitingMTMVTaskFinished(jobName)
    order_qt_first_auto """SELECT * FROM ${mvName} ORDER BY id"""

    // Second AUTO: the MV already has a complete baseline and nothing changed.
    // refreshMethod=COMPLETE means AUTO must still fully refresh instead of skipping.
    sql """REFRESH MATERIALIZED VIEW ${dbName}.${mvName} AUTO"""
    jobName = getJobName(dbName, mvName)
    waitingMTMVTaskFinished(jobName)
    order_qt_second_auto_no_change """SELECT * FROM ${mvName} ORDER BY id"""

    // Third AUTO: new base rows arrive behind the view. The sync check cannot see them,
    // but refreshMethod=COMPLETE forces the refresh, so they must show up.
    sql """INSERT INTO ${tableName} VALUES(3,30),(4,40);"""
    sql """REFRESH MATERIALIZED VIEW ${dbName}.${mvName} AUTO"""
    jobName = getJobName(dbName, mvName)
    waitingMTMVTaskFinished(jobName)
    order_qt_third_auto_after_change """SELECT * FROM ${mvName} ORDER BY id"""

    order_qt_task_modes """SELECT RefreshMode, Status FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${dbName}' AND MvName = '${mvName}'
        ORDER BY CreateTime"""
}
