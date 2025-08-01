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

suite("test_upgrade_downgrade_job_mtmv","p0,mtmv,restart_fe") {
    String suiteName = "test_up_down_job_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    String mvNameCreate = "${suiteName}_mv_create"
    String mvNameRefresh = "${suiteName}_mv_refresh"
    String mvNameAlterTrigger = "${suiteName}_mv_alter_triger"
    String mvNamePause = "${suiteName}_mv_pause"
    String mvNameResume = "${suiteName}_mv_resume"
    String mvNameReplaceSwap1 = "${suiteName}_mv_swap1"
    String mvNameReplaceSwap2 = "${suiteName}_mv_swap2"
    String mvNameReplaceNoSwap1 = "${suiteName}_mv_no_swap1"
    String mvNameReplaceNoSwap2 = "${suiteName}_mv_no_swap2"
    String mvNameDrop = "${suiteName}_mv_drop"
    String mvNameEvery = "${suiteName}_mv_every"

    order_qt_create "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameCreate}' and MvDatabaseName='${dbName}';"
    order_qt_refresh "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameRefresh}' and MvDatabaseName='${dbName}';"
    sql """
            REFRESH MATERIALIZED VIEW ${mvNameRefresh} complete
        """
    waitingMTMVTaskFinishedByMvName(mvNameRefresh)
    def res = sql """select * from tasks('type'='mv') where MvName='${mvNameRefresh}' and MvDatabaseName='${dbName}'"""
    assertTrue(res.size()>0);
    order_qt_select_refresh "select * from ${mvNameRefresh}";
    order_qt_alter_trigger "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameAlterTrigger}' and MvDatabaseName='${dbName}';"
    order_qt_alter_pause "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNamePause}' and MvDatabaseName='${dbName}';"
    order_qt_alter_resume "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameResume}' and MvDatabaseName='${dbName}';"
    order_qt_replace_swap1 "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameReplaceSwap1}' and MvDatabaseName='${dbName}';"
    order_qt_replace_swap2 "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameReplaceSwap2}' and MvDatabaseName='${dbName}';"
    order_qt_replace_no_swap1 "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameReplaceNoSwap1}' and MvDatabaseName='${dbName}';"
    order_qt_replace_no_swap2 "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameReplaceNoSwap2}' and MvDatabaseName='${dbName}';"
    order_qt_drop "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameDrop}' and MvDatabaseName='${dbName}';"

    sql """drop materialized view if exists ${mvNameEvery};"""
    order_qt_every1 "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameEvery}' and MvDatabaseName='${dbName}';"
    sql """
        CREATE MATERIALIZED VIEW ${mvNameEvery}
        BUILD DEFERRED REFRESH AUTO ON manual
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    sql """
            REFRESH MATERIALIZED VIEW ${mvNameEvery} complete
        """
    waitingMTMVTaskFinishedByMvName(mvNameEvery)
    order_qt_every2 "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvNameEvery}' and MvDatabaseName='${dbName}';"
}
