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

suite("test_upgrade_downgrade_prepare_job_mtmv","p0,mtmv,restart_fe") {
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

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvNameCreate};"""
    sql """drop materialized view if exists ${mvNameRefresh};"""
    sql """drop materialized view if exists ${mvNameAlterTrigger};"""
    sql """drop materialized view if exists ${mvNamePause};"""
    sql """drop materialized view if exists ${mvNameResume};"""
    sql """drop materialized view if exists ${mvNameReplaceSwap1};"""
    sql """drop materialized view if exists ${mvNameReplaceSwap2};"""
    sql """drop materialized view if exists ${mvNameReplaceNoSwap1};"""
    sql """drop materialized view if exists ${mvNameReplaceNoSwap2};"""
    sql """drop materialized view if exists ${mvNameDrop};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        insert into ${tableName} values (1,1);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvNameCreate}
        BUILD DEFERRED REFRESH AUTO ON manual
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvNameRefresh}
        BUILD DEFERRED REFRESH AUTO ON manual
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    sql """
            REFRESH MATERIALIZED VIEW ${mvNameRefresh} complete
        """
    waitingMTMVTaskFinishedByMvName(mvNameRefresh)


    sql """
        CREATE MATERIALIZED VIEW ${mvNameAlterTrigger}
        BUILD DEFERRED REFRESH AUTO ON manual
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    sql """
        alter MATERIALIZED VIEW ${mvNameAlterTrigger} REFRESH AUTO ON SCHEDULE EVERY 1 DAY
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvNamePause}
        BUILD DEFERRED REFRESH AUTO ON manual
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

     sql """
        pause MATERIALIZED VIEW job on ${mvNamePause};
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvNameResume}
        BUILD DEFERRED REFRESH AUTO ON manual
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

     sql """
        pause MATERIALIZED VIEW job on ${mvNameResume};
        """

    sql """
        resume MATERIALIZED VIEW job on ${mvNameResume};
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvNameReplaceSwap1}
        BUILD DEFERRED REFRESH AUTO ON manual
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvNameReplaceSwap2}
        BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 1 DAY
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

    sql """
        alter MATERIALIZED VIEW ${mvNameReplaceSwap1} replace with  MATERIALIZED VIEW ${mvNameReplaceSwap2} PROPERTIES('swap' = 'true');
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvNameReplaceNoSwap1}
        BUILD DEFERRED REFRESH AUTO ON manual
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvNameReplaceNoSwap2}
        BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 1 DAY
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

    sql """
        alter MATERIALIZED VIEW ${mvNameReplaceNoSwap1} replace with  MATERIALIZED VIEW ${mvNameReplaceNoSwap2} PROPERTIES('swap' = 'false');
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvNameDrop}
        BUILD DEFERRED REFRESH AUTO ON manual
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    sql """drop materialized view if exists ${mvNameDrop};"""
}
