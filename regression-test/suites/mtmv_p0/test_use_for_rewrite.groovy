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

suite("test_use_for_rewrite","mtmv") {
    String suiteName = "test_use_for_rewrite"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String db = context.config.getDbNameByFile(context.file)
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k1 TINYINT,
            k2 INT not null,
            k3 DATE NOT NULL
        )
        COMMENT "my first table"
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
     sql """
        insert into ${tableName} values(1,1, '2024-05-01'),(2,2, '2024-05-02'),(3,3, '2024-05-03'),
        (1,1, '2024-05-01'),(2,2, '2024-05-02'), (3,3, '2024-05-03');
        """

    // when not set use_for_rewrite, should rewrite successfully
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT k1, k2, count(*) from ${tableName} group by k1, k2;
        """

    waitingMTMVTaskFinished(getJobName(db, mvName))
    mv_rewrite_success_without_check_chosen("""
    SELECT k2, count(*) from ${tableName} group by k2;
    """ ,mvName)
    sql """drop materialized view if exists ${mvName};"""


    // when set use_for_rewrite = true, should rewrite successfully
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'use_for_rewrite' = 'true'
        )
        AS
        SELECT k1, k2, count(*) from ${tableName} group by k1, k2;
        """

    waitingMTMVTaskFinished(getJobName(db, mvName))
    mv_rewrite_success_without_check_chosen("""
    SELECT k2, count(*) from ${tableName} group by k2;
    """ ,mvName)


    // when set use_for_rewrite = false, should not partition in rewritten
    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'use_for_rewrite' = 'false'
        )
        AS
        SELECT k1, k2, count(*) from ${tableName} group by k1, k2;
        """

    waitingMTMVTaskFinished(getJobName(db, mvName))
    mv_not_part_in("""
    SELECT k2, count(*) from ${tableName} group by k2;
    """ ,mvName)

    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists `${tableName}`"""
}
