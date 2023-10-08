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

suite("test_cast_null") {
    sql """
        drop table if exists test_table_t53;
    """

    sql """
        drop table if exists test_table_t0;
    """
    
    sql """
        CREATE TABLE `test_table_t53` (
        `c0` char(238) NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`c0`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c0`) BUCKETS 7
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `test_table_t0` (
        `c0` varchar(238) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`c0`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`c0`) BUCKETS 14
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """
        set enable_nereids_planner=false;
    """

    explain {
        sql """SELECT * FROM test_table_t53 LEFT JOIN test_table_t0 ON (('I4') LIKE (CAST(CAST(DATE '1970-05-06' AS FLOAT) AS VARCHAR) ));"""
        contains "19700506"
    }

    qt_sql1 """
        select CAST(CAST(DATE '1970-05-06' AS FLOAT) AS VARCHAR);
    """

    qt_sql2 """
        select 'abc' like null;
    """

    sql """
        set enable_nereids_planner=true;
    """

    qt_sql3 """
        select CAST(CAST(DATE '1970-05-06' AS FLOAT) AS VARCHAR);
    """

    qt_sql4 """
        select 'abc' like null;
    """

    sql """
        drop table if exists test_table_tabc;
    """

    sql """
        CREATE TABLE `test_table_tabc` (
        `k1` DECIMAL(12, 5) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """
    sql """insert into test_table_tabc values(1.0);"""

    qt_sql5 """select k1 <> '' from test_table_tabc;"""

    sql """
        drop table if exists test_table_tabc;
    """
}
