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

suite("create_view_with_aggregation") {
    sql "set enable_fallback_to_original_planner = false;"

    sql """
        DROP TABLE IF EXISTS `test_tbl_src_01`;
        """
    sql """
        CREATE TABLE `test_tbl_src_01` (
            `dt` date NULL,
            `k4` varchar(120) NULL,
            `code2` varchar(120) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`dt`, `k4`)
        DISTRIBUTED BY HASH(`dt`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V1",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728"
        );
        """
    sql """
        INSERT INTO `test_tbl_src_01` VALUES ('2025-03-01','ABC123','1234567');
        """
    sql """
        DROP TABLE IF EXISTS `test_tbl_src_02`;
        """
    sql """
        CREATE TABLE `test_tbl_src_02` (
            `t1` varchar(120) NULL,
            `k1` varchar(120) NULL,
            `k2` varchar(120) NULL,
            `d1` decimal(19,4) NULL,
            `k3` varchar(120) NULL,
            `dt` date NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`t1`)
        DISTRIBUTED BY HASH(`t1`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V1",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728"
        );
        """
    sql """
        INSERT INTO `test_tbl_src_02` VALUES ('2025-03-01 10:04:25','ABC123','1234567',1.0000,'Doris001','2025-03-01');
        """
    sql """
        drop view if exists test_date_error;
        """
    sql """
        CREATE VIEW test_date_error
        AS
        SELECT dt AS `日期`, k4 AS `编码1`, code2 AS `编码2`, cnt AS `数量`
        FROM (
        SELECT dt, k4, code2, cnt
        FROM (
            SELECT aa.dt, aa.k4, aa.code2, cnt
            FROM test_tbl_src_01 aa
            LEFT JOIN (
                SELECT date(t1) AS t1, k1, k2
                , SUM(d1) AS cnt
                FROM test_tbl_src_02
                WHERE k3 IN ('Doris001', 'Doris001')
                AND dt >= date_sub(date(now()), INTERVAL 6 MONTH)
                GROUP BY date(t1), k1, k2
            ) cc
            ON aa.dt = cc.t1
                AND aa.k4 = cc.k1
                AND aa.code2 = cc.k2
        ) ee
        ) ff;
        """
    sql """
        select * from test_date_error;
        """
    
    sql """
        drop view if exists test_date_error;
        """
    sql """
        DROP TABLE IF EXISTS `test_tbl_src_01`;
        """
    sql """
        DROP TABLE IF EXISTS `test_tbl_src_02`;
        """
}