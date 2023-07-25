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

suite("map_agg") {
    sql "DROP TABLE IF EXISTS `test_map_agg`;"
    sql """
        CREATE TABLE `test_map_agg` (
            `id` int(11) NOT NULL,
            `label_name` varchar(32) NOT NULL,
            `value_field` string
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
     """

    sql """
        insert into `test_map_agg` values
            (1, "LA", "V1_1"),
            (1, "LB", "V1_2"),
            (1, "LC", "V1_3"),
            (2, "LA", "V2_1"),
            (2, "LB", "V2_2"),
            (2, "LC", "V2_3"),
            (3, "LA", "V3_1"),
            (3, "LB", "V3_2"),
            (3, "LC", "V3_3"),
            (4, "LA", "V4_1"),
            (4, "LB", "V4_2"),
            (4, "LC", "V4_3"),
            (5, "LA", "V5_1"),
            (5, "LB", "V5_2"),
            (5, "LC", "V5_3");
    """

    sql "DROP TABLE IF EXISTS test_map_agg_nullable;"
    sql """
        CREATE TABLE `test_map_agg_nullable` (
             `id` int(11) NOT NULL,
             `label_name` varchar(32) NULL,
             `value_field` string
          ) ENGINE=OLAP
          DUPLICATE KEY(`id`)
          COMMENT 'OLAP'
          DISTRIBUTED BY HASH(`id`) BUCKETS 2
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "storage_format" = "V2",
          "light_schema_change" = "true",
          "disable_auto_compaction" = "false",
          "enable_single_replica_compaction" = "false"
          );
     """
    sql """
        insert into `test_map_agg_nullable` values
            (1, "LA", "V1_1"),
            (1, "LB", "V1_2"),
            (1, "LC", null),
            (2, "LA", "V2_1"),
            (2,  null, "V2_2"),
            (2, "LC", "V2_3"),
            (3, "LA", "V3_1"),
            (3, "LB", "V3_2"),
            (3, "LC", "V3_3"),
            (4, "LA", "V4_1"),
            (4, "LB", "V4_2"),
            (4, null, null),
            (5, "LA", "V5_1"),
            (5, "LB", "V5_2"),
            (5, "LC", "V5_3");
     """

    sql "DROP TABLE IF EXISTS `test_map_agg_string_key`;"
    sql """
        CREATE TABLE `test_map_agg_numeric_key` (
            `id` int(11) NOT NULL,
            `label_name` bigint NOT NULL,
            `value_field` string
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
      """

    sql """
         insert into `test_map_agg_numeric_key` values
             (1, 1, "V1_1"),
             (1, 9223372036854775807, "V1_2"),
             (1, 22000000000, "V1_3"),
             (2, 1, "V2_1"),
             (2, 9223372036854775807, "V2_2"),
             (2, 22000000000, "V2_3"),
             (3, 1, "V3_1"),
             (3, 9223372036854775807, "V3_2"),
             (3, 22000000000, "V3_3"),
             (4, 1, "V4_1"),
             (4, 9223372036854775807, "V4_2"),
             (4, 22000000000, "V4_3"),
             (5, 1, "V5_1"),
             (5, 9223372036854775807, "V5_2"),
             (5, 22000000000, "V5_3");
     """

    qt_sql1 """
        WITH `labels` as (
            SELECT `id`, map_agg(`label_name`, `value_field`) m FROM test_map_agg GROUP BY `id`
        )
        SELECT
            id,
            m['LA'] LA,
            m['LB'] LB,
            m['LC'] LC
        FROM `labels`
        ORDER BY `id`;
     """

    qt_sql2 """
        WITH `labels` as (
            SELECT `id`, map_agg(`label_name`, `value_field`) m FROM test_map_agg_nullable GROUP BY `id`
        )
        SELECT
            id,
            m['LA'] LA,
            m['LB'] LB,
            m['LC'] LC
        FROM `labels`
        ORDER BY `id`;
     """

    qt_sql2 """
        WITH `labels` as (
            SELECT `id`, map_agg(`label_name`, `value_field`) m FROM test_map_agg_numeric_key GROUP BY `id`
        )
        SELECT
            id,
            m[1] LA,
            m[9223372036854775807] LB,
            m[22000000000] LC
        FROM `labels`
        ORDER BY `id`;
    """

     sql "DROP TABLE `test_map_agg`"
     sql "DROP TABLE `test_map_agg_nullable`"
     sql "DROP TABLE `test_map_agg_numeric_key`"
 }
