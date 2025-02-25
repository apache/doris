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

suite("test_sort_operator", "query,p0,arrow_flight_sql") {

    sql """
        DROP TABLE IF EXISTS dim_org_ful;
    """

    sql """
        CREATE TABLE `dim_org_ful` (
          `org_id` int(11) NOT NULL COMMENT '',
          `start_dt` date NOT NULL COMMENT '',
          `end_dt` date REPLACE_IF_NOT_NULL NULL COMMENT ''
        ) ENGINE=OLAP
        AGGREGATE KEY(`org_id`, `start_dt`)
        COMMENT '网点'
        DISTRIBUTED BY HASH(`org_id`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """
        DROP TABLE IF EXISTS dim_day;
    """

    sql """
        CREATE TABLE `dim_day` (
          `day_key` varchar(80) NULL,
          `day_date` date NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`day_key`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`day_key`, `day_date`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        INSERT INTO `dim_day` VALUES
            ('20231006','2023-10-06'),
            ('20231010','2023-10-10'),
            ('20230822','2023-08-22'),
            ('20230829','2023-08-29'),
            ('20230925','2023-09-25'),
            ('20230731','2023-07-31'),
            ('20230928','2023-09-28'),
            ('20230727','2023-07-27'),
            ('20230801','2023-08-01'),
            ('20231017','2023-10-17');
    """

    sql """INSERT INTO `dim_org_ful` VALUES
           (20,'2023-08-02','3000-12-31'),
           (100174,'2023-07-31','2023-08-01'),
           (100174,'2023-08-01','3000-12-31'),
           (100271,'2023-07-26','3000-12-31'),
           (100424,'2023-08-02','3000-12-31'),
           (100471,'2023-07-26','3000-12-31'),
           (100567,'2023-07-29','2023-07-30'),
           (100567,'2023-07-30','2023-07-31'),
           (100567,'2023-07-31','3000-12-31'),
           (100723,'2023-07-30','2023-07-31');"""

    sql """
        set batch_size = 9;
    """
    sql """
        set force_sort_algorithm="topn";
    """
    sql """
        set parallel_pipeline_task_num=1;
    """

    order_qt_select """
        with `dim_org` AS(
        SELECT
            `t0`.`day_date` AS `ds`,
            `org_id` AS `org_id`
        FROM
            `dim_day` t0
            INNER JOIN `dim_org_ful` t1 ON `t0`.`day_date` BETWEEN `t1`.`start_dt`
            AND `t1`.`end_dt` - 1.0
        WHERE
            `t0`.`day_date` BETWEEN '2021-01-01 00:00:00'
            AND '2023-08-07'
        )
        select org_id,null from dim_org order by 1,2 limit 1,10
    """
}
