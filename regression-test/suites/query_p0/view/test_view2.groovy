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

suite("test_view2") {
    sql "DROP TABLE IF EXISTS tableA"
    sql "DROP TABLE IF EXISTS tableB"
    sql """
    CREATE TABLE `tableB` (
     `deviceType` varchar(100) NULL ,
     `acceptTime` bigint(20) NULL ,
     `deviceNo` varchar(50) NULL ,
     `pointName` varchar(200) NULL ,
     `dasValue` varchar(200) NULL ,
     `quality` varchar(30) NULL,
     `timestamp` bigint(20) NULL ,
     `collect_time` datetime NULL ,
     `etl_time` datetime NULL 
        ) ENGINE=OLAP
        DUPLICATE KEY(`deviceType`)
        DISTRIBUTED BY HASH(`deviceType`) BUCKETS 1
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
        CREATE TABLE `tableA` (
        `node_name` varchar(100) NULL ,
        `business_name` varchar(50) NULL ,
        `business_sn` varchar(50) NULL ,
        `area_id` varchar(10) NULL ,
        `group_id` varchar(10) NULL ,
        `node_value` varchar(30) NULL,
        `devices_sn` varchar(30) NULL ,
        `node_note` varchar(100) NULL ,
        `create_time` datetime NULL ,
        `etl_time` datetime NULL 
        ) ENGINE=OLAP
        DUPLICATE KEY(`node_name`)
        DISTRIBUTED BY HASH(`node_name`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """ insert into tableA values ("a","a","a","a","a","a","a","a","2023-09-21 17:02:21","2023-09-21 17:02:21"); """
    sql """ insert into tableB values ("a",1,"a","AirSupply_Humdt","a","a",2,"2023-09-21 17:02:21","2023-09-21 17:02:21"); """
    sql """ insert into tableB values ("a",1,"a","AirSupply_Humdt","a","a",2,"2023-09-21 17:02:21","2023-09-21 17:02:21"); """
    sql """sync"""

    sql """ DROP VIEW IF EXISTS rt_view;"""
    sql """ DROP VIEW IF EXISTS tv;"""

    sql """
    create view tv (node comment "col node") as select node_name from tableA;
    """

    sql """
    CREATE VIEW rt_view AS
    SELECT
    node_name,
        create_time AS create_time,
        etl_time AS etl_time
    FROM
        (
            SELECT
                tableA.node_name AS node_name,
                tableA.create_time AS create_time,
                tableA.etl_time AS etl_time,
                row_number() OVER (
                    PARTITION BY node_name
                    ORDER BY
                        create_time DESC
                ) AS row_num
            FROM
                tableA
            WHERE
                create_time > hours_sub("2023-09-21 17:02:21", 3)
        ) t1
    WHERE
        t1.row_num = 1
    union
    all
    SELECT
        'QQQQQ' AS node_name,
        collect_time AS create_time,
        cast("2023-09-21 17:02:21" as datetime) AS etl_time
    FROM
        (
            select
                pointName,
                dasValue,
                collect_time,
                row_number() OVER (
                    PARTITION BY pointName
                    ORDER BY
                        collect_time DESC
                ) AS row_num
            from
                tableB
            where
                pointName = 'AirSupply_Humdt'
                and collect_time > hours_sub("2023-09-21 17:02:21", 3)
        ) t1
    WHERE
        t1.row_num = 1
    union
    all
    SELECT
        'BBBB' AS node_name,
        collect_time AS create_time,
        cast("2023-09-21 17:02:21" as datetime) AS etl_time
    FROM
        (
            select
                pointName,
                dasValue,
                collect_time,
                row_number() OVER (
                    PARTITION BY pointName
                    ORDER BY
                        collect_time DESC
                ) AS row_num
            from
                tableB
            where
                pointName = 'AirSupply_Humdt'
                and collect_time > hours_sub("2023-09-21 17:02:21", 3)
        ) t1
    WHERE
        t1.row_num = 1;
    """

    qt_select5 """ select /*+SET_VAR(experimental_enable_nereids_planner=true) */ * from rt_view where node_name like '%a%' order by 1; """

}