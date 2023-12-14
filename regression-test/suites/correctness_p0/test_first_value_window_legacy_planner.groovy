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

suite("test_first_value_window_legacy_planner") {
    sql """ set enable_nereids_planner = false; """

    def tableName = "test_first_value_window_state_legacy_planner"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            `myday` INT,
            `time_col` VARCHAR(40) NOT NULL,
            `state` INT
            ) ENGINE=OLAP
            DUPLICATE KEY(`myday`,time_col,state)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`myday`) BUCKETS 2
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    sql """ INSERT INTO ${tableName} VALUES
            (21,"04-21-11",1),
            (22,"04-22-10-21",0),
            (22,"04-22-10-21",1),
            (23,"04-23-10",1),
            (24,"02-24-10-21",1); """

    qt_select_default """ select *,first_value(state) over(partition by myday order by time_col range between current row and unbounded following) from ${tableName} order by myday, time_col, state; """


    def tableName1 = "test_first_value_window_array_legacy_planner"

    sql """ DROP TABLE IF EXISTS ${tableName1} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName1} (
            `myday` INT,
            `time_col` VARCHAR(40) NOT NULL,
            `state` ARRAY<STRING>
            ) ENGINE=OLAP
            DUPLICATE KEY(`myday`,time_col)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`myday`) BUCKETS 2
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    sql """ INSERT INTO ${tableName1} VALUES
            (21,"04-21-11",["amory", "clever"]),
            (22,"04-22-10-21",["is ", "cute", "tea"]),
            (22,"04-22-10-21",["doris", "aws", "greate"]),
            (23,"04-23-10", ["p7", "year4"]),
            (24,"02-24-10-21",[""]); """

    qt_select_default """ select *,first_value(state) over(partition by myday order by time_col range between current row and unbounded following) from ${tableName1} order by myday, time_col; """

    qt_select_always_nullable """
        select
            *,
            first_value(1) over(partition by myday order by time_col rows  between 1 preceding and 1 preceding) first_value,
            last_value(999) over(partition by myday order by time_col rows  between 1 preceding and 1 preceding) last_value
        from test_first_value_window_array_legacy_planner order by myday, time_col;
    """

    def tableName2 = "test_first_value_window_state_not_null_legacy_planner"

    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
            `myday` INT,
            `time_col` VARCHAR(40) NOT NULL,
            `state` INT NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`myday`,time_col,state)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`myday`) BUCKETS 2
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    sql """ INSERT INTO ${tableName2} VALUES
            (21,"04-21-11",1),
            (22,"04-22-10-21",0),
            (22,"04-22-10-21",1),
            (23,"04-23-10",1),
            (24,"02-24-10-21",1); """

    qt_select_default2 """
        select *
            ,first_value(state) over(partition by `myday` order by `time_col`) v1
            ,first_value(state, 0) over(partition by `myday` order by `time_col`) v2
            ,first_value(state, 1) over(partition by `myday` order by `time_col`) v3
        from ${tableName2} order by `myday`, `time_col`, `state`;
    """

    def tableName3 = "test_first_value_window_state_ignore_null_legacy_planner"

    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName3} (
                `id` INT,
                `myday` INT,
                `time_col` VARCHAR(40) NOT NULL,
                `state` INT
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`, `myday`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`, `myday`) BUCKETS 4
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    sql """ INSERT INTO ${tableName3} VALUES
            (1,21,"04-21-11",null),
            (2,21,"04-21-12",2),
            (3,21,"04-21-13",3),
            (4,22,"04-22-10-21",null),
            (5,22,"04-22-10-22",null),
            (6,22,"04-22-10-23",5),
            (7,22,"04-22-10-24",null),
            (8,22,"04-22-10-25",9),
            (9,23,"04-23-11",null),
            (10,23,"04-23-12",10),
            (11,23,"04-23-13",null),
            (12,24,"02-24-10-21",null); """

    qt_select_default3 """
        select *
            ,first_value(`state`) over(partition by `myday` order by `time_col` rows between 1 preceding and 1 following) v1
            ,first_value(`state`, 0) over(partition by `myday` order by `time_col` rows between 1 preceding and 1 following) v2
            ,first_value(`state`, 1) over(partition by `myday` order by `time_col` rows between 1 preceding and 1 following) v3
        from ${tableName3} order by `id`, `myday`, `time_col`;
    """
}
