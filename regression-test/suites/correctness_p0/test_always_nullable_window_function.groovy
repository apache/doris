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

suite("test_always_nullable_window_function") {
    sql """ set enable_nereids_planner = true; """
    sql """ set enable_fallback_to_original_planner = false; """
    def tableName = "test_always_nullable_window_function_table"
    def nullableTableName = "test_always_nullable_window_function_table"

    sql "set enable_nereids_planner = 1"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `myday` INT,
            `time_col` VARCHAR(40) NOT NULL,
            `state` INT not null
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

    sql """ DROP TABLE IF EXISTS ${nullableTableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${nullableTableName} (
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

    sql """ INSERT INTO ${nullableTableName} VALUES
            (21,"04-21-11",1),
            (22,"04-22-10-21",0),
            (22,"04-22-10-21",1),
            (23,"04-23-10",1),
            (24,"02-24-10-21",1); """

    qt_select_default """
        select *,
            first_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) f_value,
            last_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) l_value,
            sum(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) sum_value,
            avg(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) avg_value,
            max(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) max_value,
            min(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) min_value,
            lag(state, 1, null) over (partition by myday order by time_col) lag_value,
            lead(state, 1, null) over (partition by myday order by time_col) lead_value
        from ${tableName} order by myday, time_col, state;
    """
    qt_select_empty_window """
        select *,
            first_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) f_value,
            last_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) l_value,
            sum(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) sum_value,
            avg(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) avg_value,
            max(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) max_value,
            min(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) min_value,
            lag(state, 2, null) over (partition by myday order by time_col) lag_value,
            lead(state, 2, null) over (partition by myday order by time_col) lead_value
        from ${tableName} order by myday, time_col, state;
    """

    qt_select_default_nullable """
        select *,
            first_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) f_value,
            last_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) l_value,
            sum(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) sum_value,
            avg(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) avg_value,
            max(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) max_value,
            min(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) min_value,
            lag(state, 1, null) over (partition by myday order by time_col) lag_value,
            lead(state, 1, null) over (partition by myday order by time_col) lead_value
        from ${nullableTableName} order by myday, time_col, state;
    """
    qt_select_empty_window_nullable """
        select *,
            first_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) f_value,
            last_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) l_value,
            sum(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) sum_value,
            avg(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) avg_value,
            max(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) max_value,
            min(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) min_value,
            lag(state, 2, null) over (partition by myday order by time_col) lag_value,
            lead(state, 2, null) over (partition by myday order by time_col) lead_value
        from ${nullableTableName} order by myday, time_col, state;
    """

    sql "set enable_nereids_planner = 0"

    qt_select_default_old_planer """
        select *,
            first_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) f_value,
            last_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) l_value,
            sum(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) sum_value,
            avg(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) avg_value,
            max(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) max_value,
            min(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) min_value,
            lag(state, 1, null) over (partition by myday order by time_col) lag_value,
            lead(state, 1, null) over (partition by myday order by time_col) lead_value
        from ${tableName} order by myday, time_col, state;
    """
    qt_select_empty_window_old_planer """
        select *,
            first_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) f_value,
            last_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) l_value,
            sum(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) sum_value,
            avg(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) avg_value,
            max(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) max_value,
            min(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) min_value,
            lag(state, 2, null) over (partition by myday order by time_col) lag_value,
            lead(state, 2, null) over (partition by myday order by time_col) lead_value
        from ${tableName} order by myday, time_col, state;
    """

    qt_select_default_nullable_old_planer """
        select *,
            first_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) f_value,
            last_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) l_value,
            sum(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) sum_value,
            avg(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) avg_value,
            max(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) max_value,
            min(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 following) min_value,
            lag(state, 1, null) over (partition by myday order by time_col) lag_value,
            lead(state, 1, null) over (partition by myday order by time_col) lead_value
        from ${nullableTableName} order by myday, time_col, state;
    """
    qt_select_empty_window_nullable_old_planer """
        select *,
            first_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) f_value,
            last_value(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) l_value,
            sum(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) sum_value,
            avg(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) avg_value,
            max(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) max_value,
            min(state) over(partition by myday order by time_col rows BETWEEN 1 preceding AND 1 preceding) min_value,
            lag(state, 2, null) over (partition by myday order by time_col) lag_value,
            lead(state, 2, null) over (partition by myday order by time_col) lead_value
        from ${nullableTableName} order by myday, time_col, state;
    """
    sql "set enable_nereids_planner = true"
    qt_select_lead_lag """
        select lag(state, 0, 22) over (partition by myday order by time_col) lag_value,
               lead(state, 0, 33) over (partition by myday order by time_col) lead_value,
               state,time_col,myday
               from ${nullableTableName} order by myday, time_col, state;
    """
}
