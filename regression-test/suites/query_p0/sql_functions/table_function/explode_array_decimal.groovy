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

suite("explode_array_decimal") {
    sql "DROP TABLE IF EXISTS ods_device_data_1d_inc;"

    sql """
    CREATE TABLE `ods_device_data_1d_inc` (
                                              `id` INT NULL,
                                              `electricityPrice` VARCHAR(5000) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`, `electricityPrice`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "min_load_replica_num" = "-1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "group_commit_interval_ms" = "10000"
    );"""

    sql """insert into ods_device_data_1d_inc values(1, "[0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,0.8,1,9,5,5,5,5,5,5,5,5,5,0.8,0.8,0.8,0.8,0.8]")"""

    sql "SET enable_nereids_planner=false;"

    qt_sql_old_planner """
    SELECT * from
        (
            select
                e1,t.electricityPrice
            from
                ods_device_data_1d_inc as t
                lateral view explode(cast (electricityPrice as ARRAY<DECIMAL(10,3)>)) tmp1 as e1
        ) kk limit 1
        """

    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_planner=true'

    qt_sql_nereid """
        SELECT * from
            (
                select
                    e1,t.electricityPrice
                from
                    ods_device_data_1d_inc as t
                    lateral view explode(cast (electricityPrice as ARRAY<DECIMAL(10,3)>)) tmp1 as e1
            ) kk limit 1
            """
}
