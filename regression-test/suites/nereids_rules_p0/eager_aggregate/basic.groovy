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

suite("eager_aggregate_basic") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql "SET ENABLE_NEREIDS_RULES=push_down_min_max_through_join"
    sql "SET ENABLE_NEREIDS_RULES=push_down_sum_through_join"
    sql "SET ENABLE_NEREIDS_RULES=push_down_count_through_join"

    sql """
        DROP TABLE IF EXISTS shunt_log_com_dd_library;
    """
    sql """
        DROP TABLE IF EXISTS com_dd_library;
    """

    sql"""
    CREATE TABLE `shunt_log_com_dd_library` (
    `device_id` varchar(255) NOT NULL,
    `experiment_id` varchar(255) NOT NULL,
    `group_id` varchar(255) NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`device_id`)
    DISTRIBUTED BY HASH(`device_id`) BUCKETS 4
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql"""
    CREATE TABLE `com_dd_library` (
    `event_id` varchar(255) NULL,
    `device_id` varchar(255) NULL DEFAULT "",
    `time_stamp` datetime NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`event_id`)
    DISTRIBUTED BY HASH(`device_id`) BUCKETS 4
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    qt_1 """
    explain shape plan 
    select
        b.group_id,
        COUNT(a.event_id)
    from
            com_dd_library a
    join shunt_log_com_dd_library b on
            a.device_id = b.device_id
    where
            a.event_id = "ad_click"
            and b.experiment_id = 37
    group by
            b.group_id;
    """

    qt_2 """
    explain shape plan 
    select
            a.event_id,
            b.experiment_id,
            b.group_id,
            COUNT(a.event_id)
    from
            com_dd_library a
    join shunt_log_com_dd_library b on
            a.device_id = b.device_id
    where
            b.experiment_id = 73
    group by
            b.group_id,
            b.experiment_id,
            a.event_id;
    """

    qt_3 """
    explain shape plan 
    select
            a.event_id,
            b.experiment_id,
            b.group_id,
            COUNT(a.event_id),
            date_format(a.time_stamp, '%Y-%m-%d') as dayF
    from
            com_dd_library a
    join shunt_log_com_dd_library b on
            a.device_id = b.device_id
    where
            b.experiment_id = 73
    group by
            b.group_id,
            b.experiment_id,
            a.event_id,
            dayF;
    """

    qt_4 """
    explain shape plan 
    select
        a.event_id,
        b.experiment_id,
        b.group_id,
        COUNT(a.event_id)
    from
            com_dd_library a
    join shunt_log_com_dd_library b on
            a.device_id = b.device_id
    group by
            b.group_id,
            b.experiment_id,
            a.event_id;
    """
}
