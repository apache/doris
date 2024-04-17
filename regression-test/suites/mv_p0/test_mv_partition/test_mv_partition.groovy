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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_mv_partition") {
    sql """set enable_nereids_planner=true;"""
    sql """set enable_fallback_to_original_planner=false;"""
    sql """ DROP TABLE IF EXISTS chh; """

    sql """ 
        CREATE TABLE `chh` (
        `event_id` varchar(50) NULL COMMENT '',
        `time_stamp` datetime NULL COMMENT '',
        `device_id` varchar(150) NULL DEFAULT "" COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`event_id`)
        PARTITION BY RANGE(`time_stamp`)
        (
            PARTITION p1 VALUES LESS THAN ("2023-01-01"),
            PARTITION p2 VALUES LESS THAN ("2024-01-01"),
            PARTITION p3 VALUES LESS THAN ("2025-01-01")
        )
        DISTRIBUTED BY HASH(`device_id`) BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        ); 
        """

    sql """insert into chh(event_id,time_stamp,device_id) values('ad_sdk_request','2024-03-04 00:00:00','a');"""

    createMV("create materialized view m_view as select to_date(time_stamp),count(device_id) from chh group by to_date(time_stamp);")

    sql """insert into chh(event_id,time_stamp,device_id) values('ad_sdk_request','2024-03-04 00:00:00','a');"""
    
    qt_select_mv "select * from chh index m_view where `mv_to_date(time_stamp)` = '2024-03-04';"
}
