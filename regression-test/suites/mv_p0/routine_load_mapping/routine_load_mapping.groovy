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

suite ("routine_load_mapping") {

    sql """ DROP TABLE IF EXISTS test; """

    sql """
        CREATE TABLE `test` (
            `event_id` varchar(50) NULL COMMENT '',
            `time_stamp` datetime NULL COMMENT '',
            `device_id` varchar(150) NULL DEFAULT "" COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`event_id`)
            DISTRIBUTED BY HASH(`device_id`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); 
        """

    sql """insert into test(event_id,time_stamp,device_id) values('ad_sdk_request','2024-03-04 00:00:00','a');"""

    createMV("""create materialized view m_view as select time_stamp, count(device_id) from test group by time_stamp;""")

    streamLoad {
        table "test"
        set 'column_separator', ','
        set 'columns', 'event_id,time_stamp,device_id,time_stamp=from_unixtime(`time_stamp`)'

        file './test'
        time 10000 // limit inflight 10s
    }

    qt_select "select * from test order by 1,2,3;"
    qt_select_mv "select * from test index m_view order by 1,2;"


     sql """ DROP TABLE IF EXISTS rt_new; """

      sql """
        CREATE TABLE `rt_new` (
  `battery_id` VARCHAR(50) NULL ,
  `create_time` DATETIME(3) NULL ,
  `imei` VARCHAR(50) NULL ,
  `event_id` VARCHAR(50) NULL ,
  `event_name` VARCHAR(50) NULL,
  `heart_type` INT NULL
) ENGINE=OLAP
DUPLICATE KEY(`battery_id`, `create_time`)
PARTITION BY RANGE(`create_time`)
(PARTITION p20240421 VALUES [('2024-04-21 00:00:00'), ('2024-04-22 00:00:00')),
PARTITION p20240422 VALUES [('2024-04-22 00:00:00'), ('2024-04-23 00:00:00')),
PARTITION p20240804 VALUES [('2024-08-04 00:00:00'), ('2024-08-05 00:00:00')))
DISTRIBUTED BY HASH(`battery_id`) BUCKETS AUTO
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"file_cache_ttl_seconds" = "0",
"is_being_synced" = "false",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.time_zone" = "Asia/Shanghai",
"dynamic_partition.start" = "-2147483648",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p",
"dynamic_partition.buckets" = "10",
"dynamic_partition.create_history_partition" = "true",
"dynamic_partition.history_partition_num" = "100",
"dynamic_partition.hot_partition_num" = "0",
"dynamic_partition.reserved_history_periods" = "NULL",
"storage_medium" = "hdd",
"storage_format" = "V2",
"inverted_index_storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false",
"enable_single_replica_compaction" = "false",
"group_commit_interval_ms" = "10000",
"group_commit_data_bytes" = "134217728"
);
        """

         createMV("""CREATE MATERIALIZED VIEW location_rt_mv AS
        SELECT
        battery_id,
        create_time
        FROM
        rt_new
        WHERE
        heart_type = 1
        ;""")

    sql """ ALTER TABLE rt_new MODIFY COLUMN event_id VARCHAR(51) NULL;"""
    Thread.sleep(1000)

    streamLoad {
        table "rt_new"
        set 'column_separator', ','
        set 'columns', '`battery_id`,`create_time`,`imei`,`event_id`,`event_name`,`heart_type`'

        file './test2'
        time 10000 // limit inflight 10s
    }
}
