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

suite("test_lead_lag_nullable") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """DROP TABLE IF EXISTS `test_lead_lag_ttt`; """
    sql """ 
        CREATE TABLE `test_lead_lag_ttt` (
        `wtid` varchar(72) NOT NULL  ,
        `rectime` datetime NOT NULL  ,
        `id` bigint NOT NULL  ,
        `if_delete` tinyint NOT NULL DEFAULT "0" ,
        `endtime` datetime NULL ,
        `std_state_code` int NULL ,
        `stop_reason_code` int NULL ,
        `remark` varchar(100) NULL,
        `if_raw` tinyint NULL DEFAULT "1" ,
        `create_time` datetime NULL ,
        `update_time` datetime NULL ,
        `uuid` varchar(50) NOT NULL 
        ) ENGINE=OLAP
        UNIQUE KEY(`wtid`, `rectime`, `id`, `if_delete`)
        DISTRIBUTED BY HASH(`wtid`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"); 
    """
    sql """INSERT INTO test_lead_lag_ttt 
            (wtid, rectime, id, if_delete, endtime, std_state_code, stop_reason_code, remark, if_raw, create_time, update_time, uuid)
            VALUES
            ('device001', '2024-11-15 08:00:00', 1, 0, NULL, 100, 5, '备注信息1', 1, '2024-11-15 08:10:00', '2024-11-15 08:20:00', 'uuid-0001'),
            ('device001', '2024-11-15 09:00:00', 2, 0, NULL, 101, 10, '备注信息2', 1, '2024-11-15 09:10:00', '2024-11-15 09:20:00', 'uuid-0002'),
            ('device002', '2024-11-15 10:00:00', 3, 0, NULL, 102, 20, '备注信息3', 1, '2024-11-15 10:10:00', '2024-11-15 10:20:00', 'uuid-0003'),
            ('device003', '2024-11-15 11:00:00', 4, 0, NULL, 103, 15, '备注信息4', 1, '2024-11-15 11:10:00', '2024-11-15 11:20:00', 'uuid-0004'); """
    
    sql """INSERT
            INTO
            test_lead_lag_ttt (id,
            uuid,
            wtid,
            create_time,
            rectime,
            if_delete,
            std_state_code,
            stop_reason_code,
            endtime)
            SELECT
            id,
            uuid,
            wtid,
            LAG(rectime, 1, NULL) OVER (PARTITION BY wtid
            ORDER BY
            rectime) AS create_time,
            rectime,
            if_delete,
            std_state_code,
            stop_reason_code,
            LEAD(rectime, 1, NULL) OVER (PARTITION BY wtid
            ORDER BY
            rectime) AS endtime
            FROM
            test_lead_lag_ttt
            WHERE
            endtime is null;  """
}

