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

suite("check_rf") {
    sql """
        drop table if exists t1;
        set disable_join_reorder=true;
        set enable_parallel_result_sink=false;
        set runtime_filter_type=2;
        """ 
    sql """
        CREATE TABLE IF NOT EXISTS t1 (
        `app_name` VARCHAR(64) NULL COMMENT '标识', 
        `event_id` VARCHAR(128) NULL COMMENT '标识', 
        `decision` VARCHAR(32) NULL COMMENT '枚举值', 
        `time` DATETIME NULL COMMENT '查询时间', 
        `id` int NOT NULL COMMENT 'od'
        )
        DISTRIBUTED BY HASH(event_id)
        BUCKETS 3 PROPERTIES ("replication_num" = "1");
    """

    sql """
    insert into t1 values
    ('aa', 'bc', 'cc', '2024-07-03 01:15:30', 1),
    ('ab', 'bc', 'cc', '2024-07-03 01:15:30', 2),
    ('ac', 'bc', 'cc', '2024-07-03 01:15:30', 3),
    ('ad', 'bc', 'cc', '2024-07-03 01:15:30', 4);
    """

    // even if equalTo.right is not in aliasTransferMap, generate the rf
    qt_1 """
       explain shape plan 
       select t1.app_name
       from t1 join (select max(id) as maxId from t1 where id < 100 group by app_name) t2 on t1.id = t2.maxId;
       """
}