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

suite("mv_contains_cast") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists test;
    """

    sql """
        CREATE TABLE IF NOT EXISTS test (
        `app_name` VARCHAR(64) NULL COMMENT '标识', 
        `event_id` VARCHAR(128) NULL COMMENT '标识', 
        `decision` VARCHAR(32) NULL COMMENT '枚举值', 
        `time` DATETIME NULL COMMENT '查询时间', 
        `id` VARCHAR(35) NOT NULL COMMENT 'od', 
        `code` VARCHAR(64) NULL COMMENT '标识', 
        `event_type` VARCHAR(32) NULL COMMENT '事件类型' 
        )
        DUPLICATE KEY(app_name, event_id)
        PARTITION BY RANGE(time)                                                                                                                                                                                                                
        (                                                                                                                                                                                                                                      
         FROM ("2024-07-01 00:00:00") TO ("2024-07-15 00:00:00") INTERVAL 1 HOUR                                                                                                                                                                              
        )     
        DISTRIBUTED BY HASH(event_id)
        BUCKETS 3 PROPERTIES ("replication_num" = "1");
    """

    sql """
    insert into test values
    ('aa', 'bc', 'cc', '2024-07-03 01:15:30', 'dd', 'ee', 'ff'),
    ('as', 'bd', 'cd', '2024-07-03 06:09:30', 'dd', 'ee', 'ff'),
    ('ad', 'be', 'cc', '2024-07-03 07:06:30', 'dd', 'ee', 'ff'),
    ('af', 'bf', 'ce', '2024-07-04 10:01:30', 'dd', 'ee', 'ff'),
    ('ag', 'bc', 'cc', '2024-07-04 12:55:30', 'dd', 'ee', 'ff'),
    ('aa', 'bc', 'cc', '2024-07-05 01:15:30', 'dd', 'ee', 'ff'),
    ('as', 'bd', 'cd', '2024-07-05 06:09:30', 'dd', 'ee', 'ff'),
    ('ad', 'be', 'cc', '2024-07-06 07:06:30', 'dd', 'ee', 'ff'),
    ('af', 'bf', 'ce', '2024-07-07 10:01:30', 'dd', 'ee', 'ff'),
    ('ag', 'bc', 'cc', '2024-07-08 12:55:30', 'dd', 'ee', 'ff');
    """

    def query_sql = """
    SELECT 
    decision, 
    CONCAT(
        CONCAT(
          DATE_FORMAT(
            `time`, '%Y-%m-%d'
          ), 
          '', 
          LPAD(
            cast(FLOOR(MINUTE(`time`) / 15) as decimal(9, 0)) * 15, 
            5, 
            '00'
          ), 
          ':00'
        )
      ) as time, 
      count(id) as cnt 
    from 
      test 
    where 
    date_trunc(time, 'minute') BETWEEN '2024-07-02 18:00:00' 
      AND '2024-07-03 20:00:00' 
    group by 
      decision, 
      DATE_FORMAT(
        `time`, "%Y-%m-%d"
      ), 
      cast(FLOOR(MINUTE(`time`) / 15) as decimal(9, 0));
    """

    order_qt_query_before "${query_sql}"

    createMV("""
    CREATE MATERIALIZED VIEW sync_mv
    AS
    SELECT 
      decision,
      code, 
      app_name, 
      event_id, 
      event_type, 
      date_trunc(time, 'minute'), 
      DATE_FORMAT(
        `time`, '%Y-%m-%d'
      ), 
      cast(FLOOR(MINUTE(time) / 15) as decimal(9, 0)),
      count(id) as cnt
    from 
      test 
    group by 
      code, 
      app_name, 
      event_id, 
      event_type, 
      date_trunc(time, 'minute'), 
      decision, 
      DATE_FORMAT(time, '%Y-%m-%d'), 
      cast(FLOOR(MINUTE(`time`) / 15) as decimal(9, 0));
    """)

    explain {
        sql("""${query_sql}""")
        contains "(sync_mv)"
    }

    order_qt_query_after "${query_sql}"
}
