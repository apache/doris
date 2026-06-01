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

// This test reproduces the bug where UPDATE SET nullable_col = NULL crashes
// Bug: CHECK failed: index < data.size() in block.h:182
// Root cause: _num_columns set from partial_update_input_columns.size() but actual input has fewer columns

suite('update_after_create_rollup') {
    sql 'drop table if exists mow_table'
    sql '''
       CREATE TABLE `mow_table` (
        `user_id` bigint NOT NULL COMMENT "用户 ID",
        `event_date` date NOT NULL COMMENT "事件日期",
        `event_time` datetime NOT NULL COMMENT "事件时间",
        `country` varchar(128) NULL DEFAULT "UNKNOWN",
        `city` text NULL COMMENT "城市信息",
        `age` int NULL DEFAULT "0" COMMENT "用户年龄",
        `is_active` boolean NULL DEFAULT "TRUE" COMMENT "是否活跃",
        `balance` decimal(18,2) NULL DEFAULT "0.00" COMMENT "账户余额",
        `score` double NULL COMMENT "浮点分数",
        `last_login` datetime(3) NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT "最后登录时间",
        `last_ip` ipv4 NULL DEFAULT "0.0.0.0" COMMENT "最近一次登录 IP",
        `ipv6_addr` ipv6 NULL COMMENT "IPv6 地址",
        `json_data` json NULL COMMENT "扩展 JSON 信息",
        `user_metadata` variant NULL COMMENT "存储用户自定义半结构化数据",
        `seq_col` bigint NULL DEFAULT "0" COMMENT "顺序列，测试 sequence",
        `auto_inc_col` bigint NOT NULL AUTO_INCREMENT(1) COMMENT "自增列，用于测试",
        `create_time` datetime(6) NULL DEFAULT CURRENT_TIMESTAMP COMMENT "创建时间",
        `update_time` datetime(6) NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT "更新时间",
        `tags` array<varchar(32)> NULL,
        `metadata` json NULL,
        `status` int NULL DEFAULT "1",
        `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
        `optional_data` varchar(64) NULL,
        `required_data` int NOT NULL DEFAULT "0",
        `last_status` int NULL DEFAULT "0",
        `col1` int NULL DEFAULT "0",
        `col2` varchar(32) NULL,
        `a_very_long_column_name_that_is_just_under_the_limit` int NULL,
        `long_default` varchar(255) NULL DEFAULT "a_very_long_default_value_that_is_just_under_the_limit_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz",
        `max_int` int NULL DEFAULT "2147483647",
        `min_int` int NULL DEFAULT "-2147483648",
        `high_precision` decimal(38,10) NULL DEFAULT "1234567890123456789012345678.1234567890",
        `test_col` int NULL DEFAULT "0",
        `consistency_check` int NULL DEFAULT "42",
        INDEX idx_json_data_inverted (`user_metadata`) USING INVERTED
        ) ENGINE=OLAP
        UNIQUE KEY(`user_id`, `event_date`, `event_time`)
        PARTITION BY RANGE(`event_date`)
        (PARTITION p2015 VALUES [('2015-01-01'), ('2016-01-01')),
        PARTITION p2016 VALUES [('2016-01-01'), ('2017-01-01')),
        PARTITION p2017 VALUES [('2017-01-01'), ('2018-01-01')),
        PARTITION p2018 VALUES [('2018-01-01'), ('2019-01-01')),
        PARTITION p2019 VALUES [('2019-01-01'), ('2020-01-01')),
        PARTITION p2020 VALUES [('2020-01-01'), ('2021-01-01')),
        PARTITION p2021 VALUES [('2021-01-01'), ('2022-01-01')),
        PARTITION p2022 VALUES [('2022-01-01'), ('2023-01-01')),
        PARTITION p2023 VALUES [('2023-01-01'), ('2024-01-01')),
        PARTITION p2024 VALUES [('2024-01-01'), ('2025-01-01')),
        PARTITION p2025 VALUES [('2025-01-01'), ('2026-01-01')),
        PARTITION p2026 VALUES [('2026-01-01'), ('2027-01-01')),
        PARTITION p2027 VALUES [('2027-01-01'), ('2028-01-01')),
        PARTITION p2028 VALUES [('2028-01-01'), ('2029-01-01')),
        PARTITION p2029 VALUES [('2029-01-01'), ('2030-01-01')),
        PARTITION p2030 VALUES [('2030-01-01'), ('2030-12-31')),
        PARTITION p2031 VALUES [('2031-01-01'), ('2032-01-01')),
        PARTITION p2032 VALUES [('2032-01-01'), ('2033-01-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "bloom_filter_columns" = "country, city",
        "is_being_synced" = "false",
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "YEAR",
        "dynamic_partition.time_zone" = "Asia/Shanghai",
        "dynamic_partition.start" = "-10",
        "dynamic_partition.end" = "7",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.replication_allocation" = "tag.location.default: 1",
        "dynamic_partition.buckets" = "3",
        "dynamic_partition.create_history_partition" = "false",
        "dynamic_partition.history_partition_num" = "-1",
        "dynamic_partition.hot_partition_num" = "0",
        "dynamic_partition.reserved_history_periods" = "NULL",
        "dynamic_partition.storage_policy" = "",
        "storage_medium" = "hdd",
        "storage_format" = "V2",
        "inverted_index_storage_format" = "V3",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728",
        "enable_mow_light_delete" = "false"
        );
    '''

    sql '''
        INSERT INTO mow_table(user_id,event_date,event_time,country,city) VALUES 
        (2000,'2025-09-22','2025-09-22 10:00:00','China','Shanghai'),
        (2001,'2025-09-22','2025-09-22 11:00:00','USA','NewYork'),
        (2002,'2025-09-22','2025-09-22 12:00:00','US','London');
    '''

    qt_sql_before 'select user_id, event_date, country, city from mow_table order by user_id'

    // Add a rollup that's missing the 'city' column
    sql """
        ALTER TABLE mow_table ADD ROLLUP rollup1(event_date, event_time, user_id, country, update_time)
    """
    sleep(10000)
    explain {
        sql('''
            SELECT event_date, country, count(*) 
            FROM mow_table 
            WHERE event_date = '2025-09-22'
            GROUP BY event_date, country
        ''')
        contains "rollup1"
    }
    
    // Test 1: UPDATE column not in rollup1 (city)
    sql 'UPDATE mow_table SET city = "beijing" WHERE user_id = 2000'
    def result1 = sql 'SELECT city FROM mow_table WHERE user_id = 2000'
    assertEquals(1, result1.size())
    assertEquals('beijing', result1[0][0])
    
    // Test 2: UPDATE column in rollup1 (country)
    sql 'UPDATE mow_table SET country = "CN" WHERE user_id = 2000'
    def result2 = sql 'SELECT country FROM mow_table WHERE user_id = 2000'
    assertEquals(1, result2.size())
    assertEquals('CN', result2[0][0])

    // Test 3: UPDATE both columns (one in rollup, one not)
    sql 'UPDATE mow_table SET city = "Shenzhen", country = "China" WHERE user_id = 2001'
    def result3 = sql 'SELECT city, country FROM mow_table WHERE user_id = 2001'
    assertEquals(1, result3.size())
    assertEquals('Shenzhen', result3[0][0])
    assertEquals('China', result3[0][1])

    qt_sql_after '''
        SELECT user_id, event_date, country, city 
        FROM mow_table 
        ORDER BY user_id
    '''

    sql 'drop table if exists mow_table'
}
