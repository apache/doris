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

suite("test_alter_rollup_table") {
    def tbName = "test_alter_rollup_table"

    sql "DROP TABLE IF EXISTS ${tbName} FORCE"
    sql """
        CREATE TABLE `${tbName}` (
          `user_id` bigint NOT NULL COMMENT "用户 ID",
          `event_time` datetime NOT NULL COMMENT "事件时间",
          `event_date` date NOT NULL,
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
          `update_time` datetime(6) NULL DEFAULT CURRENT_TIMESTAMP COMMENT "更新时间",
          `tags` array<varchar(32)> NULL,
          `metadata` json NULL,
          `status` int NULL DEFAULT "1",
          `created_at` datetime NULL DEFAULT CURRENT_TIMESTAMP,
          `optional_data` varchar(64) NULL,
          `required_data` int NOT NULL DEFAULT "0",
          `total_score` bigint NULL DEFAULT "0",
          `last_status` int NULL DEFAULT "0",
          `max_value` int NULL DEFAULT "0",
          `min_value` int NULL DEFAULT "0",
          
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
        DUPLICATE KEY(`user_id`, `event_time`, `event_date`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 3
        PROPERTIES (
        "replication_num" = "1",
        "file_cache_ttl_seconds" = "0",
        "bloom_filter_columns" = "country, city",
        "is_being_synced" = "false",
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

    sql """
        insert into ${tbName}
        (user_id, event_date, event_time, country, city, age, is_active, balance, score, last_ip, json_data, seq_col)
        values
        (1001, '2025-09-19', '2025-09-19 10:00:00', 'japan', 'tokyo', 30, 1, 1000.50, 88.8, '192.168.0.1', '{"device":"iphone"}', 1),
        (1002, '2025-09-19', '2025-09-19 11:30:00', 'usa', null, null, 0, 500.00, null, '10.0.0.2', '{"device":"android"}', 2);
    """


    sql """
        ALTER TABLE ${tbName}
        ADD ROLLUP r_event_user
        (
            event_date,
            event_time,
            user_id,
            country,
            city,
            age,
            balance
        );
    """

    sleep(10000)

    sql """
        ALTER TABLE ${tbName}
        ADD ROLLUP r_complex
        (
            event_date,
            event_time,
            user_id,
            ipv6_addr,
            last_ip,
            json_data,
            create_time,
            update_time
        );
    """

    sleep(10000)

    sql """
        ALTER TABLE ${tbName} MODIFY COLUMN event_date DATETIME
    """

    sql """
        insert into ${tbName}
        (user_id, event_date, event_time, country, city, age, is_active, balance, score, last_ip, json_data, seq_col)
        values
        (1003, '2025-09-19', '2025-09-19 10:00:00', 'japan', 'tokyo', 30, 1, 1000.50, 88.8, '192.168.0.1', '{"device":"iphone"}', 1),
        (1004, '2025-09-19', '2025-09-19 11:30:00', 'usa', null, null, 0, 500.00, null, '10.0.0.2', '{"device":"android"}', 2);
    """

    sleep(10000)

    sql """
        select event_date, user_id from ${tbName}
    """
}

