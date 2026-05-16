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

suite("test_routine_load_flexible_partial_update_validate", "nonConcurrent") {
    try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_where_job; """
    try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_merge_job; """
    try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_append_job; """
    try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_order_job; """
    try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_alter_where_job; """
    try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_alter_merge_job; """
    try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_alter_delete_job; """
    try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_alter_order_job; """
    try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_table_seq_job; """

    sql """ DROP TABLE IF EXISTS test_rl_flex_validate_where force; """
    sql """
        CREATE TABLE test_rl_flex_validate_where (
            `id` int NOT NULL,
            `name` varchar(32) NULL,
            `score` int NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true"
        );
    """

    test {
        sql """
            CREATE ROUTINE LOAD test_rl_flex_validate_where_job ON test_rl_flex_validate_where
            WHERE id > 1
            PROPERTIES
            (
                "format" = "json",
                "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "127.0.0.1:9092",
                "kafka_topic" = "test_rl_flex_validate_where"
            );
        """
        exception "where"
    }

    sql """ DROP TABLE IF EXISTS test_rl_flex_validate_merge force; """
    sql """
        CREATE TABLE test_rl_flex_validate_merge (
            `id` int NOT NULL,
            `name` varchar(32) NULL,
            `is_delete` int NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true"
        );
    """

    test {
        sql """
            CREATE ROUTINE LOAD test_rl_flex_validate_merge_job ON test_rl_flex_validate_merge
            WITH MERGE
            DELETE ON is_delete = 1
            PROPERTIES
            (
                "format" = "json",
                "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "127.0.0.1:9092",
                "kafka_topic" = "test_rl_flex_validate_merge"
            );
        """
        exception "merge_type"
    }

    test {
        sql """
            CREATE ROUTINE LOAD test_rl_flex_validate_append_job ON test_rl_flex_validate_merge
            WITH APPEND
            PROPERTIES
            (
                "format" = "json",
                "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "127.0.0.1:9092",
                "kafka_topic" = "test_rl_flex_validate_append"
            );
        """
        exception "merge_type"
    }

    sql """ DROP TABLE IF EXISTS test_rl_flex_validate_order force; """
    sql """
        CREATE TABLE test_rl_flex_validate_order (
            `id` int NOT NULL,
            `seq` int NULL,
            `score` int NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true"
        );
    """

    test {
        sql """
            CREATE ROUTINE LOAD test_rl_flex_validate_order_job ON test_rl_flex_validate_order
            ORDER BY seq
            PROPERTIES
            (
                "format" = "json",
                "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "127.0.0.1:9092",
                "kafka_topic" = "test_rl_flex_validate_order"
            );
        """
        exception "function_column.sequence_col"
    }

    try {
        sql """
            CREATE ROUTINE LOAD test_rl_flex_validate_alter_where_job ON test_rl_flex_validate_where
            WHERE id > 1
            PROPERTIES
            (
                "format" = "json"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "127.0.0.1:9092",
                "kafka_topic" = "test_rl_flex_validate_alter_where"
            );
        """
        sql """ PAUSE ROUTINE LOAD FOR test_rl_flex_validate_alter_where_job; """
        test {
            sql """
                ALTER ROUTINE LOAD FOR test_rl_flex_validate_alter_where_job
                PROPERTIES
                (
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                );
            """
            exception "where"
        }
    } finally {
        try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_alter_where_job; """
    }

    try {
        sql """
            CREATE ROUTINE LOAD test_rl_flex_validate_alter_merge_job ON test_rl_flex_validate_merge
            WITH MERGE
            DELETE ON is_delete = 1
            PROPERTIES
            (
                "format" = "json"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "127.0.0.1:9092",
                "kafka_topic" = "test_rl_flex_validate_alter_merge"
            );
        """
        sql """ PAUSE ROUTINE LOAD FOR test_rl_flex_validate_alter_merge_job; """
        test {
            sql """
                ALTER ROUTINE LOAD FOR test_rl_flex_validate_alter_merge_job
                PROPERTIES
                (
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                );
            """
            exception "merge_type"
        }
    } finally {
        try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_alter_merge_job; """
    }

    try {
        sql """
            CREATE ROUTINE LOAD test_rl_flex_validate_alter_delete_job ON test_rl_flex_validate_merge
            PROPERTIES
            (
                "format" = "json",
                "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "127.0.0.1:9092",
                "kafka_topic" = "test_rl_flex_validate_alter_delete"
            );
        """
        sql """ PAUSE ROUTINE LOAD FOR test_rl_flex_validate_alter_delete_job; """
        test {
            sql """
                ALTER ROUTINE LOAD FOR test_rl_flex_validate_alter_delete_job
                DELETE ON is_delete = 1;
            """
            exception "delete"
        }
    } finally {
        try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_alter_delete_job; """
    }

    try {
        sql """
            CREATE ROUTINE LOAD test_rl_flex_validate_alter_order_job ON test_rl_flex_validate_order
            ORDER BY seq
            PROPERTIES
            (
                "format" = "json"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "127.0.0.1:9092",
                "kafka_topic" = "test_rl_flex_validate_alter_order"
            );
        """
        sql """ PAUSE ROUTINE LOAD FOR test_rl_flex_validate_alter_order_job; """
        test {
            sql """
                ALTER ROUTINE LOAD FOR test_rl_flex_validate_alter_order_job
                PROPERTIES
                (
                    "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
                );
            """
            exception "function_column.sequence_col"
        }
    } finally {
        try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_alter_order_job; """
    }

    sql """ DROP TABLE IF EXISTS test_rl_flex_validate_table_seq force; """
    sql """
        CREATE TABLE test_rl_flex_validate_table_seq (
            `id` int NOT NULL,
            `seq` int NULL,
            `score` int NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_col" = "seq"
        );
    """
    try {
        sql """
            CREATE ROUTINE LOAD test_rl_flex_validate_table_seq_job ON test_rl_flex_validate_table_seq
            PROPERTIES
            (
                "format" = "json",
                "unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"
            )
            FROM KAFKA
            (
                "kafka_broker_list" = "127.0.0.1:9092",
                "kafka_topic" = "test_rl_flex_validate_table_seq"
            );
        """
    } finally {
        try_sql """ STOP ROUTINE LOAD FOR test_rl_flex_validate_table_seq_job; """
    }
}
