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

suite("test_create_append_only_stream_without_historical_value", "nonConcurrent") {
    sql "DROP STREAM IF EXISTS test_append_only_without_historical_value_stream"
    sql "DROP STREAM IF EXISTS test_detail_without_historical_value_stream"
    sql "DROP STREAM IF EXISTS test_default_without_historical_value_stream"
    sql "DROP STREAM IF EXISTS test_min_delta_without_historical_value_stream"
    sql "DROP STREAM IF EXISTS test_invalid_type_without_historical_value_stream"
    sql "DROP TABLE IF EXISTS test_stream_without_historical_value_base"

    sql """
        CREATE TABLE test_stream_without_historical_value_base (
            id INT NOT NULL,
            value INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "false"
        )
    """

    sql """
        CREATE STREAM test_append_only_without_historical_value_stream
        ON TABLE test_stream_without_historical_value_base
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql """
        CREATE STREAM test_detail_without_historical_value_stream
        ON TABLE test_stream_without_historical_value_base
        PROPERTIES (
            "type" = "detail",
            "show_initial_rows" = "false"
        )
    """

    order_qt_stream_types """
        SELECT STREAM_NAME, CONSUME_TYPE
        FROM information_schema.table_streams
        WHERE DB_NAME = DATABASE()
          AND STREAM_NAME IN (
              'test_append_only_without_historical_value_stream',
              'test_detail_without_historical_value_stream'
          )
        ORDER BY STREAM_NAME
    """

    test {
        sql """
            CREATE STREAM test_default_without_historical_value_stream
            ON TABLE test_stream_without_historical_value_base
            PROPERTIES ("show_initial_rows" = "false")
        """
        exception "MIN_DELTA table stream requires base mow table to enable binlog.need_historical_value=true"
    }
    test {
        sql """
            CREATE STREAM test_min_delta_without_historical_value_stream
            ON TABLE test_stream_without_historical_value_base
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        exception "MIN_DELTA table stream requires base mow table to enable binlog.need_historical_value=true"
    }
    test {
        sql """
            CREATE STREAM test_invalid_type_without_historical_value_stream
            ON TABLE test_stream_without_historical_value_base
            PROPERTIES (
                "type" = "invalid_type",
                "show_initial_rows" = "false"
            )
        """
        exception "not supported type: invalid_type"
    }
}
