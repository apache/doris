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

suite("test_cloud_table_stream_schema_change") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_cloud_table_stream_schema_change_db FORCE"
    sql "CREATE DATABASE test_cloud_table_stream_schema_change_db"
    sql "USE test_cloud_table_stream_schema_change_db"

    sql """
        CREATE TABLE schema_source (
            id INT,
            value INT,
            blocked VARCHAR(2)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE TABLE schema_sink_after_add (
            id INT,
            value INT,
            extra INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE schema_sink_after_drop (
            id INT,
            extra INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO schema_source VALUES (1, 10, '10')"
    sql "sync"
    sql """
        CREATE STREAM schema_stream ON TABLE schema_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """

    sql "ALTER TABLE schema_source ADD COLUMN extra INT DEFAULT '0'"
    sql "INSERT INTO schema_source (id, value, extra) VALUES (2, 20, 200)"
    sql "sync"

    order_qt_stream_follows_added_column """
        SELECT id, value, extra
        FROM schema_stream
        ORDER BY id
    """
    sql """
        INSERT INTO schema_sink_after_add
        SELECT id, value, extra FROM schema_stream
    """
    order_qt_consumed_after_add """
        SELECT id, value, extra
        FROM schema_sink_after_add
        ORDER BY id
    """
    qt_stream_empty_after_add_consumption "SELECT count(*) FROM schema_stream"

    sql "ALTER TABLE schema_source DROP COLUMN value"
    test {
        sql "SELECT value FROM schema_stream"
        exception "Unknown column 'value'"
    }
    sql "INSERT INTO schema_source (id, extra) VALUES (3, 300)"
    sql "sync"

    order_qt_stream_follows_dropped_column """
        SELECT id, extra
        FROM schema_stream
        ORDER BY id
    """
    sql """
        INSERT INTO schema_sink_after_drop
        SELECT id, extra FROM schema_stream
    """
    order_qt_consumed_after_drop """
        SELECT id, extra
        FROM schema_sink_after_drop
        ORDER BY id
    """
    qt_stream_empty_after_drop_consumption "SELECT count(*) FROM schema_stream"

    test {
        sql "ALTER TABLE schema_source MODIFY COLUMN blocked VARCHAR(10)"
        exception "Not allowed to perform current operation on Table With binlog<row>"
    }
    qt_failed_schema_change_does_not_create_delta "SELECT count(*) FROM schema_stream"

    sql "INSERT INTO schema_source (id, extra) VALUES (4, 400)"
    sql "sync"
    order_qt_stream_after_failed_schema_change """
        SELECT id, extra
        FROM schema_stream
        ORDER BY id
    """
    sql """
        INSERT INTO schema_sink_after_drop
        SELECT id, extra FROM schema_stream
    """
    order_qt_consumption_after_failed_schema_change """
        SELECT id, extra
        FROM schema_sink_after_drop
        ORDER BY id
    """
    qt_stream_empty_after_final_consumption "SELECT count(*) FROM schema_stream"
}
