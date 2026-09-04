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

suite("test_cloud_table_stream_object_lifecycle") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_cloud_stream_lifecycle_base_table_db FORCE"
    sql "DROP DATABASE IF EXISTS test_cloud_stream_lifecycle_stream_table_db FORCE"
    sql "DROP DATABASE IF EXISTS test_cloud_stream_lifecycle_base_db_db FORCE"
    sql "DROP DATABASE IF EXISTS test_cloud_stream_lifecycle_stream_db_db FORCE"
    sql "DROP DATABASE IF EXISTS test_cloud_stream_lifecycle_owner_base_db FORCE"
    sql "DROP DATABASE IF EXISTS test_cloud_stream_lifecycle_owner_stream_db FORCE"
    sql "DROP DATABASE IF EXISTS test_cloud_stream_lifecycle_same_table_db FORCE"
    sql "DROP DATABASE IF EXISTS test_cloud_stream_lifecycle_same_database_db FORCE"

    // In one database, dropping only the base table keeps the Stream visible
    // until the Stream itself is FORCE dropped.
    sql "CREATE DATABASE test_cloud_stream_lifecycle_same_table_db"
    sql """
        CREATE TABLE test_cloud_stream_lifecycle_same_table_db.source (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE STREAM test_cloud_stream_lifecycle_same_table_db.local_stream
        ON TABLE test_cloud_stream_lifecycle_same_table_db.source
        PROPERTIES ("show_initial_rows" = "false")
    """
    sql "DROP TABLE test_cloud_stream_lifecycle_same_table_db.source FORCE"
    order_qt_local_stream_visible_after_base_table_drop """
        SHOW STREAMS FROM test_cloud_stream_lifecycle_same_table_db LIKE 'local_stream'
    """
    sql "DROP STREAM test_cloud_stream_lifecycle_same_table_db.local_stream FORCE"

    // Dropping the owning database removes both its base table and Stream from
    // FE visibility.
    sql "CREATE DATABASE test_cloud_stream_lifecycle_same_database_db"
    sql """
        CREATE TABLE test_cloud_stream_lifecycle_same_database_db.source (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE STREAM test_cloud_stream_lifecycle_same_database_db.local_stream
        ON TABLE test_cloud_stream_lifecycle_same_database_db.source
        PROPERTIES ("show_initial_rows" = "false")
    """
    sql "DROP DATABASE test_cloud_stream_lifecycle_same_database_db FORCE"
    qt_local_stream_removed_with_database """
        SELECT count(*)
        FROM information_schema.table_streams
        WHERE DB_NAME = 'test_cloud_stream_lifecycle_same_database_db'
          AND STREAM_NAME = 'local_stream'
    """

    // Dropping a base table does not silently drop a cross-database Stream.
    // The Stream remains visible and can still be FORCE dropped using the
    // base IDs retained in its catalog object.
    sql "CREATE DATABASE test_cloud_stream_lifecycle_base_table_db"
    sql "CREATE DATABASE test_cloud_stream_lifecycle_stream_table_db"
    sql """
        CREATE TABLE test_cloud_stream_lifecycle_base_table_db.source (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE TABLE test_cloud_stream_lifecycle_stream_table_db.sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE STREAM test_cloud_stream_lifecycle_stream_table_db.cross_stream
        ON TABLE test_cloud_stream_lifecycle_base_table_db.source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO test_cloud_stream_lifecycle_base_table_db.source VALUES (1, 10)"
    sql "sync"
    order_qt_cross_stream_before_base_table_drop """
        SELECT id, value
        FROM test_cloud_stream_lifecycle_stream_table_db.cross_stream
        ORDER BY id
    """
    sql """
        INSERT INTO test_cloud_stream_lifecycle_stream_table_db.sink
        SELECT id, value FROM test_cloud_stream_lifecycle_stream_table_db.cross_stream
    """
    order_qt_cross_stream_sink """
        SELECT id, value
        FROM test_cloud_stream_lifecycle_stream_table_db.sink
        ORDER BY id
    """

    sql "DROP TABLE test_cloud_stream_lifecycle_base_table_db.source FORCE"
    qt_cross_stream_visible_after_base_table_drop """
        SELECT count(*)
        FROM information_schema.table_streams
        WHERE DB_NAME = 'test_cloud_stream_lifecycle_stream_table_db'
          AND STREAM_NAME = 'cross_stream'
    """
    sql "DROP STREAM test_cloud_stream_lifecycle_stream_table_db.cross_stream FORCE"
    qt_cross_stream_removed_after_base_table_drop """
        SELECT count(*)
        FROM information_schema.table_streams
        WHERE DB_NAME = 'test_cloud_stream_lifecycle_stream_table_db'
          AND STREAM_NAME = 'cross_stream'
    """

    // Dropping the database that owns the base table has the same cross-DB
    // visibility semantics as dropping only the base table.
    sql "CREATE DATABASE test_cloud_stream_lifecycle_base_db_db"
    sql "CREATE DATABASE test_cloud_stream_lifecycle_stream_db_db"
    sql """
        CREATE TABLE test_cloud_stream_lifecycle_base_db_db.source (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE STREAM test_cloud_stream_lifecycle_stream_db_db.cross_stream
        ON TABLE test_cloud_stream_lifecycle_base_db_db.source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO test_cloud_stream_lifecycle_base_db_db.source VALUES (2, 20)"
    sql "sync"
    order_qt_cross_stream_before_base_db_drop """
        SELECT id, value
        FROM test_cloud_stream_lifecycle_stream_db_db.cross_stream
        ORDER BY id
    """

    sql "DROP DATABASE test_cloud_stream_lifecycle_base_db_db FORCE"
    order_qt_cross_stream_visible_after_base_db_drop """
        SHOW STREAMS FROM test_cloud_stream_lifecycle_stream_db_db LIKE 'cross_stream'
    """
    sql "DROP STREAM test_cloud_stream_lifecycle_stream_db_db.cross_stream FORCE"
    qt_cross_stream_removed_after_base_db_drop """
        SELECT count(*)
        FROM information_schema.table_streams
        WHERE DB_NAME = 'test_cloud_stream_lifecycle_stream_db_db'
          AND STREAM_NAME = 'cross_stream'
    """

    // Dropping the database that owns the Stream removes the Stream from FE
    // visibility without affecting its base table in another database.
    sql "CREATE DATABASE test_cloud_stream_lifecycle_owner_base_db"
    sql "CREATE DATABASE test_cloud_stream_lifecycle_owner_stream_db"
    sql """
        CREATE TABLE test_cloud_stream_lifecycle_owner_base_db.source (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE STREAM test_cloud_stream_lifecycle_owner_stream_db.cross_stream
        ON TABLE test_cloud_stream_lifecycle_owner_base_db.source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO test_cloud_stream_lifecycle_owner_base_db.source VALUES (3, 30)"
    sql "sync"
    order_qt_owner_stream_before_owner_db_drop """
        SELECT id, value
        FROM test_cloud_stream_lifecycle_owner_stream_db.cross_stream
        ORDER BY id
    """

    sql "DROP DATABASE test_cloud_stream_lifecycle_owner_stream_db FORCE"
    qt_owner_stream_removed_with_database """
        SELECT count(*)
        FROM information_schema.table_streams
        WHERE DB_NAME = 'test_cloud_stream_lifecycle_owner_stream_db'
          AND STREAM_NAME = 'cross_stream'
    """
    order_qt_base_survives_stream_owner_db_drop """
        SELECT id, value
        FROM test_cloud_stream_lifecycle_owner_base_db.source
        ORDER BY id
    """
}
