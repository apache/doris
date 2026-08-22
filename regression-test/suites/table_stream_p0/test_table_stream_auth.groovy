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

// CREATE STREAM requires CREATE on the destination database and SELECT on the
// base table. Query and DROP privileges are checked on the stream object.
suite("test_table_stream_auth", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def user = "test_table_stream_auth_user"
    def password = "TableStream_123"

    try_sql "DROP USER IF EXISTS '${user}'"
    sql "DROP DATABASE IF EXISTS test_table_stream_auth_db"
    sql "CREATE DATABASE test_table_stream_auth_db"
    sql """
        CREATE TABLE test_table_stream_auth_db.auth_base (
            id INT,
            value VARCHAR(16)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql "INSERT INTO test_table_stream_auth_db.auth_base VALUES (1, 'history')"
    sql "CREATE USER '${user}' IDENTIFIED BY '${password}'"

    connect(user, password, context.config.jdbcUrl) {
        test {
            sql """
                CREATE STREAM test_table_stream_auth_db.auth_stream
                ON TABLE test_table_stream_auth_db.auth_base
                PROPERTIES (
                    "type" = "append_only",
                    "show_initial_rows" = "true"
                )
            """
            exception "denied"
        }
    }

    sql "GRANT CREATE_PRIV ON test_table_stream_auth_db.* TO '${user}'"
    connect(user, password, context.config.jdbcUrl) {
        test {
            sql """
                CREATE STREAM test_table_stream_auth_db.auth_stream
                ON TABLE test_table_stream_auth_db.auth_base
                PROPERTIES (
                    "type" = "append_only",
                    "show_initial_rows" = "true"
                )
            """
            exception "denied"
        }
    }

    sql "GRANT SELECT_PRIV ON test_table_stream_auth_db.auth_base TO '${user}'"
    connect(user, password, context.config.jdbcUrl) {
        sql """
            CREATE STREAM test_table_stream_auth_db.auth_stream
            ON TABLE test_table_stream_auth_db.auth_base
            PROPERTIES (
                "type" = "append_only",
                "show_initial_rows" = "true"
            )
        """

        test {
            sql "SELECT id, value FROM test_table_stream_auth_db.auth_stream"
            exception "denied"
        }

        test {
            sql "DROP STREAM test_table_stream_auth_db.auth_stream"
            exception "denied"
        }
    }

    sql "GRANT SELECT_PRIV ON test_table_stream_auth_db.auth_stream TO '${user}'"
    connect(user, password, context.config.jdbcUrl) {
        def rows = sql """
            SELECT id, value FROM test_table_stream_auth_db.auth_stream ORDER BY id
        """
        assertEquals(1, rows.size())
        assertEquals(["1", "history"], rows[0].collect { value -> value.toString() })
    }

    sql "GRANT DROP_PRIV ON test_table_stream_auth_db.auth_stream TO '${user}'"
    connect(user, password, context.config.jdbcUrl) {
        sql "DROP STREAM test_table_stream_auth_db.auth_stream"
    }

    assertEquals(0, sql("""
        SELECT COUNT(*) FROM information_schema.table_streams
        WHERE DB_NAME = 'test_table_stream_auth_db'
          AND STREAM_NAME = 'auth_stream'
    """)[0][0] as int)

    try_sql "DROP USER IF EXISTS '${user}'"
}
