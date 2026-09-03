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

// Cover updating a stream's comment after creation via
// ALTER STREAM ... SET COMMENT and reflecting the change in
// information_schema.table_streams.
suite("test_table_stream_alter_comment") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_alter_comment_db"
    sql "CREATE DATABASE test_table_stream_alter_comment_db"
    sql "USE test_table_stream_alter_comment_db"

    def db = "test_table_stream_alter_comment_db"

    sql """
        CREATE TABLE base_dup (
            k1 INT, v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    // 1. create the stream with an initial comment.
    sql """
        CREATE STREAM s_alter_comment ON TABLE base_dup
        COMMENT 'initial comment'
        PROPERTIES ("type" = "append_only")
    """

    def fetchComment = {
        def rows = sql """
            select STREAM_COMMENT from information_schema.table_streams
            where DB_NAME = '${db}' and STREAM_NAME = 's_alter_comment'
        """
        assertEquals(1, rows.size())
        return rows[0][0]
    }

    // the initial comment is visible in table_streams.
    assertEquals("initial comment", fetchComment())

    // 2. ALTER STREAM SET COMMENT updates the comment.
    sql "ALTER STREAM s_alter_comment SET COMMENT 'updated comment'"
    assertEquals("updated comment", fetchComment())

    // 3. altering again keeps applying the latest value.
    sql "ALTER STREAM s_alter_comment SET COMMENT 'final comment'"
    assertEquals("final comment", fetchComment())

    sql "DROP DATABASE IF EXISTS test_table_stream_alter_comment_db"
}
