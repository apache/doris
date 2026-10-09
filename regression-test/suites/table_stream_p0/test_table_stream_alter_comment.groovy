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

// ALTER STREAM ... SET/MODIFY COMMENT updates the comment of a table stream.
suite("test_table_stream_alter_comment") {
    if (isCloudMode()) {
        logger.info("skip test_table_stream_alter_comment in cloud mode")
        return
    }

    sql "DROP STREAM IF EXISTS test_stream_alter_comment_stream"
    sql "DROP TABLE IF EXISTS test_stream_alter_comment_base FORCE"

    sql """
        CREATE TABLE test_stream_alter_comment_base (
            k1 INT NOT NULL,
            v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    sql """
        CREATE STREAM test_stream_alter_comment_stream ON TABLE test_stream_alter_comment_base
        COMMENT 'initial comment'
        PROPERTIES ("type" = "append_only")
    """

    order_qt_comment_after_create """
        SELECT STREAM_COMMENT FROM information_schema.table_streams
        WHERE DB_NAME = DATABASE() AND STREAM_NAME = 'test_stream_alter_comment_stream'
    """

    // SET COMMENT
    sql """ALTER STREAM test_stream_alter_comment_stream SET COMMENT 'updated comment'"""
    order_qt_comment_after_set """
        SELECT STREAM_COMMENT FROM information_schema.table_streams
        WHERE DB_NAME = DATABASE() AND STREAM_NAME = 'test_stream_alter_comment_stream'
    """

    // the new comment is part of SHOW CREATE STREAM as well. The whole DDL is not used as the
    // expected output on purpose: its PROPERTIES section changes as the stream feature evolves.
    def createStmt = sql("SHOW CREATE STREAM test_stream_alter_comment_stream")[0][1].toString()
    assertTrue(createStmt.contains("COMMENT 'updated comment'"), createStmt)

    // MODIFY COMMENT is accepted as well, to stay consistent with ALTER TABLE
    sql """ALTER STREAM test_stream_alter_comment_stream MODIFY COMMENT 'modified comment'"""
    order_qt_comment_after_modify """
        SELECT STREAM_COMMENT FROM information_schema.table_streams
        WHERE DB_NAME = DATABASE() AND STREAM_NAME = 'test_stream_alter_comment_stream'
    """

    // the literal is decoded the same way CREATE STREAM decodes it: a doubled quote is one quote
    sql """ALTER STREAM test_stream_alter_comment_stream SET COMMENT 'a''b'"""
    order_qt_comment_doubled_quote """
        SELECT STREAM_COMMENT, LENGTH(STREAM_COMMENT) FROM information_schema.table_streams
        WHERE DB_NAME = DATABASE() AND STREAM_NAME = 'test_stream_alter_comment_stream'
    """

    // an empty comment clears the comment
    sql """ALTER STREAM test_stream_alter_comment_stream SET COMMENT ''"""
    order_qt_comment_after_clear """
        SELECT STREAM_COMMENT FROM information_schema.table_streams
        WHERE DB_NAME = DATABASE() AND STREAM_NAME = 'test_stream_alter_comment_stream'
    """
    createStmt = sql("SHOW CREATE STREAM test_stream_alter_comment_stream")[0][1].toString()
    assertFalse(createStmt.contains("COMMENT '"), createStmt)

    sql """ALTER STREAM test_stream_alter_comment_stream SET COMMENT 'final comment'"""
    order_qt_comment_final """
        SELECT STREAM_COMMENT FROM information_schema.table_streams
        WHERE DB_NAME = DATABASE() AND STREAM_NAME = 'test_stream_alter_comment_stream'
    """

    // altering a normal table through ALTER STREAM is rejected
    test {
        sql """ALTER STREAM test_stream_alter_comment_base SET COMMENT 'not a stream'"""
        exception "is not STREAM"
    }

    // altering an unknown stream is rejected
    test {
        sql """ALTER STREAM test_stream_alter_comment_not_exist SET COMMENT 'no such stream'"""
        exception "Unknown table"
    }
}
