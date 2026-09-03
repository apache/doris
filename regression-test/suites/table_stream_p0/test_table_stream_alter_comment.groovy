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

    def baseTable = "test_stream_alter_comment_base"
    def streamName = "test_stream_alter_comment_stream"

    def streamComment = { name ->
        def rows = sql """
            SELECT STREAM_COMMENT FROM information_schema.table_streams
            WHERE DB_NAME = DATABASE() AND STREAM_NAME = '${name}'
        """
        assertEquals(1, rows.size())
        return rows[0][0].toString()
    }

    sql "DROP STREAM IF EXISTS ${streamName}"
    sql "DROP TABLE IF EXISTS ${baseTable} FORCE"

    sql """
        CREATE TABLE ${baseTable} (
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
        CREATE STREAM ${streamName} ON TABLE ${baseTable}
        COMMENT 'initial comment'
        PROPERTIES ("type" = "append_only")
    """
    assertEquals("initial comment", streamComment(streamName))

    // SET COMMENT
    sql """ALTER STREAM ${streamName} SET COMMENT 'updated comment'"""
    assertEquals("updated comment", streamComment(streamName))
    def createStmt = sql("SHOW CREATE STREAM ${streamName}")[0][1].toString()
    assertTrue(createStmt.contains("COMMENT 'updated comment'"), createStmt)

    // MODIFY COMMENT is accepted as well, to stay consistent with ALTER TABLE
    sql """ALTER STREAM ${streamName} MODIFY COMMENT 'modified comment'"""
    assertEquals("modified comment", streamComment(streamName))

    // an empty comment clears the comment
    sql """ALTER STREAM ${streamName} SET COMMENT ''"""
    assertEquals("", streamComment(streamName))
    createStmt = sql("SHOW CREATE STREAM ${streamName}")[0][1].toString()
    assertFalse(createStmt.contains("COMMENT '"), createStmt)

    sql """ALTER STREAM ${streamName} SET COMMENT 'final comment'"""
    assertEquals("final comment", streamComment(streamName))

    // altering a normal table through ALTER STREAM is rejected
    test {
        sql """ALTER STREAM ${baseTable} SET COMMENT 'not a stream'"""
        exception "is not STREAM"
    }

    // altering an unknown stream is rejected
    test {
        sql """ALTER STREAM test_stream_alter_comment_not_exist SET COMMENT 'no such stream'"""
        exception "Unknown table"
    }

    sql "DROP STREAM IF EXISTS ${streamName}"
    sql "DROP TABLE IF EXISTS ${baseTable} FORCE"
}
