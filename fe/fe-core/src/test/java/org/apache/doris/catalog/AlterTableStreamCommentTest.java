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

package org.apache.doris.catalog;

import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AlterTableStreamCommentTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
    }

    @Test
    public void testAlterStreamComment() throws Exception {
        createDatabase("test_alter_stream_comment");
        createTable("create table test_alter_stream_comment.base_tbl (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        createTable("create stream test_alter_stream_comment.s1 on table test_alter_stream_comment.base_tbl\n"
                + "comment 'initial comment'\n"
                + "properties('type' = 'append_only');");

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test_alter_stream_comment");
        BaseTableStream stream = (BaseTableStream) db.getTableOrDdlException("s1");
        Assertions.assertEquals("initial comment", stream.getComment());

        executeSql("alter stream test_alter_stream_comment.s1 set comment 'updated comment'");
        Assertions.assertEquals("updated comment", stream.getComment());

        // MODIFY COMMENT is accepted as well, to stay consistent with ALTER TABLE
        executeSql("alter stream test_alter_stream_comment.s1 modify comment 'modified comment'");
        Assertions.assertEquals("modified comment", stream.getComment());

        // an empty comment clears the comment
        executeSql("alter stream test_alter_stream_comment.s1 set comment ''");
        Assertions.assertEquals("", stream.getComment());

        // altering a normal table through ALTER STREAM is rejected
        ExceptionChecker.expectThrowsWithMsg(IllegalStateException.class, "is not STREAM",
                () -> executeSql("alter stream test_alter_stream_comment.base_tbl set comment 'not a stream'"));

        // altering an unknown stream is rejected
        ExceptionChecker.expectThrowsWithMsg(IllegalStateException.class, "Unknown table",
                () -> executeSql("alter stream test_alter_stream_comment.not_exist set comment 'no such stream'"));

        dropDatabase("test_alter_stream_comment");
    }

    @Test
    public void testAlterStreamCommentStringLiteral() throws Exception {
        createDatabase("test_alter_stream_comment_literal");
        createTable("create table test_alter_stream_comment_literal.base_tbl (k1 int, v1 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW');");
        // CREATE STREAM and ALTER STREAM must decode the string literal in the same way:
        // a doubled quote is one quote and, unless NO_BACKSLASH_ESCAPES is set, backslash escapes are decoded
        createTable("create stream test_alter_stream_comment_literal.s1"
                + " on table test_alter_stream_comment_literal.base_tbl\n"
                + "comment 'a''b\\nc'\n"
                + "properties('type' = 'append_only');");

        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException("test_alter_stream_comment_literal");
        BaseTableStream stream = (BaseTableStream) db.getTableOrDdlException("s1");
        Assertions.assertEquals("a'b\nc", stream.getComment());

        executeSql("alter stream test_alter_stream_comment_literal.s1 set comment 'x''y\\tz'");
        Assertions.assertEquals("x'y\tz", stream.getComment());

        // a double quoted literal doubles the double quote instead
        executeSql("alter stream test_alter_stream_comment_literal.s1 set comment \"p\"\"q\"");
        Assertions.assertEquals("p\"q", stream.getComment());

        // under NO_BACKSLASH_ESCAPES a backslash is an ordinary character. Both the lexer and
        // SqlLiteralUtils read the sql mode from the thread local ConnectContext, so bind it first.
        connectContext.setThreadLocalInfo();
        long originalSqlMode = connectContext.getSessionVariable().getSqlMode();
        try {
            connectContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
            executeSql("alter stream test_alter_stream_comment_literal.s1 set comment 'a\\nb'");
            Assertions.assertEquals("a\\nb", stream.getComment());
        } finally {
            connectContext.getSessionVariable().setSqlMode(originalSqlMode);
        }

        dropDatabase("test_alter_stream_comment_literal");
    }
}
