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

package org.apache.doris.sqlparser;

import org.apache.doris.nereids.DorisParser.ExpressionContext;
import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.exceptions.ParseException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DorisSqlParserTest {
    private final DorisSqlParser parser = new DorisSqlParser();

    @Test
    void parsesSimpleSelect() {
        SingleStatementContext ctx = parser.parseStatement("SELECT 1");
        Assertions.assertNotNull(ctx);
        Assertions.assertNotNull(ctx.statement());
    }

    @Test
    void parsesSelectWithFromAndWhere() {
        SingleStatementContext ctx = parser.parseStatement("SELECT a, b FROM t WHERE a > 1");
        Assertions.assertNotNull(ctx);
        Assertions.assertNotNull(ctx.statement());
    }

    @Test
    void parsesMultipleStatements() {
        MultiStatementsContext ctx = parser.parseStatements("SELECT 1; SELECT 2; SELECT 3");
        Assertions.assertNotNull(ctx);
        Assertions.assertEquals(3, ctx.statement().size());
    }

    @Test
    void parsesExpression() {
        ExpressionContext expr = parser.parseExpression("a + 1 * b");
        Assertions.assertNotNull(expr);
    }

    @Test
    void parsesDdl() {
        SingleStatementContext ctx = parser.parseStatement(
                "CREATE TABLE t (id INT, name VARCHAR(64)) DISTRIBUTED BY HASH(id) BUCKETS 4 "
                        + "PROPERTIES (\"replication_num\" = \"1\")");
        Assertions.assertNotNull(ctx);
    }

    @Test
    void rejectsMalformedSql() {
        Assertions.assertThrows(ParseException.class, () -> parser.parseStatement("SELEKT 1"));
    }

    @Test
    void rejectsTrailingGarbageInExpression() {
        Assertions.assertThrows(ParseException.class, () -> parser.parseExpression("1 + 2 BAD GARBAGE"));
    }

    @Test
    void parsesPlainBlockComment() {
        SingleStatementContext ctx = parser.parseStatement("SELECT 1 /* a plain block comment */ FROM t");
        Assertions.assertNotNull(ctx);
        Assertions.assertNotNull(ctx.statement());
        // comment is on a non-default channel, so it is excluded from getText()
        Assertions.assertTrue(ctx.getText().startsWith("SELECT1FROMt"), ctx.getText());
        Assertions.assertFalse(ctx.getText().contains("plain"), ctx.getText());
    }

    // #59691: block comments must NOT nest. A `/*` is closed by the FIRST following
    // `*/`, so the inner `/*` is just comment text. This is the exact shape from the
    // issue: `/* ... /* */ <active sql> -- */`. The first `*/` (inside `/* */`) closes
    // the comment, so `AND id = 3` stays active and `-- */` is a trailing line comment.
    // With the previous nesting grammar the outer comment consumed both `*/` markers
    // and silently swallowed `AND id = 3`, leaving only `WHERE id = 1` (wrong result).
    @Test
    void blockCommentDoesNotNest() {
        SingleStatementContext ctx = parser.parseStatement(
                "SELECT id FROM t WHERE id = 1 /* AND id = 2 /* */ AND id = 3 -- */");
        Assertions.assertNotNull(ctx);
        // The surviving SQL must keep `AND id = 3`; the `AND id = 2` inside the comment
        // must be dropped.
        String text = ctx.getText();
        Assertions.assertTrue(text.contains("ANDid=3"),
                "trailing `AND id = 3` must survive a non-nesting block comment, got: " + text);
        Assertions.assertFalse(text.contains("id=2"),
                "`AND id = 2` is inside the comment and must be dropped, got: " + text);
    }

    // #59691 second form: `/* ... /*/`. The `/*` closes at the `*/` inside `/*/`,
    // so `AND id = 3` stays active.
    @Test
    void blockCommentSecondFormFromIssue() {
        SingleStatementContext ctx = parser.parseStatement(
                "SELECT id FROM t WHERE id = 1 /* AND id = 2 /*/ AND id = 3");
        Assertions.assertNotNull(ctx);
        String text = ctx.getText();
        Assertions.assertTrue(text.contains("ANDid=3"),
                "trailing `AND id = 3` must survive, got: " + text);
        Assertions.assertFalse(text.contains("id=2"),
                "`AND id = 2` is inside the comment and must be dropped, got: " + text);
    }
}
