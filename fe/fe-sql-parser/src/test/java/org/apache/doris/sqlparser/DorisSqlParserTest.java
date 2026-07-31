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
}
