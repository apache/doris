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

import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.ExpressionContext;
import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.exceptions.ParseException;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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
    void normalizesIdentifiersInGrammarRules() {
        String sql = "SELECT ordinary, actions, `AD``D` FROM catalog.`db``name`.`table name`";
        List<String> expected = List.of("ordinary", "actions", "AD`D", "catalog", "db`name", "table name");

        SingleStatementContext facadeTree = parser.parseStatement(sql);
        Assertions.assertEquals(expected, identifierTexts(facadeTree));

        DorisParser generatedParser = parser.newParser(parser.newLexer(sql));
        Assertions.assertTrue(generatedParser.getParseListeners().isEmpty());
        SingleStatementContext generatedTree = generatedParser.singleStatement();
        Assertions.assertEquals(expected, identifierTexts(generatedTree));

        List<Token> identifiers = identifierTokens(facadeTree);
        Token nonReserved = identifiers.get(1);
        Assertions.assertEquals(DorisParser.IDENTIFIER, nonReserved.getType());
        Assertions.assertEquals(sql.indexOf("actions"), nonReserved.getStartIndex());
        Assertions.assertEquals(sql.indexOf("actions") + "actions".length() - 1, nonReserved.getStopIndex());

        Token quoted = identifiers.get(2);
        Assertions.assertEquals(DorisParser.IDENTIFIER, quoted.getType());
        Assertions.assertEquals(sql.indexOf("`AD``D`") + 1, quoted.getStartIndex());
        Assertions.assertEquals(sql.indexOf("`AD``D`") + "`AD``D`".length() - 2, quoted.getStopIndex());
    }

    @Test
    void rejectsUnquotedIdentifiersInGrammarRule() {
        ParseException reservedSuffixException = Assertions.assertThrows(
                ParseException.class, () -> parser.parseStatement("SELECT * FROM test-table"));
        Assertions.assertTrue(reservedSuffixException.getMessage().contains(
                "Possibly unquoted identifier test- detected"));

        String sql = "SELECT * FROM test-tbl";
        ParseException facadeException = Assertions.assertThrows(
                ParseException.class, () -> parser.parseStatement(sql));

        DorisParser generatedParser = parser.newParser(parser.newLexer(sql));
        ParseException generatedException = Assertions.assertThrows(
                ParseException.class, generatedParser::singleStatement);

        Assertions.assertEquals(facadeException.getMessage(), generatedException.getMessage());
        Assertions.assertTrue(facadeException.getMessage().contains(
                "Possibly unquoted identifier test-tbl detected"));
    }

    @Test
    void parsesWithoutBuildingParseTree() {
        DorisParser generatedParser = parser.newParser(
                parser.newLexer("SELECT actions, `AD``D` FROM t"));
        generatedParser.setBuildParseTree(false);
        Assertions.assertDoesNotThrow(generatedParser::singleStatement);
    }

    private static List<String> identifierTexts(ParseTree tree) {
        List<String> texts = new ArrayList<>();
        for (Token token : identifierTokens(tree)) {
            texts.add(token.getText());
        }
        return texts;
    }

    private static List<Token> identifierTokens(ParseTree tree) {
        List<Token> tokens = new ArrayList<>();
        collectIdentifierTokens(tree, tokens);
        return tokens;
    }

    private static void collectIdentifierTokens(ParseTree tree, List<Token> tokens) {
        if (tree instanceof TerminalNode) {
            Token token = ((TerminalNode) tree).getSymbol();
            if (token.getType() == DorisParser.IDENTIFIER) {
                tokens.add(token);
            }
            return;
        }
        for (int index = 0; index < tree.getChildCount(); index++) {
            collectIdentifierTokens(tree.getChild(index), tokens);
        }
    }
}
