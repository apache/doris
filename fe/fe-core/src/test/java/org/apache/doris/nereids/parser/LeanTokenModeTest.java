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

package org.apache.doris.nereids.parser;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.properties.SelectHintOrdered;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.sqlparser.DorisSqlParser;

import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class LeanTokenModeTest extends ParserTestBase {
    private final NereidsParser parser = new NereidsParser();

    @Test
    void preservesHintsInDirectAstParsing() {
        LogicalPlan plan = parser.parseSingle("SELECT /*+ ORDERED */ a -- ordinary\nFROM t");
        List<LogicalSelectHint<Plan>> hintPlans = plan.collectToList(LogicalSelectHint.class::isInstance);

        Assertions.assertEquals(1, hintPlans.size());
        Assertions.assertTrue(hintPlans.get(0).getHints().stream()
                .anyMatch(SelectHintOrdered.class::isInstance));
    }

    @Test
    void keepsFullTokensForScanAndCommentNormalization() {
        String sql = "SELECT /* ordinary */ /*+ ORDERED */ a -- tail\nFROM t";
        List<Integer> tokenTypes = new ArrayList<>();
        TokenSource tokenSource = NereidsParser.scan(sql);
        Token token;
        do {
            token = tokenSource.nextToken();
            tokenTypes.add(token.getType());
        } while (token.getType() != Token.EOF);

        Assertions.assertTrue(tokenTypes.contains(DorisLexer.WS));
        Assertions.assertTrue(tokenTypes.contains(DorisLexer.SIMPLE_COMMENT));
        Assertions.assertTrue(tokenTypes.contains(DorisLexer.BRACKETED_COMMENT));
        Assertions.assertEquals("SELECT /*+ ORDERED */ a FROM t",
                NereidsParser.removeCommentAndTrimBlank(sql));
    }

    @Test
    void preservesCharacterIntervalsAcrossSkippedTokens() {
        String sql = "SELECT /* leading */ db -- between\n . tbl . col FROM t";
        LogicalPlan plan = parser.parseForCreateView(sql);
        UnboundSlot slot = plan.<LogicalPlan>collectToList(ignored -> true).stream()
                .flatMap(node -> node.getExpressions().stream())
                .flatMap(expression -> expression.<UnboundSlot>collectToList(
                        UnboundSlot.class::isInstance).stream())
                .filter(unboundSlot -> unboundSlot.getNameParts().equals(List.of("db", "tbl", "col")))
                .findFirst()
                .orElseThrow();

        Assertions.assertEquals(Pair.of(sql.indexOf("db"), sql.indexOf("col") + 2),
                slot.getIndexInSqlString().orElseThrow());
        Assertions.assertEquals(sql, parser.parseForSyncMv(
                "CREATE MATERIALIZED VIEW mv AS " + sql).orElseThrow());
    }

    @Test
    void preservesErrorsInBothQueryOrganizationModes() {
        boolean previous = GlobalVariable.enable_ansi_query_organization_behavior;
        try {
            for (boolean ansi : new boolean[] {false, true}) {
                GlobalVariable.enable_ansi_query_organization_behavior = ansi;
                for (String sql : List.of(
                        "SELECT a\n-- skipped comment\nFROM t WHERE )",
                        "CREATE TABLE t ( k1 BOOL )")) {
                    ParseException fullException = Assertions.assertThrows(ParseException.class,
                            () -> new DorisSqlParser(false, ansi).parseStatement(sql));
                    ParseException leanException = Assertions.assertThrows(ParseException.class,
                            () -> parser.parseSingle(sql));
                    Assertions.assertEquals(fullException.getClass(), leanException.getClass());
                    Assertions.assertEquals(fullException.getMessage(), leanException.getMessage());
                }
            }
        } finally {
            GlobalVariable.enable_ansi_query_organization_behavior = previous;
        }
    }
}
