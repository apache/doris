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

import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LancePreparedStatementTest {
    private ConnectContext previous;

    @BeforeEach
    public void setUp() {
        previous = ConnectContext.get();
        MemoTestUtils.createStatementContext("");
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        if (previous != null) {
            previous.setThreadLocalInfo();
        }
    }

    @Test
    public void testRejectParameterizedTableAndColumn() {
        for (String property : new String[] {"table", "column", "use_index"}) {
            Assertions.assertThrows(AnalysisException.class, () -> new NereidsParser().parseSingle(
                    "select * from vector_search('" + property + "'=?)"));
        }
    }

    @Test
    public void testRejectParameterizedKeysAndDuplicateProperties() {
        Assertions.assertThrows(AnalysisException.class, () -> new NereidsParser().parseSingle(
                "select * from vector_search(?='value')"));
        Assertions.assertThrows(AnalysisException.class, () -> new NereidsParser().parseSingle(
                "select * from vector_search('query_vector'=?, 'QUERY_VECTOR'='[1,2]')"));
    }

    @Test
    public void testQuotedQuestionMarkRemainsLiteral() {
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select * from vector_search('table'='catalog.db.items', "
                        + "'column'='embedding', 'query_vector'='[1,2]', 'filter'=\"label = '?'\")");
        UnboundTVFRelation relation = plan.<UnboundTVFRelation>collectToList(
                UnboundTVFRelation.class::isInstance).get(0);
        Assertions.assertEquals("label = '?'", relation.getProperties().getMap().get("filter"));
        Assertions.assertTrue(relation.getExpressions().isEmpty());
    }

    @Test
    public void testVectorParameterSurvivesParsing() {
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select * from vector_search('table'='catalog.db.items', "
                        + "'column'='embedding', 'QUERY_VECTOR'=?)");
        UnboundTVFRelation relation = plan.<UnboundTVFRelation>collectToList(
                UnboundTVFRelation.class::isInstance).get(0);
        Assertions.assertTrue(relation.getExpressions().stream()
                .anyMatch(expression -> expression.anyMatch(Placeholder.class::isInstance)),
                "The retained TVF must keep the placeholder instead of the text '?'");
    }
}
