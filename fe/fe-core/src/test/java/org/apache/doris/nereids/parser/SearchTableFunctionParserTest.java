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

/**
 * The parameters of a prepared statement in a search table function's arguments: any argument but {@code table} may
 * be a parameter, and which of them may vary between executions is the connector's to decide when the search is
 * bound. The parser-side cases of branch-4.2's LancePreparedStatementTest, for both search functions.
 */
public class SearchTableFunctionParserTest {
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
    public void testParametersOfEveryArgumentButTableSurviveParsing() {
        for (String function : new String[] {"vector_search", "full_text_search"}) {
            UnboundTVFRelation relation = parseRelation("select * from " + function
                    + "('table'='catalog.db.items', 'COLUMN'=?, 'Query_Vector'=?, 'top_k'='5')");

            // By their lower-case names, as the connector receives them; the constant stays a property.
            Assertions.assertEquals(2, relation.getPropertyParameters().size(), function);
            Assertions.assertTrue(relation.getPropertyParameters().containsKey("column"), function);
            Assertions.assertTrue(relation.getPropertyParameters().containsKey("query_vector"), function);
            Assertions.assertEquals("5", relation.getProperties().getMap().get("top_k"));
            Assertions.assertFalse(relation.getProperties().getMap().containsKey("query_vector"));
            Assertions.assertTrue(relation.getExpressions().stream()
                    .anyMatch(expression -> expression.anyMatch(Placeholder.class::isInstance)),
                    "the unbound relation must keep the placeholders instead of the text '?'");
        }
    }

    @Test
    public void testRejectsAParameterizedTableOrArgumentName() {
        assertRejected("select * from vector_search('table'=?)", "vector_search property 'table' must be constant");
        assertRejected("select * from full_text_search('TABLE'=?)",
                "full_text_search property 'table' must be constant");
        assertRejected("select * from vector_search(?='value')", "vector_search property names must be constant");
    }

    @Test
    public void testRejectsArgumentsThatDifferOnlyInCase() {
        assertRejected("select * from vector_search('query_vector'=?, 'QUERY_VECTOR'='[1,2]')",
                "Duplicate vector_search property: query_vector");
        assertRejected("select * from full_text_search('query'='a', 'Query'='b')",
                "Duplicate full_text_search property: query");
    }

    @Test
    public void testQuotedQuestionMarkRemainsLiteral() {
        UnboundTVFRelation relation = parseRelation("select * from vector_search('table'='catalog.db.items', "
                + "'column'='embedding', 'query_vector'='[1,2]', 'filter'=\"label = '?'\")");

        Assertions.assertEquals("label = '?'", relation.getProperties().getMap().get("filter"));
        Assertions.assertTrue(relation.getPropertyParameters().isEmpty());
        Assertions.assertTrue(relation.getExpressions().isEmpty());
    }

    private static UnboundTVFRelation parseRelation(String sql) {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        return plan.<UnboundTVFRelation>collectToList(UnboundTVFRelation.class::isInstance).get(0);
    }

    private static void assertRejected(String sql, String message) {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> new NereidsParser().parseSingle(sql));
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
    }
}
