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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.PlaceholderSnapshot;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.spm.placeholder.TestSPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Tests occurrence identity, container value typing and positional SELECT references. */
public class SPMPlaceholderContextTest {
    @Test
    public void testRootOccurrencePathsAreIndependentOfSubqueryCallSite() {
        TestSPMPlaceholderBuilder root = new TestSPMPlaceholderBuilder();
        root.parameterizeExpressions(List.of(new IntegerLiteral(1)));
        Assertions.assertEquals("", root.getPlaceholderExprs().get(0).occurrencePath());

        TestSPMPlaceholderBuilder builder = parameterize("SELECT 1 AS x FROM t WHERE k = 1"
                + " AND (EXISTS (SELECT 1 FROM u WHERE k = 1)"
                + " OR EXISTS (SELECT 1 FROM v WHERE k = 1))");
        Set<Long> blocks = new HashSet<>();
        for (PlaceholderSnapshot record : builder.getPlaceholderExprs()) {
            blocks.add(record.blockId());
            Assertions.assertFalse(record.occurrencePath().contains("-1"));
            if (record.parentExpr() == null) {
                Assertions.assertEquals("", record.occurrencePath());
            }
            if (record.blockId() > 0 && record.childIndex() == 1) {
                Assertions.assertEquals("1", record.occurrencePath(),
                        "a subquery filter root must not inherit its outer expression path");
            }
        }
        Assertions.assertEquals(3, blocks.size());
        int size = builder.getPlaceholderExprs().size();
        builder.startNewTree();
        SPMPlanTreeSupport.transform(parse("SELECT 1 AS x FROM t WHERE k = 1"
                + " AND (EXISTS (SELECT 1 FROM u WHERE k = 1)"
                + " OR EXISTS (SELECT 1 FROM v WHERE k = 1))"), builder);
        Assertions.assertEquals(size, builder.getPlaceholderExprs().size(),
                "corresponding bind and plan occurrences must reuse the same ids");
    }

    @Test
    public void testAllNullContainerProducersPreserveLiteralTypes() {
        String[] containers = {
                "array(NULL)", "map('x', NULL)", "array_repeat(NULL, 2)",
                "array_concat(array(NULL), array_repeat(NULL, 2))",
                "array_reverse_sort(array(NULL))", "map_values(map('x', NULL))",
                "map_filter(map('x', NULL), (mk, mv) -> TRUE)"
        };
        for (String container : containers) {
            String index = container.startsWith("map(") || container.startsWith("map_filter(")
                    ? "'x'" : "1";
            TestSPMPlaceholderBuilder builder = parameterize("SELECT k > 0 OR element_at("
                    + container + ", " + index + ") AS c FROM t");
            int nulls = 0;
            for (PlaceholderSnapshot record : builder.getPlaceholderExprs()) {
                if (record.originalExpr().isNullLiteral()) {
                    nulls++;
                    Assertions.assertTrue(record.placeholderExpr().getDataType().isNullType(), container);
                } else {
                    Assertions.assertEquals(record.originalExpr().getDataType(),
                            ((SpmConstVar) record.placeholderExpr()).getDataType(), container);
                }
            }
            Assertions.assertTrue(nulls > 0, container);
        }
    }

    @Test
    public void testBareNullContainerArgumentsRemainNullType() {
        TestSPMPlaceholderBuilder builder = parameterize("SELECT k > 0 OR element_at("
                + "array_concat(NULL, array_sort(NULL)), 1) AS c FROM t");
        int arrays = 0;
        for (PlaceholderSnapshot record : builder.getPlaceholderExprs()) {
            if (record.originalExpr().isNullLiteral()) {
                Assertions.assertTrue(record.placeholderExpr().getDataType().isNullType());
                arrays++;
            }
        }
        Assertions.assertEquals(2, arrays);
    }

    @Test
    public void testExcludedContainerArgumentsKeepTheirDescendantsTypes() {
        String[] containers = {
                "map(ifnull(NULL, NULL), NULL)",
                "array_repeat(NULL, ifnull(NULL, NULL))",
                "array(CASE WHEN k = NULL THEN NULL ELSE NULL END)",
                "array(if(k = NULL, NULL, NULL))"
        };
        for (String container : containers) {
            TestSPMPlaceholderBuilder builder = parameterize("SELECT k > 0 OR element_at("
                    + container + ", 1) AS c FROM t");
            int untypedNulls = 0;
            for (PlaceholderSnapshot record : builder.getPlaceholderExprs()) {
                if (record.originalExpr().isNullLiteral()) {
                    Assertions.assertTrue(record.placeholderExpr().getDataType().isNullType(), container);
                    untypedNulls++;
                }
            }
            Assertions.assertTrue(untypedNulls > 1, container);
        }
    }

    @Test
    public void testOrderByOrdinalsRemainPositionalWithParameterizedSelectItems() {
        String[] queries = {
                "SELECT k + 1 AS c FROM t ORDER BY 1",
                "SELECT k + 1 AS c FROM t ORDER BY 1 DESC LIMIT 2",
                "SELECT k, sum(v + 1) AS c FROM t GROUP BY k HAVING count(*) > 0 ORDER BY 2",
                "SELECT 1 AS c ORDER BY 1",
                "SELECT k + 1 AS c FROM t UNION ALL SELECT k + 2 AS c FROM u ORDER BY 1"
        };
        for (String query : queries) {
            LogicalPlan plan = parse(query);
            TestSPMPlaceholderBuilder builder = new TestSPMPlaceholderBuilder();
            LogicalPlan parameterized = SPMPlanTreeSupport.transform(plan, builder);
            List<String> originalOrder = SPMPlanTreeSupport.rootOrderContract(plan);
            Assertions.assertFalse(originalOrder.isEmpty(), query);
            Assertions.assertEquals(originalOrder, SPMPlanTreeSupport.rootOrderContract(parameterized), query);
            Assertions.assertFalse(builder.getPlaceholderExprs().isEmpty(), query);
            Assertions.assertTrue(builder.getGroupingColumnTargets().isEmpty(), query);
        }
    }

    @Test
    public void testParameterizedGroupByOrdinalRecordsTargetIds() {
        String query = "SELECT k + 1 AS c, count(*) AS n FROM t GROUP BY 1";
        TestSPMPlaceholderBuilder builder = parameterize(query);
        Assertions.assertEquals(Set.of(Set.of(1L)), builder.getGroupingColumnTargets());

        builder = parameterize("SELECT k + 1 AS c, v * 2 AS d, count(*) AS n FROM t GROUP BY 2, 1");
        Assertions.assertEquals(Set.of(Set.of(1L), Set.of(2L)), builder.getGroupingColumnTargets());
        builder = parameterize("SELECT (SELECT 1 AS c FROM u GROUP BY 1) FROM t");
        Assertions.assertEquals(Set.of(Set.of(1L)), builder.getGroupingColumnTargets());
    }

    @Test
    public void testGroupingSetTargetsShareMarkersWithGroupingFunctions() {
        for (String grouping : List.of("ROLLUP(1)", "CUBE(1)", "GROUPING SETS((1), (1), ())")) {
            String query = "SELECT k + 1 AS c, grouping(k + 1) AS g, count(*) AS n FROM t GROUP BY " + grouping;
            TestSPMPlaceholderBuilder builder = parameterize(query);
            Assertions.assertEquals(Set.of(Set.of(1L)), builder.getGroupingColumnTargets(), query);
            Assertions.assertEquals(1, builder.getPlaceholderExprs().size(), query);
        }
        for (String query : List.of(
                "SELECT grouping(k + 1) AS g, k + 1 AS c, count(*) FROM t GROUP BY ROLLUP(2)",
                "SELECT 1 AS c, grouping(1) AS g, count(*) FROM t GROUP BY ROLLUP(1)",
                "SELECT k + 1 AS c, grouping(k + 1) AS g, count(*) FROM t GROUP BY ROLLUP(k + 1)")) {
            TestSPMPlaceholderBuilder builder = parameterize(query);
            Assertions.assertEquals(Set.of(Set.of(1L)), builder.getGroupingColumnTargets(), query);
            Assertions.assertEquals(1, builder.getPlaceholderExprs().size(), query);
        }
    }

    @Test
    public void testOrdinalsOverPlainSlotsDoNotPreventFreezing() {
        String[] queries = {
                "SELECT k FROM t WHERE v = 1 ORDER BY 1",
                "SELECT k, sum(v + 1) AS c FROM t GROUP BY 1 ORDER BY 1",
                "SELECT k, k + 1 AS c FROM t ORDER BY 1",
                "SELECT k + 1 AS c FROM t ORDER BY 9223372036854775808",
                "SELECT k + 1 AS c FROM t GROUP BY 9223372036854775808"
        };
        for (String query : queries) {
            Assertions.assertTrue(parameterize(query).getGroupingColumnTargets().isEmpty(), query);
        }
    }

    private static TestSPMPlaceholderBuilder parameterize(String sql) {
        TestSPMPlaceholderBuilder builder = new TestSPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(parse(sql), builder);
        return builder;
    }

    private static LogicalPlan parse(String sql) {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setSessionVariable(new SessionVariable());
        ctx.setDatabase("db1");
        ctx.setThreadLocalInfo();
        return (LogicalPlan) new NereidsParser().parseSingle(sql);
    }
}
