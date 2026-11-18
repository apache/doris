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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BooleanType;
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
    public void testAllNullContainerProducersTypeOnlyTheirValues() {
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
                    Assertions.assertEquals(BooleanType.INSTANCE,
                            ((SpmConstVar) record.placeholderExpr()).getDataType(), container);
                } else {
                    Assertions.assertEquals(record.originalExpr().getDataType(),
                            ((SpmConstVar) record.placeholderExpr()).getDataType(), container);
                }
            }
            Assertions.assertTrue(nulls > 0, container);
        }
    }

    @Test
    public void testBareNullContainerArgumentsRemainArrays() {
        TestSPMPlaceholderBuilder builder = parameterize("SELECT k > 0 OR element_at("
                + "array_concat(NULL, array_sort(NULL)), 1) AS c FROM t");
        int arrays = 0;
        for (PlaceholderSnapshot record : builder.getPlaceholderExprs()) {
            if (record.originalExpr().isNullLiteral()) {
                Assertions.assertTrue(record.placeholderExpr().getDataType()
                        instanceof org.apache.doris.nereids.types.ArrayType);
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
            int typedNulls = 0;
            int untypedNulls = 0;
            for (PlaceholderSnapshot record : builder.getPlaceholderExprs()) {
                if (record.originalExpr().isNullLiteral()) {
                    Expression marker = record.placeholderExpr();
                    if (marker.getDataType().isNullType()) {
                        untypedNulls++;
                    } else {
                        Assertions.assertEquals(BooleanType.INSTANCE, marker.getDataType(), container);
                        typedNulls++;
                    }
                }
            }
            Assertions.assertTrue(typedNulls > 0, container);
            Assertions.assertTrue(untypedNulls > 0, container);
        }
    }

    @Test
    public void testOrdinalsReferenceParameterizedSelectItems() {
        String[] queries = {
                "SELECT k + 1 AS c FROM t ORDER BY 1",
                "SELECT k + 1 AS c FROM t ORDER BY 1 DESC LIMIT 2",
                "SELECT k + 1 AS c, count(*) AS n FROM t GROUP BY 1",
                "SELECT k, sum(v + 1) AS c FROM t GROUP BY k HAVING count(*) > 0 ORDER BY 2",
                "SELECT 1 AS c ORDER BY 1",
                "SELECT k + 1 AS c FROM t UNION ALL SELECT k + 2 AS c FROM u ORDER BY 1",
                "SELECT * FROM t WHERE EXISTS (SELECT k + 1 AS c FROM u ORDER BY 1 LIMIT 1)"
        };
        for (String query : queries) {
            TestSPMPlaceholderBuilder builder = parameterize(query);
            Assertions.assertTrue(builder.hasParameterizedOrdinalTarget(), query);
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
            Assertions.assertFalse(parameterize(query).hasParameterizedOrdinalTarget(), query);
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
