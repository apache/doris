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

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.datasets.tpch.AnalyzeCheckTestBase;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.rewrite.rules.TypeCoercion;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Count;
import org.apache.doris.nereids.trees.expressions.functions.Min;
import org.apache.doris.nereids.trees.expressions.functions.Sum;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

public class HavingClauseTest extends AnalyzeCheckTestBase implements PatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_having");
        connectContext.setDatabase("default_cluster:test_having");
        createTables(
                "CREATE TABLE t1 (\n"
                        + "    pk TINYINT,\n"
                        + "    a1 TINYINT,\n"
                        + "    a2 TINYINT\n"
                        + ")\n"
                        + "DUPLICATE KEY (pk)\n"
                        + "DISTRIBUTED BY HASH (pk)\n"
                        + "PROPERTIES(\n"
                        + "    'replication_num' = '1'\n"
                        + ");",
                "CREATE TABLE t2 (\n"
                        + "    pk TINYINT,\n"
                        + "    b1 TINYINT,\n"
                        + "    b2 TINYINT\n"
                        + ")\n"
                        + "DUPLICATE KEY (pk)\n"
                        + "DISTRIBUTED BY HASH (pk)\n"
                        + "PROPERTIES(\n"
                        + "    'replication_num' = '1'\n"
                        + ");"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Test
    public void testHavingGroupBySlot() throws Exception {
        String sql = "SELECT a1 FROM t1 GROUP BY a1 HAVING a1 > 0";
        SlotReference a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalFilter(
                        logicalAggregate(
                            logicalOlapScan()
                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1)))
                    ).when(FieldChecker.check("predicates", new GreaterThan(a1, new TinyIntLiteral((byte) 0))))
                );
        NamedExpressionUtil.clear();

        sql = "SELECT a1 as value FROM t1 GROUP BY a1 HAVING a1 > 0";
        a1 = new SlotReference(
                new ExprId(2), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        Alias value = new Alias(new ExprId(0), a1, "value");
        PlanChecker.from(connectContext).analyze(sql)
                .applyBottomUp(new ExpressionRewrite(TypeCoercion.INSTANCE))
                .matchesFromRoot(
                    logicalFilter(
                        logicalAggregate(
                            logicalOlapScan()
                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(value)))
                    ).when(FieldChecker.check("predicates", new GreaterThan(value.toSlot(), new TinyIntLiteral((byte) 0)))));
        NamedExpressionUtil.clear();

        sql = "SELECT a1 as value FROM t1 GROUP BY a1 HAVING value > 0";
        PlanChecker.from(connectContext).analyze(sql)
                .applyBottomUp(new ExpressionRewrite(TypeCoercion.INSTANCE))
                .matchesFromRoot(
                    logicalFilter(
                        logicalAggregate(
                            logicalOlapScan()
                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(value)))
                    ).when(FieldChecker.check("predicates", new GreaterThan(value.toSlot(), new TinyIntLiteral((byte) 0)))));
        NamedExpressionUtil.clear();

        sql = "SELECT SUM(a2) FROM t1 GROUP BY a1 HAVING a1 > 0";
        a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        Alias sumA2 = new Alias(new ExprId(3), new Sum(a2), "SUM(a2)");
        PlanChecker.from(connectContext).analyze(sql)
                .applyBottomUp(new ExpressionRewrite(TypeCoercion.INSTANCE))
                .matchesFromRoot(
                    logicalProject(
                        logicalFilter(
                            logicalAggregate(
                                logicalOlapScan()
                            ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(sumA2, a1)))
                        ).when(FieldChecker.check("predicates", new GreaterThan(a1, new TinyIntLiteral((byte) 0))))
                    ).when(FieldChecker.check("projects", Lists.newArrayList(sumA2.toSlot()))));
        NamedExpressionUtil.clear();
    }

    @Test
    public void testHavingAggregateFunction() throws Exception {
        String sql = "SELECT a1 FROM t1 GROUP BY a1 HAVING SUM(a2) > 0";
        SlotReference a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        Alias sumA2 = new Alias(new ExprId(3), new Sum(a2), "sum(a2)");
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalProject(
                        logicalFilter(
                            logicalAggregate(
                                logicalOlapScan()
                            ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2)))
                        ).when(FieldChecker.check("predicates", new GreaterThan(sumA2.toSlot(), new TinyIntLiteral((byte) 0))))
                    ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot()))));
        NamedExpressionUtil.clear();

        sql = "SELECT a1, SUM(a2) FROM t1 GROUP BY a1 HAVING SUM(a2) > 0";
        sumA2 = new Alias(new ExprId(3), new Sum(a2), "SUM(a2)");
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalFilter(
                        logicalAggregate(
                            logicalOlapScan()
                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2)))
                    ).when(FieldChecker.check("predicates", new GreaterThan(sumA2.toSlot(), new TinyIntLiteral((byte) 0)))));
        NamedExpressionUtil.clear();

        sql = "SELECT a1, SUM(a2) as value FROM t1 GROUP BY a1 HAVING SUM(a2) > 0";
        a1 = new SlotReference(
                new ExprId(2), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        a2 = new SlotReference(
                new ExprId(3), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        Alias value = new Alias(new ExprId(0), new Sum(a2), "value");
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalFilter(
                        logicalAggregate(
                            logicalOlapScan()
                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, value)))
                    ).when(FieldChecker.check("predicates", new GreaterThan(value.toSlot(), new TinyIntLiteral((byte) 0)))));
        NamedExpressionUtil.clear();

        sql = "SELECT a1, SUM(a2) as value FROM t1 GROUP BY a1 HAVING value > 0";
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalFilter(
                        logicalAggregate(
                            logicalOlapScan()
                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, value)))
                    ).when(FieldChecker.check("predicates", new GreaterThan(value.toSlot(), new TinyIntLiteral((byte) 0)))));
        NamedExpressionUtil.clear();

        sql = "SELECT a1, SUM(a2) FROM t1 GROUP BY a1 HAVING MIN(pk) > 0";
        a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        sumA2 = new Alias(new ExprId(3), new Sum(a2), "SUM(a2)");
        SlotReference pk = new SlotReference(
                new ExprId(0), "pk", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        Alias minPK = new Alias(new ExprId(4), new Min(pk), "min(pk)");
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalProject(
                        logicalFilter(
                            logicalAggregate(
                                logicalOlapScan()
                            ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2, minPK)))
                        ).when(FieldChecker.check("predicates", new GreaterThan(minPK.toSlot(), new TinyIntLiteral((byte) 0))))
                    ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot(), sumA2.toSlot()))));
        NamedExpressionUtil.clear();

        sql = "SELECT a1, SUM(a1 + a2) FROM t1 GROUP BY a1 HAVING SUM(a1 + a2) > 0";
        Alias sumA1A2 = new Alias(new ExprId(3), new Sum(new Add(a1, a2)), "SUM((a1 + a2))");
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalFilter(
                        logicalAggregate(
                            logicalOlapScan()
                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA1A2)))
                    ).when(FieldChecker.check("predicates", new GreaterThan(sumA1A2.toSlot(), new TinyIntLiteral((byte) 0)))));
        NamedExpressionUtil.clear();

        sql = "SELECT a1, SUM(a1 + a2) FROM t1 GROUP BY a1 HAVING SUM(a1 + a2 + 3) > 0";
        Alias sumA1A23 = new Alias(new ExprId(4), new Sum(new Add(new Add(a1, a2), new TinyIntLiteral((byte) 3))),
                "sum(((a1 + a2) + 3))");
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalProject(
                        logicalFilter(
                            logicalAggregate(
                                logicalOlapScan()
                            ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA1A2, sumA1A23)))
                        ).when(FieldChecker.check("predicates", new GreaterThan(sumA1A23.toSlot(), new TinyIntLiteral((byte) 0))))
                    ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot(), sumA1A2.toSlot()))));
        NamedExpressionUtil.clear();

        sql = "SELECT a1 FROM t1 GROUP BY a1 HAVING COUNT(*) > 0";
        Alias countStar = new Alias(new ExprId(3), new Count(), "count(*)");
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalProject(
                        logicalFilter(
                            logicalAggregate(
                                logicalOlapScan()
                            ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, countStar)))
                        ).when(FieldChecker.check("predicates", new GreaterThan(countStar.toSlot(), new TinyIntLiteral((byte) 0))))
                    ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot()))));
        NamedExpressionUtil.clear();
    }

    @Test
    void testJoin() throws Exception {
        String sql = "SELECT a1, sum(a2) FROM t1, t2 WHERE t1.pk = t2.pk GROUP BY a1 HAVING a1 > SUM(b1)";
        SlotReference a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        SlotReference b1 = new SlotReference(
                new ExprId(4), "b1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t2")
        );
        Alias sumA2 = new Alias(new ExprId(6), new Sum(a2), "sum(a2)");
        Alias sumB1 = new Alias(new ExprId(7), new Sum(b1), "sum(b1)");
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalProject(
                        logicalFilter(
                            logicalAggregate(
                                logicalFilter(
                                    logicalJoin(
                                        logicalOlapScan(),
                                        logicalOlapScan()
                                    )
                                )
                            ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2, sumB1)))
                        ).when(FieldChecker.check("predicates", new GreaterThan(a1, sumB1.toSlot())))
                    ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot(), sumA2.toSlot()))));
        NamedExpressionUtil.clear();
    }

    @Test
    void testInvalidHaving() {
        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                "a2 in having clause should be grouped by.",
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT a1 FROM t1 GROUP BY a1 HAVING a2 > 0"
                ));

        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                "Aggregate functions in having clause can't be nested: sum((a1 + avg(a2))).",
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT a1 FROM t1 GROUP BY a1 HAVING SUM(a1 + AVG(a2)) > 0"
                ));

        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                "Aggregate functions in having clause can't be nested: sum(((a1 + a2) + avg(a2))).",
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT a1 FROM t1 GROUP BY a1 HAVING SUM(a1 + a2 + AVG(a2)) > 0"
                ));
    }

    @Test
    void testComplexQuery() throws Exception {
        String sql = "SELECT t1.pk + 1, t1.pk + 1 + 1, t1.pk + 2, SUM(a1), COUNT(a1) + 1, SUM(a1 + a2), COUNT(a2) as v1\n"
                + "FROM t1, t2 WHERE t1.pk = t2.pk GROUP BY t1.pk, t1.pk + 1\n"
                + "HAVING t1.pk > 0 AND COUNT(a1) + 1 > 0 AND SUM(a1 + a2) + 1 > 0 AND v1 + 1 > 0 AND v1 > 0";
        SlotReference pk = new SlotReference(
                new ExprId(1), "pk", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        SlotReference a1 = new SlotReference(
                new ExprId(2), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(3), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_having", "t1")
        );
        Alias pk1 = new Alias(new ExprId(7), new Add(pk, Literal.of((byte) 1)), "(pk + 1)");
        Alias pk11 = new Alias(new ExprId(8), new Add(new Add(pk, Literal.of((byte) 1)), Literal.of((byte) 1)), "((pk + 1) + 1)");
        Alias pk2 = new Alias(new ExprId(9), new Add(pk, Literal.of((byte) 2)), "(pk + 2)");
        Alias sumA1 = new Alias(new ExprId(10), new Sum(a1), "SUM(a1)");
        Alias countA11 = new Alias(new ExprId(11), new Add(new Count(a1), Literal.of((byte) 1)), "(COUNT(a1) + 1)");
        Alias sumA1A2 = new Alias(new ExprId(12), new Sum(new Add(a1, a2)), "SUM((a1 + a2))");
        Alias v1 = new Alias(new ExprId(0), new Count(a2), "v1");
        PlanChecker.from(connectContext).analyze(sql)
                .matchesFromRoot(
                    logicalProject(
                        logicalFilter(
                            logicalAggregate(
                                logicalFilter(
                                    logicalJoin(
                                        logicalOlapScan(),
                                        logicalOlapScan()
                                    )
                                )
                            ).when(FieldChecker.check("outputExpressions",
                                    Lists.newArrayList(pk1, pk11, pk2, sumA1, countA11, sumA1A2, v1, pk)))
                        ).when(FieldChecker.check("predicates",
                                new And(
                                        new And(
                                                new And(
                                                        new And(
                                                                new GreaterThan(pk.toSlot(), Literal.of((byte) 0)),
                                                                new GreaterThan(countA11.toSlot(), Literal.of((byte) 0))),
                                                        new GreaterThan(new Add(sumA1A2.toSlot(), Literal.of((byte) 1)), Literal.of((byte) 0))),
                                                new GreaterThan(new Add(v1.toSlot(), Literal.of((byte) 1)), Literal.of((byte) 0))
                                        ),
                                        new GreaterThan(v1.toSlot(), Literal.of((byte) 0))
                                )
                        ))
                    ).when(FieldChecker.check(
                        "projects", Lists.newArrayList(
                            pk1, pk11, pk2, sumA1, countA11, sumA1A2, v1).stream()
                                    .map(Alias::toSlot).collect(Collectors.toList()))
                    ));
        NamedExpressionUtil.clear();
    }
}
