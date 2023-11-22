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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.nereids.datasets.tpch.AnalyzeCheckTestBase;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.expression.ExpressionRewrite;
import org.apache.doris.nereids.rules.expression.rules.FunctionBinder;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class FillUpMissingSlotsTest extends AnalyzeCheckTestBase implements MemoPatternMatchSupported {

    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test_resolve_aggregate_functions");
        connectContext.setDatabase("default_cluster:test_resolve_aggregate_functions");
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

    @Test
    public void testHavingGroupBySlot() {
        String sql = "SELECT a1 FROM t1 GROUP BY a1 HAVING a1 > 0";
        SlotReference a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalFilter(
                                logicalProject(
                                        logicalAggregate(
                                                logicalProject(logicalOlapScan())
                                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1))))));

        sql = "SELECT a1 as value FROM t1 GROUP BY a1 HAVING a1 > 0";
        SlotReference value = new SlotReference(new ExprId(3), "value", TinyIntType.INSTANCE, true,
                ImmutableList.of());
        PlanChecker.from(connectContext).analyze(sql)
                .applyBottomUp(new ExpressionRewrite(FunctionBinder.INSTANCE))
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1)))
                                        ).when(FieldChecker.check("projects", ImmutableList.of(new Alias(new ExprId(3), a1, value.toSql()))))
                                ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(value.toSlot(), new TinyIntLiteral((byte) 0)))))));

        sql = "SELECT a1 as value FROM t1 GROUP BY a1 HAVING value > 0";
        PlanChecker.from(connectContext).analyze(sql)
                .applyBottomUp(new ExpressionRewrite(FunctionBinder.INSTANCE))
                .matches(
                        logicalFilter(
                                logicalProject(
                                        logicalAggregate(
                                                logicalProject(logicalOlapScan())
                                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1)))
                                ).when(FieldChecker.check("projects", ImmutableList.of(new Alias(new ExprId(3), a1, value.toSql()))))
                        ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(value.toSlot(), new TinyIntLiteral((byte) 0))))));

        sql = "SELECT sum(a2) FROM t1 GROUP BY a1 HAVING a1 > 0";
        a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        Alias sumA2 = new Alias(new ExprId(3), new Sum(a2), "sum(a2)");
        PlanChecker.from(connectContext).analyze(sql)
                .applyBottomUp(new ExpressionRewrite(FunctionBinder.INSTANCE))
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2))))
                                ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(a1, new TinyIntLiteral((byte) 0)))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(sumA2.toSlot()))));
    }

    @Test
    public void testHavingAggregateFunction() {
        String sql = "SELECT a1 FROM t1 GROUP BY a1 HAVING sum(a2) > 0";
        SlotReference a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        Alias sumA2 = new Alias(new ExprId(3), new Sum(a2), "sum(a2)");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2))))
                                ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(sumA2.toSlot(), Literal.of(0L)))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot()))));

        sql = "SELECT a1, sum(a2) FROM t1 GROUP BY a1 HAVING sum(a2) > 0";
        sumA2 = new Alias(new ExprId(3), new Sum(a2), "sum(a2)");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(
                                                                logicalOlapScan()
                                                        )
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2))))
                                ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(sumA2.toSlot(), Literal.of(0L)))))));

        sql = "SELECT a1, sum(a2) as value FROM t1 GROUP BY a1 HAVING sum(a2) > 0";
        a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        Alias value = new Alias(new ExprId(3), new Sum(a2), "value");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(
                                                                logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, value))))
                                ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(value.toSlot(), Literal.of(0L)))))));

        sql = "SELECT a1, sum(a2) as value FROM t1 GROUP BY a1 HAVING value > 0";
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalFilter(
                                logicalProject(
                                        logicalAggregate(
                                                logicalProject(
                                                        logicalOlapScan())
                                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, value))))
                        ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(value.toSlot(), Literal.of(0L))))));

        sql = "SELECT a1, sum(a2) FROM t1 GROUP BY a1 HAVING MIN(pk) > 0";
        a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        sumA2 = new Alias(new ExprId(3), new Sum(a2), "sum(a2)");
        SlotReference pk = new SlotReference(
                new ExprId(0), "pk", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        Alias minPK = new Alias(new ExprId(4), new Min(pk), "min(pk)");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2, minPK))))
                                ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(minPK.toSlot(), Literal.of((byte) 0)))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot(), sumA2.toSlot()))));

        sql = "SELECT a1, sum(a1 + a2) FROM t1 GROUP BY a1 HAVING sum(a1 + a2) > 0";
        Alias sumA1A2 = new Alias(new ExprId(3), new Sum(new Add(a1, a2)), "sum((a1 + a2))");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA1A2))))
                                ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(sumA1A2.toSlot(), Literal.of(0L)))))));

        sql = "SELECT a1, sum(a1 + a2) FROM t1 GROUP BY a1 HAVING sum(a1 + a2 + 3) > 0";
        Alias sumA1A23 = new Alias(new ExprId(4), new Sum(new Add(new Add(a1, a2), new TinyIntLiteral((byte) 3))),
                "sum(((a1 + a2) + 3))");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA1A2, sumA1A23))))
                                ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(sumA1A23.toSlot(), Literal.of(0L)))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot(), sumA1A2.toSlot()))));

        sql = "SELECT a1 FROM t1 GROUP BY a1 HAVING count(*) > 0";
        Alias countStar = new Alias(new ExprId(3), new Count(), "count(*)");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, countStar))))
                                ).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(countStar.toSlot(), Literal.of(0L)))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot()))));
    }

    @Test
    void testJoinWithHaving() {
        String sql = "SELECT a1, sum(a2) FROM t1, t2 WHERE t1.pk = t2.pk GROUP BY a1 HAVING a1 > sum(b1)";
        SlotReference a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        SlotReference b1 = new SlotReference(
                new ExprId(4), "b1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t2")
        );
        Alias sumA2 = new Alias(new ExprId(6), new Sum(a2), "sum(a2)");
        Alias sumB1 = new Alias(new ExprId(7), new Sum(b1), "sum(b1)");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(
                                                                logicalFilter(
                                                                        logicalJoin(
                                                                                logicalOlapScan(),
                                                                                logicalOlapScan()
                                                                        )
                                                                ))
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2, sumB1)))
                                        )).when(FieldChecker.check("conjuncts", ImmutableSet.of(new GreaterThan(new Cast(a1, BigIntType.INSTANCE),
                                        sumB1.toSlot()))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot(), sumA2.toSlot()))));
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
                "Aggregate functions in having clause can't be nested:"
                        + " sum((cast(a1 as DOUBLE) + avg(a2))).",
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT a1 FROM t1 GROUP BY a1 HAVING sum(a1 + AVG(a2)) > 0"
                ));

        ExceptionChecker.expectThrowsWithMsg(
                AnalysisException.class,
                "Aggregate functions in having clause can't be nested:"
                        + " sum((cast((a1 + a2) as DOUBLE) + avg(a2))).",
                () -> PlanChecker.from(connectContext).analyze(
                        "SELECT a1 FROM t1 GROUP BY a1 HAVING sum(a1 + a2 + AVG(a2)) > 0"
                ));
    }

    @Test
    void testComplexQueryWithHaving() {
        String sql = "SELECT t1.pk + 1, t1.pk + 1 + 1, t1.pk + 2, sum(a1), count(a1) + 1, sum(a1 + a2), count(a2) as v1\n"
                + "FROM t1, t2 WHERE t1.pk = t2.pk GROUP BY t1.pk, t1.pk + 1\n"
                + "HAVING t1.pk > 0 AND count(a1) + 1 > 0 AND sum(a1 + a2) + 1 > 0 AND v1 + 1 > 0 AND v1 > 0";
        SlotReference pk = new SlotReference(
                new ExprId(0), "pk", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        SlotReference pk1 = new SlotReference(
                new ExprId(6), "(pk + 1)", IntegerType.INSTANCE, true,
                ImmutableList.of()
        );
        SlotReference a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        Alias pk11 = new Alias(new ExprId(7), new Add(new Add(pk, Literal.of((byte) 1)), Literal.of((byte) 1)), "((pk + 1) + 1)");
        Alias pk2 = new Alias(new ExprId(8), new Add(pk, Literal.of((byte) 2)), "(pk + 2)");
        Alias sumA1 = new Alias(new ExprId(9), new Sum(a1), "sum(a1)");
        Alias countA1 = new Alias(new ExprId(13), new Count(a1), "count(a1)");
        Alias countA11 = new Alias(new ExprId(10), new Add(countA1.toSlot(), Literal.of((byte) 1)), "(count(a1) + 1)");
        Alias sumA1A2 = new Alias(new ExprId(11), new Sum(new Add(a1, a2)), "sum((a1 + a2))");
        Alias v1 = new Alias(new ExprId(12), new Count(a2), "v1");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(
                                                                logicalFilter(
                                                                        logicalJoin(
                                                                                logicalOlapScan(),
                                                                                logicalOlapScan()
                                                                        )
                                                                ))
                                                ).when(FieldChecker.check("outputExpressions",
                                                Lists.newArrayList(pk, pk1, sumA1, countA1, sumA1A2, v1))))
                                ).when(FieldChecker.check("conjuncts",
                                        ImmutableSet.of(
                                                new GreaterThan(pk.toSlot(), Literal.of((byte) 0)),
                                                new GreaterThan(countA11.toSlot(), Literal.of(0L)),
                                                new GreaterThan(new Add(sumA1A2.toSlot(), Literal.of((byte) 1)), Literal.of(0L)),
                                                new GreaterThan(new Add(v1.toSlot(), Literal.of((byte) 1)), Literal.of(0L)),
                                                new GreaterThan(v1.toSlot(), Literal.of(0L))
                                        ))
                                )
                        ).when(FieldChecker.check(
                                "projects", Lists.newArrayList(
                                                pk1, pk11.toSlot(), pk2.toSlot(), sumA1.toSlot(), countA11.toSlot(), sumA1A2.toSlot(), v1.toSlot())
                                       )
                        ));
    }

    @Test
    public void testSortAggregateFunction() {
        String sql = "SELECT a1 FROM t1 GROUP BY a1 ORDER BY sum(a2)";
        SlotReference a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        Alias sumA2 = new Alias(new ExprId(3), new Sum(a2), "sum(a2)");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalSort(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2))))
                                ).when(FieldChecker.check("orderKeys", ImmutableList.of(new OrderKey(sumA2.toSlot(), true, true))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot()))));

        sql = "SELECT a1, sum(a2) FROM t1 GROUP BY a1 ORDER BY sum(a2)";
        sumA2 = new Alias(new ExprId(3), new Sum(a2), "sum(a2)");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalSort(
                                logicalProject(
                                        logicalAggregate(
                                                logicalProject(logicalOlapScan())
                                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2))))
                        ).when(FieldChecker.check("orderKeys", ImmutableList.of(new OrderKey(sumA2.toSlot(), true, true)))));

        sql = "SELECT a1, sum(a2) as value FROM t1 GROUP BY a1 ORDER BY sum(a2)";
        a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        Alias value = new Alias(new ExprId(3), new Sum(a2), "value");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalSort(
                                logicalProject(
                                        logicalAggregate(
                                                logicalProject(logicalOlapScan())
                                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, value))))
                        ).when(FieldChecker.check("orderKeys", ImmutableList.of(new OrderKey(sumA2.toSlot(), true, true)))));

        sql = "SELECT a1, sum(a2) FROM t1 GROUP BY a1 ORDER BY MIN(pk)";
        a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        sumA2 = new Alias(new ExprId(3), new Sum(a2), "sum(a2)");
        SlotReference pk = new SlotReference(
                new ExprId(0), "pk", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        Alias minPK = new Alias(new ExprId(4), new Min(pk), "min(pk)");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalSort(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA2, minPK))))
                                ).when(FieldChecker.check("orderKeys", ImmutableList.of(new OrderKey(minPK.toSlot(), true, true))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot(), sumA2.toSlot()))));

        sql = "SELECT a1, sum(a1 + a2) FROM t1 GROUP BY a1 ORDER BY sum(a1 + a2)";
        Alias sumA1A2 = new Alias(new ExprId(3), new Sum(new Add(a1, a2)), "sum((a1 + a2))");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalSort(
                                logicalProject(
                                        logicalAggregate(
                                                logicalProject(logicalOlapScan())
                                        ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA1A2))))
                        ).when(FieldChecker.check("orderKeys", ImmutableList.of(new OrderKey(sumA1A2.toSlot(), true, true)))));

        sql = "SELECT a1, sum(a1 + a2) FROM t1 GROUP BY a1 ORDER BY sum(a1 + a2 + 3)";
        Alias sumA1A23 = new Alias(new ExprId(4), new Sum(new Add(new Add(a1, a2), new TinyIntLiteral((byte) 3))),
                "sum(((a1 + a2) + 3))");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalSort(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, sumA1A2, sumA1A23))))
                                ).when(FieldChecker.check("orderKeys", ImmutableList.of(new OrderKey(sumA1A23.toSlot(), true, true))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot(), sumA1A2.toSlot()))));

        sql = "SELECT a1 FROM t1 GROUP BY a1 ORDER BY count(*)";
        Alias countStar = new Alias(new ExprId(3), new Count(), "count(*)");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(
                        logicalProject(
                                logicalSort(
                                        logicalProject(
                                                logicalAggregate(
                                                        logicalProject(logicalOlapScan())
                                                ).when(FieldChecker.check("outputExpressions", Lists.newArrayList(a1, countStar))))
                                ).when(FieldChecker.check("orderKeys", ImmutableList.of(new OrderKey(countStar.toSlot(), true, true))))
                        ).when(FieldChecker.check("projects", Lists.newArrayList(a1.toSlot()))));
    }

    @Test
    void testComplexQueryWithOrderBy() {
        String sql = "SELECT t1.pk + 1, t1.pk + 1 + 1, t1.pk + 2, sum(a1), count(a1) + 1, sum(a1 + a2), count(a2) as v1\n"
                + "FROM t1, t2 WHERE t1.pk = t2.pk GROUP BY t1.pk, t1.pk + 1\n"
                + "ORDER BY t1.pk, count(a1) + 1, sum(a1 + a2) + 1, v1 + 1, v1";
        SlotReference pk = new SlotReference(
                new ExprId(0), "pk", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        SlotReference pk1 = new SlotReference(
                new ExprId(6), "(pk + 1)", IntegerType.INSTANCE, true,
                ImmutableList.of()
        );
        SlotReference a1 = new SlotReference(
                new ExprId(1), "a1", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        SlotReference a2 = new SlotReference(
                new ExprId(2), "a2", TinyIntType.INSTANCE, true,
                ImmutableList.of("default_cluster:test_resolve_aggregate_functions", "t1")
        );
        Alias pk11 = new Alias(new ExprId(7), new Add(new Add(pk, Literal.of((byte) 1)), Literal.of((byte) 1)), "((pk + 1) + 1)");
        Alias pk2 = new Alias(new ExprId(8), new Add(pk, Literal.of((byte) 2)), "(pk + 2)");
        Alias sumA1 = new Alias(new ExprId(9), new Sum(a1), "sum(a1)");
        Alias countA1 = new Alias(new ExprId(13), new Count(a1), "count(a1)");
        Alias countA11 = new Alias(new ExprId(10), new Add(new Count(a1), Literal.of((byte) 1)), "(count(a1) + 1)");
        Alias sumA1A2 = new Alias(new ExprId(11), new Sum(new Add(a1, a2)), "sum((a1 + a2))");
        Alias v1 = new Alias(new ExprId(12), new Count(a2), "v1");
        PlanChecker.from(connectContext).analyze(sql)
                .matches(logicalProject(logicalSort(logicalProject(logicalAggregate(logicalProject(
                        logicalFilter(logicalJoin(logicalOlapScan(), logicalOlapScan())))).when(
                                FieldChecker.check("outputExpressions", Lists.newArrayList(pk, pk1,
                                        sumA1, countA1, sumA1A2, v1))))).when(FieldChecker.check(
                                                "orderKeys",
                                                ImmutableList.of(new OrderKey(pk, true, true),
                                                        new OrderKey(
                                                                countA11.toSlot(), true, true),
                                                        new OrderKey(
                                                                new Add(sumA1A2.toSlot(),
                                                                        new TinyIntLiteral(
                                                                                (byte) 1)),
                                                                true, true),
                                                        new OrderKey(
                                                                new Add(v1.toSlot(),
                                                                        new TinyIntLiteral(
                                                                                (byte) 1)),
                                                                true, true),
                                                        new OrderKey(v1.toSlot(), true, true)))))
                                                                .when(FieldChecker.check("projects",
                                                                        Lists.newArrayList(pk1,
                                                                                pk11.toSlot(),
                                                                                pk2.toSlot(),
                                                                                sumA1.toSlot(),
                                                                                countA11.toSlot(),
                                                                                sumA1A2.toSlot(),
                                                                                v1.toSlot()))));
    }

    @Test
    void testSortHavingAgg() {
        String sql = "SELECT pk FROM t1 GROUP BY pk HAVING sum(a1) > (SELECT AVG(a1) FROM t1) ORDER BY sum(a1)";
        PlanChecker.from(connectContext).analyze(sql)
                .matches(logicalFilter());
    }
}
