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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.logical.OutputPrunable;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class UniqueFunctionTest extends SqlTestBase {

    @Override
    protected void runBeforeAll() throws Exception {
        super.runBeforeAll();
        dropTable("t", false);
        createTable("create table t(id int, a double,  b double) distributed by hash(id) buckets 10 properties('replication_num' = '1')");
    }

    @Test
    void testEquals() {
        Random rand0 = new Random();
        Random rand1 = new Random(new BigIntLiteral(10L));
        Random rand2 = new Random(new BigIntLiteral(1L), new BigIntLiteral(10L));
        Assertions.assertNotEquals(rand0, new Random());
        Assertions.assertEquals(rand0.withIgnoreUniqueId(true), new Random().withIgnoreUniqueId(true));
        Assertions.assertEquals(rand0, rand0.withChildren());
        Assertions.assertEquals(rand0, rand0.withChildren(new BigIntLiteral(10L))); // only compare unique id
        Assertions.assertNotEquals(rand1, new Random(new BigIntLiteral(10L)));
        Assertions.assertEquals(rand1.withIgnoreUniqueId(true), new Random(new BigIntLiteral(10L)).withIgnoreUniqueId(true));
        Assertions.assertEquals(rand1, rand1.withChildren(new BigIntLiteral(10L)));
        Assertions.assertEquals(rand1,
                rand1.withChildren(new BigIntLiteral(1L), new BigIntLiteral(10L))); // only compare unique id
        Assertions.assertNotEquals(rand2, new Random(new BigIntLiteral(1L), new BigIntLiteral(10L)));
        Assertions.assertEquals(rand2.withIgnoreUniqueId(true), new Random(new BigIntLiteral(1L), new BigIntLiteral(10L)).withIgnoreUniqueId(true));
        Assertions.assertEquals(rand2, rand2.withChildren(new BigIntLiteral(1L), new BigIntLiteral(10L)));

        RandomBytes randb = new RandomBytes(new BigIntLiteral(10L));
        Assertions.assertNotEquals(randb, new RandomBytes(new BigIntLiteral(10L)));
        Assertions.assertEquals(randb.withIgnoreUniqueId(true), new RandomBytes(new BigIntLiteral(10L)).withIgnoreUniqueId(true));
        Assertions.assertEquals(randb, randb.withChildren(new BigIntLiteral(10L)));
        Assertions.assertEquals(randb, randb.withChildren(new BigIntLiteral(1L))); // only compare unique id

        Uuid uuid = new Uuid();
        Assertions.assertNotEquals(uuid, new Uuid());
        Assertions.assertEquals(uuid.withIgnoreUniqueId(true), new Uuid().withIgnoreUniqueId(true));
        Assertions.assertEquals(uuid, uuid.withChildren());

        UuidNumeric uuidNum = new UuidNumeric();
        Assertions.assertNotEquals(uuidNum, new UuidNumeric());
        Assertions.assertEquals(uuidNum.withIgnoreUniqueId(true), new UuidNumeric().withIgnoreUniqueId(true));
        Assertions.assertEquals(uuidNum, uuidNum.withChildren());
    }

    @Test
    void testNoAggregate1() {
        // when no group by, all the 'a + random()' will be different
        String sql = "select a + random(),  a + random(), sum(a + random()) over(partition by a + random()),  sum(a + random()) over(partition by a + random())"
                + " from t"
                + " where a + random() > 10 and a + random() > 10"
                // + " qualify sum(a + random()) over() > 20" // BUG: raise error "Unknown column 'a' in 'table list' in SORT clause"
                + " order by a + random(), a + random()"
                ;

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Set<Expression> expressionSet = Sets.newHashSet();
        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of("a + random()", "a + random()",
                        "sum(a + random()) over(partition by a + random())", "sum(a + random()) over(partition by a + random())"),
                toSqls(project.getProjects()));
        checkOutputDifferent(project, expressionSet, exprIdToOriginExprMap);

        LogicalSort<?> sort = (LogicalSort<?>) project.child();
        Assertions.assertEquals(ImmutableList.of("(a + random())", "(a + random())"), toSqls(sort.getExpressions()));
        for (Expression expr : sort.getExpressions()) {
            assertExpressionInstanceAndSql(expr, SlotReference.class, "(a + random())");
            Assertions.assertTrue(expressionSet.add(exprIdToOriginExprMap.get(((SlotReference) expr).getExprId())));
        }

        LogicalWindow<?> window = (LogicalWindow<?>) sort.child().child(0);
        Assertions.assertEquals(2, window.getWindowExpressions().size());
        for (NamedExpression output : window.getWindowExpressions()) {
            WindowExpression windowExpr = (WindowExpression) output.child(0);
            Assertions.assertEquals("sum((a + random())) OVER(PARTITION BY (a + random()) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", windowExpr.toSql());
            assertExpressionInstanceAndSql(windowExpr.getFunction(), Sum.class, "sum((a + random()))");
            assertExpressionInstanceAndSql(windowExpr.getFunction().child(0), SlotReference.class, "(a + random())");
            Assertions.assertTrue(expressionSet.add(exprIdToOriginExprMap.get(((SlotReference) windowExpr.getFunction().child(0)).getExprId())));
            assertExpressionInstanceAndSql(windowExpr.getPartitionKeys().get(0), SlotReference.class, "(a + random())");
            Assertions.assertTrue(expressionSet.add(exprIdToOriginExprMap.get(((SlotReference) windowExpr.getPartitionKeys().get(0)).getExprId())));
        }

        LogicalFilter<?> filter = (LogicalFilter<?>) window.child().child(0);
        Assertions.assertEquals(2, filter.getConjuncts().size());
        for (Expression conjunct : filter.getConjuncts()) {
            Assertions.assertEquals("((a + random()) > 10.0)", conjunct.toSql());
            assertExpressionInstanceAndSql(conjunct.child(0), Add.class, "(a + random())");
            Assertions.assertTrue(expressionSet.add(conjunct.child(0)));
        }
    }

    @Test
    void testNoAggregate2() {
        // when no group by, all the 'a + random()' will be different
        String sql = "select a + random(),  a + random(), sum(a + random()) over(partition by a + random()),  sum(a + random()) over(partition by a + random())"
                + " from t"
                + " where a + random() > 10 and a + random() > 10"
                + " qualify sum(a + random()) over() > 20 AND sum(a + random()) over() > 20"
                ;

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Set<Expression> expressionSet = Sets.newHashSet();
        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of("(a + random()) AS `a + random()`",
                        "(a + random()) AS `a + random()`",
                        "sum(a + random()) over(partition by a + random())", "sum(a + random()) over(partition by a + random())"),
                toSqls(project.getProjects()));
        checkOutputDifferent(project, expressionSet, exprIdToOriginExprMap);

        LogicalFilter<?> filter = (LogicalFilter<?>) project.child();
        Assertions.assertEquals(2, filter.getConjuncts().size());
        for (Expression conjunct : filter.getConjuncts()) {
            Assertions.assertEquals("(sum((a + random())) OVER() > 20.0)", conjunct.toSql());
            assertExpressionInstanceAndSql(conjunct.child(0), SlotReference.class, "sum((a + random())) OVER()");
        }

        LogicalWindow<?> window = (LogicalWindow<?>) filter.child();
        Assertions.assertEquals(4, window.getWindowExpressions().size());
        for (int i = 0; i < 2; i++) {
            NamedExpression output = window.getWindowExpressions().get(i);
            WindowExpression windowExpr = (WindowExpression) output.child(0);
            Assertions.assertEquals("sum((a + random())) OVER(PARTITION BY (a + random()) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", windowExpr.toSql());
            assertExpressionInstanceAndSql(windowExpr.getFunction(), Sum.class, "sum((a + random()))");
            assertExpressionInstanceAndSql(windowExpr.getFunction().child(0), SlotReference.class, "(a + random())");
            Assertions.assertTrue(expressionSet.add(exprIdToOriginExprMap.get(((SlotReference) windowExpr.getFunction().child(0)).getExprId())));
            assertExpressionInstanceAndSql(windowExpr.getPartitionKeys().get(0), SlotReference.class, "(a + random())");
            Assertions.assertTrue(expressionSet.add(exprIdToOriginExprMap.get(((SlotReference) windowExpr.getPartitionKeys().get(0)).getExprId())));
        }

        for (int i = 2; i < 4; i++) {
            NamedExpression output = window.getWindowExpressions().get(i);
            WindowExpression windowExpr = (WindowExpression) output.child(0);
            Assertions.assertEquals("sum((a + random())) OVER(RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", windowExpr.toSql());
            assertExpressionInstanceAndSql(windowExpr.getFunction(), Sum.class, "sum((a + random()))");
            assertExpressionInstanceAndSql(windowExpr.getFunction().child(0), SlotReference.class, "(a + random())");
            Assertions.assertTrue(expressionSet.add(exprIdToOriginExprMap.get(((SlotReference) windowExpr.getFunction().child(0)).getExprId())));
        }

        filter = (LogicalFilter<?>) window.child().child(0);
        Assertions.assertEquals(2, filter.getConjuncts().size());
        for (Expression conjunct : filter.getConjuncts()) {
            Assertions.assertEquals("((a + random()) > 10.0)", conjunct.toSql());
            assertExpressionInstanceAndSql(conjunct.child(0), Add.class, "(a + random())");
            Assertions.assertTrue(expressionSet.add(conjunct.child(0)));
        }
    }

    @Test
    void testAggregateNoGroupBy1() {
        String sql = "select random(), random(), sum(a + random()), sum(a + random())"
                + " from t"
                ;

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Set<Expression> expressionSet = Sets.newHashSet();
        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of("random() AS `random()`", "random() AS `random()`", "sum(a + random())", "sum(a + random())"),
                toSqls(project.getProjects()));
        checkOutputDifferent(project, expressionSet, exprIdToOriginExprMap);

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) project.child();
        Assertions.assertEquals(ImmutableList.of("sum((a + random())) AS `sum(a + random())`", "sum((a + random())) AS `sum(a + random())`"),
                toSqls(aggregate.getOutputs()));
        checkOutputDifferent(aggregate, Sets.newHashSet(), exprIdToOriginExprMap);
    }

    @Test
    void testAggregateNoGroupBy2() {
        String sql = "select 1"
                + " from t"
                + " having sum(a + random()) > 20 and sum(a + random()) > 20";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        // this function will also check all random() are unique
        getExprIdToOriginExpressionMap(root);
        Set<Expression> expressionSet = Sets.newHashSet();

        LogicalFilter<?> filter = (LogicalFilter<?>) root.child(0).child(0);
        Assertions.assertEquals(2, filter.getConjuncts().size());
        for (Expression conjunct : filter.getConjuncts()) {
            Assertions.assertEquals("(sum((a + random())) > 20.0)", conjunct.toSql());
            assertExpressionInstanceAndSql(conjunct.child(0), SlotReference.class, "sum((a + random()))");
        }

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) filter.child(0);
        Assertions.assertEquals(2, aggregate.getOutputs().size());
        for (NamedExpression output : aggregate.getOutputs()) {
            Assertions.assertEquals("sum((a + random())) AS `sum((a + random()))`", output.toSql());
            Expression random = output.child(0).child(0).child(1);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
    }

    @Test
    void testAggregateNoGroupBy3() {
        String sql = "select random(), random(), sum(a + random()), sum(a + random())"
                + " from t"
                + " having sum(a + random()) > 20 and sum(a + random()) > 20"
                ;

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Set<Expression> expressionSet = Sets.newHashSet();
        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of("random() AS `random()`", "random() AS `random()`", "sum(a + random())", "sum(a + random())"),
                toSqls(project.getProjects()));
        checkOutputDifferent(project, Sets.newHashSet(), exprIdToOriginExprMap);
        for (int i = 0; i < 2; i++) {
            NamedExpression output = project.getOutputs().get(i);
            Expression random = output.child(0);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
        for (int i = 2; i < 4; i++) {
            NamedExpression output = project.getOutputs().get(i);
            assertExpressionInstanceAndSql(output, SlotReference.class, "sum(a + random())");
        }

        LogicalFilter<?> filter = (LogicalFilter<?>) project.child();
        Assertions.assertEquals(2, filter.getConjuncts().size());
        for (Expression conjunct : filter.getConjuncts()) {
            Assertions.assertEquals("(sum((a + random())) > 20.0)", conjunct.toSql());
            assertExpressionInstanceAndSql(conjunct.child(0), SlotReference.class, "sum((a + random()))");
        }

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) filter.child();
        Assertions.assertEquals(4, aggregate.getOutputs().size());
        for (NamedExpression output : aggregate.getOutputs()) {
            Assertions.assertEquals("sum((a + random()))", output.child(0).toSql());
            Expression random = output.child(0).child(0).child(1);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
    }

    @Test
    void testAggregateNoGroupBy4() {
        String sql = "select random(), random(), sum(random()), sum(random())";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Set<Expression> expressionSet = Sets.newHashSet();
        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of("random() AS `random()`", "random() AS `random()`", "sum(random())", "sum(random())"),
                toSqls(project.getProjects()));
        checkOutputDifferent(project, Sets.newHashSet(), exprIdToOriginExprMap);
        for (int i = 0; i < 2; i++) {
            NamedExpression output = project.getOutputs().get(i);
            Expression random = output.child(0);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
        for (int i = 2; i < 4; i++) {
            NamedExpression output = project.getOutputs().get(i);
            assertExpressionInstanceAndSql(output, SlotReference.class, "sum(random())");
        }

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) project.child();
        Assertions.assertEquals(2, aggregate.getOutputs().size());
        for (NamedExpression output : aggregate.getOutputs()) {
            Assertions.assertEquals("sum(random())", output.child(0).toSql());
            Expression random = output.child(0).child(0);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
    }

    @Test
    void testAggregateWithDistinct1() {
        String sql = "select distinct random(), random(), a + random(),  a + random()"
                + " from t";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) root.child(0);
        checkOutputDifferent(aggregate, Sets.newHashSet(), exprIdToOriginExprMap);
        Assertions.assertEquals(4, aggregate.getOutputs().size());
        Assertions.assertEquals(aggregate.getGroupByExpressions(), aggregate.getOutputs());
        for (int i = 0; i < 2; i++) {
            NamedExpression output = aggregate.getOutputs().get(i);
            assertExpressionInstanceAndSql(output, SlotReference.class, "random()");
        }
        for (int i = 2; i < 4; i++) {
            NamedExpression output = aggregate.getOutputs().get(i);
            assertExpressionInstanceAndSql(output, SlotReference.class, "a + random()");
        }

        Set<Expression> expressionSet = Sets.newHashSet();
        LogicalProject<?> project = (LogicalProject<?>) aggregate.child();
        for (int i = 0; i < 2; i++) {
            NamedExpression output = project.getOutputs().get(i);
            Assertions.assertEquals("random() AS `random()`", output.toSql());
            Expression random = output.child(0);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
        for (int i = 2; i < 4; i++) {
            NamedExpression output = project.getOutputs().get(i);
            Assertions.assertEquals("(a + random()) AS `a + random()`", output.toSql());
            Expression random = output.child(0).child(1);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
    }

    @Test
    void testAggregateWithDistinct2() {
        String sql = "select distinct random(), random(), sum(a + random()), sum(a + random())"
                + " from t";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Set<Expression> expressionSet = Sets.newHashSet();
        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        checkOutputDifferent(project, Sets.newHashSet(), exprIdToOriginExprMap);
        Assertions.assertEquals(4, project.getProjects().size());
        for (int i = 0; i < 2; i++) {
            NamedExpression output = project.getProjects().get(i);
            assertExpressionInstanceAndSql(output, Alias.class, "random() AS `random()`");
            Expression random = output.child(0);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
        for (int i = 2; i < 4; i++) {
            NamedExpression output = project.getOutputs().get(i);
            assertExpressionInstanceAndSql(output, SlotReference.class, "sum(a + random())");
        }

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) project.child();
        Assertions.assertEquals(2, aggregate.getOutputs().size());
        for (NamedExpression output : aggregate.getOutputs()) {
            Assertions.assertEquals("sum((a + random()))", output.child(0).toSql());
            Expression random = output.child(0).child(0).child(1);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
    }

    @Test
    void testAggregateWithDistinct3() {
        String sql = "select distinct random(), random()";
        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Set<Expression> expressionSet = Sets.newHashSet();
        getExprIdToOriginExpressionMap(root);

        LogicalOneRowRelation oneRowRelation = (LogicalOneRowRelation) root.child(0);
        Assertions.assertEquals(2, oneRowRelation.getOutputs().size());
        for (int i = 0; i < 2; i++) {
            NamedExpression output = oneRowRelation.getOutputs().get(i);
            assertExpressionInstanceAndSql(output, Alias.class, "random() AS `random()`");
            Expression random = output.child(0);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
    }

    @Test
    void testAggregateWithDistinct4() {
        String sql = "select distinct random(), random(), sum(random()), sum(random())";
        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Set<Expression> expressionSet = Sets.newHashSet();
        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        checkOutputDifferent(project, Sets.newHashSet(), exprIdToOriginExprMap);
        Assertions.assertEquals(4, project.getProjects().size());
        for (int i = 0; i < 2; i++) {
            NamedExpression output = project.getProjects().get(i);
            assertExpressionInstanceAndSql(output, Alias.class, "random() AS `random()`");
            Expression random = output.child(0);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
        for (int i = 2; i < 4; i++) {
            NamedExpression output = project.getOutputs().get(i);
            assertExpressionInstanceAndSql(output, SlotReference.class, "sum(random())");
        }

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) project.child();
        Assertions.assertEquals(2, aggregate.getOutputs().size());
        for (NamedExpression output : aggregate.getOutputs()) {
            Assertions.assertEquals("sum(random())", output.child(0).toSql());
            Expression random = output.child(0).child(0);
            assertExpressionInstanceAndSql(random, Random.class, "random()");
            Assertions.assertTrue(expressionSet.add(random));
        }
    }

    @Test
    void testAggregateWithDistinct5() {
        // so no contains windows here
        String sql = "select distinct random(), random(), "
                + " a + random(),  a + random()"
                // TODO: distinct + windows => convert to aggregate, but currently project with distinct to agg has bug,
                // + " ,sum(a + random()) over(partition by a + random()),  sum(a + random()) over(partition by a + random())"
                + " from t"
                + " where random() > 0.5 and random() > 0.5 and a + random() > 10 and a + random() > 10"
                + " having random() > 0.5 and random() > 0.5 and a + random() > 10 and a + random() > 10"
                // + " qualify sum(a + random()) over() > 20" // BUG: raise error "Unknown column 'a' in 'table list' in SORT clause"
                + " order by random(), random(), a + random(), a + random()";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        // all the random in sink are different
        Set<Expression> expressionSet = Sets.newHashSet();
        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);
        LogicalResultSink<?> sink = (LogicalResultSink<?>) root;
        Expression random1 = getOriginExpression(sink.getExpressions().get(0), exprIdToOriginExprMap);
        assertExpressionInstanceAndSql(random1, Random.class, "random()");
        Assertions.assertTrue(expressionSet.add(random1));
        Expression random2 = getOriginExpression(sink.getExpressions().get(1), exprIdToOriginExprMap);
        assertExpressionInstanceAndSql(random2, Random.class, "random()");
        Assertions.assertTrue(expressionSet.add(random2));
        Expression aAddRandom1 = getOriginExpression(sink.getExpressions().get(2), exprIdToOriginExprMap);
        assertExpressionInstanceAndSql(aAddRandom1, Add.class, "(a + random())");
        Expression aAddRandom1Right = getOriginExpression(aAddRandom1.child(1), exprIdToOriginExprMap);
        assertExpressionInstanceAndSql(aAddRandom1Right, Random.class, "random()");
        Assertions.assertTrue(expressionSet.add(aAddRandom1Right));
        Expression aAddRandom2 = getOriginExpression(sink.getExpressions().get(3), exprIdToOriginExprMap);
        assertExpressionInstanceAndSql(aAddRandom2, Add.class, "(a + random())");
        Expression aAddRandom2Right = getOriginExpression(aAddRandom2.child(1), exprIdToOriginExprMap);
        assertExpressionInstanceAndSql(aAddRandom2Right, Random.class, "random()");
        Assertions.assertTrue(expressionSet.add(aAddRandom2Right));

        LogicalSort<?> sort = (LogicalSort<?>) sink.child();
        List<? extends Expression> sortExprs = sort.getExpressions();
        Assertions.assertEquals(2, sortExprs.size());
        Expression key1 = sortExprs.get(0);
        assertExpressionInstanceAndSql(key1, SlotReference.class, "random()");
        Assertions.assertEquals(random1, getOriginExpression(key1, exprIdToOriginExprMap));
        Expression key2 = sortExprs.get(1);
        assertExpressionInstanceAndSql(key2, SlotReference.class, "a + random()");
        Assertions.assertEquals(aAddRandom1, getOriginExpression(key2, exprIdToOriginExprMap));

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) sort.child();
        checkOutputDifferent(aggregate, Sets.newHashSet(), exprIdToOriginExprMap);
        Assertions.assertEquals(aggregate.getGroupByExpressions(), aggregate.getOutputExpressions());
        Assertions.assertEquals(4, aggregate.getOutputExpressions().size());
        NamedExpression aggOutput1 = aggregate.getOutputExpressions().get(0);
        assertExpressionInstanceAndSql(aggOutput1, SlotReference.class, "random()");
        Assertions.assertEquals(getOriginExpression(aggOutput1, exprIdToOriginExprMap), random1);
        NamedExpression aggOutput2 = aggregate.getOutputExpressions().get(1);
        assertExpressionInstanceAndSql(aggOutput2, SlotReference.class, "random()");
        Assertions.assertEquals(getOriginExpression(aggOutput2, exprIdToOriginExprMap), random2);
        NamedExpression aggOutput3 = aggregate.getOutputExpressions().get(2);
        assertExpressionInstanceAndSql(aggOutput3, SlotReference.class, "a + random()");
        Assertions.assertEquals(getOriginExpression(aggOutput3, exprIdToOriginExprMap), aAddRandom1);
        NamedExpression aggOutput4 = aggregate.getOutputExpressions().get(3);
        assertExpressionInstanceAndSql(aggOutput4, SlotReference.class, "a + random()");
        Assertions.assertEquals(getOriginExpression(aggOutput4, exprIdToOriginExprMap), aAddRandom2);

        LogicalFilter<?> filter = (LogicalFilter<?>) aggregate.child();
        Set<Expression> conjunctLeftChildren = Sets.newHashSet();
        Assertions.assertEquals(2, filter.getConjuncts().size());
        for (Expression conjunct : filter.getConjuncts()) {
            Assertions.assertTrue(conjunct.toSql().equals("(random() > 0.5)")
                    || conjunct.toSql().equals("(a + random() > 10.0)"));
            Expression slot = conjunct.child(0);
            Assertions.assertInstanceOf(SlotReference.class, slot);
            conjunctLeftChildren.add(getOriginExpression(slot, exprIdToOriginExprMap));
        }
        Assertions.assertEquals(ImmutableSet.of(random1, aAddRandom1), conjunctLeftChildren);

        LogicalProject<?> project = (LogicalProject<?>) filter.child();
        List<NamedExpression> projectOutputs = project.getProjects();
        Assertions.assertEquals(4, projectOutputs.size());
        Assertions.assertEquals(random1, projectOutputs.get(0).child(0));
        Assertions.assertEquals(random2, projectOutputs.get(1).child(0));
        Assertions.assertEquals(aAddRandom1, projectOutputs.get(2).child(0));
        Assertions.assertEquals(aAddRandom2, projectOutputs.get(3).child(0));

        filter = (LogicalFilter<?>) project.child();
        Assertions.assertEquals(4, filter.getConjuncts().size());
        for (Expression conjunct : filter.getConjuncts()) {
            List<Expression> randoms = conjunct.collectToList(e -> e instanceof Random);
            Assertions.assertEquals(1, randoms.size());
            Assertions.assertTrue(expressionSet.add(randoms.get(0)));
        }
    }

    @Test
    void testAggregateWithGroupBy1() {
        // all the random will be diff
        String sql = "select random(), random(), "
                + " a + random(), a + random(), "
                + " sum(a + random()), sum(a + random()), "
                + " sum(a + random()) over(partition by a + random()),  sum(a + random()) over(partition by a + random())"
                + " from t"
                + " where random() > 0.5 and random() > 0.5 and a + random() > 10 and a + random() > 10"
                + " group by a"
                + " having random() > 0.5 and random() > 0.5 and a + random() > 10 and a + random() > 10 and sum(a + random()) > 30 and sum(a + random()) > 30"
                // + " qualify sum(a + random()) over() > 20" // BUG: raise error "Unknown column 'a' in 'table list' in SORT clause"
                + " order by random(), random(), a + random(), a + random(), sum(a + random()), sum(a + random()) over (partition by a + random())";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);
        DifferentValidator differentValidator = new DifferentValidator(exprIdToOriginExprMap);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of(
                        "random()", "random()", "a + random()", "a + random()",
                        "sum(a + random())", "sum(a + random())",
                        "sum(a + random()) over(partition by a + random())",
                        "sum(a + random()) over(partition by a + random())"),
                toSqls(project.getProjects()));
        project.getProjects().forEach(differentValidator::addAndCheckDifferent);

        LogicalSort<?> sort = (LogicalSort<?>) project.child();
        Assertions.assertEquals(ImmutableList.of("random()", "random()", "(a + random())", "(a + random())",
                        "sum((a + random()))", "sum((a + random())) OVER(PARTITION BY (a + random()))"),
                toSqls(sort.getExpressions()));
        sort.getExpressions().forEach(differentValidator::addAndCheckDifferent);

        LogicalWindow<?> window = (LogicalWindow<?>) sort.child().child(0);

        LogicalFilter<?> having = (LogicalFilter<?>) window.child().child(0);
        assertEqualsIgnoreElemOrder(ImmutableList.of("(random() > 0.5)", "(random() > 0.5)",
                        "((a + random()) > 10.0)", "((a + random()) > 10.0)",
                        "(sum((a + random())) > 30.0)", "(sum((a + random())) > 30.0)"),
                toSqls(having.getConjuncts()));
        having.getConjuncts().forEach(differentValidator::addAndCheckDifferent);

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) having.child();

        LogicalFilter<?> filter = (LogicalFilter<?>) aggregate.child().child(0);
        assertEqualsIgnoreElemOrder(ImmutableList.of("(random() > 0.5)", "(random() > 0.5)",
                        "((a + random()) > 10.0)", "((a + random()) > 10.0)"),
                toSqls(filter.getConjuncts()));
        filter.getConjuncts().forEach(differentValidator::addAndCheckDifferent);
    }

    @Test
    void testAggregateWithGroupBy2() {
        // all the random will be the same except the WHERE
        String sql = "select random(), random(), "
                + " sum(a + random()), sum(a + random()), "
                + " sum(random()) over(partition by random()),  sum(random()) over(partition by random())"
                + " from t"
                + " where random() > 0.5 and random() > 0.5 and a + random() > 10 and a + random() > 10"
                + " group by random()"
                + " having random() > 0.5 and random() > 0.5 and random() > 10 and random() > 10 and sum(a + random()) > 30 and sum(a + random()) > 30"
                // + " qualify sum(a + random()) over() > 20" // BUG: raise error "Unknown column 'a' in 'table list' in SORT clause"
                + " order by random(), random(), sum(a + random()), sum(random()) over (partition by random())";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);
        RandomEqualValidator equalValidator = new RandomEqualValidator(exprIdToOriginExprMap);

        LogicalResultSink<?> sink = (LogicalResultSink<?>) root;
        Assertions.assertEquals(ImmutableList.of("random()", "random()", "sum((a + random()))", "sum((a + random()))", "random()", "random()"),
                toSqls(sink.getOutputExprs().stream().map(output -> getOriginExpression(output, exprIdToOriginExprMap)).collect(Collectors.toList())));
        sink.getOutputExprs().forEach(equalValidator::checkRandomEqual);

        LogicalSort<?> sort = (LogicalSort<?>) sink.child();
        Assertions.assertEquals(ImmutableList.of("random()"), toSqls(sort.getExpressions()));
        sort.getExpressions().forEach(equalValidator::checkRandomEqual);

        LogicalFilter<?> having = (LogicalFilter<?>) sort.child().child(0);
        assertEqualsIgnoreElemOrder(ImmutableList.of("(sum((a + random())) > 30.0)"), toSqls(having.getConjuncts()));
        having.getConjuncts().forEach(equalValidator::checkRandomEqual);

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) having.child();
        aggregate.getGroupByExpressions().forEach(equalValidator::checkRandomEqual);
        aggregate.getOutputExpressions().forEach(equalValidator::checkRandomEqual);

        // having push down aggregate
        LogicalFilter<?> pushDownHaving = (LogicalFilter<?>) aggregate.child();
        assertEqualsIgnoreElemOrder(ImmutableList.of("(random() > 10.0)"), toSqls(pushDownHaving.getConjuncts()));
        pushDownHaving.getConjuncts().forEach(equalValidator::checkRandomEqual);

        // group by expressions push down to a project
        LogicalProject<?> project = (LogicalProject<?>) pushDownHaving.child();
        Assertions.assertEquals(ImmutableList.of("random() AS `random()`", "a"), toSqls(project.getProjects()));
        equalValidator.checkRandomEqual(project.getProjects().get(0));

        Assertions.assertTrue(equalValidator.getRandom().isPresent());
        DifferentValidator differentValidator = new DifferentValidator(exprIdToOriginExprMap);
        differentValidator.addAndCheckDifferent(equalValidator.getRandom().get());

        // WHERE
        LogicalFilter<?> filter = (LogicalFilter<?>) project.child();
        assertEqualsIgnoreElemOrder(ImmutableList.of("(random() > 0.5)", "(random() > 0.5)",
                        "((a + random()) > 10.0)", "((a + random()) > 10.0)"),
                toSqls(filter.getConjuncts()));
        filter.getConjuncts().forEach(differentValidator::addAndCheckDifferent);
    }

    @Test
    void testAggregateWithGroupBy4() {
        // all the 'a + random() will be equal', all the 'random()' will be different
        String sql = "select random(), random(), "
                + " a + random(), a + random(), "
                + " sum(a + random()), sum(a + random()), "
                + " sum(a + random()) over(partition by a + random()),  sum(a + random()) over(partition by a + random())"
                + " from t"
                + " where random() > 0.5 and random() > 0.5 and a + random() > 10 and a + random() > 10"
                + " group by a, a + random()"
                + " having random() > 0.5 and random() > 0.5 and a + random() > 10 and a + random() > 10 and sum(a + random()) > 30 and sum(a + random()) > 30"
                // + " qualify sum(a + random()) over() > 20" // BUG: raise error "Unknown column 'a' in 'table list' in SORT clause"
                + " order by random(), random(), a + random(), a + random(), sum(a + random()), sum(a + random()) over (partition by a + random())";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);
        RandomEqualValidator aAddRandomEqualValidator = new RandomEqualValidator(exprIdToOriginExprMap);
        DifferentValidator differentRandomValidator = new DifferentValidator(exprIdToOriginExprMap);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of("random()", "random()", "a + random()", "a + random()",
                        "sum(a + random())", "sum(a + random())",
                        "sum(a + random()) over(partition by a + random())",
                        "sum(a + random()) over(partition by a + random())"),
                toSqls(project.getProjects()));
        Lists.transform(ImmutableList.of(0, 1), project.getProjects()::get).forEach(differentRandomValidator::addAndCheckDifferent);
        Lists.transform(ImmutableList.of(2, 3, 4, 5, 6, 7), project.getProjects()::get).forEach(aAddRandomEqualValidator::checkRandomEqual);

        Assertions.assertTrue(aAddRandomEqualValidator.getRandom().isPresent());
        differentRandomValidator.addAndCheckDifferent(aAddRandomEqualValidator.getRandom().get());

        LogicalSort<?> sort = (LogicalSort<?>) project.child();
        Assertions.assertEquals(ImmutableList.of("random()", "random()",
                        "a + random()", "sum(a + random())",
                        "sum(a + random()) over(partition by a + random())"),
                toSqls(sort.getExpressions()));
        Lists.transform(ImmutableList.of(0, 1), sort.getExpressions()::get).forEach(differentRandomValidator::addAndCheckDifferent);
        Lists.transform(ImmutableList.of(2, 3, 4), sort.getExpressions()::get).forEach(aAddRandomEqualValidator::checkRandomEqual);

        // BUG: having should below window
        LogicalFilter<?> having1 = (LogicalFilter<?>) sort.child().child(0);
        assertEqualsIgnoreElemOrder(ImmutableList.of("(sum(a + random()) > 30.0)"), toSqls(having1.getConjuncts()));
        having1.getConjuncts().forEach(aAddRandomEqualValidator::checkRandomEqual);

        LogicalWindow<?> window = (LogicalWindow<?>) having1.child();
        Assertions.assertEquals(ImmutableList.of(
                        "sum(a + random()) OVER(PARTITION BY a + random() RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
                        + " AS `sum(a + random()) over(partition by a + random())`"),
                toSqls(window.getExpressions()));
        window.getExpressions().forEach(aAddRandomEqualValidator::checkRandomEqual);

        LogicalFilter<?> having2 = (LogicalFilter<?>) window.child().child(0);
        assertEqualsIgnoreElemOrder(ImmutableList.of("(random() > 0.5)", "(random() > 0.5)"), toSqls(having2.getConjuncts()));
        having2.getConjuncts().forEach(differentRandomValidator::addAndCheckDifferent);

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) having2.child();
        Assertions.assertEquals(ImmutableList.of("a", "a + random()"), toSqls(aggregate.getGroupByExpressions()));
        aAddRandomEqualValidator.checkRandomEqual(aggregate.getGroupByExpressions().get(1));
        Assertions.assertEquals(ImmutableList.of("a", "a + random()", "sum(a + random()) AS `sum(a + random())`"),
                toSqls(aggregate.getOutputExpressions()));
        Lists.transform(ImmutableList.of(1, 2), aggregate.getOutputExpressions()::get).forEach(aAddRandomEqualValidator::checkRandomEqual);

        // push down having
        LogicalFilter<?> having3 = (LogicalFilter<?>) aggregate.child();
        assertEqualsIgnoreElemOrder(ImmutableList.of("(a + random() > 10.0)"), toSqls(having3.getConjuncts()));
        having3.getConjuncts().forEach(aAddRandomEqualValidator::checkRandomEqual);

        // where
        LogicalFilter<?> filter = (LogicalFilter<?>) having3.child().child(0);
        assertEqualsIgnoreElemOrder(ImmutableList.of("(random() > 0.5)", "(random() > 0.5)", "((a + random()) > 10.0)", "((a + random()) > 10.0)"),
                toSqls(filter.getConjuncts()));
        filter.getConjuncts().forEach(differentRandomValidator::addAndCheckDifferent);
    }

    @Test
    void testAggregateWithGroupBy5() {
        // all the 'a + random()' equals,  all the 'a + random() + 1' equals
        String sql = "select a + random(), a + random() + 1, a + random() + 2, "
                + " sum(a + random()), sum(a + random() + 1), sum(a + random() + 2), "
                + " sum(a + random()) over(), "
                + " sum(a + random() + 1) over(), "
                + " sum(a + random() + 2) over()"
                + " from t"
                + " where a + random() > 1 and a + random() + 1 > 1 and a + random() + 2 > 1"
                + " group by a + random(), a + random() + 1"
                + " having a + random() > 10 and a + random() + 1 > 20 and a + random() + 1 > 30"
                + " qualify max(a + random()) over() > 10 and max(a + random() + 1) over() > 20";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        // validate 'a + random()' equal
        RandomEqualValidator random0EqualValidator = new RandomEqualValidator(exprIdToOriginExprMap);

        // validate 'a + random() + 1' equals
        RandomEqualValidator random1EqualValidator = new RandomEqualValidator(exprIdToOriginExprMap);

        DifferentValidator differentValidator = new DifferentValidator(exprIdToOriginExprMap);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of("a + random()", "a + random() + 1", "a + random() + 2",
                        "sum(a + random()) AS `sum(a + random())`", "sum(a + random() + 1) AS `sum(a + random() + 1)`",
                        "sum((a + random() + cast(2 as DOUBLE))) AS `sum(a + random() + 2)`",
                        "sum(a + random()) over()", "sum(a + random() + 1) over()", "sum(a + random() + 2) over()"),
                toSqls(project.getProjects()));
        Lists.transform(ImmutableList.of(0, 2, 3, 5, 6, 8), project.getProjects()::get).forEach(random0EqualValidator::checkRandomEqual);
        Lists.transform(ImmutableList.of(1, 4, 7), project.getProjects()::get).forEach(random1EqualValidator::checkRandomEqual);

        LogicalFilter<?> qualify = (LogicalFilter<?>) project.child();
        Assertions.assertEquals(2, qualify.getConjuncts().size());
        for (Expression expr : qualify.getConjuncts()) {
            String exprSql = expr.toSql();
            Expression originExpr = getOriginExpression(expr, exprIdToOriginExprMap);
            if (exprSql.contains("20.0")) {
                Assertions.assertEquals("(max(((a + random()) + cast(1 as DOUBLE))) OVER() > 20.0)", exprSql);
                random1EqualValidator.checkRandomEqual(originExpr);
            } else {
                Assertions.assertEquals("(max((a + random())) OVER() > 10.0)", exprSql);
                random0EqualValidator.checkRandomEqual(originExpr);
            }
        }

        LogicalWindow<?> window = (LogicalWindow<?>) qualify.child();
        Assertions.assertEquals(ImmutableList.of("sum(a + random()) over()", "sum(a + random() + 1) over()", "sum(a + random() + 2) over()",
                        "max((a + random())) OVER()", "max(((a + random()) + cast(1 as DOUBLE))) OVER()"),
                Lists.transform(window.getWindowExpressions(), NamedExpression::getName));
        Lists.transform(ImmutableList.of(0, 2, 3), window.getWindowExpressions()::get).forEach(random0EqualValidator::checkRandomEqual);
        Lists.transform(ImmutableList.of(1, 4), window.getWindowExpressions()::get).forEach(random1EqualValidator::checkRandomEqual);

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) window.child().child(0);
        Assertions.assertEquals(ImmutableList.of("a + random()", "a + random() + 1"), toSqls(aggregate.getGroupByExpressions()));
        random0EqualValidator.checkRandomEqual(aggregate.getGroupByExpressions().get(0));
        random1EqualValidator.checkRandomEqual(aggregate.getGroupByExpressions().get(1));
        Assertions.assertEquals(ImmutableList.of("a + random()", "a + random() + 1",
                        "sum(a + random()) AS `sum(a + random())`",
                        "sum(a + random() + 1) AS `sum(a + random() + 1)`",
                        "sum((a + random() + 2.0)) AS `sum((a + random() + cast(2 as DOUBLE)))`"),
                toSqls(aggregate.getOutputExpressions()));
        Lists.transform(ImmutableList.of(0, 2, 4), aggregate.getOutputExpressions()::get).forEach(random0EqualValidator::checkRandomEqual);
        Lists.transform(ImmutableList.of(1, 3), aggregate.getOutputExpressions()::get).forEach(random1EqualValidator::checkRandomEqual);

        LogicalFilter<?> having = (LogicalFilter<?>) aggregate.child();
        Assertions.assertEquals(2, having.getConjuncts().size());
        for (Expression expr : having.getConjuncts()) {
            String exprSql = expr.toSql();
            Expression originExpr = getOriginExpression(expr, exprIdToOriginExprMap);
            if (exprSql.contains("10.0")) {
                Assertions.assertEquals("(a + random() > 10.0)", exprSql);
                random0EqualValidator.checkRandomEqual(originExpr);
            } else {
                Assertions.assertEquals("(a + random() + 1 > 30.0)", exprSql);
                random1EqualValidator.checkRandomEqual(originExpr);
            }
        }

        // where
        LogicalFilter<?> filter = (LogicalFilter<?>) having.child().child(0);
        assertEqualsIgnoreElemOrder(ImmutableList.of("((a + random()) > 1.0)", "((a + random()) > 0.0)", "((a + random()) > -1.0)"), toSqls(filter.getConjuncts()));
        filter.getConjuncts().forEach(differentValidator::addAndCheckDifferent);

        Assertions.assertTrue(random0EqualValidator.getRandom().isPresent());
        Assertions.assertTrue(random1EqualValidator.getRandom().isPresent());
        differentValidator.addAndCheckDifferent(random0EqualValidator.getRandom().get());
        differentValidator.addAndCheckDifferent(random1EqualValidator.getRandom().get());
    }

    @Test
    void testAggregateWithGroupBy6() {
        // the two group by will be equal, and all the random() will be equal
        String sql = "select a + random(), sum(a + random() + 1.0), max(a + random() + 2.0)"
                + " from t"
                + " group by a + random(), a + random()"
                + " having a + random() > 10";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);
        RandomEqualValidator equalValidator = new RandomEqualValidator(exprIdToOriginExprMap);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of("a + random()",
                        "sum((a + random() + cast(1.0 as DOUBLE))) AS `sum(a + random() + 1.0)`",
                        "max((a + random() + cast(2.0 as DOUBLE))) AS `max(a + random() + 2.0)`"),
                toSqls(project.getProjects()));
        project.getProjects().forEach(equalValidator::checkRandomEqual);

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) project.child();
        // two group by equal, remove duplicate one
        Assertions.assertEquals(ImmutableList.of("a + random()"), toSqls(aggregate.getGroupByExpressions()));
        aggregate.getGroupByExpressions().forEach(equalValidator::checkRandomEqual);
        Assertions.assertEquals(ImmutableList.of("a + random()",
                        "sum((a + random() + 1.0)) AS `sum((a + random() + cast(1.0 as DOUBLE)))`",
                        "max((a + random() + 2.0)) AS `max((a + random() + cast(2.0 as DOUBLE)))`"),
                toSqls(aggregate.getOutputExpressions()));
        aggregate.getOutputExpressions().forEach(equalValidator::checkRandomEqual);

        // push down having
        LogicalFilter<?> filter = (LogicalFilter<?>) aggregate.child();
        assertEqualsIgnoreElemOrder(ImmutableList.of("(a + random() > 10.0)"), toSqls(filter.getConjuncts()));
        filter.getConjuncts().forEach(equalValidator::checkRandomEqual);

        // push down group by expression to project
        LogicalProject<?> groupByProject = (LogicalProject<?>) filter.child();
        Assertions.assertEquals(ImmutableList.of("(a + random()) AS `a + random()`"),
                toSqls(groupByProject.getProjects()));
        groupByProject.getProjects().forEach(equalValidator::checkRandomEqual);
    }

    @Test
    void testRepeat1() {
        // all the 'random()' equal,  all the 'a + random()' equal
        String sql = "select random(), a + random(), sum(random()), sum(a + random()), max(random()) over(), max(a + random()) over()"
                + " from t"
                + " group by grouping sets((), (random()), (random(), random()), (random(), a + random()))";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);
        // validate 'random()' equal
        RandomEqualValidator randomEqualValidator = new RandomEqualValidator(exprIdToOriginExprMap);
        // validate 'random()' in 'a + random()' equal
        RandomEqualValidator aAddRandomEqualValidator = new RandomEqualValidator(exprIdToOriginExprMap);

        LogicalResultSink<?> sink = (LogicalResultSink<?>) root;
        List<Expression> sinkOutputExprs = Lists.transform(sink.getOutputExprs(), output -> getOriginExpression(output, exprIdToOriginExprMap));
        Assertions.assertEquals(ImmutableList.of("random()", "(a + random())",
                        "sum(random())", "sum((a + random()))",
                        "max(random()) OVER(RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                        "max((a + random())) OVER(RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"),
                toSqls(sinkOutputExprs));
        Lists.transform(ImmutableList.of(0, 2, 4), sinkOutputExprs::get).forEach(randomEqualValidator::checkRandomEqual);
        Lists.transform(ImmutableList.of(1, 3, 5), sinkOutputExprs::get).forEach(aAddRandomEqualValidator::checkRandomEqual);

        LogicalWindow<?> window = (LogicalWindow<?>) sink.child();
        Assertions.assertEquals(ImmutableList.of(
                    "max(random()) OVER(RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `max(random()) over()`",
                    "max(a + random()) OVER(RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `max(a + random()) over()`"),
                toSqls(window.getWindowExpressions()));
        randomEqualValidator.checkRandomEqual(window.getWindowExpressions().get(0));
        aAddRandomEqualValidator.checkRandomEqual(window.getWindowExpressions().get(1));

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) window.child().child(0);
        Assertions.assertEquals(ImmutableList.of("random()", "a + random()", "GROUPING_ID"), toSqls(aggregate.getGroupByExpressions()));
        Lists.transform(ImmutableList.of(0), aggregate.getGroupByExpressions()::get).forEach(randomEqualValidator::checkRandomEqual);
        Lists.transform(ImmutableList.of(1), aggregate.getGroupByExpressions()::get).forEach(aAddRandomEqualValidator::checkRandomEqual);
        Assertions.assertEquals(ImmutableList.of("random()", "a + random()", "GROUPING_ID",
                        "sum(random()) AS `sum(random())`",
                        "sum(a + random()) AS `sum(a + random())`"),
                toSqls(aggregate.getOutputExpressions()));
        Lists.transform(ImmutableList.of(0, 3), aggregate.getOutputExpressions()::get).forEach(randomEqualValidator::checkRandomEqual);
        Lists.transform(ImmutableList.of(1, 4), aggregate.getOutputExpressions()::get).forEach(aAddRandomEqualValidator::checkRandomEqual);

        LogicalRepeat<?> repeat = (LogicalRepeat<?>) aggregate.child();
        Assertions.assertEquals(4, repeat.getGroupingSets().size());
        Assertions.assertEquals(ImmutableList.of(), toSqls(repeat.getGroupingSets().get(0)));
        Assertions.assertEquals(ImmutableList.of("random()"), toSqls(repeat.getGroupingSets().get(1)));
        randomEqualValidator.checkRandomEqual(repeat.getGroupingSets().get(1).get(0));
        Assertions.assertEquals(ImmutableList.of("random()"), toSqls(repeat.getGroupingSets().get(2)));
        randomEqualValidator.checkRandomEqual(repeat.getGroupingSets().get(2).get(0));
        Assertions.assertEquals(ImmutableList.of("random()", "a + random()"), toSqls(repeat.getGroupingSets().get(3)));
        randomEqualValidator.checkRandomEqual(repeat.getGroupingSets().get(3).get(0));
        aAddRandomEqualValidator.checkRandomEqual(repeat.getGroupingSets().get(3).get(1));

        Assertions.assertTrue(randomEqualValidator.getRandom().isPresent());
        Assertions.assertTrue(aAddRandomEqualValidator.getRandom().isPresent());
        Assertions.assertNotEquals(randomEqualValidator.getRandom().get(), aAddRandomEqualValidator.getRandom().get());
    }

    // add its add expressions are different, and the randoms from them are different too
    private static class DifferentValidator {
        private Map<ExprId, Expression> exprIdToOriginExprMap;
        private Set<Expression> expressionSet;
        private Set<Expression> randomSet;

        public DifferentValidator(Map<ExprId, Expression> exprIdToOriginExprMap) {
            this.exprIdToOriginExprMap = exprIdToOriginExprMap;
            expressionSet = Sets.newHashSet();
            randomSet = Sets.newHashSet();
        }

        public void addAndCheckDifferent(Expression expression) {
            Expression originExpression = getOriginExpression(expression, exprIdToOriginExprMap);
            Assertions.assertTrue(expressionSet.add(originExpression));
            List<Expression> randoms = originExpression.collectToList(e -> e instanceof Random);
            for (Expression random : randoms) {
                Assertions.assertTrue(randomSet.add(random));
            }
        }
    }

    private static class RandomEqualValidator {
        private Map<ExprId, Expression> exprIdToOriginExprMap;
        private Optional<Random> random;

        public RandomEqualValidator(Map<ExprId, Expression> exprIdToOriginExprMap) {
            this.exprIdToOriginExprMap = exprIdToOriginExprMap;
            random = Optional.empty();
        }

        public void checkRandomEqual(Expression expression) {
            Expression originExpression = getOriginExpression(expression, exprIdToOriginExprMap);
            List<Expression> randoms = originExpression.collectToList(e -> e instanceof Random);
            Assertions.assertFalse(randoms.isEmpty());
            if (!random.isPresent()) {
                random = Optional.of((Random) randoms.get(0));
            }
            for (Expression r : randoms) {
                Assertions.assertEquals(random.get(), r);
            }
        }

        public Optional<Random> getRandom() {
            return random;
        }
    }

    private Map<ExprId, Expression> getExprIdToOriginExpressionMap(Plan root) {
        Map<ExprId, Expression> exprIdToExpressionMap = Maps.newHashMap();
        Set<Expression> uniqueExpressions = Sets.newHashSet();
        Set<Expression> uniqueOutputExpressions = Sets.newHashSet();
        root.foreachUp(p -> {
            List<NamedExpression> outputs = ImmutableList.of();
            if (p instanceof OutputPrunable) {
                outputs = ((OutputPrunable) p).getOutputs();
            } else if (p instanceof LogicalWindow) {
                outputs = ((LogicalWindow<?>) p).getWindowExpressions();
            }
            for (NamedExpression output : outputs) {
                if (output instanceof Alias) {
                    Alias alias = (Alias) output;
                    exprIdToExpressionMap.put(alias.getExprId(), alias.child());
                    if (output.containsUniqueFunction()) {
                        boolean notExists = uniqueOutputExpressions.add(alias.child());
                        Assertions.assertTrue(notExists);
                    }
                }
            }
            for (Expression expression : ((LogicalPlan) p).getExpressions()) {
                expression.foreach(e -> {
                    if (e instanceof UniqueFunction) {
                        // after rewrite, all the unique expressions should be different.
                        boolean notExists = uniqueExpressions.add((Expression) e);
                        Assertions.assertTrue(notExists);
                    }
                });
            }
        });
        return exprIdToExpressionMap;
    }

    private static Expression getOriginExpression(Expression expression, Map<ExprId, Expression> exprIdToOriginExpressionMap) {
        Expression afterExpression = expression;
        while (afterExpression.containsType(NamedExpression.class)) {
            if (afterExpression instanceof Alias) {
                afterExpression = ((Alias) afterExpression).child();
                continue;
            }
            Expression beforeExpression = afterExpression;
            afterExpression = beforeExpression.rewriteDownShortCircuit(
                    e -> e instanceof NamedExpression ? exprIdToOriginExpressionMap.getOrDefault(((NamedExpression) e).getExprId(), e) : e);
            if (beforeExpression.equals(afterExpression)) {
                break;
            }
        }
        return afterExpression;
    }

    private List<String> toSqls(Collection<? extends Expression> expressions) {
        return expressions.stream()
                .map(Expression::toSql)
                .collect(Collectors.toList());
    }

    private void checkOutputDifferent(OutputPrunable outputPrunable, Set<Expression> expressionSet, Map<ExprId, Expression> exprIdToOriginExprMap) {
        for (NamedExpression namedExpression : outputPrunable.getOutputs()) {
            Expression originExpression = getOriginExpression(namedExpression, exprIdToOriginExprMap);
            Assertions.assertTrue(expressionSet.add(originExpression));
        }
    }

    private void assertExpressionInstanceAndSql(Expression expression, Class exprClass, String exprSql) {
        Assertions.assertInstanceOf(exprClass, expression);
        Assertions.assertEquals(exprSql, expression.toSql());
    }

    private void assertEqualsIgnoreElemOrder(List<?> list1, List<?> list2) {
        Assertions.assertEquals(list1.stream().sorted().collect(Collectors.toList()),
                list2.stream().sorted().collect(Collectors.toList()));
    }
}
