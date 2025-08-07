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
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.logical.OutputPrunable;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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
                //.matchesFromRoot(logicalOlapScan())
                .getPlan();

        Set<Expression> expressionSet = Sets.newHashSet();
        Map<ExprId, Expression> exprIdToOriginExprMap = getExprIdToOriginExpressionMap(root);

        LogicalProject<?> project = (LogicalProject<?>) root.child(0);
        Assertions.assertEquals(ImmutableList.of("a + random()", "a + random()",
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
        /*
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
                + " order by random(), random(), a + random(), a + random()";

        Plan root = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();
        */
    }

    private Map<ExprId, Expression> getExprIdToOriginExpressionMap(Plan root) {
        Map<ExprId, Expression> exprIdToExpressionMap = Maps.newHashMap();
        Set<Expression> uniqueExpressions = Sets.newHashSet();
        Set<Expression> uniqueOutputExpressions = Sets.newHashSet();
        root.foreachUp(p -> {
            if (p instanceof OutputPrunable) {
                for (NamedExpression output : ((OutputPrunable) p).getOutputs()) {
                    if (output instanceof Alias) {
                        Alias alias = (Alias) output;
                        exprIdToExpressionMap.put(alias.getExprId(), alias.child());
                        if (output.containsUniqueFunction()) {
                            boolean notExists = uniqueOutputExpressions.add(alias.child());
                            Assertions.assertTrue(notExists);
                        }
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

    private Expression getOriginExpression(Expression expression, Map<ExprId, Expression> exprIdToOriginExpressionMap) {
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
}
