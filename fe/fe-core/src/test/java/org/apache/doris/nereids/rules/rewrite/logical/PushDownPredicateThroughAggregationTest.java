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

package org.apache.doris.nereids.rules.rewrite.logical;


import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PushDownPredicateThroughAggregationTest {

    /**
    * origin plan:
    *                project
    *                  |
    *                filter gender=1
    *                  |
    *               aggregation group by gender
    *                  |
    *               scan(student)
    *
    *  transformed plan:
    *                project
    *                  |
    *               aggregation group by gender
    *                  |
    *               filter gender=1
    *                  |
    *               scan(student)
    */
    @Test
    public void pushDownPredicateOneFilterTest() {
        Table student = new Table(0L, "student", Table.TableType.OLAP,
                ImmutableList.<Column>of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("gender", Type.INT, false, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", ""),
                        new Column("age", Type.INT, true, AggregateType.NONE, "", "")));
        Plan scan = new LogicalOlapScan(student, ImmutableList.of("student"));
        Slot gender = scan.getOutput().get(1);
        Slot age = scan.getOutput().get(3);

        List<Expression> groupByKeys = Lists.newArrayList(age, gender);
        List<NamedExpression> outputExpressionList = Lists.newArrayList(gender, age);
        Plan aggregation = new LogicalAggregate<>(groupByKeys, outputExpressionList, scan);
        Expression filterPredicate = new GreaterThan(gender, Literal.of(1));
        LogicalFilter filter = new LogicalFilter(filterPredicate, aggregation);
        Plan root = new LogicalProject<>(
                Lists.newArrayList(gender),
                filter
        );

        Memo memo = rewrite(root);
        System.out.println(memo.copyOut().treeString());
        Group rootGroup = memo.getRoot();

        GroupExpression groupExpression = rootGroup
                .getLogicalExpression().child(0)
                .getLogicalExpression();
        aggregation = groupExpression.getPlan();
        Assertions.assertTrue(aggregation instanceof LogicalAggregate);

        groupExpression = groupExpression.child(0).getLogicalExpression();
        Plan bottomFilter = groupExpression.getPlan();
        Assertions.assertTrue(bottomFilter instanceof LogicalFilter);
        Expression greater = ((LogicalFilter<?>) bottomFilter).getPredicates();
        Assertions.assertTrue(greater instanceof GreaterThan);
        Assertions.assertTrue(greater.child(0) instanceof Slot);
        Assertions.assertEquals("gender", ((Slot) greater.child(0)).getName());

        groupExpression = groupExpression.child(0).getLogicalExpression();
        Plan scan2 = groupExpression.getPlan();
        Assertions.assertTrue(scan2 instanceof LogicalOlapScan);
    }

    /**
     * origin plan:
     *                project
     *                  |
     *                filter gender=1 and name="abc" and (gender+10)<100
     *                  |
     *               aggregation group by gender
     *                  |
     *               scan(student)
     *
     *  transformed plan:
     *                project
     *                  |
     *                filter name="abc"
     *                  |
     *               aggregation group by gender
     *                  |
     *               filter gender=1 and  and (gender+10)<100
     *                  |
     *               scan(student)
     */
    @Test
    public void pushDownPredicateTwoFilterTest() {
        Table student = new Table(0L, "student", Table.TableType.OLAP,
                ImmutableList.<Column>of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("gender", Type.INT, false, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "", ""),
                        new Column("age", Type.INT, true, AggregateType.NONE, "", "")));
        Plan scan = new LogicalOlapScan(student, ImmutableList.of("student"));
        Slot gender = scan.getOutput().get(1);
        Slot name = scan.getOutput().get(2);
        Slot age = scan.getOutput().get(3);

        List<Expression> groupByKeys = Lists.newArrayList(age, gender);
        List<NamedExpression> outputExpressionList = Lists.newArrayList(gender, age);
        Plan aggregation = new LogicalAggregate<>(groupByKeys, outputExpressionList, scan);
        Expression filterPredicate = ExpressionUtils.and(
                new GreaterThan(gender, Literal.of(1)),
                new LessThanEqual(
                        new Add(
                                gender,
                                Literal.of(10)
                        ),
                        Literal.of(100)
                ),
                new EqualTo(name, Literal.of("abc"))
        );
        LogicalFilter filter = new LogicalFilter(filterPredicate, aggregation);
        Plan root = new LogicalProject<>(
                Lists.newArrayList(gender),
                filter
        );

        Memo memo = rewrite(root);
        System.out.println(memo.copyOut().treeString());
        Group rootGroup = memo.getRoot();
        GroupExpression groupExpression = rootGroup.getLogicalExpression().child(0).getLogicalExpression();
        Plan upperFilter = groupExpression.getPlan();
        Assertions.assertTrue(upperFilter instanceof LogicalFilter);
        Expression upperPredicates = ((LogicalFilter<?>) upperFilter).getPredicates();
        Assertions.assertTrue(upperPredicates instanceof EqualTo);
        Assertions.assertTrue(upperPredicates.child(0) instanceof Slot);
        groupExpression = groupExpression.child(0).getLogicalExpression();
        aggregation = groupExpression.getPlan();
        Assertions.assertTrue(aggregation instanceof LogicalAggregate);
        groupExpression = groupExpression.child(0).getLogicalExpression();
        Plan bottomFilter = groupExpression.getPlan();
        Assertions.assertTrue(bottomFilter instanceof LogicalFilter);
        Expression bottomPredicates = ((LogicalFilter<?>) bottomFilter).getPredicates();
        Assertions.assertTrue(bottomPredicates instanceof And);
        Assertions.assertEquals(2, bottomPredicates.children().size());
        Expression greater = bottomPredicates.child(0);
        Assertions.assertTrue(greater instanceof GreaterThan);
        Assertions.assertTrue(greater.child(0) instanceof Slot);
        Assertions.assertEquals("gender", ((Slot) greater.child(0)).getName());
        Expression less = bottomPredicates.child(1);
        Assertions.assertTrue(less instanceof LessThanEqual);
        Assertions.assertTrue(less.child(0) instanceof Add);

        groupExpression = groupExpression.child(0).getLogicalExpression();
        Plan scan2 = groupExpression.getPlan();
        Assertions.assertTrue(scan2 instanceof LogicalOlapScan);
    }

    private Memo rewrite(Plan plan) {
        return PlanRewriter.topDownRewriteMemo(plan, new ConnectContext(), new PushPredicateThroughAggregation());
    }
}
