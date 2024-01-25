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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.NormalizeAggregate;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AggregateStrategiesTest implements MemoPatternMatchSupported {
    private Plan rStudent;

    @BeforeAll
    public final void beforeAll() {
        rStudent = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), PlanConstructor.student,
                ImmutableList.of(""));
    }

    /**
     * <pre>
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [age, SUM(id) as sum], groupByExpr: [age])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [a, SUM(b) as c], groupByExpr: [a])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [age as a, SUM(id) as b], groupByExpr: [age])
     *       +--childPlan(id, name, age)
     * </pre>
     */
    @Test
    public void slotReferenceGroupBy() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot(),
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        Expression localOutput0 = rStudent.getOutput().get(2).toSlot();
        Sum localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Slot localGroupBy = rStudent.getOutput().get(2).toSlot();

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyImplementation(twoPhaseAggregateWithoutDistinct())
                .matches(
                    physicalHashAggregate(
                        physicalHashAggregate()
                            .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL))
                            .when(agg -> agg.getOutputExpressions().size() == 2)
                            .when(agg -> agg.getOutputExpressions().get(0).equals(localOutput0))
                            .when(agg -> agg.getOutputExpressions().get(1).child(0).child(0)
                                    .children().equals(localOutput1.children()))
                            .when(agg -> agg.getGroupByExpressions().size() == 1)
                            .when(agg -> agg.getGroupByExpressions().get(0).equals(localGroupBy))
                    ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL))
                            .when(agg -> agg.getOutputExpressions().size() == 2)
                            .when(agg -> agg.getOutputExpressions().get(0)
                                    .equals(agg.child().getOutputExpressions().get(0).toSlot()))
                            .when(agg -> agg.getOutputExpressions().get(1).child(0).child(0)
                                    .equals(agg.child().getOutputExpressions().get(1).toSlot()))
                            .when(agg -> agg.getGroupByExpressions().size() == 1)
                            .when(agg -> agg.getGroupByExpressions().get(0)
                                    .equals(agg.child().getOutputExpressions().get(0).toSlot()))
                            // check id:
                            .when(agg -> agg.getOutputExpressions().get(0).getExprId()
                                    .equals(outputExpressionList.get(0).getExprId()))
                            .when(agg -> agg.getOutputExpressions().get(1).getExprId()
                                    .equals(outputExpressionList.get(1).getExprId()))
                );
    }

    /**
     * <pre>
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(id) as sum], groupByExpr: [])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(b) as b], groupByExpr: [])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [SUM(id) as a], groupByExpr: [])
     *       +--childPlan(id, name, age)
     * </pre>
     */
    @Test
    public void globalAggregate() {
        List<Expression> groupExpressionList = Lists.newArrayList();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Sum(rStudent.getOutput().get(0)), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        Sum localOutput0 = new Sum(rStudent.getOutput().get(0).toSlot());

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyImplementation(twoPhaseAggregateWithoutDistinct())
                .matches(
                    physicalHashAggregate(
                        physicalHashAggregate()
                            .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL))
                            .when(agg -> agg.getOutputExpressions().size() == 1)
                            .when(agg -> agg.getOutputExpressions().get(0).child(0).child(0)
                                    .equals(localOutput0))
                            .when(agg -> agg.getGroupByExpressions().size() == 0)
                    ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL))
                        .when(agg -> agg.getOutputExpressions().size() == 1)
                        .when(agg -> agg.getOutputExpressions().get(0) instanceof Alias)
                        .when(agg -> agg.getOutputExpressions().get(0).child(0).child(0)
                                .equals(agg.child().getOutputExpressions().get(0).toSlot()))
                        .when(agg -> agg.getGroupByExpressions().size() == 0)
                        // check id:
                        .when(agg -> agg.getOutputExpressions().get(0).getExprId()
                                .equals(outputExpressionList.get(0).getExprId()))
                );
    }

    /**
     * <pre>
     * the initial plan is:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(id) as sum], groupByExpr: [age])
     *   +--childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [SUM(b) as c], groupByExpr: [a])
     *   +--Aggregate(phase: [LOCAL], outputExpr: [age as a, SUM(id) as b], groupByExpr: [age])
     *       +--childPlan(id, name, age)
     * </pre>
     */
    @Test
    public void groupExpressionNotInOutput() {
        List<Expression> groupExpressionList = Lists.newArrayList(
                rStudent.getOutput().get(2).toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Sum(rStudent.getOutput().get(0).toSlot()), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        Expression localOutput0 = rStudent.getOutput().get(2).toSlot();
        Sum localOutput1 = new Sum(rStudent.getOutput().get(0).toSlot());
        Expression localGroupBy = rStudent.getOutput().get(2).toSlot();

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyImplementation(twoPhaseAggregateWithoutDistinct())
                .matches(
                    physicalHashAggregate(
                        physicalHashAggregate()
                            .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL))
                            .when(agg -> agg.getOutputExpressions().size() == 2)
                            .when(agg -> agg.getOutputExpressions().get(0).equals(localOutput0))
                            .when(agg -> agg.getOutputExpressions().get(1).child(0).child(0)
                                    .equals(localOutput1))
                            .when(agg -> agg.getGroupByExpressions().size() == 1)
                            .when(agg -> agg.getGroupByExpressions().get(0).equals(localGroupBy))
                    ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL))
                            .when(agg -> agg.getOutputExpressions().size() == 1)
                            .when(agg -> agg.getOutputExpressions().get(0) instanceof Alias)
                            .when(agg -> agg.getOutputExpressions().get(0).child(0).child(0)
                                    .equals(agg.child().getOutputExpressions().get(1).toSlot()))
                            .when(agg -> agg.getGroupByExpressions().size() == 1)
                            .when(agg -> agg.getGroupByExpressions().get(0)
                                    .equals(agg.child().getOutputExpressions().get(0).toSlot()))
                            // check id:
                            .when(agg -> agg.getOutputExpressions().get(0).getExprId()
                                    .equals(outputExpressionList.get(0).getExprId()))
                );
    }

    /**
     * <pre>
     * the initial plan is:
     *   Aggregate(phase: [LOCAL], outputExpr: [(COUNT(distinct age) + 2) as c], groupByExpr: [])
     *   +-- childPlan(id, name, age)
     * we should rewrite to:
     *   Aggregate(phase: [GLOBAL], outputExpr: [count(distinct c)], groupByExpr: [])
     *   +-- Aggregate(phase: [LOCAL], outputExpr: [age], groupByExpr: [age])
     *       +-- childPlan(id, name, age)
     * </pre>
     */
    @Test
    @Disabled
    @Developing("reopen it after we could choose agg phase by CBO")
    public void distinctAggregateWithoutGroupByApply2PhaseRule() {
        List<Expression> groupExpressionList = new ArrayList<>();
        List<NamedExpression> outputExpressionList = Lists.newArrayList(new Alias(
                new Add(new Count(true, rStudent.getOutput().get(2).toSlot()),
                        new IntegerLiteral(2)), "c"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                false, Optional.empty(), rStudent);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyBottomUp(new NormalizeAggregate())
                .applyImplementation(twoPhaseAggregateWithDistinct())
                .matches(
                    physicalHashAggregate(
                        physicalHashAggregate()
                            .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL))
                            .when(agg -> agg.getOutputExpressions().size() == 1)
                            .when(agg -> agg.getGroupByExpressions().size() == 1
                                    && agg.getGroupByExpressions().get(0).equals(rStudent.getOutput().get(2).toSlot())) // group by name
                    ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL))
                            .when(agg -> agg.getOutputExpressions().size() == 1
                                    && agg.getOutputExpressions().get(0).child(0) instanceof AggregateExpression
                                    && agg.getOutputExpressions().get(0).child(0).child(0) instanceof Count
                                    && agg.getOutputExpressions().get(0).child(0).child(0).child(0).equals(rStudent.getOutput().get(2).toSlot())) // count(name)
                            .when(agg -> agg.getGroupByExpressions().isEmpty())
                );
    }

    // TODO aggregate estimation is not accurate enough.
    //  we choose 3Phase as RBO. Re-open this case when we could compare cost between 2phase and 3phase.
    @Test
    @Disabled
    @Developing("reopen this case when we could choose agg phase by CBO")
    public void distinctWithNormalAggregateFunctionApply2PhaseRule() {
        Slot id = rStudent.getOutput().get(0);
        Slot name = rStudent.getOutput().get(2).toSlot();
        List<Expression> groupExpressionList = Lists.newArrayList(id.toSlot());
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Count(true, name), "c"),
                new Alias(new Sum(id.toSlot()), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        // check local:
        // id
        AggregateParam phaseTwoCountAggParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_RESULT);
        AggregateParam phaseOneSumAggParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        AggregateParam phaseTwoSumAggParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        // sum
        Sum sumId = new Sum(false, id.toSlot());

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyImplementation(twoPhaseAggregateWithDistinct())
                .matches(
                    physicalHashAggregate(
                        physicalHashAggregate()
                                .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL)
                                        && agg.getOutputExpressions().size() == 3
                                        && agg.getGroupByExpressions().size() == 2)
                                .when(agg -> agg.getOutputExpressions().get(0).equals(id))
                                .when(agg -> agg.getOutputExpressions().get(1).equals(name))
                                .when(agg -> agg.getOutputExpressions().get(2).child(0).child(0).equals(sumId)
                                        && ((AggregateExpression) agg.getOutputExpressions().get(2).child(0)).getAggregateParam().equals(phaseOneSumAggParam))
                                .when(agg -> agg.getGroupByExpressions().equals(ImmutableList.of(id, name)))
                    ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL))
                    .when(agg -> {
                        Slot child = agg.child().getOutputExpressions().get(1).toSlot();
                        Assertions.assertTrue(agg.getOutputExpressions().get(0).child(0).child(0) instanceof Count);
                        return agg.getOutputExpressions().get(0).child(0).child(0).child(0).equals(child);
                    })
                    .when(agg -> {
                        Slot partialSum = agg.child().getOutputExpressions().get(2).toSlot();
                        Assertions.assertTrue(agg.getOutputExpressions().get(1).child(0) instanceof AggregateExpression);
                        Assertions.assertEquals(phaseTwoSumAggParam, ((AggregateExpression) agg.getOutputExpressions().get(1).child(0)).getAggregateParam());
                        Assertions.assertTrue(agg.getOutputExpressions().get(1).child(0).child(0).equals(partialSum));

                        Assertions.assertEquals(phaseTwoCountAggParam, ((AggregateExpression) agg.getOutputExpressions().get(0).child(0)).getAggregateParam());
                        return true;
                    })
                    .when(agg -> agg.getGroupByExpressions().get(0)
                            .equals(agg.child().getOutputExpressions().get(0)))
                );
    }

    @Test
    @Disabled
    @Developing("not support four phase aggregate")
    public void distinctWithNormalAggregateFunctionApply4PhaseRule() {
        Slot id = rStudent.getOutput().get(0).toSlot();
        Slot name = rStudent.getOutput().get(2).toSlot();
        List<Expression> groupExpressionList = Lists.newArrayList(id);
        List<NamedExpression> outputExpressionList = Lists.newArrayList(
                new Alias(new Count(true, name), "c"),
                new Alias(new Sum(id), "sum"));
        Plan root = new LogicalAggregate<>(groupExpressionList, outputExpressionList,
                true, Optional.empty(), rStudent);

        // check local:
        // count
        Count phaseOneCountName = new Count(true, name);
        // sum
        Sum phaseOneSumId = new Sum(id);

        PlanChecker.from(MemoTestUtils.createConnectContext(), root)
                .applyImplementation(fourPhaseAggregateWithDistinct())
                .matchesFromRoot(
                    physicalHashAggregate(
                        physicalHashAggregate(
                            physicalHashAggregate(
                                physicalHashAggregate() // select id, count(distinct name), sum(id) group by id
                                    .when(agg -> agg.getAggPhase().equals(AggPhase.LOCAL))
                                    .when(agg -> agg.getOutputExpressions().get(0).equals(id))
                                    .when(agg -> agg.getOutputExpressions().get(1).child(0).equals(phaseOneCountName))
                                    .when(agg -> agg.getOutputExpressions().get(2).child(0).equals(phaseOneSumId))
                                    .when(agg -> agg.getGroupByExpressions().get(0).equals(id))
                            ).when(agg -> agg.getAggPhase().equals(AggPhase.GLOBAL)) // select id, count(distinct name), sum(id) group by id
                                .when(agg -> {
                                    Slot child = agg.child().getOutputExpressions().get(1).toSlot();
                                    Assertions.assertTrue(agg.getOutputExpressions().get(1).child(0) instanceof Count);
                                    return agg.getOutputExpressions().get(1).child(0).child(0).equals(child);
                                })
                                .when(agg -> {
                                    Slot child = agg.child().getOutputExpressions().get(2).toSlot();
                                    Assertions.assertTrue(agg.getOutputExpressions().get(2).child(0) instanceof Sum);
                                    return ((Sum) agg.getOutputExpressions().get(2).child(0)).child().equals(child);
                                })
                                .when(agg -> agg.getGroupByExpressions().get(0)
                                        .equals(agg.child().getOutputExpressions().get(0)))
                        ).when(agg -> agg.getAggPhase().equals(AggPhase.DISTINCT_LOCAL))
                                .when(agg -> {
                                    Slot child = agg.child().getOutputExpressions().get(1).toSlot();
                                    Assertions.assertTrue(agg.getOutputExpressions().get(1).child(0) instanceof Count);
                                    return agg.getOutputExpressions().get(1).child(0).child(0).equals(child);
                                })
                                .when(agg -> {
                                    Slot child = agg.child().getOutputExpressions().get(2).toSlot();
                                    Assertions.assertTrue(agg.getOutputExpressions().get(2).child(0) instanceof Sum);
                                    return ((Sum) agg.getOutputExpressions().get(2).child(0)).child().equals(child);
                                })
                                .when(agg -> agg.getGroupByExpressions().get(0)
                                        .equals(agg.child().getOutputExpressions().get(0)))
                    ).when(agg -> agg.getAggPhase().equals(AggPhase.DISTINCT_GLOBAL))
                            .when(agg -> agg.getOutputExpressions().size() == 2)
                            .when(agg -> agg.getOutputExpressions().get(0) instanceof Alias)
                            .when(agg -> agg.getOutputExpressions().get(0).child(0) instanceof Count)
                            .when(agg -> agg.getOutputExpressions().get(1).child(0) instanceof Sum)
                            .when(agg -> agg.getOutputExpressions().get(0).getExprId() == outputExpressionList.get(
                                    0).getExprId())
                            .when(agg -> agg.getOutputExpressions().get(1).getExprId() == outputExpressionList.get(
                                    1).getExprId())
                            .when(agg -> agg.getGroupByExpressions().get(0)
                                    .equals(agg.child().child().child().getOutputExpressions().get(0)))
                );
    }

    private Rule twoPhaseAggregateWithoutDistinct() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType() == RuleType.TWO_PHASE_AGGREGATE_WITHOUT_DISTINCT)
                .findFirst()
                .get();
    }

    private Rule twoPhaseAggregateWithDistinct() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType() == RuleType.TWO_PHASE_AGGREGATE_WITH_DISTINCT)
                .findFirst()
                .get();
    }

    @Developing
    private Rule fourPhaseAggregateWithDistinct() {
        return new AggregateStrategies().buildRules()
                .stream()
                .filter(rule -> rule.getRuleType() == RuleType.TWO_PHASE_AGGREGATE_WITH_DISTINCT)
                .findFirst()
                .get();
    }
}
