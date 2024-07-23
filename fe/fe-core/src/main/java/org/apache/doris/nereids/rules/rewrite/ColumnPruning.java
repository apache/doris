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

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning.PruneContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.logical.OutputPrunable;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * ColumnPruning.
 *
 * you should implement OutputPrunable for your plan to provide the ability of column pruning
 *
 * functions:
 *
 * 1. prune/shrink output field for OutputPrunable, e.g.
 *
 *            project(projects=[k1, sum(v1)])                              project(projects=[k1, sum(v1)])
 *                      |                                 ->                      |
 *    agg(groupBy=[k1], output=[k1, sum(v1), sum(v2)]                  agg(groupBy=[k1], output=[k1, sum(v1)])
 *
 * 2. add project for the plan which prune children's output failed, e.g. the filter not record
 *    the output, and we can not prune/shrink output field for the filter, so we should add project on filter.
 *
 *          agg(groupBy=[a])                              agg(groupBy=[a])
 *                |                                              |
 *           filter(b > 10)                ->                project(a)
 *                |                                              |
 *              plan                                       filter(b > 10)
 *                                                               |
 *                                                              plan
 */
public class ColumnPruning extends DefaultPlanRewriter<PruneContext> implements CustomRewriter {
    private Set<Slot> keys;

    /**
     * collect all columns used in expressions, which should not be pruned
     * the purpose to collect keys are:
     * 1. used for count(*), '*' is replaced by the smallest(data type in byte size) column
     * 2. for StatsDerive, only when col-stats of keys are not available, we fall back to no-stats algorithm
     */
    public static class KeyColumnCollector
            extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
        public Set<Slot> keys = Sets.newHashSet();

        @Override
        public Plan rewriteRoot(Plan plan, JobContext jobContext) {
            return plan.accept(this, jobContext);
        }

        @Override
        public Plan visit(Plan plan, JobContext jobContext) {
            for (Plan child : plan.children()) {
                child.accept(this, jobContext);
            }
            for (Expression expression : plan.getExpressions()) {
                if (!(expression instanceof SlotReference)) {
                    keys.addAll(expression.getInputSlots());
                }
            }
            return plan;
        }

        @Override
        public LogicalAggregate<? extends Plan> visitLogicalAggregate(LogicalAggregate<? extends Plan> agg,
                JobContext jobContext) {
            agg.child().accept(this, jobContext);
            for (Expression expression : agg.getExpressions()) {
                if (expression instanceof SlotReference) {
                    keys.add((Slot) expression);
                } else {
                    keys.addAll(expression.getInputSlots());
                }
            }
            return agg;
        }
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        KeyColumnCollector keyColumnCollector = new KeyColumnCollector();
        plan.accept(keyColumnCollector, jobContext);
        keys = keyColumnCollector.keys;
        if (ConnectContext.get() != null) {
            StatementContext stmtContext = ConnectContext.get().getStatementContext();
            // in ut, stmtContext is null
            if (stmtContext != null) {
                for (Slot key : keys) {
                    if (key instanceof SlotReference) {
                        stmtContext.addKeySlot((SlotReference) key);
                    }
                }
            }
        }

        return plan.accept(this, new PruneContext(plan.getOutputSet(), null));
    }

    @Override
    public Plan visit(Plan plan, PruneContext context) {
        if (plan instanceof OutputPrunable) {
            // the case 1 in the class comment
            // two steps: prune current output and prune children
            OutputPrunable outputPrunable = (OutputPrunable) plan;
            plan = pruneOutput(plan, outputPrunable.getOutputs(), outputPrunable::pruneOutputs, context);
            return pruneChildren(plan);
        } else {
            // e.g.
            //
            //       project(a)
            //           |
            //           |  require: [a]
            //           v
            //       filter(b > 1)    <-  process currently
            //           |
            //           |  require: [a, b]
            //           v
            //       child plan
            //
            // the filter is not OutputPrunable, we should pass through the parent required slots
            // (slot a, which in the context.requiredSlots) and the used slots currently(slot b) to child plan.
            return pruneChildren(plan, context.requiredSlots);
        }
    }

    // union can not prune children by the common logic, we must override visit method to write special code.
    @Override
    public Plan visitLogicalUnion(LogicalUnion union, PruneContext context) {
        if (union.getQualifier() == Qualifier.DISTINCT) {
            return skipPruneThisAndFirstLevelChildren(union);
        }
        LogicalUnion prunedOutputUnion = pruneUnionOutput(union, context);
        // start prune children of union
        List<Slot> originOutput = union.getOutput();
        Set<Slot> prunedOutput = prunedOutputUnion.getOutputSet();
        List<Integer> prunedOutputIndexes = IntStream.range(0, originOutput.size())
                .filter(index -> prunedOutput.contains(originOutput.get(index)))
                .boxed()
                .collect(ImmutableList.toImmutableList());

        ImmutableList.Builder<Plan> prunedChildren = ImmutableList.builder();
        ImmutableList.Builder<List<SlotReference>> prunedChildrenOutputs = ImmutableList.builder();
        for (int i = 0; i < prunedOutputUnion.arity(); i++) {
            List<SlotReference> regularChildOutputs = prunedOutputUnion.getRegularChildOutput(i);
            List<SlotReference> prunedChildOutput = prunedOutputIndexes.stream()
                    .map(regularChildOutputs::get)
                    .collect(ImmutableList.toImmutableList());
            Set<Slot> prunedChildOutputSet = ImmutableSet.copyOf(prunedChildOutput);
            Plan prunedChild = doPruneChild(prunedOutputUnion, prunedOutputUnion.child(i), prunedChildOutputSet);
            prunedChildrenOutputs.add(prunedChildOutput);
            prunedChildren.add(prunedChild);
        }
        return prunedOutputUnion.withChildrenAndTheirOutputs(prunedChildren.build(), prunedChildrenOutputs.build());
    }

    // we should keep the output of LogicalSetOperation and all the children
    @Override
    public Plan visitLogicalExcept(LogicalExcept except, PruneContext context) {
        return skipPruneThisAndFirstLevelChildren(except);
    }

    @Override
    public Plan visitLogicalIntersect(LogicalIntersect intersect, PruneContext context) {
        return skipPruneThisAndFirstLevelChildren(intersect);
    }

    @Override
    public Plan visitLogicalSink(LogicalSink<? extends Plan> logicalSink, PruneContext context) {
        return skipPruneThisAndFirstLevelChildren(logicalSink);
    }

    // the backend not support filter(project(agg)), so we can not prune the key set in the agg,
    // only prune the agg functions here
    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, PruneContext context) {
        return pruneAggregate(aggregate, context);
    }

    // same as aggregate
    @Override
    public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, PruneContext context) {
        return pruneAggregate(repeat, context);
    }

    @Override
    public Plan visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, PruneContext context) {
        return skipPruneThisAndFirstLevelChildren(cteProducer);
    }

    @Override
    public Plan visitLogicalCTEConsumer(LogicalCTEConsumer cteConsumer, PruneContext context) {
        return super.visitLogicalCTEConsumer(cteConsumer, context);
    }

    @Override
    public Plan visitLogicalWindow(LogicalWindow<? extends Plan> window, PruneContext context) {
        boolean pruned = false;
        boolean reserved = false;
        ImmutableList.Builder<NamedExpression> reservedWindowExpressions = ImmutableList.builder();
        for (NamedExpression windowExpression : window.getWindowExpressions()) {
            if (context.requiredSlots.contains(windowExpression.toSlot())) {
                reservedWindowExpressions.add(windowExpression);
                reserved = true;
            } else {
                pruned = true;
            }
        }
        if (!pruned) {
            return pruneChildren(window, context.requiredSlots);
        }
        if (!reserved) {
            return window.child().accept(this, context);
        }
        LogicalWindow<? extends Plan> prunedWindow
                = window.withExpressionsAndChild(reservedWindowExpressions.build(), window.child());
        return pruneChildren(prunedWindow, context.requiredSlots);
    }

    private Plan pruneAggregate(Aggregate<?> agg, PruneContext context) {
        // first try to prune group by and aggregate functions
        Aggregate<? extends Plan> prunedOutputAgg = pruneOutput(agg, agg.getOutputs(), agg::pruneOutputs, context);
        Aggregate<?> fillUpAggregate = fillUpGroupByAndOutput(prunedOutputAgg);
        return pruneChildren(fillUpAggregate);
    }

    private Plan skipPruneThisAndFirstLevelChildren(Plan plan) {
        ImmutableSet.Builder<Slot> requireAllOutputOfChildren = ImmutableSet.builder();
        for (Plan child : plan.children()) {
            requireAllOutputOfChildren.addAll(child.getOutput());
        }
        return pruneChildren(plan, requireAllOutputOfChildren.build());
    }

    private static Aggregate<? extends Plan> fillUpGroupByAndOutput(Aggregate<? extends Plan> prunedOutputAgg) {
        List<Expression> groupBy = prunedOutputAgg.getGroupByExpressions();
        List<NamedExpression> output = prunedOutputAgg.getOutputExpressions();

        if (!(prunedOutputAgg instanceof LogicalAggregate)) {
            return prunedOutputAgg;
        }

        ImmutableList.Builder<NamedExpression> newOutputListBuilder
                = ImmutableList.builderWithExpectedSize(output.size());
        newOutputListBuilder.addAll((List) groupBy);
        for (NamedExpression ne : output) {
            if (!groupBy.contains(ne)) {
                newOutputListBuilder.add(ne);
            }
        }

        List<NamedExpression> newOutputList = newOutputListBuilder.build();
        Set<AggregateFunction> aggregateFunctions = prunedOutputAgg.getAggregateFunctions();
        ImmutableList.Builder<Expression> newGroupByExprList
                = ImmutableList.builderWithExpectedSize(newOutputList.size());
        for (NamedExpression e : newOutputList) {
            if (!(e instanceof Alias && aggregateFunctions.contains(e.child(0)))) {
                newGroupByExprList.add(e);
            }
        }
        return ((LogicalAggregate<? extends Plan>) prunedOutputAgg).withGroupByAndOutput(
                newGroupByExprList.build(), newOutputList);
    }

    /** prune output */
    public <P extends Plan> P pruneOutput(P plan, List<NamedExpression> originOutput,
            Function<List<NamedExpression>, P> withPrunedOutput, PruneContext context) {
        if (originOutput.isEmpty()) {
            return plan;
        }
        List<NamedExpression> prunedOutputs =
                Utils.filterImmutableList(originOutput, output -> context.requiredSlots.contains(output.toSlot()));

        if (prunedOutputs.isEmpty()) {
            List<NamedExpression> candidates = Lists.newArrayList(originOutput);
            candidates.retainAll(keys);
            if (candidates.isEmpty()) {
                candidates = originOutput;
            }
            NamedExpression minimumColumn = ExpressionUtils.selectMinimumColumn(candidates);
            prunedOutputs = ImmutableList.of(minimumColumn);
        }

        if (prunedOutputs.equals(originOutput)) {
            return plan;
        } else {
            return withPrunedOutput.apply(prunedOutputs);
        }
    }

    private LogicalUnion pruneUnionOutput(LogicalUnion union, PruneContext context) {
        List<NamedExpression> originOutput = union.getOutputs();
        if (originOutput.isEmpty()) {
            return union;
        }
        List<NamedExpression> prunedOutputs = Lists.newArrayList();
        List<List<NamedExpression>> constantExprsList = union.getConstantExprsList();
        List<Integer> extractColumnIndex = Lists.newArrayList();
        for (int i = 0; i < originOutput.size(); i++) {
            NamedExpression output = originOutput.get(i);
            if (context.requiredSlots.contains(output.toSlot())) {
                prunedOutputs.add(output);
                extractColumnIndex.add(i);
            }
        }
        int len = extractColumnIndex.size();
        ImmutableList.Builder<List<NamedExpression>> prunedConstantExprsList
                = ImmutableList.builderWithExpectedSize(constantExprsList.size());
        for (List<NamedExpression> row : constantExprsList) {
            ImmutableList.Builder<NamedExpression> newRow = ImmutableList.builderWithExpectedSize(len);
            for (int idx : extractColumnIndex) {
                newRow.add(row.get(idx));
            }
            prunedConstantExprsList.add(newRow.build());
        }

        if (prunedOutputs.isEmpty()) {
            List<NamedExpression> candidates = Lists.newArrayList(originOutput);
            candidates.retainAll(keys);
            if (candidates.isEmpty()) {
                candidates = originOutput;
            }
            NamedExpression minimumColumn = ExpressionUtils.selectMinimumColumn(candidates);
            prunedOutputs = ImmutableList.of(minimumColumn);
        }

        if (prunedOutputs.equals(originOutput)) {
            return union;
        } else {
            return union.withNewOutputsAndConstExprsList(prunedOutputs, prunedConstantExprsList.build());
        }
    }

    private <P extends Plan> P pruneChildren(P plan) {
        return pruneChildren(plan, ImmutableSet.of());
    }

    private <P extends Plan> P pruneChildren(P plan, Set<Slot> parentRequiredSlots) {
        if (plan.arity() == 0) {
            // leaf
            return plan;
        }

        Set<Slot> currentUsedSlots = plan.getInputSlots();
        Set<Slot> childrenRequiredSlots = parentRequiredSlots.isEmpty()
                ? currentUsedSlots
                : ImmutableSet.<Slot>builderWithExpectedSize(parentRequiredSlots.size() + currentUsedSlots.size())
                        .addAll(parentRequiredSlots)
                        .addAll(currentUsedSlots)
                        .build();

        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            Set<Slot> childRequiredSlots;
            List<Slot> childOutputs = child.getOutput();
            ImmutableSet.Builder<Slot> childRequiredSlotBuilder
                    = ImmutableSet.builderWithExpectedSize(childOutputs.size());
            for (Slot childOutput : childOutputs) {
                if (childrenRequiredSlots.contains(childOutput)) {
                    childRequiredSlotBuilder.add(childOutput);
                }
            }
            childRequiredSlots = childRequiredSlotBuilder.build();
            Plan prunedChild = doPruneChild(plan, child, childRequiredSlots);
            if (prunedChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(prunedChild);
        }
        return hasNewChildren ? (P) plan.withChildren(newChildren.build()) : plan;
    }

    private Plan doPruneChild(Plan plan, Plan child, Set<Slot> childRequiredSlots) {
        if (child instanceof LogicalCTEProducer) {
            return child;
        }
        boolean isProject = plan instanceof LogicalProject;
        Plan prunedChild = child.accept(this, new PruneContext(childRequiredSlots, plan));

        // the case 2 in the class comment, prune child's output failed
        if (!isProject && !Sets.difference(prunedChild.getOutputSet(), childRequiredSlots).isEmpty()) {
            prunedChild = new LogicalProject<>(Utils.fastToImmutableList(childRequiredSlots), prunedChild);
        }
        return prunedChild;
    }

    /** PruneContext */
    public static class PruneContext {
        public Set<Slot> requiredSlots;
        public Optional<Plan> parent;

        public PruneContext(Set<Slot> requiredSlots, Plan parent) {
            this.requiredSlots = requiredSlots;
            this.parent = Optional.ofNullable(parent);
        }
    }
}
