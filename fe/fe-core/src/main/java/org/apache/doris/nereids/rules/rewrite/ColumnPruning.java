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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning.PruneContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.OutputPrunable;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
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
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
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

        LogicalUnion prunedOutputUnion = pruneOutput(union, union.getOutputs(), union::pruneOutputs, context);
        if (prunedOutputUnion == union) {
            return union;
        }

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

    private Plan pruneAggregate(Aggregate agg, PruneContext context) {
        // first try to prune group by and aggregate functions
        Aggregate prunedOutputAgg = pruneOutput(agg, agg.getOutputs(), agg::pruneOutputs, context);

        List<Expression> groupByExpressions = prunedOutputAgg.getGroupByExpressions();
        List<NamedExpression> outputExpressions = prunedOutputAgg.getOutputExpressions();

        // then fill up group by
        Aggregate fillUpOutputRepeat = fillUpGroupByToOutput(groupByExpressions, outputExpressions)
                .map(fullOutput -> prunedOutputAgg.withAggOutput(fullOutput))
                .orElse(prunedOutputAgg);

        return pruneChildren(fillUpOutputRepeat);
    }

    private Plan skipPruneThisAndFirstLevelChildren(Plan plan) {
        Set<Slot> requireAllOutputOfChildren = plan.children()
                .stream()
                .flatMap(child -> child.getOutputSet().stream())
                .collect(Collectors.toSet());
        return pruneChildren(plan, requireAllOutputOfChildren);
    }

    private static Optional<List<NamedExpression>> fillUpGroupByToOutput(
            List<Expression> groupBy, List<NamedExpression> output) {

        if (output.containsAll(groupBy)) {
            return Optional.empty();
        }

        List<NamedExpression> aggFunctions = Lists.newArrayList(output);
        aggFunctions.removeAll(groupBy);

        return Optional.of(ImmutableList.<NamedExpression>builder()
                .addAll((List) groupBy)
                .addAll(aggFunctions)
                .build());
    }

    /** prune output */
    public static <P extends Plan> P pruneOutput(P plan, List<NamedExpression> originOutput,
            Function<List<NamedExpression>, P> withPrunedOutput, PruneContext context) {
        List<NamedExpression> prunedOutputs = originOutput.stream()
                .filter(output -> context.requiredSlots.contains(output.toSlot()))
                .collect(ImmutableList.toImmutableList());

        if (prunedOutputs.isEmpty()) {
            NamedExpression minimumColumn = ExpressionUtils.selectMinimumColumn(originOutput);
            prunedOutputs = ImmutableList.of(minimumColumn);
        }

        if (prunedOutputs.equals(originOutput)) {
            return plan;
        } else {
            return withPrunedOutput.apply(prunedOutputs);
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
                : ImmutableSet.<Slot>builder()
                        .addAll(parentRequiredSlots)
                        .addAll(currentUsedSlots)
                        .build();

        List<Plan> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            Set<Slot> childOutputSet = child.getOutputSet();
            Set<Slot> childRequiredSlots = childOutputSet.stream()
                    .filter(childrenRequiredSlots::contains).collect(Collectors.toSet());
            Plan prunedChild = doPruneChild(plan, child, childRequiredSlots);
            if (prunedChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(prunedChild);
        }
        return hasNewChildren ? (P) plan.withChildren(newChildren) : plan;
    }

    private Plan doPruneChild(Plan plan, Plan child, Set<Slot> childRequiredSlots) {
        if (child instanceof LogicalCTEProducer) {
            return child;
        }
        boolean isProject = plan instanceof LogicalProject;
        Plan prunedChild = child.accept(this, new PruneContext(childRequiredSlots, plan));

        // the case 2 in the class comment, prune child's output failed
        if (!isProject && !Sets.difference(prunedChild.getOutputSet(), childRequiredSlots).isEmpty()) {
            prunedChild = new LogicalProject<>(ImmutableList.copyOf(childRequiredSlots), prunedChild);
        }
        return prunedChild;
    }

    @Override
    public Plan visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer, PruneContext context) {
        return skipPruneThisAndFirstLevelChildren(cteProducer);
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
