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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.logical.ColumnPruning.PruneContext;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.OutputPrunable;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
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
 *            project(projects=[sum(v1)])                              project(projects=[sum(v1)])
 *                      |                                 ->                      |
 *    agg(groupBy=[k1], output=[sum(v1), sum(v2)]                  agg(groupBy=[k1], output=[sum(v1)])
 *
 * 2. add project for the project which prune children's output failed, e.g. the filter not record
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
            // the filter is not OutputSavePoint, we should pass through the parent required slots
            // (slot a, which in the context.requiredSlots) and the used slots currently(slot b) to child plan.
            return pruneChildren(plan, context.requiredSlots);
        }
    }


    // union can not be pruned by the common logic, we must override visit method to write special code.
    @Override
    public Plan visitLogicalUnion(LogicalUnion union, PruneContext context) {
        LogicalUnion prunedOutputUnion = pruneOutput(union, union.getOutputs(), union::pruneOutputs, context);
        List<Slot> originOutput = union.getOutput();
        Set<Slot> prunedOutput = prunedOutputUnion.getOutputSet();
        Set<Integer> prunedOutputIndexes = IntStream.range(0, originOutput.size())
                .filter(index -> prunedOutput.contains(originOutput.get(index)))
                .boxed()
                .collect(ImmutableSet.toImmutableSet());

        AtomicBoolean changed = new AtomicBoolean(false);
        List<Plan> prunedChildren = prunedOutputUnion.children().stream()
                .map(child -> {
                    List<Slot> childOutput = child.getOutput();
                    Set<Slot> prunedChildOutput = prunedOutputIndexes.stream()
                            .map(childOutput::get)
                            .collect(ImmutableSet.toImmutableSet());
                    Plan prunedChild = child.accept(this, new PruneContext(prunedChildOutput, prunedOutputUnion));
                    if (prunedChild != child) {
                        changed.set(true);
                    }
                    return prunedChild;
                })
                .collect(ImmutableList.toImmutableList());

        if (!changed.get()) {
            return prunedOutputUnion;
        }

        return prunedOutputUnion.withChildren(prunedChildren);
    }

    public static final <P extends Plan> P pruneOutput(P plan, List<NamedExpression> originOutput,
            Function<List<NamedExpression>, P> withPrunedOutput, PruneContext context) {
        Optional<List<NamedExpression>> prunedOutputs = pruneOutput(originOutput, context);
        return prunedOutputs.map(withPrunedOutput).orElse(plan);
    }

    /** prune output */
    public static Optional<List<NamedExpression>> pruneOutput(
            List<NamedExpression> originOutput, PruneContext context) {
        List<NamedExpression> prunedOutputs = originOutput.stream()
                .filter(output -> context.requiredSlots.contains(output.toSlot()))
                .distinct()
                .collect(ImmutableList.toImmutableList());

        if (prunedOutputs.isEmpty()) {
            NamedExpression minimumColumn = ExpressionUtils.selectMinimumColumn(originOutput);
            prunedOutputs = ImmutableList.of(minimumColumn);
        }

        return prunedOutputs.equals(originOutput)
                ? Optional.empty()
                : Optional.of(prunedOutputs);
    }

    private final <P extends Plan> P pruneChildren(P plan) {
        return pruneChildren(plan, ImmutableSet.of());
    }

    private final <P extends Plan> P pruneChildren(P plan, Set<Slot> parentRequiredSlots) {
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

        boolean isProject = plan instanceof LogicalProject;
        List<Plan> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            Set<Slot> childOutputSet = child.getOutputSet();
            SetView<Slot> childRequiredSlots = Sets.intersection(childrenRequiredSlots, childOutputSet);
            Plan prunedChild = child.accept(this, new PruneContext(childRequiredSlots, plan));

            // the case 2 in the class comment, prune child's output failed
            if (!isProject && prunedChild.getOutputSet().size() > childRequiredSlots.size()) {
                if (childRequiredSlots.isEmpty()) {
                    Slot minimumColumn = ExpressionUtils.selectMinimumColumn(childOutputSet);
                    prunedChild = new LogicalProject<>(ImmutableList.of(minimumColumn), prunedChild);
                } else {
                    prunedChild = new LogicalProject<>(ImmutableList.copyOf(childRequiredSlots), prunedChild);
                }
            }

            if (prunedChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(prunedChild);
        }
        return hasNewChildren ? (P) plan.withChildren(newChildren) : plan;
    }

    /** PruneContext */
    public static class PruneContext {
        Set<Slot> requiredSlots;
        Optional<Plan> parent;

        public PruneContext(Set<Slot> requiredSlots, Plan parent) {
            this.requiredSlots = requiredSlots;
            this.parent = Optional.ofNullable(parent);
        }
    }
}
