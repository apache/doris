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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.nereids.rules.rewrite.ColumnPruning.PruneContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.logical.OutputPrunable;
import org.apache.doris.nereids.trees.plans.logical.ProjectMergeable;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.BitSet;
import java.util.LinkedHashSet;
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
    private JobContext jobContext;
    private boolean skipInnerCheck;
    private boolean alreadyCheckedPrivileges;

    /**
     * collect all columns used in expressions, which should not be pruned
     * the purpose to collect keys are:
     * 1. used for count(*), '*' is replaced by the smallest(data type in byte size) column
     * 2. for StatsDerive, only when col-stats of keys are not available, we fall back to no-stats algorithm
     */
    public static class KeyColumnCollector {
        /** collect */
        public static Set<Slot> collect(Plan plan) {
            Set<Slot> keys = Sets.newLinkedHashSet();
            plan.foreachUp(p -> {
                if (p instanceof LogicalAggregate) {
                    LogicalAggregate<?> agg = (LogicalAggregate) p;
                    for (Expression expression : agg.getExpressions()) {
                        if (expression instanceof SlotReference) {
                            keys.add((Slot) expression);
                        } else {
                            keys.addAll(expression.getInputSlots());
                        }
                    }
                } else {
                    for (Expression expression : plan.getExpressions()) {
                        if (!(expression instanceof SlotReference)) {
                            keys.addAll(expression.getInputSlots());
                        }
                    }
                }
            });
            return keys;
        }
    }

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        this.jobContext = jobContext;
        StatementContext statementContext = jobContext.getCascadesContext().getStatementContext();
        this.alreadyCheckedPrivileges = statementContext.isPrivChecked();
        try {
            keys = KeyColumnCollector.collect(plan);
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

            return plan.accept(this, new PruneContext(null, plan.getOutputExprIdBitSet(), ImmutableList.of(), true));
        } finally {
            if (!alreadyCheckedPrivileges) {
                statementContext.setPrivChecked(true);
            }
        }
    }

    // @Override
    // public Plan visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation, PruneContext context) {
    //     List<Slot> output = catalogRelation.getOutput();
    //     ImmutableList.Builder<Slot> prunedOutputBuilder = ImmutableList.builderWithExpectedSize(output.size());
    //     for (Slot slot : output) {
    //         if (context.requiredSlotsIds.get(slot.getExprId().asInt())) {
    //             prunedOutputBuilder.add(slot);
    //         } else if (slot instanceof SlotReference && !((SlotReference) slot).isVisible()) {
    //             prunedOutputBuilder.add(slot);
    //         }
    //     }
    //     List<Slot> prunedOutput = prunedOutputBuilder.build();
    //     DataTrait trait = catalogRelation.getLogicalProperties().getTrait();
    //     LogicalProperties prunedProperties = new LogicalProperties(() -> prunedOutput, () -> trait);
    //     return catalogRelation.withGroupExprLogicalPropChildren(
    //             Optional.empty(), Optional.of(prunedProperties), catalogRelation.children());
    // }

    @Override
    public Plan visit(Plan plan, PruneContext context) {
        if (plan instanceof OutputPrunable) {
            // the case 1 in the class comment
            // two steps: prune current output and prune children
            if (context.needPrune) {
                OutputPrunable outputPrunable = (OutputPrunable) plan;
                plan = pruneOutput(plan, outputPrunable.getOutputs(), outputPrunable::pruneOutputs, context);
            }
            return pruneChildren(plan, new BitSet());
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
            return pruneChildren(plan, context.requiredSlotsIds);
        }
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, PruneContext context) {
        Plan child = filter.child();
        if (child instanceof LeafPlan && !(child instanceof OutputPrunable)) {
            // just traverse to check privilege
            if (!alreadyCheckedPrivileges) {
                child.accept(this, context);
            }
            return filter;
        } else {
            return super.visitLogicalFilter(filter, context);
        }
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, PruneContext context) {
        if (context.needPrune) {
            project = (LogicalProject) pruneOutput(project, project.getOutputs(), project::pruneOutputs, context);
        }
        Plan plan = ProjectMergeable.mergeContinuedProjects(project.getProjects(), project.child())
                .orElse(project);
        return pruneChildren(plan, new BitSet());
    }

    @Override
    public Plan visitLogicalView(LogicalView<? extends Plan> view, PruneContext context) {
        return withOuterCheck(
                () -> checkColumnPrivileges(view.getView(), computeUsedColumns(view, context.requiredSlotsIds)),
                () -> {
                    Plan plan = super.visitLogicalView(view, context);
                    while (plan instanceof LogicalView) {
                        plan = plan.child(0);
                    }
                    return plan;
                }
        );
    }

    @Override
    public Plan visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, PruneContext context) {
        TableValuedFunction tvf = tvfRelation.getFunction();

        return withOuterCheck(
                () -> tvf.checkAuth(jobContext.getCascadesContext().getConnectContext()),
                () -> super.visitLogicalTVFRelation(tvfRelation, context)
        );
    }

    @Override
    public Plan visitLogicalRelation(LogicalRelation relation, PruneContext context) {
        return withOuterCheck(
                () -> {
                    if (relation instanceof LogicalCatalogRelation) {
                        TableIf table = ((LogicalCatalogRelation) relation).getTable();
                        checkColumnPrivileges(table, computeUsedColumns(relation, context.requiredSlotsIds));
                    }
                },
                () -> super.visitLogicalRelation(relation, context)
        );
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

            BitSet prunedChildOutputExprIds = new BitSet();
            Builder<SlotReference> prunedChildOutputBuilder
                    = ImmutableList.builderWithExpectedSize(regularChildOutputs.size());
            for (Integer index : prunedOutputIndexes) {
                SlotReference slot = regularChildOutputs.get(index);
                prunedChildOutputBuilder.add(slot);
                prunedChildOutputExprIds.set(slot.getExprId().asInt());
            }

            List<SlotReference> prunedChildOutput = prunedChildOutputBuilder.build();
            Plan prunedChild = doPruneChild(
                    prunedOutputUnion, prunedOutputUnion.child(i), prunedChildOutputExprIds,
                    prunedChildOutput, true
            );
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
        return pruneChildren(logicalSink, context.requiredSlotsIds);
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
            if (context.requiredSlotsIds.get(windowExpression.getExprId().asInt())) {
                reservedWindowExpressions.add(windowExpression);
                reserved = true;
            } else {
                pruned = true;
            }
        }
        if (!pruned) {
            return pruneChildren(window, context.requiredSlotsIds);
        }
        if (!reserved) {
            return window.child().accept(this, context);
        }
        LogicalWindow<? extends Plan> prunedWindow
                = window.withExpressionsAndChild(reservedWindowExpressions.build(), window.child());
        return pruneChildren(prunedWindow, context.requiredSlotsIds);
    }

    private Plan pruneAggregate(Aggregate<?> agg, PruneContext context) {
        Aggregate<? extends Plan> prunedOutputAgg = pruneOutput(agg, agg.getOutputs(), agg::pruneOutputs, context);
        Aggregate<?> fillUpAggregate = prunedOutputAgg == agg
                ? prunedOutputAgg
                : fillUpGroupByKeysToOutput(prunedOutputAgg); // we will not prune the group by keys in the output
        return pruneChildren(fillUpAggregate, new BitSet());
    }

    private Plan skipPruneThisAndFirstLevelChildren(Plan plan) {
        return pruneChildren(plan, plan.getChildrenOutputExprIdBitSet());
    }

    // some rules want to match the aggregate which contains all the group by keys and aggregate functions
    // in the output of LogicalAggregate, so here we not to prune the group by keys in the output of aggregate,
    // for example:
    //  PushDownAggThroughJoinOnPkFk:
    //      logicalAggregate(logicalJoin()) -> logicalAggregate(logicalJoin(logicalAggregate(), any())
    // the transform condition of PushDownAggThroughJoinOnPkFk contains: the bottom LogicalAggregate should output
    // all the slots used in the LogicalJoin, if we prune the group by keys in the output of the top LogicalAggregate,
    // the LogicalJoin can not see the join keys which provided by group by keys in the LogicalAggregate and
    // give up to optimize this case
    private static Aggregate<? extends Plan> fillUpGroupByKeysToOutput(Aggregate<? extends Plan> prunedOutputAgg) {
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
                Utils.filterImmutableList(originOutput,
                        output -> context.requiredSlotsIds.get(output.getExprId().asInt())
                );

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
        List<List<SlotReference>> regularChildrenOutputs = union.getRegularChildrenOutputs();
        List<Plan> children = union.children();
        List<Integer> extractColumnIndex = Lists.newArrayList();
        for (int i = 0; i < originOutput.size(); i++) {
            NamedExpression output = originOutput.get(i);
            if (context.requiredSlotsIds.get(output.getExprId().asInt())) {
                prunedOutputs.add(output);
                extractColumnIndex.add(i);
            }
        }

        ImmutableList.Builder<List<NamedExpression>> prunedConstantExprsList
                = ImmutableList.builderWithExpectedSize(constantExprsList.size());
        if (prunedOutputs.isEmpty()) {
            // process prune all columns
            NamedExpression originSlot = originOutput.get(0);
            prunedOutputs = ImmutableList.of(new SlotReference(originSlot.getExprId(), originSlot.getName(),
                    TinyIntType.INSTANCE, false, originSlot.getQualifier()));
            regularChildrenOutputs = Lists.newArrayListWithCapacity(regularChildrenOutputs.size());
            children = Lists.newArrayListWithCapacity(children.size());
            for (int i = 0; i < union.getArity(); i++) {
                Plan child = union.child(i);
                List<NamedExpression> newProjectOutput = ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1)));
                LogicalProject<?> project;
                if (child instanceof LogicalProject) {
                    LogicalProject<Plan> childProject = (LogicalProject<Plan>) child;
                    List<NamedExpression> mergeProjections = PlanUtils.mergeProjections(
                            childProject.getProjects(), newProjectOutput);
                    project = new LogicalProject<>(mergeProjections, childProject.child());
                } else {
                    project = new LogicalProject<>(newProjectOutput, child);
                }
                regularChildrenOutputs.add((List) project.getOutput());
                children.add(project);
            }
            for (int i = 0; i < constantExprsList.size(); i++) {
                prunedConstantExprsList.add(ImmutableList.of(new Alias(new TinyIntLiteral((byte) 1))));
            }
        } else {
            int len = extractColumnIndex.size();
            for (List<NamedExpression> row : constantExprsList) {
                ImmutableList.Builder<NamedExpression> newRow = ImmutableList.builderWithExpectedSize(len);
                for (int idx : extractColumnIndex) {
                    newRow.add(row.get(idx));
                }
                prunedConstantExprsList.add(newRow.build());
            }
        }

        if (prunedOutputs.equals(originOutput) && !context.requiredSlotsIds.isEmpty()) {
            return union;
        } else {
            return union.withNewOutputsChildrenAndConstExprsList(prunedOutputs, children,
                    regularChildrenOutputs, prunedConstantExprsList.build());
        }
    }

    private <P extends Plan> P pruneChildren(P plan, BitSet parentRequiredSlotIds) {
        if (plan.arity() == 0) {
            // leaf
            return plan;
        }

        BitSet childrenRequiredSlotIds = (BitSet) parentRequiredSlotIds.clone();
        for (Expression expression : plan.getExpressions()) {
            expression.foreach(e -> {
                if (e instanceof Slot) {
                    childrenRequiredSlotIds.set(((Slot) e).getExprId().asInt());
                }
            });
        }

        ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            List<Slot> childOutputs = child.getOutput();
            BitSet childRequiredSlotIds = new BitSet();
            ImmutableList.Builder<Slot> childRequiredSlotBuilder
                    = ImmutableList.builderWithExpectedSize(childOutputs.size());
            boolean needPrune = false;
            for (Slot childOutput : childOutputs) {
                int id = childOutput.getExprId().asInt();
                if (childrenRequiredSlotIds.get(id)) {
                    childRequiredSlotIds.set(id);
                    childRequiredSlotBuilder.add(childOutput);
                } else {
                    needPrune = true;
                }
            }
            Plan prunedChild = doPruneChild(
                    plan, child, childRequiredSlotIds, childRequiredSlotBuilder.build(), needPrune
            );
            if (prunedChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(prunedChild);
        }
        return hasNewChildren ? (P) plan.withChildren(newChildren.build()) : plan;
    }

    private Plan doPruneChild(Plan plan, Plan child, BitSet childRequiredSlotIds,
            List<? extends Slot> childRequiredSlots, boolean needPrune) {
        Plan prunedChild = child.accept(this,
                new PruneContext(plan, childRequiredSlotIds, childRequiredSlots, needPrune));
        // the case 2 in the class comment, prune child's output failed, whatever the child is a OutputPrunable,
        // because some OutputPrunable may not prune outputs, for example, LogicalAggregate will not prune the
        // group by keys in the output
        if (!(plan instanceof Project)) {
            prunedChild = newProjectIfNotPruned(prunedChild, childRequiredSlotIds, childRequiredSlots);
        }
        return prunedChild;
    }

    private Plan newProjectIfNotPruned(
            Plan prunedChild, BitSet childRequiredSlotIds, List<? extends Slot> childRequiredSlots) {
        if (childRequiredSlots.isEmpty()) {
            return prunedChild;
        }
        for (Slot prunedChildOutput : prunedChild.getOutput()) {
            if (!childRequiredSlotIds.get(prunedChildOutput.getExprId().asInt())) {
                prunedChild = new LogicalProject<>((List) childRequiredSlots, prunedChild);
                break;
            }
        }
        return prunedChild;
    }

    /** PruneContext */
    public static class PruneContext {
        public BitSet requiredSlotsIds;
        public Optional<Plan> parent;
        public List<? extends Slot> childRequiredSlots;
        public boolean needPrune;

        public PruneContext(
                Plan parent, BitSet requiredSlotsIds, List<? extends Slot> childRequiredSlots, boolean needPrune) {
            this.parent = Optional.ofNullable(parent);
            this.childRequiredSlots = childRequiredSlots;
            this.requiredSlotsIds = requiredSlotsIds;
            this.needPrune = needPrune;
        }
    }

    private Set<String> computeUsedColumns(Plan plan, BitSet requiredSlotsIds) {
        Set<String> usedColumnNames = new LinkedHashSet<>();
        for (Slot outputSlot : plan.getOutput()) {
            if (!requiredSlotsIds.get(outputSlot.getExprId().asInt())) {
                continue;
            }
            // don't check privilege for hidden column, e.g. __DORIS_DELETE_SIGN__
            if (outputSlot instanceof SlotReference && ((SlotReference) outputSlot).getColumn().isPresent()
                    && !((SlotReference) outputSlot).getColumn().get().isVisible()) {
                continue;
            }
            usedColumnNames.add(outputSlot.getName());
        }
        return usedColumnNames;
    }

    private void checkColumnPrivileges(TableIf table, Set<String> usedColumns) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        ConnectContext connectContext = cascadesContext.getConnectContext();
        try {
            UserAuthentication.checkPermission(table, connectContext, usedColumns);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        StatementContext statementContext = cascadesContext.getStatementContext();
        Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
        if (sqlCacheContext.isPresent()) {
            sqlCacheContext.get().addCheckPrivilegeTablesOrViews(table, usedColumns);
        }
    }

    private <T> T withOuterCheck(Runnable check, Supplier<T> traverse) {
        if (alreadyCheckedPrivileges || skipInnerCheck) {
            return traverse.get();
        } else {
            try {
                skipInnerCheck = true;
                check.run();
                return traverse.get();
            } finally {
                skipInnerCheck = false;
            }
        }
    }
}
