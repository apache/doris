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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds the delta plan of an append-only detail IVM MV.
 *
 * <p>Such an MV holds the query's own rows, and its base tables only ever append, so the increment of a
 * window is the rows those tables appended, joined and projected exactly as the query joins and projects
 * them. Every row the plan produces is a row to insert: nothing here carries a sign, a sequence, or a
 * row-id, because there is no retraction to express, no order to keep, and no row of the MV to reach.
 * That is why the whole rewrite returns a plan rather than the plan-plus-delta-metadata a unique-key MV's
 * delta needs, and why one class is enough for it.
 *
 * <p>What it accepts mirrors what {@link IvmNormalizeDupKeysMTMV} accepts, and what it rejects is what
 * would need the unique-key layout: an aggregate changes the row of a group, and a GROUPING SETS repeat,
 * an outer join, or a UNION DISTINCT retracts or folds rows the MV already holds. The normalizer rejects
 * those before any refresh runs, so the rejections here are the other half of the same statement: a plan
 * that reaches this rewriter and needs the row-id path is a plan the normalizer should not have accepted.
 */
class IvmDupDeltaRewriter extends PlanVisitor<Optional<Plan>, Void> {
    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;
    private final IvmDeltaRewriteState rewriteState;

    private IvmDupDeltaRewriter(IvmDeltaRewriteState rewriteState) {
        this.rewriteState = rewriteState;
    }

    /**
     * Builds the delta plan of {@code queryPlan}, already projected into the shape the sink writes, or
     * empty when no base table has anything to append.
     */
    static Optional<Plan> generateIncrRefreshPlan(Plan queryPlan, IvmIncrRefreshContext ctx,
            Map<BaseTableInfo, Set<Long>> scopePartitionIds) {
        IvmDeltaRewriteState rewriteState = IvmDeltaRewriteState.forAppendOnlyDup(queryPlan, ctx, scopePartitionIds);
        Optional<Plan> deltaPlan = queryPlan.accept(new IvmDupDeltaRewriter(rewriteState), null);
        return deltaPlan.map(plan -> IvmDeltaRewriteHelper.INSTANCE.finalizeAppendOnlyDupQuery(
                plan, ctx.getMtmv()));
    }

    @Override
    public Optional<Plan> visit(Plan plan, Void context) {
        // Every node type not accepted below needs the row-id path the detail model has no room for.
        throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                "IVM append-only detail MV delta rewrite does not support: " + plan.getClass().getSimpleName());
    }

    @Override
    public Optional<Plan> visitLogicalOlapScan(LogicalOlapScan scan, Void context) {
        // The rows this table appended in the window, in the shape the query reads the table in.
        return rewriteState.createDeltaScan(scan).map(deltaScan -> new LogicalProject<>(
                helper.remapDeltaScanOutputs(scan, deltaScan), deltaScan));
    }

    /** A one-row relation is a static input of a refresh: it appends nothing. */
    @Override
    public Optional<Plan> visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, Void context) {
        return Optional.empty();
    }

    @Override
    public Optional<Plan> visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        return filter.child().accept(this, context)
                .map(delta -> filter.withChildren(ImmutableList.of(delta)));
    }

    @Override
    public Optional<Plan> visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias, Void context) {
        return alias.child().accept(this, context)
                .map(delta -> alias.withChildren(ImmutableList.of(delta)));
    }

    @Override
    public Optional<Plan> visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        Optional<Plan> childDelta = project.child().accept(this, context);
        if (!childDelta.isPresent()) {
            return Optional.empty();
        }
        Plan delta = childDelta.get();
        Map<Slot, Slot> childOutputMapping = new HashMap<>();
        for (int i = 0; i < project.child().getOutput().size(); i++) {
            childOutputMapping.put(project.child().getOutput().get(i), delta.getOutput().get(i));
        }
        ImmutableList.Builder<NamedExpression> newOutputs =
                ImmutableList.builderWithExpectedSize(project.getProjects().size());
        for (NamedExpression output : project.getProjects()) {
            for (Slot inputSlot : output.getInputSlots()) {
                if (!childOutputMapping.containsKey(inputSlot)) {
                    childOutputMapping.put(inputSlot, helper.findSlotByName(delta.getOutput(), inputSlot.getName()));
                }
            }
            newOutputs.add((NamedExpression) ExpressionUtils.replace(output, childOutputMapping));
        }
        return Optional.of(project.withProjectsAndChild(newOutputs.build(), delta));
    }

    @Override
    public Optional<Plan> visitLogicalUnion(LogicalUnion union, Void context) {
        List<Plan> armDeltas = new ArrayList<>(union.children().size());
        for (int i = 0; i < union.children().size(); i++) {
            Optional<Plan> armDelta = union.child(i).accept(this, context);
            if (!armDelta.isPresent()) {
                continue;
            }
            armDeltas.add(projectArmToUnionOutputs(union, i, armDelta.get()));
        }
        return helper.combineDeltaPlans(armDeltas, union.getOutput());
    }

    @Override
    public Optional<Plan> visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        if (!join.getJoinType().isInnerOrCrossJoin()) {
            // An outer join retracts the rows its null side stopped matching, and the normalizer rejects
            // that shape before any refresh runs.
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM append-only detail MV delta rewrite does not support: " + join.getJoinType());
        }
        List<Plan> contributions = new ArrayList<>(join.children().size());
        Optional<Plan> leftDelta = join.left().accept(this, context);
        if (leftDelta.isPresent()) {
            // The left delta joins the right as it was before the window, so it matches the rows both
            // sides already agreed on.
            contributions.add(rewriteInnerJoinContribution(join, true, leftDelta.get(),
                    IvmDeltaRewriter.preSnapshot(join.right(), rewriteState)));
        }
        Optional<Plan> rightDelta = join.right().accept(this, context);
        if (rightDelta.isPresent()) {
            // The right delta joins the left as it is after the window, which is the side carrying the
            // rows the left appended in the same window.
            contributions.add(rewriteInnerJoinContribution(join, false, rightDelta.get(),
                    IvmDeltaRewriter.postSnapshot(join.left(), rewriteState)));
        }
        return helper.combineDeltaPlans(contributions, join.getOutput());
    }

    /**
     * Maps an arm's delta to the union's output shape, so the arms of the combined delta line up.
     */
    private Plan projectArmToUnionOutputs(LogicalUnion union, int armIndex, Plan armDelta) {
        List<SlotReference> childMapping = union.getRegularChildrenOutputs().get(armIndex);
        List<NamedExpression> unionOutputs = union.getOutputs();
        ImmutableList.Builder<NamedExpression> projections =
                ImmutableList.builderWithExpectedSize(unionOutputs.size());
        for (int i = 0; i < unionOutputs.size(); i++) {
            NamedExpression unionOut = unionOutputs.get(i);
            projections.add(new Alias(unionOut.getExprId(), childMapping.get(i), unionOut.getName()));
        }
        return new LogicalProject<>(projections.build(), armDelta);
    }

    /**
     * Expands one side's delta against the other side's snapshot, and projects the result back to the
     * join's own output so the two contributions combine as one relation. The join's own row-id
     * machinery the unique-key model composes here has no counterpart: a detail MV's rows are the join's
     * rows.
     */
    private Plan rewriteInnerJoinContribution(LogicalJoin<? extends Plan, ? extends Plan> join, boolean deltaOnLeft,
            Plan delta, Pair<Plan, Map<Slot, Slot>> snapshot) {
        Plan leftPlan = deltaOnLeft ? delta : snapshot.first;
        Plan rightPlan = deltaOnLeft ? snapshot.first : delta;
        Map<Slot, Slot> slotMapping = new HashMap<>();
        slotMapping.putAll(helper.mapBusinessOutputs(join.left().getOutput(), leftPlan.getOutput()));
        slotMapping.putAll(helper.mapBusinessOutputs(join.right().getOutput(), rightPlan.getOutput()));
        helper.mapJoinConjunctInputs(join, leftPlan, rightPlan, slotMapping);
        LogicalJoin<Plan, Plan> rewrittenJoin = new LogicalJoin<>(join.getJoinType(),
                ExpressionUtils.replace(join.getHashJoinConjuncts(), slotMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), slotMapping), join.getDistributeHint(),
                leftPlan, rightPlan, JoinReorderContext.EMPTY);
        ImmutableList.Builder<NamedExpression> projects =
                ImmutableList.builderWithExpectedSize(join.getOutput().size());
        for (Slot output : join.getOutput()) {
            projects.add(new Alias(output.getExprId(), slotMapping.get(output), output.getName()));
        }
        return new LogicalProject<>(projects.build(), rewrittenJoin);
    }
}
