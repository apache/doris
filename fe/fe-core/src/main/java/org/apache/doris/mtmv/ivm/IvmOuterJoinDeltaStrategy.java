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

import org.apache.doris.catalog.Column;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Delta rewrite strategy for the restricted single LEFT OUTER JOIN shape.
 *
 * <p>The strategy emits the regular joined delta for the preserved-side change:
 * <ul>
 *   <li>left-side delta: {@code delta_left LEFT JOIN right_snapshot}</li>
 *   <li>right-side delta: joined rows plus padded-null state migration</li>
 * </ul>
 */
public class IvmOuterJoinDeltaStrategy extends IvmLinearDeltaStrategy {

    private static final String RIGHT_INSERT_DELTA_ALIAS = "__DORIS_IVM_RIGHT_INSERT_DELTA__";
    private static final String RIGHT_DELETE_DELTA_ALIAS = "__DORIS_IVM_RIGHT_DELETE_DELTA__";
    private static final String RIGHT_PRE_SNAPSHOT_ALIAS = "__DORIS_IVM_RIGHT_PRE_SNAPSHOT__";
    private static final String RIGHT_POST_SNAPSHOT_ALIAS = "__DORIS_IVM_RIGHT_POST_SNAPSHOT__";

    public IvmOuterJoinDeltaStrategy(IvmRefreshContext ctx) {
        super(ctx);
    }

    @Override
    public List<Command> rewrite(Plan normalizedPlan) {
        RewriteResult result = rewritePlan(normalizedPlan);
        Plan finalPlan = buildSinkProject(result);
        return ImmutableList.of(buildInsertCommandWithDeleteSign(finalPlan));
    }

    @Override
    public RewriteResult visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        if (join.getJoinType() != JoinType.LEFT_OUTER_JOIN) {
            return super.visitLogicalJoin(join, context);
        }

        RewriteResult leftResult = join.left().accept(this, context);
        RewriteResult rightResult = join.right().accept(this, context);
        if (leftResult.dmlFactorSlot != null && rightResult.dmlFactorSlot != null) {
            throw new AnalysisException(
                    "IVM: both sides of left outer join have dml_factor; expected at most one delta side");
        }
        if (leftResult.dmlFactorSlot == null && rightResult.dmlFactorSlot == null) {
            return new RewriteResult(join.withChildren(leftResult.plan, rightResult.plan), null);
        }
        if (leftResult.dmlFactorSlot != null) {
            return rewritePreservedSideDelta(join, leftResult, rightResult);
        } else {
            return rewriteNullableSideDelta(join, leftResult, rightResult);
        }
    }

    private RewriteResult rewritePreservedSideDelta(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult) {
        LogicalJoin<Plan, Plan> newJoin = (LogicalJoin<Plan, Plan>) join.withChildren(
                leftResult.plan, rightResult.plan);
        return addNonDetGuardForJoinDelta(newJoin, leftResult, rightResult);
    }

    private RewriteResult rewriteNullableSideDelta(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult) {
        // Nullable-side delta for:
        //   left_snapshot LEFT JOIN right_delta
        //
        // It has three parts:
        //   1. Bare joined rows:
        //        left_snapshot INNER JOIN right_delta
        //
        //   2. Remove old pad-null rows when right inserts create the first match:
        //        left_snapshot LEFT SEMI JOIN right_insert_delta
        //          LEFT ANTI JOIN right_pre_snapshot
        //      The semi join keeps preserved-side rows affected by this delta without
        //      multiplying them by matched delta rows. The anti join then keeps only rows
        //      that had no matching right row before this delta. For those rows, the old MV
        //      contained one row with right columns padded to NULL, so we emit that
        //      NULL-padded row with dml_factor = -1.
        //
        //   3. Add new pad-null rows when right deletes remove the last match:
        //        left_snapshot LEFT SEMI JOIN right_delete_delta
        //          LEFT ANTI JOIN right_post_snapshot
        //      The semi join keeps preserved-side rows affected by this delta without
        //      multiplying them by matched delta rows. The anti join then keeps only rows
        //      that have no matching right row after this delta. For those rows, the new MV
        //      needs one NULL-padded row, so we emit that row with dml_factor = +1.
        RewriteResult joinedResult = rewriteNullableSideBareJoinDelta(join, leftResult, rightResult);

        Pair<Plan, Map<Slot, Slot>> insertedRightDelta = remapOutputs(aliasPlan(freshPlan(rightResult.plan),
                RIGHT_INSERT_DELTA_ALIAS));
        Slot insertedRightDmlFactor = findSlotByName(insertedRightDelta.first.getOutput(), Column.IVM_DML_FACTOR_COL);
        Pair<Plan, Map<Slot, Slot>> deletedRightDelta = remapOutputs(aliasPlan(freshPlan(rightResult.plan),
                RIGHT_DELETE_DELTA_ALIAS));
        Slot deletedRightDmlFactor = findSlotByName(deletedRightDelta.first.getOutput(), Column.IVM_DML_FACTOR_COL);
        Plan rightInserts = new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(insertedRightDmlFactor, new TinyIntLiteral((byte) 0))), insertedRightDelta.first);
        Plan rightDeletes = new LogicalFilter<>(ImmutableSet.of(
                new LessThan(deletedRightDmlFactor, new TinyIntLiteral((byte) 0))), deletedRightDelta.first);
        // Build right_pre/right_post from the original nullable-side plan, not from rightResult.plan.
        // rightResult.plan may already be linearly rewritten; for example UNION ALL keeps only
        // the delta arm and prunes other snapshot arms. Pad-null repair must compare against the
        // full nullable-side relation, so preserve all branches and only replace the one delta scan
        // with its pre/post snapshot.
        Pair<Plan, Map<Slot, Slot>> rightPreSnapshot = remapOutputs(aliasPlan(
                freshPlan(copyDeltaScanAsSnapshot(join.right(), false)), RIGHT_PRE_SNAPSHOT_ALIAS));
        Pair<Plan, Map<Slot, Slot>> rightPostSnapshot = remapOutputs(aliasPlan(
                freshPlan(copyDeltaScanAsSnapshot(join.right(), true)), RIGHT_POST_SNAPSHOT_ALIAS));
        Plan joinedProject = remapOutputs(joinedResult.plan).first;
        LogicalProject<Plan> preNullProject = buildPaddedNullRepairProject(join,
                remapOutputs(freshPlan(leftResult.plan)), insertedRightDelta.second,
                rightInserts, rightPreSnapshot, new TinyIntLiteral((byte) -1));
        LogicalProject<Plan> postNullProject = buildPaddedNullRepairProject(join,
                remapOutputs(freshPlan(leftResult.plan)), deletedRightDelta.second,
                rightDeletes, rightPostSnapshot, new TinyIntLiteral((byte) 1));

        LogicalUnion union = buildUnionAll(joinedProject, preNullProject, postNullProject);
        LogicalProject<Plan> outputProject = projectUnionOutputs(union, joinedResult.plan.getOutput());
        Slot dmlFactor = findSlotByName(outputProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        return new RewriteResult(outputProject, dmlFactor);
    }

    private RewriteResult rewriteNullableSideBareJoinDelta(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult) {
        LogicalJoin<Plan, Plan> innerJoin = join.withTypeChildren(JoinType.INNER_JOIN,
                leftResult.plan, rightResult.plan, JoinReorderContext.EMPTY);
        return new RewriteResult(innerJoin, rightResult.dmlFactorSlot);
    }

    private LogicalProject<Plan> buildPaddedNullRepairProject(LogicalJoin<? extends Plan, ? extends Plan> join,
            Pair<Plan, Map<Slot, Slot>> leftSnapshot, Map<Slot, Slot> rightDeltaMapping, Plan rightDelta,
            Pair<Plan, Map<Slot, Slot>> rightSnapshot, Expression dmlFactor) {
        Map<Slot, Slot> candidateMapping = ImmutableMap.<Slot, Slot>builder()
                .putAll(leftSnapshot.second)
                .putAll(rightDeltaMapping)
                .buildKeepingLast();
        LogicalJoin<Plan, Plan> candidateJoin = new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN,
                ExpressionUtils.replace(join.getHashJoinConjuncts(), candidateMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), candidateMapping), join.getDistributeHint(),
                leftSnapshot.first, rightDelta, JoinReorderContext.EMPTY);
        Map<Slot, Slot> antiJoinMapping = ImmutableMap.<Slot, Slot>builder()
                .putAll(leftSnapshot.second)
                .putAll(rightSnapshot.second)
                .buildKeepingLast();
        LogicalJoin<Plan, Plan> antiJoin = new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN,
                ExpressionUtils.replace(join.getHashJoinConjuncts(), antiJoinMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), antiJoinMapping), join.getDistributeHint(),
                candidateJoin, rightSnapshot.first, JoinReorderContext.EMPTY);
        return projectPaddedNullOutputs(join, antiJoin, dmlFactor, leftSnapshot.second);
    }

    private LogicalProject<Plan> projectPaddedNullOutputs(LogicalJoin<? extends Plan, ? extends Plan> join,
            Plan source, Expression dmlFactor, Map<Slot, Slot> leftOutputMapping) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        Map<Slot, Expression> leftSourceSlots = new HashMap<>();
        for (Slot slot : source.getOutput()) {
            leftSourceSlots.put(slot, slot);
        }
        Slot leftRowId = IvmUtil.findRowIdSlot(join.left().getOutput(), "left child of left outer join");
        Slot rightRowId = IvmUtil.findRowIdSlot(join.right().getOutput(), "right child of left outer join");
        for (Slot slot : join.getOutput()) {
            if (slot.equals(leftRowId)) {
                projects.add(new Alias(resolveLeftOutput(slot, leftOutputMapping, leftSourceSlots), slot.getName()));
            } else if (slot.equals(rightRowId)) {
                // The nullable side has no matching row, so the parent normalize Project computes
                // hash(leftRowId, NULL) as the final MV row id for this padded-null repair row.
                projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
            } else if (join.left().getOutputSet().contains(slot)) {
                projects.add(new Alias(resolveLeftOutput(slot, leftOutputMapping, leftSourceSlots), slot.getName()));
            } else if (join.right().getOutputSet().contains(slot)) {
                projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
            } else {
                throw new AnalysisException("IVM left outer join rewrite found unknown output slot: " + slot);
            }
        }
        projects.add(new Alias(dmlFactor, Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(projects.build(), (LogicalPlan) source);
    }

    private Expression resolveLeftOutput(Slot slot, Map<Slot, Slot> leftOutputMapping,
            Map<Slot, Expression> leftSourceSlots) {
        Expression expr = leftOutputMapping.get(slot);
        if (expr == null) {
            expr = leftSourceSlots.get(slot);
        }
        if (expr == null) {
            throw new AnalysisException("IVM left outer join rewrite lost left output slot: " + slot);
        }
        return expr;
    }

    private Pair<Plan, Map<Slot, Slot>> remapOutputs(Plan plan) {
        Map<Slot, Slot> identityMapping = new HashMap<>();
        for (Slot slot : plan.getOutput()) {
            identityMapping.put(slot, slot);
        }
        return remapOutputs(plan, identityMapping);
    }

    private Pair<Plan, Map<Slot, Slot>> remapOutputs(Pair<Plan, Map<Slot, Slot>> plan) {
        return remapOutputs(plan.first, plan.second);
    }

    private Pair<Plan, Map<Slot, Slot>> remapOutputs(Plan plan, Map<Slot, Slot> sourceToPlanOutput) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                plan.getOutput().size());
        Map<Slot, Slot> planOutputToAlias = new HashMap<>();
        Map<Slot, Slot> outputMapping = new HashMap<>();
        for (Slot slot : plan.getOutput()) {
            Alias alias = new Alias(slot, slot.getName());
            projects.add(alias);
            planOutputToAlias.put(slot, alias.toSlot());
        }
        for (Map.Entry<Slot, Slot> entry : sourceToPlanOutput.entrySet()) {
            outputMapping.put(entry.getKey(), planOutputToAlias.get(entry.getValue()));
        }
        LogicalProject<Plan> project = new LogicalProject<>(projects.build(), (LogicalPlan) plan);
        return Pair.of(project, outputMapping);
    }

    private Pair<Plan, Map<Slot, Slot>> aliasPlan(Pair<Plan, Map<Slot, Slot>> plan, String alias) {
        // The nullable-side repair plan uses several copies of the same right child
        // (delta/pre/post). Nereids rejects multiple raw scans of the same table name
        // during binding, so wrap each internal copy with a unique alias. This is only
        // an internal disambiguation node; user-authored subquery aliases are still
        // rejected by IVM normalize until that plan shape is supported.
        Plan aliasNode = new LogicalSubQueryAlias<>(alias, plan.first);
        return Pair.of(aliasNode, remapOutputMapping(plan.second, plan.first.getOutput(), aliasNode.getOutput()));
    }

    private LogicalUnion buildUnionAll(Plan first, Plan second, Plan third) {
        List<Plan> children = ImmutableList.of(first, second, third);
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (int i = 0; i < first.getOutput().size(); i++) {
            Slot slot = first.getOutput().get(i);
            outputs.add(new SlotReference(slot.getName(), slot.getDataType(), unionOutputNullable(children, i)));
        }
        ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList.builder();
        for (Plan child : children) {
            ImmutableList.Builder<SlotReference> childOutput = ImmutableList.builder();
            for (Slot slot : child.getOutput()) {
                childOutput.add((SlotReference) slot);
            }
            childrenOutputs.add(childOutput.build());
        }
        return new LogicalUnion(Qualifier.ALL, outputs.build(), childrenOutputs.build(),
                ImmutableList.of(), false, children);
    }

    private boolean unionOutputNullable(List<Plan> children, int index) {
        for (Plan child : children) {
            if (child.getOutput().get(index).nullable()) {
                return true;
            }
        }
        return false;
    }

    private LogicalProject<Plan> projectUnionOutputs(LogicalUnion union, List<Slot> targetOutputs) {
        if (union.getOutput().size() != targetOutputs.size()) {
            throw new AnalysisException("IVM left outer join rewrite changed union output size from "
                    + targetOutputs.size() + " to " + union.getOutput().size());
        }
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                targetOutputs.size());
        for (int i = 0; i < targetOutputs.size(); i++) {
            Slot source = union.getOutput().get(i);
            Slot target = targetOutputs.get(i);
            projects.add(new Alias(target.getExprId(), source, target.getName()));
        }
        return new LogicalProject<>(projects.build(), union);
    }

    private Plan copyDeltaScanAsSnapshot(Plan plan, boolean postSnapshot) {
        List<Long> missingTableIds = new ArrayList<>();
        int[] deltaScanCount = new int[1];
        Plan snapshot = plan.rewriteDownShortCircuit(node -> {
            if (!(node instanceof LogicalOlapScan)) {
                return node;
            }
            LogicalOlapScan scan = (LogicalOlapScan) node;
            if (!scan.isDelta()) {
                return node;
            }
            deltaScanCount[0]++;
            IvmStreamRef ref = ctx.getBaseTableStream(scan);
            if (ref == null) {
                missingTableIds.add(scan.getTable().getId());
                return node;
            }
            long tso = postSnapshot ? ref.getLatestTso() : ref.getConsumedTso();
            return scan.withIsDelta(false).withTso(tso);
        });
        if (!missingTableIds.isEmpty()) {
            throw new AnalysisException("IVM: no stream ref found for base table id: " + missingTableIds.get(0));
        }
        if (deltaScanCount[0] != 1) {
            throw new AnalysisException("IVM: expected exactly one nullable-side delta scan, got " + deltaScanCount[0]);
        }
        return snapshot;
    }

    private Pair<Plan, Map<Slot, Slot>> freshPlan(Plan plan) {
        DeepCopierContext copierContext = new DeepCopierContext();
        LogicalPlan freshPlan = LogicalPlanDeepCopier.INSTANCE.deepCopy((LogicalPlan) plan, copierContext);
        return Pair.of(freshPlan, mapCopiedOutputs(plan.getOutput(), freshPlan.getOutput(), copierContext));
    }

    private Map<Slot, Slot> mapCopiedOutputs(List<Slot> sourceOutput, List<Slot> targetOutput,
            DeepCopierContext copierContext) {
        Map<ExprId, Slot> targetOutputByExprId = new HashMap<>();
        for (Slot slot : targetOutput) {
            targetOutputByExprId.put(slot.getExprId(), slot);
        }
        Map<Slot, Slot> outputMapping = new HashMap<>();
        for (Slot sourceSlot : sourceOutput) {
            ExprId copiedExprId = copierContext.exprIdReplaceMap.get(sourceSlot.getExprId());
            Slot targetSlot = copiedExprId == null ? null : targetOutputByExprId.get(copiedExprId);
            if (targetSlot == null) {
                throw new AnalysisException("IVM left outer join rewrite lost copied output slot: " + sourceSlot);
            }
            outputMapping.put(sourceSlot, targetSlot);
        }
        return outputMapping;
    }

    private Map<Slot, Slot> mapOutputs(List<Slot> sourceOutput, List<Slot> targetOutput) {
        if (sourceOutput.size() != targetOutput.size()) {
            throw new AnalysisException("IVM left outer join rewrite changed output size from "
                    + sourceOutput.size() + " to " + targetOutput.size());
        }
        Map<Slot, Slot> outputMapping = new HashMap<>();
        for (int i = 0; i < sourceOutput.size(); i++) {
            outputMapping.put(sourceOutput.get(i), targetOutput.get(i));
        }
        return outputMapping;
    }

    private Map<Slot, Slot> remapOutputMapping(Map<Slot, Slot> sourceToOldOutput,
            List<Slot> oldOutput, List<Slot> newOutput) {
        Map<Slot, Slot> oldToNew = mapOutputs(oldOutput, newOutput);
        Map<Slot, Slot> sourceToNewOutput = new HashMap<>();
        for (Map.Entry<Slot, Slot> entry : sourceToOldOutput.entrySet()) {
            sourceToNewOutput.put(entry.getKey(), oldToNew.get(entry.getValue()));
        }
        return sourceToNewOutput;
    }

}
