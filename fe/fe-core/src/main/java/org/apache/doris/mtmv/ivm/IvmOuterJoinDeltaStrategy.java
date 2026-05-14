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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Delta rewrite strategy for the restricted LEFT OUTER JOIN topology.
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
    private static final String RIGHT_DETAIL_DELTA_ALIAS = "__DORIS_IVM_RIGHT_DETAIL_DELTA__";
    private static final String RIGHT_EVENT_KEY_PREFIX = "__DORIS_IVM_RIGHT_EVENT_KEY_";
    private static final String RIGHT_KEY_DELTA_ALIAS = "__DORIS_IVM_RIGHT_KEY_DELTA__";
    private static final String RIGHT_KEY_POSITIVE_ALIAS = "__DORIS_IVM_RIGHT_KEY_POSITIVE__";
    private static final String RIGHT_KEY_NEGATIVE_ALIAS = "__DORIS_IVM_RIGHT_KEY_NEGATIVE__";
    private static final String RIGHT_PRE_SNAPSHOT_ALIAS = "__DORIS_IVM_RIGHT_PRE_SNAPSHOT__";
    private static final String RIGHT_POST_SNAPSHOT_ALIAS = "__DORIS_IVM_RIGHT_POST_SNAPSHOT__";

    public IvmOuterJoinDeltaStrategy(IvmRefreshContext ctx) {
        super(ctx);
    }

    /**
     * Dispatch a normalized LEFT OUTER JOIN by checking which side carries the base-table delta.
     */
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

    /**
     * Delta from the preserved side does not need pad-null repair; the original left outer join shape is enough.
     */
    private RewriteResult rewritePreservedSideDelta(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult) {
        LogicalJoin<Plan, Plan> newJoin = (LogicalJoin<Plan, Plan>) join.withChildren(
                leftResult.plan, rightResult.plan);
        return addNonDetGuardForJoinDelta(newJoin, leftResult, rightResult);
    }

    /**
     * Delta from the nullable side may change both joined rows and padded-null rows.
     */
    private RewriteResult rewriteNullableSideDelta(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult) {
        EquiJoinKeys equiJoinKeys = extractEquiJoinKeys(join);
        if (equiJoinKeys != null) {
            return rewriteNullableSideDeltaWithRightEvents(join, leftResult, rightResult, equiJoinKeys);
        } else {
            return rewriteNullableSideDeltaWithRepairBranches(join, leftResult, rightResult);
        }
    }

    private RewriteResult rewriteNullableSideDeltaWithRepairBranches(LogicalJoin<? extends Plan, ? extends Plan> join,
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

        LogicalUnion union = buildUnionAll(ImmutableList.of(joinedProject, preNullProject, postNullProject));
        LogicalProject<Plan> outputProject = projectUnionOutputs(union, joinedResult.plan.getOutput());
        Slot dmlFactor = findSlotByName(outputProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        return new RewriteResult(outputProject, dmlFactor);
    }

    private RewriteResult rewriteNullableSideDeltaWithRightEvents(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult, EquiJoinKeys equiJoinKeys) {
        // Nullable-side delta for equi left outer join can be reduced to one probe:
        //   left_snapshot INNER JOIN right_events
        //
        // The important point is that right_events is not just "right_delta with
        // another name". It encodes every MV row change caused by the nullable side
        // into rows that can be joined by key with the preserved-side snapshot.
        // After that encoding, left_snapshot does not need to know whether an event
        // is a real right row change or a padded-NULL repair; it only probes once by
        // the event key and projects the event payload.
        //
        // right_events has three parts:
        //   1. Detail right delta rows:
        //        right_delta
        //      These rows keep the original right-side outputs and right delta dml_factor,
        //      so the final join emits normal joined row changes.
        //
        //   2. Remove old pad-null rows when right inserts create the first match:
        //        affected right_insert_delta keys LEFT ANTI JOIN right_pre_snapshot
        //      For those keys, the old MV contained one row with right columns padded
        //      to NULL. The event carries the join keys, pads right outputs to NULL,
        //      and uses dml_factor = -1.
        //
        //   3. Add new pad-null rows when right deletes remove the last match:
        //        affected right_delete_delta keys LEFT ANTI JOIN right_post_snapshot
        //      For those keys, the new MV needs one NULL-padded row. The event carries
        //      the join keys, pads right outputs to NULL, and uses dml_factor = +1.
        //
        // By merging the bare join and pad-null repair rows into right_events, the
        // preserved-side snapshot is scanned/probed once instead of three times.
        //
        // This requires pure deterministic equi keys. Expressions like
        //   f(left_slots) = g(right_slots)
        // are supported, because the right event relation can materialize
        // g(right_slots) as event_key and the final probe can evaluate
        // f(left_snapshot_slots) = event_key. Conditions such as
        //   left.k = right.k AND left.v > right.v
        // are not supported here, because the right side alone cannot decide which
        // left rows are affected by the non-hash predicate. Such joins fall back to
        // the three repair branches. Unique functions such as random()/uuid() are
        // also rejected before this path, because recomputing them in different
        // event branches would produce unstable keys.
        RewriteResult joinedResult = rewriteNullableSideBareJoinDelta(join, leftResult, rightResult);
        Pair<Plan, Map<Slot, Slot>> leftSnapshot = remapOutputs(freshPlan(leftResult.plan));
        RightEventPlan rightEvents = buildRightEventPlan(join, rightResult, equiJoinKeys);

        ImmutableList.Builder<Expression> hashConjuncts = ImmutableList.builderWithExpectedSize(
                equiJoinKeys.leftExpressions.size());
        for (int i = 0; i < equiJoinKeys.leftExpressions.size(); i++) {
            hashConjuncts.add(new EqualTo(
                    ExpressionUtils.replace(equiJoinKeys.leftExpressions.get(i), leftSnapshot.second),
                    rightEvents.eventKeySlots.get(i)));
        }
        LogicalJoin<Plan, Plan> eventJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                hashConjuncts.build(), ImmutableList.of(), join.getDistributeHint(),
                leftSnapshot.first, rightEvents.plan, JoinReorderContext.EMPTY);
        LogicalProject<Plan> outputProject = projectEventJoinOutputs(joinedResult.plan.getOutput(),
                eventJoin, leftSnapshot.second, rightEvents.rightOutputMapping, rightEvents.dmlFactorSlot);
        Slot dmlFactor = findSlotByName(outputProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        return new RewriteResult(outputProject, dmlFactor);
    }

    /**
     * Build the ordinary joined-row change:
     *   left_snapshot INNER JOIN right_delta
     *
     * This is shared by both nullable-side strategies. The dml factor comes from the nullable-side delta.
     */
    private RewriteResult rewriteNullableSideBareJoinDelta(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult) {
        LogicalJoin<Plan, Plan> innerJoin = join.withTypeChildren(JoinType.INNER_JOIN,
                leftResult.plan, rightResult.plan, JoinReorderContext.EMPTY);
        return new RewriteResult(innerJoin, rightResult.dmlFactorSlot);
    }

    /**
     * Build one pad-null repair branch:
     *   left_snapshot LEFT SEMI JOIN right_delta
     *     LEFT ANTI JOIN right_snapshot
     *
     * The semi join finds preserved-side rows affected by the nullable-side delta. The anti join keeps only rows
     * whose match existence changed across the snapshot boundary.
     */
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

    /**
     * Project a repair branch back to the original left outer join output schema, padding nullable-side columns
     * with NULL and setting the repair dml factor.
     */
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

    /**
     * Build the nullable-side event relation consumed by the optimized one-probe rewrite.
     *
     * Output layout:
     *   right join keys, right-side value slots, dml_factor
     */
    private RightEventPlan buildRightEventPlan(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult rightResult, EquiJoinKeys equiJoinKeys) {
        Plan detailEvent = buildRightDetailEvent(rightResult, equiJoinKeys);
        Plan preNullEvent = buildRightNullEvent(join, rightResult, equiJoinKeys, false,
                new TinyIntLiteral((byte) -1));
        Plan postNullEvent = buildRightNullEvent(join, rightResult, equiJoinKeys, true,
                new TinyIntLiteral((byte) 1));
        LogicalUnion union = buildUnionAll(ImmutableList.of(detailEvent, preNullEvent, postNullEvent));

        List<Slot> unionOutputs = union.getOutput();
        Map<Slot, Slot> rightOutputMapping = new HashMap<>();
        int rightOutputStart = equiJoinKeys.rightExpressions.size();
        int rightOutputIndex = 0;
        for (Slot slot : rightValueSlots(rightResult)) {
            rightOutputMapping.put(slot, unionOutputs.get(rightOutputStart + rightOutputIndex));
            rightOutputIndex++;
        }
        List<Slot> eventKeySlots = unionOutputs.subList(0, equiJoinKeys.rightExpressions.size());
        Slot dmlFactorSlot = unionOutputs.get(unionOutputs.size() - 1);
        return new RightEventPlan(union, rightOutputMapping, eventKeySlots, dmlFactorSlot);
    }

    /**
     * Build detail events from raw nullable-side delta rows. These events produce normal joined-row changes after
     * probing the preserved-side snapshot.
     */
    private Plan buildRightDetailEvent(RewriteResult rightResult, EquiJoinKeys equiJoinKeys) {
        Pair<Plan, Map<Slot, Slot>> rightDelta = remapOutputs(aliasPlan(freshPlan(rightResult.plan),
                RIGHT_DETAIL_DELTA_ALIAS));
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        for (int i = 0; i < equiJoinKeys.rightExpressions.size(); i++) {
            projects.add(new Alias(ExpressionUtils.replace(equiJoinKeys.rightExpressions.get(i), rightDelta.second),
                    eventKeyName(i)));
        }
        for (Slot slot : rightValueSlots(rightResult)) {
            projects.add(new Alias(rightDelta.second.get(slot), slot.getName()));
        }
        projects.add(new Alias(rightDelta.second.get(rightResult.dmlFactorSlot), Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(projects.build(), (LogicalPlan) rightDelta.first);
    }

    /**
     * Build one pad-null event branch for affected nullable-side keys.
     *
     * preSnapshot branch: right inserts with no pre-existing right match emit dml_factor = -1.
     * postSnapshot branch: right deletes with no remaining right match emit dml_factor = +1.
     */
    private Plan buildRightNullEvent(LogicalJoin<? extends Plan, ? extends Plan> join, RewriteResult rightResult,
            EquiJoinKeys equiJoinKeys, boolean postSnapshot, Expression dmlFactor) {
        RightDeltaKeyPlan deltaKeys = buildRightDeltaKeyPlan(rightResult, equiJoinKeys);
        Slot flagSlot = postSnapshot ? deltaKeys.negativeSlot : deltaKeys.positiveSlot;
        Plan affectedKeys = new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(flagSlot, new TinyIntLiteral((byte) 0))), deltaKeys.plan);
        String snapshotAlias = postSnapshot ? RIGHT_POST_SNAPSHOT_ALIAS : RIGHT_PRE_SNAPSHOT_ALIAS;
        Pair<Plan, Map<Slot, Slot>> rightSnapshot = remapOutputs(aliasPlan(
                freshPlan(copyDeltaScanAsSnapshot(join.right(), postSnapshot)), snapshotAlias));

        ImmutableList.Builder<Expression> antiConjuncts = ImmutableList.builderWithExpectedSize(
                equiJoinKeys.rightExpressions.size());
        for (int i = 0; i < equiJoinKeys.rightExpressions.size(); i++) {
            antiConjuncts.add(new EqualTo(deltaKeys.keySlots.get(i),
                    ExpressionUtils.replace(equiJoinKeys.rightExpressions.get(i), rightSnapshot.second)));
        }
        LogicalJoin<Plan, Plan> antiJoin = new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN,
                antiConjuncts.build(), ImmutableList.of(), join.getDistributeHint(),
                affectedKeys, rightSnapshot.first, JoinReorderContext.EMPTY);

        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        for (int i = 0; i < deltaKeys.keySlots.size(); i++) {
            projects.add(new Alias(deltaKeys.keySlots.get(i), eventKeyName(i)));
        }
        for (Slot slot : rightValueSlots(rightResult)) {
            projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
        }
        projects.add(new Alias(dmlFactor, Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(projects.build(), antiJoin);
    }

    /**
     * Aggregate nullable-side delta rows by join key and mark whether each key has positive and/or negative delta
     * rows. Pad-null event branches use these flags to avoid multiplying one key by all matching delta rows.
     */
    private RightDeltaKeyPlan buildRightDeltaKeyPlan(RewriteResult rightResult, EquiJoinKeys equiJoinKeys) {
        Pair<Plan, Map<Slot, Slot>> rightDelta = remapOutputs(aliasPlan(freshPlan(rightResult.plan),
                RIGHT_KEY_DELTA_ALIAS));
        ImmutableList.Builder<Expression> groupBy = ImmutableList.builderWithExpectedSize(
                equiJoinKeys.rightExpressions.size());
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (int i = 0; i < equiJoinKeys.rightExpressions.size(); i++) {
            Expression key = ExpressionUtils.replace(equiJoinKeys.rightExpressions.get(i), rightDelta.second);
            groupBy.add(key);
            outputs.add(new Alias(key, eventKeyName(i)));
        }
        Slot dmlFactor = rightDelta.second.get(rightResult.dmlFactorSlot);
        outputs.add(new Alias(new Max(new If(new GreaterThan(dmlFactor, new TinyIntLiteral((byte) 0)),
                new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0))), RIGHT_KEY_POSITIVE_ALIAS));
        outputs.add(new Alias(new Max(new If(new LessThan(dmlFactor, new TinyIntLiteral((byte) 0)),
                new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0))), RIGHT_KEY_NEGATIVE_ALIAS));

        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(groupBy.build(), outputs.build(), rightDelta.first);
        List<Slot> output = aggregate.getOutput();
        return new RightDeltaKeyPlan(aggregate,
                output.subList(0, equiJoinKeys.rightExpressions.size()),
                output.get(output.size() - 2), output.get(output.size() - 1));
    }

    /**
     * Project the one-probe event join back to the same schema as the bare join result.
     */
    private LogicalProject<Plan> projectEventJoinOutputs(List<Slot> targetOutputs, Plan source,
            Map<Slot, Slot> leftOutputMapping, Map<Slot, Slot> rightOutputMapping, Slot dmlFactorSlot) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                targetOutputs.size());
        for (Slot target : targetOutputs) {
            Expression expr;
            if (Column.IVM_DML_FACTOR_COL.equals(target.getName())) {
                expr = dmlFactorSlot;
            } else {
                expr = leftOutputMapping.get(target);
                if (expr == null) {
                    expr = rightOutputMapping.get(target);
                }
            }
            if (expr == null) {
                throw new AnalysisException("IVM left outer join event rewrite lost output slot: " + target);
            }
            projects.add(new Alias(target.getExprId(), expr, target.getName()));
        }
        return new LogicalProject<>(projects.build(), (LogicalPlan) source);
    }

    /**
     * Resolve a preserved-side output slot through the current remap. Some slots may already be present in the
     * source plan output after join rewrites, so use source slots as a second lookup table.
     */
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

    /**
     * Add an identity Project so later branches can depend on stable output slots after aliasing or copying.
     */
    private Pair<Plan, Map<Slot, Slot>> remapOutputs(Plan plan) {
        Map<Slot, Slot> identityMapping = new HashMap<>();
        for (Slot slot : plan.getOutput()) {
            identityMapping.put(slot, slot);
        }
        return remapOutputs(plan, identityMapping);
    }

    /**
     * Remap an existing source-to-output mapping through a fresh identity Project.
     */
    private Pair<Plan, Map<Slot, Slot>> remapOutputs(Pair<Plan, Map<Slot, Slot>> plan) {
        return remapOutputs(plan.first, plan.second);
    }

    /**
     * Add an identity Project and return a mapping from the original source slots to the new Project outputs.
     */
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

    /**
     * Wrap an internal copy with a unique subquery alias and keep its slot mapping valid.
     */
    private Pair<Plan, Map<Slot, Slot>> aliasPlan(Pair<Plan, Map<Slot, Slot>> plan, String alias) {
        // The nullable-side repair plan uses several copies of the same right child
        // (delta/pre/post). Nereids rejects multiple raw scans of the same table name
        // during binding, so wrap each internal copy with a unique alias. This is only
        // an internal disambiguation node; user-authored subquery aliases are still
        // rejected by IVM normalize until that plan shape is supported.
        Plan aliasNode = new LogicalSubQueryAlias<>(alias, plan.first);
        return Pair.of(aliasNode, remapOutputMapping(plan.second, plan.first.getOutput(), aliasNode.getOutput()));
    }

    /**
     * Build UNION ALL with synthetic output slots so the union does not reuse child ExprIds.
     */
    private LogicalUnion buildUnionAll(List<Plan> children) {
        Plan first = children.get(0);
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

    /**
     * Extract pure equi-join keys from both hash conjuncts and other conjuncts.
     *
     * Return null when there is no hashable equality, or when any residual non-hash condition remains. The null
     * result makes the nullable-side rewrite fall back to the general repair-branch strategy.
     *
     * This intentionally accepts expression keys, not only slot-to-slot keys. For example,
     *   date_trunc(left.dt) = date_trunc(right.dt)
     * can be rewritten as long as each side of the equality is bound to exactly one join side.
     *
     * Unique functions are filtered out here. The right-event rewrite evaluates right key expressions while
     * building the event relation and evaluates left key expressions again while probing it. For random(), uuid(),
     * random_bytes(), etc., those two evaluations are not stable enough to serve as an event key.
     */
    private EquiJoinKeys extractEquiJoinKeys(LogicalJoin<? extends Plan, ? extends Plan> join) {
        ImmutableList<Expression> conjuncts = ImmutableList.<Expression>builder()
                .addAll(join.getHashJoinConjuncts())
                .addAll(join.getOtherJoinConjuncts())
                .build();
        if (conjuncts.isEmpty()) {
            return null;
        }
        Pair<List<Expression>, List<Expression>> extractedConjuncts = JoinUtils.extractExpressionForHashTable(
                join.left().getOutput(), join.right().getOutput(), conjuncts);
        if (extractedConjuncts.first.isEmpty() || !extractedConjuncts.second.isEmpty()) {
            return null;
        }
        Set<ExprId> leftExprIds = outputExprIds(join.left());
        Set<ExprId> rightExprIds = outputExprIds(join.right());
        ImmutableList.Builder<Expression> leftKeys = ImmutableList.builder();
        ImmutableList.Builder<Expression> rightKeys = ImmutableList.builder();
        for (Expression conjunct : extractedConjuncts.first) {
            if (!(conjunct instanceof EqualTo)) {
                return null;
            }
            EqualTo equalTo = (EqualTo) conjunct;
            Expression left = equalTo.left();
            Expression right = equalTo.right();
            if (left.containsUniqueFunction() || right.containsUniqueFunction()) {
                return null;
            }
            if (isBoundBy(left, leftExprIds) && isBoundBy(right, rightExprIds)) {
                leftKeys.add(left);
                rightKeys.add(right);
            } else if (isBoundBy(left, rightExprIds) && isBoundBy(right, leftExprIds)) {
                leftKeys.add(right);
                rightKeys.add(left);
            } else {
                return null;
            }
        }
        return new EquiJoinKeys(leftKeys.build(), rightKeys.build());
    }

    /**
     * Collect output ExprIds for side ownership checks.
     */
    private Set<ExprId> outputExprIds(Plan plan) {
        Set<ExprId> exprIds = new HashSet<>();
        for (Slot slot : plan.getOutput()) {
            exprIds.add(slot.getExprId());
        }
        return exprIds;
    }

    /**
     * Check whether all input slots of an expression come from one join side.
     */
    private boolean isBoundBy(Expression expression, Set<ExprId> exprIds) {
        Set<Slot> inputSlots = expression.getInputSlots();
        if (inputSlots.isEmpty()) {
            return false;
        }
        for (Slot slot : inputSlots) {
            if (!exprIds.contains(slot.getExprId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return nullable-side output slots carried by right events, excluding the synthetic dml factor.
     */
    private List<Slot> rightValueSlots(RewriteResult rightResult) {
        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        for (Slot slot : rightResult.plan.getOutput()) {
            if (!Column.IVM_DML_FACTOR_COL.equals(slot.getName())) {
                slots.add(slot);
            }
        }
        return slots.build();
    }

    /**
     * Generate stable internal names for event join keys.
     */
    private String eventKeyName(int index) {
        return RIGHT_EVENT_KEY_PREFIX + index;
    }

    /**
     * A UNION output is nullable if any corresponding child output is nullable.
     */
    private boolean unionOutputNullable(List<Plan> children, int index) {
        for (Plan child : children) {
            if (child.getOutput().get(index).nullable()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Project union outputs back to the target schema and preserve target ExprIds for downstream row-id projection.
     */
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

    /**
     * Replace the single nullable-side delta scan with its pre- or post-refresh snapshot.
     */
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

    /**
     * Deep copy a plan before reusing it in another branch, and return the copied output mapping.
     */
    private Pair<Plan, Map<Slot, Slot>> freshPlan(Plan plan) {
        DeepCopierContext copierContext = new DeepCopierContext();
        LogicalPlan freshPlan = LogicalPlanDeepCopier.INSTANCE.deepCopy((LogicalPlan) plan, copierContext);
        return Pair.of(freshPlan, mapCopiedOutputs(plan.getOutput(), freshPlan.getOutput(), copierContext));
    }

    /**
     * Map source outputs to deep-copied outputs using the ExprId replacement map produced by the copier.
     */
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

    /**
     * Pair two output lists by position after checking their arity.
     */
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

    /**
     * Rewrite a source-to-old-output mapping to point at a replacement output list.
     */
    private Map<Slot, Slot> remapOutputMapping(Map<Slot, Slot> sourceToOldOutput,
            List<Slot> oldOutput, List<Slot> newOutput) {
        Map<Slot, Slot> oldToNew = mapOutputs(oldOutput, newOutput);
        Map<Slot, Slot> sourceToNewOutput = new HashMap<>();
        for (Map.Entry<Slot, Slot> entry : sourceToOldOutput.entrySet()) {
            sourceToNewOutput.put(entry.getKey(), oldToNew.get(entry.getValue()));
        }
        return sourceToNewOutput;
    }

    /**
     * Equi-join key expressions split by preserved side and nullable side.
     */
    private static class EquiJoinKeys {
        private final List<Expression> leftExpressions;
        private final List<Expression> rightExpressions;

        private EquiJoinKeys(List<Expression> leftExpressions, List<Expression> rightExpressions) {
            this.leftExpressions = leftExpressions;
            this.rightExpressions = rightExpressions;
        }
    }

    /**
     * Nullable-side event relation plus the slots needed by the final event join projection.
     */
    private static class RightEventPlan {
        private final Plan plan;
        private final Map<Slot, Slot> rightOutputMapping;
        private final List<Slot> eventKeySlots;
        private final Slot dmlFactorSlot;

        private RightEventPlan(Plan plan, Map<Slot, Slot> rightOutputMapping,
                List<Slot> eventKeySlots, Slot dmlFactorSlot) {
            this.plan = plan;
            this.rightOutputMapping = rightOutputMapping;
            this.eventKeySlots = eventKeySlots;
            this.dmlFactorSlot = dmlFactorSlot;
        }
    }

    /**
     * Aggregated nullable-side delta keys and flags indicating positive/negative delta existence.
     */
    private static class RightDeltaKeyPlan {
        private final Plan plan;
        private final List<Slot> keySlots;
        private final Slot positiveSlot;
        private final Slot negativeSlot;

        private RightDeltaKeyPlan(Plan plan, List<Slot> keySlots, Slot positiveSlot, Slot negativeSlot) {
            this.plan = plan;
            this.keySlots = keySlots;
            this.positiveSlot = positiveSlot;
            this.negativeSlot = negativeSlot;
        }
    }

}
