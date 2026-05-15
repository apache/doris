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
 * Delta rewrite strategy for the restricted LEFT/RIGHT OUTER JOIN topology.
 *
 * <p>The strategy emits the regular joined delta for the preserved-side change:
 * <ul>
 *   <li>preserved-side delta: {@code delta_preserved OUTER JOIN nullable_snapshot}</li>
 *   <li>nullable-side delta: joined rows plus padded-null state migration</li>
 * </ul>
 */
public class IvmOuterJoinDeltaStrategy extends IvmLinearDeltaStrategy {

    private static final String NULLABLE_INSERT_DELTA_ALIAS = "__DORIS_IVM_NULLABLE_INSERT_DELTA__";
    private static final String NULLABLE_DELETE_DELTA_ALIAS = "__DORIS_IVM_NULLABLE_DELETE_DELTA__";
    private static final String NULLABLE_DETAIL_DELTA_ALIAS = "__DORIS_IVM_NULLABLE_DETAIL_DELTA__";
    private static final String NULLABLE_EVENT_KEY_PREFIX = "__DORIS_IVM_NULLABLE_EVENT_KEY_";
    private static final String NULLABLE_KEY_DELTA_ALIAS = "__DORIS_IVM_NULLABLE_KEY_DELTA__";
    private static final String NULLABLE_KEY_POSITIVE_ALIAS = "__DORIS_IVM_NULLABLE_KEY_POSITIVE__";
    private static final String NULLABLE_KEY_NEGATIVE_ALIAS = "__DORIS_IVM_NULLABLE_KEY_NEGATIVE__";
    private static final String NULLABLE_PRE_SNAPSHOT_ALIAS = "__DORIS_IVM_NULLABLE_PRE_SNAPSHOT__";
    private static final String NULLABLE_POST_SNAPSHOT_ALIAS = "__DORIS_IVM_NULLABLE_POST_SNAPSHOT__";

    public IvmOuterJoinDeltaStrategy(IvmRefreshContext ctx) {
        super(ctx);
    }

    /**
     * Dispatch a normalized LEFT/RIGHT OUTER JOIN by checking which side carries the base-table delta.
     */
    @Override
    public RewriteResult visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        if (!isSupportedOuterJoin(join.getJoinType())) {
            return super.visitLogicalJoin(join, context);
        }

        RewriteResult leftResult = join.left().accept(this, context);
        RewriteResult rightResult = join.right().accept(this, context);
        if (leftResult.dmlFactorSlot != null && rightResult.dmlFactorSlot != null) {
            throw new AnalysisException(
                    "IVM: both sides of outer join have dml_factor; expected at most one delta side");
        }
        if (leftResult.dmlFactorSlot == null && rightResult.dmlFactorSlot == null) {
            return new RewriteResult(join.withChildren(leftResult.plan, rightResult.plan), null);
        }
        OuterJoinSideHelper sideHelper = new OuterJoinSideHelper(join, leftResult, rightResult);
        if (sideHelper.isDeltaOnPreservedSide()) {
            return rewritePreservedSideDelta(join, leftResult, rightResult);
        } else {
            return rewriteNullableSideDelta(join, leftResult, rightResult, sideHelper);
        }
    }

    /**
     * Delta from the preserved side does not need pad-null repair; the original outer join shape is enough.
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
            RewriteResult leftResult, RewriteResult rightResult, OuterJoinSideHelper sideHelper) {
        EquiJoinKeys equiJoinKeys = extractEquiJoinKeys(join);
        if (equiJoinKeys != null) {
            return rewriteNullableSideDeltaWithNullableEvents(join, leftResult, rightResult, sideHelper, equiJoinKeys);
        } else {
            return rewriteNullableSideDeltaWithRepairBranches(join, leftResult, rightResult, sideHelper);
        }
    }

    private RewriteResult rewriteNullableSideDeltaWithRepairBranches(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult, OuterJoinSideHelper sideHelper) {
        // Nullable-side delta for:
        //   preserved_snapshot OUTER JOIN nullable_delta
        //
        // It has three parts:
        //   1. Bare joined rows:
        //        preserved_snapshot INNER JOIN nullable_delta
        //
        //   2. Remove old pad-null rows when nullable-side inserts create the first match:
        //        preserved_snapshot LEFT SEMI JOIN nullable_insert_delta
        //          LEFT ANTI JOIN nullable_pre_snapshot
        //      The semi join keeps preserved-side rows affected by this delta without
        //      multiplying them by matched delta rows. The anti join then keeps only rows
        //      that had no matching nullable-side row before this delta. For those rows, the old MV
        //      contained one row with nullable-side columns padded to NULL, so we emit that
        //      NULL-padded row with dml_factor = -1.
        //
        //   3. Add new pad-null rows when nullable-side deletes remove the last match:
        //        preserved_snapshot LEFT SEMI JOIN nullable_delete_delta
        //          LEFT ANTI JOIN nullable_post_snapshot
        //      The semi join keeps preserved-side rows affected by this delta without
        //      multiplying them by matched delta rows. The anti join then keeps only rows
        //      that have no matching nullable-side row after this delta. For those rows, the new MV
        //      needs one NULL-padded row, so we emit that row with dml_factor = +1.
        RewriteResult joinedResult = rewriteNullableSideBareJoinDelta(join, leftResult, rightResult, sideHelper);

        Pair<Plan, Map<Slot, Slot>> insertedNullableDelta = remapOutputs(aliasPlan(
                freshPlan(sideHelper.nullableResult.plan), NULLABLE_INSERT_DELTA_ALIAS));
        Slot insertedNullableDmlFactor = findSlotByName(insertedNullableDelta.first.getOutput(),
                Column.IVM_DML_FACTOR_COL);
        Pair<Plan, Map<Slot, Slot>> deletedNullableDelta = remapOutputs(aliasPlan(
                freshPlan(sideHelper.nullableResult.plan), NULLABLE_DELETE_DELTA_ALIAS));
        Slot deletedNullableDmlFactor = findSlotByName(deletedNullableDelta.first.getOutput(),
                Column.IVM_DML_FACTOR_COL);
        Plan nullableInserts = new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(insertedNullableDmlFactor, new TinyIntLiteral((byte) 0))),
                insertedNullableDelta.first);
        Plan nullableDeletes = new LogicalFilter<>(ImmutableSet.of(
                new LessThan(deletedNullableDmlFactor, new TinyIntLiteral((byte) 0))),
                deletedNullableDelta.first);
        // Build nullable pre/post from the original nullable-side plan, not from nullableResult.plan.
        // nullableResult.plan may already be linearly rewritten; for example UNION ALL keeps only
        // the delta arm and prunes other snapshot arms. Pad-null repair must compare against the
        // full nullable-side relation, so preserve all branches and only replace the one delta scan
        // with its pre/post snapshot.
        Pair<Plan, Map<Slot, Slot>> nullablePreSnapshot = remapOutputs(aliasPlan(
                freshPlan(copyDeltaScanAsSnapshot(sideHelper.nullableChild(), false)), NULLABLE_PRE_SNAPSHOT_ALIAS));
        Pair<Plan, Map<Slot, Slot>> nullablePostSnapshot = remapOutputs(aliasPlan(
                freshPlan(copyDeltaScanAsSnapshot(sideHelper.nullableChild(), true)), NULLABLE_POST_SNAPSHOT_ALIAS));
        Plan joinedProject = remapOutputs(joinedResult.plan).first;
        LogicalProject<Plan> preNullProject = buildPaddedNullRepairProject(join,
                remapOutputs(freshPlan(sideHelper.preservedResult.plan)), insertedNullableDelta.second,
                nullableInserts, nullablePreSnapshot, new TinyIntLiteral((byte) -1), sideHelper);
        LogicalProject<Plan> postNullProject = buildPaddedNullRepairProject(join,
                remapOutputs(freshPlan(sideHelper.preservedResult.plan)), deletedNullableDelta.second,
                nullableDeletes, nullablePostSnapshot, new TinyIntLiteral((byte) 1), sideHelper);

        LogicalUnion union = buildUnionAll(ImmutableList.of(joinedProject, preNullProject, postNullProject));
        LogicalProject<Plan> outputProject = projectUnionOutputs(union, joinedResult.plan.getOutput());
        Slot dmlFactor = findSlotByName(outputProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        return new RewriteResult(outputProject, dmlFactor);
    }

    private RewriteResult rewriteNullableSideDeltaWithNullableEvents(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult, OuterJoinSideHelper sideHelper,
            EquiJoinKeys equiJoinKeys) {
        // Nullable-side delta for equi outer join can be reduced to one probe:
        //   preserved_snapshot INNER JOIN nullable_events
        //
        // The important point is that nullable_events is not just "nullable_delta with
        // another name". It encodes every MV row change caused by the nullable side
        // into rows that can be joined by key with the preserved-side snapshot.
        // After that encoding, preserved_snapshot does not need to know whether an event
        // is a real nullable-side row change or a padded-NULL repair; it only probes once by
        // the event key and projects the event payload.
        //
        // nullable_events has three parts:
        //   1. Detail nullable-side delta rows:
        //        nullable_delta
        //      These rows keep the original nullable-side outputs and nullable-side delta dml_factor,
        //      so the final join emits normal joined row changes.
        //
        //   2. Remove old pad-null rows when nullable-side inserts create the first match:
        //        affected nullable_insert_delta keys LEFT ANTI JOIN nullable_pre_snapshot
        //      For those keys, the old MV contained one row with nullable-side columns padded
        //      to NULL. The event carries the join keys, pads nullable-side outputs to NULL,
        //      and uses dml_factor = -1.
        //
        //   3. Add new pad-null rows when nullable-side deletes remove the last match:
        //        affected nullable_delete_delta keys LEFT ANTI JOIN nullable_post_snapshot
        //      For those keys, the new MV needs one NULL-padded row. The event carries
        //      the join keys, pads nullable-side outputs to NULL, and uses dml_factor = +1.
        //
        // By merging the bare join and pad-null repair rows into nullable_events, the
        // preserved-side snapshot is scanned/probed once instead of three times.
        //
        // This requires pure deterministic equi keys. Expressions like
        //   f(left_slots) = g(right_slots)
        // are supported, because the nullable event relation can materialize
        // the nullable-side key as event_key and the final probe can evaluate
        // the preserved-side key against event_key. Conditions such as
        //   left.k = right.k AND left.v > right.v
        // are not supported here, because the nullable side alone cannot decide which
        // preserved-side rows are affected by the non-hash predicate. Such joins fall back to
        // the three repair branches. Unique functions such as random()/uuid() are
        // also rejected before this path, because recomputing them in different
        // event branches would produce unstable keys.
        RewriteResult joinedResult = rewriteNullableSideBareJoinDelta(join, leftResult, rightResult, sideHelper);
        Pair<Plan, Map<Slot, Slot>> preservedSnapshot = remapOutputs(freshPlan(sideHelper.preservedResult.plan));
        NullableEventPlan nullableEvents = buildNullableEventPlan(join, sideHelper, equiJoinKeys);

        ImmutableList.Builder<Expression> hashConjuncts = ImmutableList.builderWithExpectedSize(
                sideHelper.preservedKeyExpressions(equiJoinKeys).size());
        for (int i = 0; i < sideHelper.preservedKeyExpressions(equiJoinKeys).size(); i++) {
            hashConjuncts.add(new EqualTo(
                    ExpressionUtils.replace(sideHelper.preservedKeyExpressions(equiJoinKeys).get(i),
                            preservedSnapshot.second),
                    nullableEvents.eventKeySlots.get(i)));
        }
        LogicalJoin<Plan, Plan> eventJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                hashConjuncts.build(), ImmutableList.of(), join.getDistributeHint(),
                preservedSnapshot.first, nullableEvents.plan, JoinReorderContext.EMPTY);
        LogicalProject<Plan> outputProject = projectEventJoinOutputs(joinedResult.plan.getOutput(),
                eventJoin, preservedSnapshot.second, nullableEvents.nullableOutputMapping,
                nullableEvents.dmlFactorSlot);
        Slot dmlFactor = findSlotByName(outputProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        return new RewriteResult(outputProject, dmlFactor);
    }

    /**
     * Build the ordinary joined-row change:
     *   preserved_snapshot INNER JOIN nullable_delta
     *
     * This is shared by both nullable-side strategies. The dml factor comes from the nullable-side delta.
     */
    private RewriteResult rewriteNullableSideBareJoinDelta(LogicalJoin<? extends Plan, ? extends Plan> join,
            RewriteResult leftResult, RewriteResult rightResult, OuterJoinSideHelper sideHelper) {
        LogicalJoin<Plan, Plan> innerJoin = join.withTypeChildren(JoinType.INNER_JOIN,
                leftResult.plan, rightResult.plan, JoinReorderContext.EMPTY);
        return new RewriteResult(innerJoin, sideHelper.nullableResult.dmlFactorSlot);
    }

    /**
     * Build one pad-null repair branch:
     *   preserved_snapshot LEFT SEMI JOIN nullable_delta
     *     LEFT ANTI JOIN nullable_snapshot
     *
     * The semi join finds preserved-side rows affected by the nullable-side delta. The anti join keeps only rows
     * whose match existence changed across the snapshot boundary.
     */
    private LogicalProject<Plan> buildPaddedNullRepairProject(LogicalJoin<? extends Plan, ? extends Plan> join,
            Pair<Plan, Map<Slot, Slot>> preservedSnapshot, Map<Slot, Slot> nullableDeltaMapping, Plan nullableDelta,
            Pair<Plan, Map<Slot, Slot>> nullableSnapshot, Expression dmlFactor, OuterJoinSideHelper sideHelper) {
        Map<Slot, Slot> candidateMapping = ImmutableMap.<Slot, Slot>builder()
                .putAll(preservedSnapshot.second)
                .putAll(nullableDeltaMapping)
                .buildKeepingLast();
        LogicalJoin<Plan, Plan> candidateJoin = new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN,
                ExpressionUtils.replace(join.getHashJoinConjuncts(), candidateMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), candidateMapping), join.getDistributeHint(),
                preservedSnapshot.first, nullableDelta, JoinReorderContext.EMPTY);
        Map<Slot, Slot> antiJoinMapping = ImmutableMap.<Slot, Slot>builder()
                .putAll(preservedSnapshot.second)
                .putAll(nullableSnapshot.second)
                .buildKeepingLast();
        LogicalJoin<Plan, Plan> antiJoin = new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN,
                ExpressionUtils.replace(join.getHashJoinConjuncts(), antiJoinMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), antiJoinMapping), join.getDistributeHint(),
                candidateJoin, nullableSnapshot.first, JoinReorderContext.EMPTY);
        return projectPaddedNullOutputs(join, antiJoin, dmlFactor, preservedSnapshot.second, sideHelper);
    }

    /**
     * Project a repair branch back to the original outer join output schema, padding nullable-side columns
     * with NULL and setting the repair dml factor.
     */
    private LogicalProject<Plan> projectPaddedNullOutputs(LogicalJoin<? extends Plan, ? extends Plan> join,
            Plan source, Expression dmlFactor, Map<Slot, Slot> preservedOutputMapping,
            OuterJoinSideHelper sideHelper) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        Map<Slot, Expression> preservedSourceSlots = new HashMap<>();
        for (Slot slot : source.getOutput()) {
            preservedSourceSlots.put(slot, slot);
        }
        Slot leftRowId = IvmUtil.findRowIdSlot(join.left().getOutput(), "left child of outer join");
        Slot rightRowId = IvmUtil.findRowIdSlot(join.right().getOutput(), "right child of outer join");
        for (Slot slot : join.getOutput()) {
            if (slot.equals(leftRowId) && sideHelper.isNullableSlot(slot)) {
                projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
            } else if (slot.equals(rightRowId) && sideHelper.isNullableSlot(slot)) {
                // The nullable side has no matching row, so the parent normalize Project computes
                // hash(leftRowId, NULL) for LEFT JOIN and hash(NULL, rightRowId) for RIGHT JOIN.
                projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
            } else if (sideHelper.isPreservedSlot(slot)) {
                projects.add(new Alias(resolvePreservedOutput(slot, preservedOutputMapping,
                        preservedSourceSlots), slot.getName()));
            } else if (sideHelper.isNullableSlot(slot)) {
                projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
            } else {
                throw new AnalysisException("IVM outer join rewrite found unknown output slot: " + slot);
            }
        }
        projects.add(new Alias(dmlFactor, Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(projects.build(), (LogicalPlan) source);
    }

    /**
     * Build the nullable-side event relation consumed by the optimized one-probe rewrite.
     *
     * Output layout:
     *   nullable join keys, nullable-side value slots, dml_factor
     */
    private NullableEventPlan buildNullableEventPlan(LogicalJoin<? extends Plan, ? extends Plan> join,
            OuterJoinSideHelper sideHelper, EquiJoinKeys equiJoinKeys) {
        Plan detailEvent = buildNullableDetailEvent(sideHelper, equiJoinKeys);
        Plan preNullEvent = buildNullableNullEvent(join, sideHelper, equiJoinKeys, false,
                new TinyIntLiteral((byte) -1));
        Plan postNullEvent = buildNullableNullEvent(join, sideHelper, equiJoinKeys, true,
                new TinyIntLiteral((byte) 1));
        LogicalUnion union = buildUnionAll(ImmutableList.of(detailEvent, preNullEvent, postNullEvent));

        List<Slot> unionOutputs = union.getOutput();
        Map<Slot, Slot> nullableOutputMapping = new HashMap<>();
        int nullableOutputStart = sideHelper.nullableKeyExpressions(equiJoinKeys).size();
        int nullableOutputIndex = 0;
        for (Slot slot : nullableValueSlots(sideHelper)) {
            nullableOutputMapping.put(slot, unionOutputs.get(nullableOutputStart + nullableOutputIndex));
            nullableOutputIndex++;
        }
        List<Slot> eventKeySlots = unionOutputs.subList(0, sideHelper.nullableKeyExpressions(equiJoinKeys).size());
        Slot dmlFactorSlot = unionOutputs.get(unionOutputs.size() - 1);
        return new NullableEventPlan(union, nullableOutputMapping, eventKeySlots, dmlFactorSlot);
    }

    /**
     * Build detail events from raw nullable-side delta rows. These events produce normal joined-row changes after
     * probing the preserved-side snapshot.
     */
    private Plan buildNullableDetailEvent(OuterJoinSideHelper sideHelper, EquiJoinKeys equiJoinKeys) {
        Pair<Plan, Map<Slot, Slot>> nullableDelta = remapOutputs(aliasPlan(
                freshPlan(sideHelper.nullableResult.plan), NULLABLE_DETAIL_DELTA_ALIAS));
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        List<Expression> nullableKeyExpressions = sideHelper.nullableKeyExpressions(equiJoinKeys);
        for (int i = 0; i < nullableKeyExpressions.size(); i++) {
            projects.add(new Alias(ExpressionUtils.replace(nullableKeyExpressions.get(i), nullableDelta.second),
                    eventKeyName(i)));
        }
        for (Slot slot : nullableValueSlots(sideHelper)) {
            projects.add(new Alias(nullableDelta.second.get(slot), slot.getName()));
        }
        projects.add(new Alias(nullableDelta.second.get(sideHelper.nullableResult.dmlFactorSlot),
                Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(projects.build(), (LogicalPlan) nullableDelta.first);
    }

    /**
     * Build one pad-null event branch for affected nullable-side keys.
     *
     * preSnapshot branch: nullable-side inserts with no pre-existing nullable match emit dml_factor = -1.
     * postSnapshot branch: nullable-side deletes with no remaining nullable match emit dml_factor = +1.
     */
    private Plan buildNullableNullEvent(LogicalJoin<? extends Plan, ? extends Plan> join,
            OuterJoinSideHelper sideHelper, EquiJoinKeys equiJoinKeys, boolean postSnapshot, Expression dmlFactor) {
        NullableDeltaKeyPlan deltaKeys = buildNullableDeltaKeyPlan(sideHelper, equiJoinKeys);
        Slot flagSlot = postSnapshot ? deltaKeys.negativeSlot : deltaKeys.positiveSlot;
        Plan affectedKeys = new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(flagSlot, new TinyIntLiteral((byte) 0))), deltaKeys.plan);
        String snapshotAlias = postSnapshot ? NULLABLE_POST_SNAPSHOT_ALIAS : NULLABLE_PRE_SNAPSHOT_ALIAS;
        Pair<Plan, Map<Slot, Slot>> nullableSnapshot = remapOutputs(aliasPlan(
                freshPlan(copyDeltaScanAsSnapshot(sideHelper.nullableChild(), postSnapshot)), snapshotAlias));

        ImmutableList.Builder<Expression> antiConjuncts = ImmutableList.builderWithExpectedSize(
                sideHelper.nullableKeyExpressions(equiJoinKeys).size());
        List<Expression> nullableKeyExpressions = sideHelper.nullableKeyExpressions(equiJoinKeys);
        for (int i = 0; i < nullableKeyExpressions.size(); i++) {
            antiConjuncts.add(new EqualTo(deltaKeys.keySlots.get(i),
                    ExpressionUtils.replace(nullableKeyExpressions.get(i), nullableSnapshot.second)));
        }
        LogicalJoin<Plan, Plan> antiJoin = new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN,
                antiConjuncts.build(), ImmutableList.of(), join.getDistributeHint(),
                affectedKeys, nullableSnapshot.first, JoinReorderContext.EMPTY);

        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        for (int i = 0; i < deltaKeys.keySlots.size(); i++) {
            projects.add(new Alias(deltaKeys.keySlots.get(i), eventKeyName(i)));
        }
        for (Slot slot : nullableValueSlots(sideHelper)) {
            projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
        }
        projects.add(new Alias(dmlFactor, Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(projects.build(), antiJoin);
    }

    /**
     * Aggregate nullable-side delta rows by join key and mark whether each key has positive and/or negative delta
     * rows. Pad-null event branches use these flags to avoid multiplying one key by all matching delta rows.
     */
    private NullableDeltaKeyPlan buildNullableDeltaKeyPlan(OuterJoinSideHelper sideHelper,
            EquiJoinKeys equiJoinKeys) {
        Pair<Plan, Map<Slot, Slot>> nullableDelta = remapOutputs(aliasPlan(
                freshPlan(sideHelper.nullableResult.plan), NULLABLE_KEY_DELTA_ALIAS));
        List<Expression> nullableKeyExpressions = sideHelper.nullableKeyExpressions(equiJoinKeys);
        ImmutableList.Builder<Expression> groupBy = ImmutableList.builderWithExpectedSize(
                nullableKeyExpressions.size());
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (int i = 0; i < nullableKeyExpressions.size(); i++) {
            Expression key = ExpressionUtils.replace(nullableKeyExpressions.get(i), nullableDelta.second);
            groupBy.add(key);
            outputs.add(new Alias(key, eventKeyName(i)));
        }
        Slot dmlFactor = nullableDelta.second.get(sideHelper.nullableResult.dmlFactorSlot);
        outputs.add(new Alias(new Max(new If(new GreaterThan(dmlFactor, new TinyIntLiteral((byte) 0)),
                new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0))), NULLABLE_KEY_POSITIVE_ALIAS));
        outputs.add(new Alias(new Max(new If(new LessThan(dmlFactor, new TinyIntLiteral((byte) 0)),
                new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0))), NULLABLE_KEY_NEGATIVE_ALIAS));

        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(groupBy.build(), outputs.build(),
                nullableDelta.first);
        List<Slot> output = aggregate.getOutput();
        return new NullableDeltaKeyPlan(aggregate,
                output.subList(0, nullableKeyExpressions.size()),
                output.get(output.size() - 2), output.get(output.size() - 1));
    }

    /**
     * Project the one-probe event join back to the same schema as the bare join result.
     */
    private LogicalProject<Plan> projectEventJoinOutputs(List<Slot> targetOutputs, Plan source,
            Map<Slot, Slot> preservedOutputMapping, Map<Slot, Slot> nullableOutputMapping, Slot dmlFactorSlot) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                targetOutputs.size());
        for (Slot target : targetOutputs) {
            Expression expr;
            if (Column.IVM_DML_FACTOR_COL.equals(target.getName())) {
                expr = dmlFactorSlot;
            } else {
                expr = preservedOutputMapping.get(target);
                if (expr == null) {
                    expr = nullableOutputMapping.get(target);
                }
            }
            if (expr == null) {
                throw new AnalysisException("IVM outer join event rewrite lost output slot: " + target);
            }
            projects.add(new Alias(target.getExprId(), expr, target.getName()));
        }
        return new LogicalProject<>(projects.build(), (LogicalPlan) source);
    }

    /**
     * Resolve a preserved-side output slot through the current remap. Some slots may already be present in the
     * source plan output after join rewrites, so use source slots as a second lookup table.
     */
    private Expression resolvePreservedOutput(Slot slot, Map<Slot, Slot> preservedOutputMapping,
            Map<Slot, Expression> preservedSourceSlots) {
        Expression expr = preservedOutputMapping.get(slot);
        if (expr == null) {
            expr = preservedSourceSlots.get(slot);
        }
        if (expr == null) {
            throw new AnalysisException("IVM outer join rewrite lost preserved output slot: " + slot);
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
        // The nullable-side repair plan uses several copies of the same nullable child
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
     * Unique functions are filtered out here. The event rewrite evaluates nullable key expressions while
     * building the event relation and evaluates preserved key expressions again while probing it. For random(), uuid(),
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
     * Return nullable-side output slots carried by nullable-side events, excluding the synthetic dml factor.
     */
    private List<Slot> nullableValueSlots(OuterJoinSideHelper sideHelper) {
        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        for (Slot slot : sideHelper.nullableResult.plan.getOutput()) {
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
        return NULLABLE_EVENT_KEY_PREFIX + index;
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
            throw new AnalysisException("IVM outer join rewrite changed union output size from "
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
                throw new AnalysisException("IVM outer join rewrite lost copied output slot: " + sourceSlot);
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
            throw new AnalysisException("IVM outer join rewrite changed output size from "
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

    private boolean isSupportedOuterJoin(JoinType joinType) {
        return joinType == JoinType.LEFT_OUTER_JOIN || joinType == JoinType.RIGHT_OUTER_JOIN;
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
     * Helper that maps physical left/right children to logical preserved/nullable sides.
     */
    private static class OuterJoinSideHelper {
        private final LogicalJoin<? extends Plan, ? extends Plan> join;
        private final boolean preservedOnLeft;
        private final RewriteResult preservedResult;
        private final RewriteResult nullableResult;

        private OuterJoinSideHelper(LogicalJoin<? extends Plan, ? extends Plan> join,
                RewriteResult leftResult, RewriteResult rightResult) {
            this.join = join;
            this.preservedOnLeft = join.getJoinType() == JoinType.LEFT_OUTER_JOIN;
            this.preservedResult = preservedOnLeft ? leftResult : rightResult;
            this.nullableResult = preservedOnLeft ? rightResult : leftResult;
        }

        private boolean isDeltaOnPreservedSide() {
            return preservedResult.dmlFactorSlot != null;
        }

        private Plan nullableChild() {
            return preservedOnLeft ? join.right() : join.left();
        }

        private boolean isPreservedSlot(Slot slot) {
            return (preservedOnLeft ? join.left() : join.right()).getOutputSet().contains(slot);
        }

        private boolean isNullableSlot(Slot slot) {
            return (preservedOnLeft ? join.right() : join.left()).getOutputSet().contains(slot);
        }

        private List<Expression> preservedKeyExpressions(EquiJoinKeys equiJoinKeys) {
            return preservedOnLeft ? equiJoinKeys.leftExpressions : equiJoinKeys.rightExpressions;
        }

        private List<Expression> nullableKeyExpressions(EquiJoinKeys equiJoinKeys) {
            return preservedOnLeft ? equiJoinKeys.rightExpressions : equiJoinKeys.leftExpressions;
        }
    }

    /**
     * Nullable-side event relation plus the slots needed by the final event join projection.
     */
    private static class NullableEventPlan {
        private final Plan plan;
        private final Map<Slot, Slot> nullableOutputMapping;
        private final List<Slot> eventKeySlots;
        private final Slot dmlFactorSlot;

        private NullableEventPlan(Plan plan, Map<Slot, Slot> nullableOutputMapping,
                List<Slot> eventKeySlots, Slot dmlFactorSlot) {
            this.plan = plan;
            this.nullableOutputMapping = nullableOutputMapping;
            this.eventKeySlots = eventKeySlots;
            this.dmlFactorSlot = dmlFactorSlot;
        }
    }

    /**
     * Aggregated nullable-side delta keys and flags indicating positive/negative delta existence.
     */
    private static class NullableDeltaKeyPlan {
        private final Plan plan;
        private final List<Slot> keySlots;
        private final Slot positiveSlot;
        private final Slot negativeSlot;

        private NullableDeltaKeyPlan(Plan plan, List<Slot> keySlots, Slot positiveSlot, Slot negativeSlot) {
            this.plan = plan;
            this.keySlots = keySlots;
            this.positiveSlot = positiveSlot;
            this.negativeSlot = negativeSlot;
        }
    }

}
