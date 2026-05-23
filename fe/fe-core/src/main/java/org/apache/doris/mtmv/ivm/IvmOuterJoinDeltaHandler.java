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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
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
 * Delta rewrite handler for the restricted LEFT/RIGHT/FULL OUTER JOIN topology.
 *
 * <p>Only one child subtree should contain the base-table delta after the linear rewrite. The side carrying that
 * delta determines which rows can appear or disappear:
 * <ul>
 *   <li>delta side is retained by the join: keep its unmatched rows with LEFT/RIGHT OUTER JOIN</li>
 *   <li>delta side is the null side: emit joined rows, plus repair rows for old/new null-side MV rows</li>
 * </ul>
 *
 * <p>For FULL OUTER JOIN both physical sides retain unmatched rows. A left delta is therefore handled as
 * {@code left_delta LEFT OUTER JOIN right_snapshot} plus repair rows for right-only dangling rows; a right delta is
 * symmetric. Those repair rows are modeled with a branch-local retained side and null side instead of a global
 * retained/null-side assumption.
 */
class IvmOuterJoinDeltaHandler {

    private static final String NULL_SIDE_INSERT_DELTA_ALIAS = "__DORIS_IVM_NULL_SIDE_INSERT_DELTA__";
    private static final String NULL_SIDE_DELETE_DELTA_ALIAS = "__DORIS_IVM_NULL_SIDE_DELETE_DELTA__";
    private static final String NULL_SIDE_DETAIL_DELTA_ALIAS = "__DORIS_IVM_NULL_SIDE_DETAIL_DELTA__";
    private static final String NULL_SIDE_EVENT_KEY_PREFIX = "__DORIS_IVM_NULL_SIDE_EVENT_KEY_";
    private static final String NULL_SIDE_KEY_DELTA_ALIAS = "__DORIS_IVM_NULL_SIDE_KEY_DELTA__";
    private static final String NULL_SIDE_KEY_POSITIVE_ALIAS = "__DORIS_IVM_NULL_SIDE_KEY_POSITIVE__";
    private static final String NULL_SIDE_KEY_NEGATIVE_ALIAS = "__DORIS_IVM_NULL_SIDE_KEY_NEGATIVE__";
    private static final String NULL_SIDE_PRE_SNAPSHOT_ALIAS = "__DORIS_IVM_NULL_SIDE_PRE_SNAPSHOT__";
    private static final String NULL_SIDE_POST_SNAPSHOT_ALIAS = "__DORIS_IVM_NULL_SIDE_POST_SNAPSHOT__";

    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;

    /**
     * Dispatch a normalized LEFT/RIGHT/FULL OUTER JOIN by checking which side carries the base-table delta.
     */
    IvmDeltaRewriteResult rewriteJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
            IvmDeltaRewriteVisitor visitor, IvmRefreshContext context) {
        if (!join.getJoinType().isOuterJoin()) {
            throw new AnalysisException("IVM outer join handler received unsupported join type: " + join.getJoinType());
        }

        IvmDeltaRewriteResult leftResult = join.left().accept(visitor, context);
        IvmDeltaRewriteResult rightResult = join.right().accept(visitor, context);
        if (leftResult.dmlFactorSlot != null && rightResult.dmlFactorSlot != null) {
            throw new AnalysisException(
                    "IVM: both sides of outer join have dml_factor; expected at most one delta side");
        }
        if (leftResult.dmlFactorSlot == null && rightResult.dmlFactorSlot == null) {
            return new IvmDeltaRewriteResult(join.withChildren(leftResult.plan, rightResult.plan), null);
        }
        boolean deltaOnLeft = leftResult.dmlFactorSlot != null;
        boolean isPreservedSideDelta = (deltaOnLeft && join.getJoinType().isLeftOuterJoin())
                || (!deltaOnLeft && join.getJoinType().isRightOuterJoin());
        if (isPreservedSideDelta) {
            return rewritePreservedSideDelta(join, leftResult, rightResult, context);
        } else {
            NullSideDeltaContext deltaContext = new NullSideDeltaContext(
                    join, leftResult, rightResult, deltaOnLeft);
            return rewriteNullSideDelta(deltaContext, context);
        }
    }

    /**
     * LEFT/RIGHT OUTER JOIN non-null-side delta keeps dangling delta-side rows directly.
     *
     * <p>This path is only for LEFT JOIN left delta and RIGHT JOIN right delta. FULL OUTER JOIN is normalized with
     * deterministic row IDs on both children, so it does not need the non-deterministic row-id guard here.
     */
    private IvmDeltaRewriteResult rewritePreservedSideDelta(
            LogicalJoin<? extends Plan, ? extends Plan> join, IvmDeltaRewriteResult leftResult,
            IvmDeltaRewriteResult rightResult, IvmRefreshContext context) {
        LogicalJoin<Plan, Plan> newJoin = join.withChildren(ImmutableList.of(leftResult.plan, rightResult.plan));
        return helper.addNonDetGuardForJoinDelta(newJoin, leftResult, rightResult, context);
    }

    /**
     * Project the joined-row branch back to the original outer join output schema before UNION.
     */
    private LogicalProject<Plan> projectJoinDeltaOutputs(
            LogicalJoin<? extends Plan, ? extends Plan> originalJoin, IvmDeltaRewriteResult joinResult) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        for (Slot slot : originalJoin.getOutput()) {
            projects.add(new Alias(slot.getExprId(), resolveOutputSlot(joinResult.plan, slot), slot.getName()));
        }
        projects.add(new Alias(joinResult.dmlFactorSlot.getExprId(),
                joinResult.dmlFactorSlot, joinResult.dmlFactorSlot.getName()));
        return new LogicalProject<>(projects.build(), joinResult.plan);
    }

    /**
     * Resolve an original outer join output slot from the current joined-row branch output.
     */
    private Slot resolveOutputSlot(Plan plan, Slot target) {
        for (Slot output : plan.getOutput()) {
            if (output.equals(target)) {
                return output;
            }
        }
        throw new AnalysisException("IVM: outer join delta output missing slot: " + target);
    }

    /**
     * Delta from the null side may change both joined rows and null-side rows.
     *
     * <p>LEFT/RIGHT OUTER JOIN null-side delta can use the optimized event path for pure deterministic equality
     * predicates. FULL OUTER JOIN uses the general three-branch path because the first branch must also keep
     * unmatched delta-side rows with LEFT/RIGHT OUTER JOIN.
     */
    private IvmDeltaRewriteResult rewriteNullSideDelta(
            NullSideDeltaContext deltaContext, IvmRefreshContext context) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        boolean isFullOuterJoin = join.getJoinType().isFullOuterJoin();
        EquiJoinKeys equiJoinKeys = isFullOuterJoin ? null : extractEquiJoinKeys(join);
        if (equiJoinKeys != null) {
            return rewriteNullSideDeltaWithNullSideEvents(deltaContext, equiJoinKeys, context);
        } else {
            return rewriteNullSideDeltaWithRepairBranches(deltaContext, context);
        }
    }

    /**
     * General null-side delta rewrite using one joined-row branch and two NULL-row repair branches.
     */
    private IvmDeltaRewriteResult rewriteNullSideDeltaWithRepairBranches(
            NullSideDeltaContext deltaContext, IvmRefreshContext context) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        // Null-side delta for:
        //   retained_snapshot OUTER JOIN null_side_delta
        //
        // It has three parts:
        //   1. Joined rows:
        //        retained_snapshot INNER JOIN null_side_delta
        //      or for FULL OUTER JOIN:
        //        left_delta LEFT OUTER JOIN right_snapshot
        //        left_snapshot RIGHT OUTER JOIN right_delta
        //      FULL OUTER JOIN uses LEFT/RIGHT OUTER JOIN here to keep unmatched delta-side rows, and skips the
        //      null-side event path because that path probes retained_snapshot with INNER JOIN.
        //
        //   2. Remove old null-side rows when null-side inserts create the first match:
        //        retained_snapshot LEFT SEMI JOIN null_side_insert_delta
        //          LEFT ANTI JOIN null_side_pre_snapshot
        //      The semi join keeps retained-side rows affected by this delta without
        //      multiplying them by matched delta rows. The anti join then keeps only rows
        //      that had no matching null-side row before this delta. For those rows, the old MV
        //      contained one row with null-side columns filled as NULL, so we emit that
        //      null-side row with dml_factor = -1.
        //
        //   3. Add new null-side rows when null-side deletes remove the last match:
        //        retained_snapshot LEFT SEMI JOIN null_side_delete_delta
        //          LEFT ANTI JOIN null_side_post_snapshot
        //      The semi join keeps retained-side rows affected by this delta without
        //      multiplying them by matched delta rows. The anti join then keeps only rows
        //      that have no matching null-side row after this delta. For those rows, the new MV
        //      needs one null-side row, so we emit that row with dml_factor = +1.
        LogicalProject<Plan> joinedProject = rewriteNullSideJoinedRowsDelta(deltaContext);
        List<LogicalProject<Plan>> repairProjects = buildNullSideRepairProjects(deltaContext, context);

        LogicalUnion union = helper.buildUnionAll(ImmutableList.of(
                joinedProject, repairProjects.get(0), repairProjects.get(1)));
        LogicalProject<Plan> outputProject = helper.projectUnionOutputs(union, joinedProject.getOutput());
        Slot dmlFactor = findSlotByName(outputProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        return new IvmDeltaRewriteResult(outputProject, dmlFactor);
    }

    /**
     * Build the joined-row delta branch for the general NULL-row repair rewrite.
     *
     * <p>LEFT/RIGHT OUTER JOIN uses INNER JOIN. FULL OUTER JOIN uses LEFT/RIGHT OUTER JOIN selected by the delta
     * side, so unmatched delta rows are emitted by this first branch.
     */
    private LogicalProject<Plan> rewriteNullSideJoinedRowsDelta(NullSideDeltaContext deltaContext) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        JoinType joinType = join.getJoinType().isFullOuterJoin()
                ? (deltaContext.isDeltaOnLeft ? JoinType.LEFT_OUTER_JOIN : JoinType.RIGHT_OUTER_JOIN)
                : JoinType.INNER_JOIN;
        LogicalJoin<Plan, Plan> newJoin = join.withTypeChildren(
                joinType, deltaContext.leftResult.plan, deltaContext.rightResult.plan, JoinReorderContext.EMPTY);
        // The joined-row branch changes the join type, so its output slots/schema are not the same as the
        // original outer join output. Project it back before unioning with the repair branches.
        return projectJoinDeltaOutputs(join,
                new IvmDeltaRewriteResult(newJoin, deltaContext.deltaSideResult().dmlFactorSlot));
    }

    /**
     * Build the two NULL-row repair projects for null-side delta.
     *
     * <p>The insert branch uses the null-side pre-refresh snapshot and emits {@code -1} for retained rows that move
     * from a null-side row to a matched row. The delete branch uses the post-refresh snapshot and emits {@code +1}
     * for retained rows that move from matched to a null-side row.
     */
    private List<LogicalProject<Plan>> buildNullSideRepairProjects(
            NullSideDeltaContext deltaContext, IvmRefreshContext context) {
        Pair<Plan, Map<Slot, Slot>> insertedNullSideDelta = helper.remapOutputs(helper.aliasPlan(
                helper.freshPlan(deltaContext.deltaSideResult().plan), NULL_SIDE_INSERT_DELTA_ALIAS));
        Slot insertedNullSideDmlFactor = findSlotByName(insertedNullSideDelta.first.getOutput(),
                Column.IVM_DML_FACTOR_COL);
        Pair<Plan, Map<Slot, Slot>> deletedNullSideDelta = helper.remapOutputs(helper.aliasPlan(
                helper.freshPlan(deltaContext.deltaSideResult().plan), NULL_SIDE_DELETE_DELTA_ALIAS));
        Slot deletedNullSideDmlFactor = findSlotByName(deletedNullSideDelta.first.getOutput(),
                Column.IVM_DML_FACTOR_COL);
        Plan nullSideInserts = new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(insertedNullSideDmlFactor, new TinyIntLiteral((byte) 0))),
                insertedNullSideDelta.first);
        Plan nullSideDeletes = new LogicalFilter<>(ImmutableSet.of(
                new LessThan(deletedNullSideDmlFactor, new TinyIntLiteral((byte) 0))),
                deletedNullSideDelta.first);
        // Build delta-side pre/post from the original delta-side plan, not from the rewritten delta plan.
        // The rewritten delta plan may already be linearized; for example UNION ALL keeps only
        // the delta arm and prunes other snapshot arms. NULL-row repair must compare against the
        // full null-side relation, so retain all branches and only replace the one delta scan
        // with its pre/post snapshot.
        Pair<Plan, Map<Slot, Slot>> nullSidePreSnapshot = helper.remapOutputs(helper.aliasPlan(
                helper.freshPlan(copyDeltaScanAsSnapshot(deltaContext.deltaSideChild(), false, context)),
                NULL_SIDE_PRE_SNAPSHOT_ALIAS));
        Pair<Plan, Map<Slot, Slot>> nullSidePostSnapshot = helper.remapOutputs(helper.aliasPlan(
                helper.freshPlan(copyDeltaScanAsSnapshot(deltaContext.deltaSideChild(), true, context)),
                NULL_SIDE_POST_SNAPSHOT_ALIAS));
        LogicalProject<Plan> preNullProject = buildNullSideRepairProject(deltaContext,
                helper.remapOutputs(helper.freshPlan(deltaContext.nonDeltaSideResult().plan)), insertedNullSideDelta.second,
                nullSideInserts, nullSidePreSnapshot, new TinyIntLiteral((byte) -1));
        LogicalProject<Plan> postNullProject = buildNullSideRepairProject(deltaContext,
                helper.remapOutputs(helper.freshPlan(deltaContext.nonDeltaSideResult().plan)), deletedNullSideDelta.second,
                nullSideDeletes, nullSidePostSnapshot, new TinyIntLiteral((byte) 1));
        return ImmutableList.of(preNullProject, postNullProject);
    }

    /**
     * Optimized null-side delta rewrite that encodes joined-row changes and NULL-row repair as key events.
     */
    private IvmDeltaRewriteResult rewriteNullSideDeltaWithNullSideEvents(
            NullSideDeltaContext deltaContext, EquiJoinKeys equiJoinKeys, IvmRefreshContext context) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        // Null-side delta for equi outer join can be reduced to one probe:
        //   retained_snapshot INNER JOIN null_side_events
        //
        // The important point is that null_side_events is not just "null_side_delta with
        // another name". It encodes every MV row change caused by the null side
        // into rows that can be joined by key with the retained-side snapshot.
        // After that encoding, retained_snapshot does not need to know whether an event
        // is a real null-side row change or a NULL-row repair; it only probes once by
        // the event key and projects the event payload.
        //
        // null_side_events has three parts:
        //   1. Detail null-side delta rows:
        //        null_side_delta
        //      These rows keep the original null-side outputs and null-side delta dml_factor,
        //      so the final join emits normal joined row changes.
        //
        //   2. Remove old null-side rows when null-side inserts create the first match:
        //        affected null_side_insert_delta keys LEFT ANTI JOIN null_side_pre_snapshot
        //      For those keys, the old MV contained one row with null-side columns filled
        //      as NULL. The event carries the join keys, sets null-side outputs to NULL,
        //      and uses dml_factor = -1.
        //
        //   3. Add new null-side rows when null-side deletes remove the last match:
        //        affected null_side_delete_delta keys LEFT ANTI JOIN null_side_post_snapshot
        //      For those keys, the new MV needs one null-side row. The event carries
        //      the join keys, sets null-side outputs to NULL, and uses dml_factor = +1.
        //
        // By merging the bare join and NULL-row repair rows into null_side_events, the
        // retained-side snapshot is scanned/probed once instead of three times.
        //
        // This requires pure deterministic equi keys. Expressions like
        //   f(left_slots) = g(right_slots)
        // are supported, because the null-side event relation can materialize
        // the null-side key as event_key and the final probe can evaluate
        // the retained-side key against event_key. Conditions such as
        //   left.k = right.k AND left.v > right.v
        // are not supported here, because the null side alone cannot decide which
        // retained-side rows are affected by the non-hash predicate. Such joins fall back to
        // the three repair branches. Unique functions such as random()/uuid() are
        // also rejected before this path, because recomputing them in different
        // event branches would produce unstable keys.
        IvmDeltaRewriteResult joinedResult = rewriteNullSideBareJoinDelta(deltaContext);
        Pair<Plan, Map<Slot, Slot>> retainedSnapshot = helper.remapOutputs(
                helper.freshPlan(deltaContext.nonDeltaSideResult().plan));
        NullSideEventPlan nullSideEvents = buildNullSideEventPlan(deltaContext, equiJoinKeys, context);

        // Join retained rows with the event relation by the extracted equality keys. A detail event produces a
        // normal joined-row change; a repair event produces the same retained row with null-side payloads set to NULL.
        ImmutableList.Builder<Expression> hashConjuncts = ImmutableList.builderWithExpectedSize(
                deltaContext.nonDeltaSideKeyExpressions(equiJoinKeys).size());
        for (int i = 0; i < deltaContext.nonDeltaSideKeyExpressions(equiJoinKeys).size(); i++) {
            hashConjuncts.add(new EqualTo(
                    ExpressionUtils.replace(deltaContext.nonDeltaSideKeyExpressions(equiJoinKeys).get(i),
                            retainedSnapshot.second),
                    nullSideEvents.eventKeySlots.get(i)));
        }
        LogicalJoin<Plan, Plan> eventJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                hashConjuncts.build(), ImmutableList.of(), join.getDistributeHint(),
                retainedSnapshot.first, nullSideEvents.plan, JoinReorderContext.EMPTY);
        LogicalProject<Plan> outputProject = projectEventJoinOutputs(joinedResult.plan.getOutput(),
                eventJoin, retainedSnapshot.second, nullSideEvents.nullSideOutputMapping,
                nullSideEvents.dmlFactorSlot);
        Slot dmlFactor = findSlotByName(outputProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        return new IvmDeltaRewriteResult(outputProject, dmlFactor);
    }

    /**
     * Build the ordinary joined-row change:
     *   retained_snapshot INNER JOIN null_side_delta
     *
     * This is shared by both null-side strategies. The dml factor comes from the null-side delta.
     */
    private IvmDeltaRewriteResult rewriteNullSideBareJoinDelta(NullSideDeltaContext deltaContext) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        LogicalJoin<Plan, Plan> innerJoin = join.withTypeChildren(JoinType.INNER_JOIN,
                deltaContext.leftResult.plan, deltaContext.rightResult.plan, JoinReorderContext.EMPTY);
        return new IvmDeltaRewriteResult(innerJoin, deltaContext.deltaSideResult().dmlFactorSlot);
    }

    /**
     * Build one NULL-row repair branch:
     *   retained_snapshot LEFT SEMI JOIN null_side_delta
     *     LEFT ANTI JOIN null_side_snapshot
     *
     * The semi join finds retained-side rows affected by the null-side delta. The anti join keeps only rows
     * whose match existence changed across the snapshot boundary.
     *
     * <p>For insert repair, {@code null_side_snapshot} is the pre-refresh snapshot, so the anti join keeps retained
     * rows that had no match before the inserted delta rows. For delete repair, it is the post-refresh snapshot, so
     * the anti join keeps retained rows that have no match after the deleted delta rows.
     */
    private LogicalProject<Plan> buildNullSideRepairProject(
            NullSideDeltaContext deltaContext,
            Pair<Plan, Map<Slot, Slot>> retainedSnapshot, Map<Slot, Slot> nullSideDeltaMapping, Plan nullSideDelta,
            Pair<Plan, Map<Slot, Slot>> nullSideSnapshot, Expression dmlFactor) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        // Candidate retained rows are selected with LEFT SEMI JOIN so one retained row is emitted once per repair
        // branch, no matter how many matching null-side delta rows the same key has.
        Map<Slot, Slot> candidateMapping = ImmutableMap.<Slot, Slot>builder()
                .putAll(retainedSnapshot.second)
                .putAll(nullSideDeltaMapping)
                .buildKeepingLast();
        LogicalJoin<Plan, Plan> candidateJoin = new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN,
                ExpressionUtils.replace(join.getHashJoinConjuncts(), candidateMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), candidateMapping), join.getDistributeHint(),
                retainedSnapshot.first, nullSideDelta, JoinReorderContext.EMPTY);
        // Anti join checks whether the candidate retained rows were unmatched in the relevant snapshot. Only those
        // rows need NULL-row insert/delete compensation.
        Map<Slot, Slot> antiJoinMapping = ImmutableMap.<Slot, Slot>builder()
                .putAll(retainedSnapshot.second)
                .putAll(nullSideSnapshot.second)
                .buildKeepingLast();
        LogicalJoin<Plan, Plan> antiJoin = new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN,
                ExpressionUtils.replace(join.getHashJoinConjuncts(), antiJoinMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), antiJoinMapping), join.getDistributeHint(),
                candidateJoin, nullSideSnapshot.first, JoinReorderContext.EMPTY);
        return projectNullSideRepairOutputs(deltaContext, antiJoin, dmlFactor, retainedSnapshot.second);
    }

    /**
     * Project a repair branch back to the original outer join output schema, filling null-side columns
     * with NULL and setting the repair dml factor.
     *
     * <p>The retained side is read from the semi/anti join output. The null side is not read from the source at all;
     * every original null-side column is projected as NULL to recreate the outer-join unmatched row.
     */
    private LogicalProject<Plan> projectNullSideRepairOutputs(
            NullSideDeltaContext deltaContext, Plan source, Expression dmlFactor,
            Map<Slot, Slot> retainedOutputMapping) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        Map<Slot, Expression> retainedSourceSlots = new HashMap<>();
        for (Slot slot : source.getOutput()) {
            retainedSourceSlots.put(slot, slot);
        }
        Slot leftRowId = IvmUtil.findRowIdSlot(join.left().getOutput(), "left child of outer join");
        Slot rightRowId = IvmUtil.findRowIdSlot(join.right().getOutput(), "right child of outer join");
        for (Slot slot : join.getOutput()) {
            if (slot.equals(leftRowId) && deltaContext.isDeltaSideSlot(slot)) {
                projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
            } else if (slot.equals(rightRowId) && deltaContext.isDeltaSideSlot(slot)) {
                // The delta side has no matching row here, so the parent normalize Project computes the row id
                // with this side filled as NULL.
                projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
            } else if (deltaContext.isNonDeltaSideSlot(slot)) {
                projects.add(new Alias(resolveRetainedOutput(slot, retainedOutputMapping,
                        retainedSourceSlots), slot.getName()));
            } else if (deltaContext.isDeltaSideSlot(slot)) {
                projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
            } else {
                throw new AnalysisException("IVM outer join rewrite found unknown output slot: " + slot);
            }
        }
        projects.add(new Alias(dmlFactor, Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(projects.build(), (LogicalPlan) source);
    }

    /**
     * Build the null-side event relation consumed by the optimized one-probe rewrite.
     *
     * Output layout:
     *   null-side join keys, null-side value slots, dml_factor
     *
     * <p>The final event join only needs null-side keys and payload columns. The retained-side columns come from the
     * retained snapshot after probing by the event keys.
     */
    private NullSideEventPlan buildNullSideEventPlan(
            NullSideDeltaContext deltaContext, EquiJoinKeys equiJoinKeys, IvmRefreshContext context) {
        Plan detailEvent = buildNullSideDetailEvent(deltaContext, equiJoinKeys);
        Plan preNullEvent = buildNullSideRepairEvent(deltaContext, equiJoinKeys, false,
                new TinyIntLiteral((byte) -1), context);
        Plan postNullEvent = buildNullSideRepairEvent(deltaContext, equiJoinKeys, true,
                new TinyIntLiteral((byte) 1), context);
        LogicalUnion union = helper.buildUnionAll(ImmutableList.of(detailEvent, preNullEvent, postNullEvent));

        List<Slot> unionOutputs = union.getOutput();
        Map<Slot, Slot> nullSideOutputMapping = new HashMap<>();
        int nullSideOutputStart = deltaContext.deltaSideKeyExpressions(equiJoinKeys).size();
        int nullSideOutputIndex = 0;
        for (Slot slot : nullSideValueSlots(deltaContext)) {
            nullSideOutputMapping.put(slot, unionOutputs.get(nullSideOutputStart + nullSideOutputIndex));
            nullSideOutputIndex++;
        }
        List<Slot> eventKeySlots = unionOutputs.subList(0, deltaContext.deltaSideKeyExpressions(equiJoinKeys).size());
        Slot dmlFactorSlot = unionOutputs.get(unionOutputs.size() - 1);
        return new NullSideEventPlan(union, nullSideOutputMapping, eventKeySlots, dmlFactorSlot);
    }

    /**
     * Build detail events from raw null-side delta rows. These events produce normal joined-row changes after
     * probing the retained-side snapshot.
     */
    private Plan buildNullSideDetailEvent(NullSideDeltaContext deltaContext, EquiJoinKeys equiJoinKeys) {
        Pair<Plan, Map<Slot, Slot>> nullSideDelta = helper.remapOutputs(helper.aliasPlan(
                helper.freshPlan(deltaContext.deltaSideResult().plan), NULL_SIDE_DETAIL_DELTA_ALIAS));
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        List<Expression> nullSideKeyExpressions = deltaContext.deltaSideKeyExpressions(equiJoinKeys);
        for (int i = 0; i < nullSideKeyExpressions.size(); i++) {
            projects.add(new Alias(ExpressionUtils.replace(nullSideKeyExpressions.get(i), nullSideDelta.second),
                    eventKeyName(i)));
        }
        for (Slot slot : nullSideValueSlots(deltaContext)) {
            projects.add(new Alias(nullSideDelta.second.get(slot), slot.getName()));
        }
        projects.add(new Alias(nullSideDelta.second.get(deltaContext.deltaSideResult().dmlFactorSlot),
                Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(projects.build(), (LogicalPlan) nullSideDelta.first);
    }

    /**
     * Build one NULL-row repair event branch for affected null-side keys.
     *
     * preSnapshot branch: null-side inserts with no pre-existing match emit dml_factor = -1.
     * postSnapshot branch: null-side deletes with no remaining match emit dml_factor = +1.
     */
    private Plan buildNullSideRepairEvent(
            NullSideDeltaContext deltaContext, EquiJoinKeys equiJoinKeys, boolean postSnapshot,
            Expression dmlFactor, IvmRefreshContext context) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        NullSideDeltaKeyPlan deltaKeys = buildNullSideDeltaKeyPlan(deltaContext, equiJoinKeys);
        Slot flagSlot = postSnapshot ? deltaKeys.negativeSlot : deltaKeys.positiveSlot;
        Plan affectedKeys = new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(flagSlot, new TinyIntLiteral((byte) 0))), deltaKeys.plan);
        String snapshotAlias = postSnapshot ? NULL_SIDE_POST_SNAPSHOT_ALIAS : NULL_SIDE_PRE_SNAPSHOT_ALIAS;
        Pair<Plan, Map<Slot, Slot>> nullSideSnapshot = helper.remapOutputs(helper.aliasPlan(
                helper.freshPlan(copyDeltaScanAsSnapshot(deltaContext.deltaSideChild(), postSnapshot, context)),
                snapshotAlias));

        ImmutableList.Builder<Expression> antiConjuncts = ImmutableList.builderWithExpectedSize(
                deltaContext.deltaSideKeyExpressions(equiJoinKeys).size());
        List<Expression> nullSideKeyExpressions = deltaContext.deltaSideKeyExpressions(equiJoinKeys);
        for (int i = 0; i < nullSideKeyExpressions.size(); i++) {
            antiConjuncts.add(new EqualTo(deltaKeys.keySlots.get(i),
                    ExpressionUtils.replace(nullSideKeyExpressions.get(i), nullSideSnapshot.second)));
        }
        LogicalJoin<Plan, Plan> antiJoin = new LogicalJoin<>(JoinType.LEFT_ANTI_JOIN,
                antiConjuncts.build(), ImmutableList.of(), join.getDistributeHint(),
                affectedKeys, nullSideSnapshot.first, JoinReorderContext.EMPTY);

        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        for (int i = 0; i < deltaKeys.keySlots.size(); i++) {
            projects.add(new Alias(deltaKeys.keySlots.get(i), eventKeyName(i)));
        }
        for (Slot slot : nullSideValueSlots(deltaContext)) {
            projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
        }
        projects.add(new Alias(dmlFactor, Column.IVM_DML_FACTOR_COL));
        return new LogicalProject<>(projects.build(), antiJoin);
    }

    /**
     * Aggregate null-side delta rows by join key and mark whether each key has positive and/or negative delta
     * rows. NULL-row repair event branches use these flags to avoid multiplying one key by all matching delta rows.
     */
    private NullSideDeltaKeyPlan buildNullSideDeltaKeyPlan(NullSideDeltaContext deltaContext,
            EquiJoinKeys equiJoinKeys) {
        Pair<Plan, Map<Slot, Slot>> nullSideDelta = helper.remapOutputs(helper.aliasPlan(
                helper.freshPlan(deltaContext.deltaSideResult().plan), NULL_SIDE_KEY_DELTA_ALIAS));
        List<Expression> nullSideKeyExpressions = deltaContext.deltaSideKeyExpressions(equiJoinKeys);
        ImmutableList.Builder<Expression> groupBy = ImmutableList.builderWithExpectedSize(
                nullSideKeyExpressions.size());
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (int i = 0; i < nullSideKeyExpressions.size(); i++) {
            Expression key = ExpressionUtils.replace(nullSideKeyExpressions.get(i), nullSideDelta.second);
            groupBy.add(key);
            outputs.add(new Alias(key, eventKeyName(i)));
        }
        Slot dmlFactor = nullSideDelta.second.get(deltaContext.deltaSideResult().dmlFactorSlot);
        outputs.add(new Alias(new Max(new If(new GreaterThan(dmlFactor, new TinyIntLiteral((byte) 0)),
                new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0))), NULL_SIDE_KEY_POSITIVE_ALIAS));
        outputs.add(new Alias(new Max(new If(new LessThan(dmlFactor, new TinyIntLiteral((byte) 0)),
                new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0))), NULL_SIDE_KEY_NEGATIVE_ALIAS));

        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(groupBy.build(), outputs.build(),
                nullSideDelta.first);
        List<Slot> output = aggregate.getOutput();
        return new NullSideDeltaKeyPlan(aggregate,
                output.subList(0, nullSideKeyExpressions.size()),
                output.get(output.size() - 2), output.get(output.size() - 1));
    }

    /**
     * Project the one-probe event join back to the same schema as the bare join result.
     */
    private LogicalProject<Plan> projectEventJoinOutputs(List<Slot> targetOutputs, Plan source,
            Map<Slot, Slot> retainedOutputMapping, Map<Slot, Slot> nullSideOutputMapping, Slot dmlFactorSlot) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                targetOutputs.size());
        for (Slot target : targetOutputs) {
            Expression expr;
            if (Column.IVM_DML_FACTOR_COL.equals(target.getName())) {
                expr = dmlFactorSlot;
            } else {
                expr = retainedOutputMapping.get(target);
                if (expr == null) {
                    expr = nullSideOutputMapping.get(target);
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
     * Resolve a retained-side output slot through the current remap. Some slots may already be present in the
     * source plan output after join rewrites, so use source slots as a second lookup table.
     */
    private Expression resolveRetainedOutput(Slot slot, Map<Slot, Slot> retainedOutputMapping,
            Map<Slot, Expression> retainedSourceSlots) {
        Expression expr = retainedOutputMapping.get(slot);
        if (expr == null) {
            expr = retainedSourceSlots.get(slot);
        }
        if (expr == null) {
            throw new AnalysisException("IVM outer join rewrite lost retained output slot: " + slot);
        }
        return expr;
    }

    /**
     * Extract pure equi-join keys from both hash conjuncts and other conjuncts.
     *
     * Return null when there is no hashable equality, or when any residual non-hash condition remains. The null
     * result makes the null-side rewrite fall back to the general repair-branch path.
     *
     * This intentionally accepts expression keys, not only slot-to-slot keys. For example,
     *   date_trunc(left.dt) = date_trunc(right.dt)
     * can be rewritten as long as each side of the equality is bound to exactly one join side.
     *
     * Unique functions are filtered out here. The event rewrite evaluates null-side key expressions while
     * building the event relation and evaluates retained key expressions again while probing it. For random(), uuid(),
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
            if (left.containsVolatileExpression() || right.containsVolatileExpression()) {
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
     * Return null-side output slots carried by null-side events, excluding the synthetic dml factor.
     */
    private List<Slot> nullSideValueSlots(NullSideDeltaContext deltaContext) {
        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        for (Slot slot : deltaContext.deltaSideResult().plan.getOutput()) {
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
        return NULL_SIDE_EVENT_KEY_PREFIX + index;
    }

    /**
     * Replace the single null-side delta scan with its pre- or post-refresh snapshot.
     */
    private Plan copyDeltaScanAsSnapshot(Plan plan, boolean postSnapshot, IvmRefreshContext context) {
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
            IvmStreamRef ref = context.getBaseTableStream(scan);
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
            throw new AnalysisException("IVM: expected exactly one null-side delta scan, got " + deltaScanCount[0]);
        }
        return snapshot;
    }

    /**
     * Delegate slot lookup to the shared IVM rewrite helper.
     */
    private Slot findSlotByName(List<Slot> slots, String name) {
        return helper.findSlotByName(slots, name);
    }

    /**
     * Equi-join key expressions split by physical join side.
     */
    private static class EquiJoinKeys {
        private final List<Expression> leftExpressions;
        private final List<Expression> rightExpressions;

        /**
         * Store key expressions in physical join-child order.
         */
        private EquiJoinKeys(List<Expression> leftExpressions, List<Expression> rightExpressions) {
            this.leftExpressions = leftExpressions;
            this.rightExpressions = rightExpressions;
        }
    }

    /**
     * Context for null-side delta rewrites.
     *
     * <p>For FULL OUTER JOIN both physical sides can be filled as NULL globally, so this class avoids retained/null
     * side names and models the rewrite by delta side and non-delta side instead.
     *
     * <p>All methods in this class translate between physical left/right plan children and these rewrite-local roles.
     */
    private static class NullSideDeltaContext {
        private final LogicalJoin<? extends Plan, ? extends Plan> join;
        private final boolean isDeltaOnLeft;
        private final IvmDeltaRewriteResult leftResult;
        private final IvmDeltaRewriteResult rightResult;

        /**
         * Map physical left/right results to rewrite-local delta and non-delta roles.
         */
        private NullSideDeltaContext(LogicalJoin<? extends Plan, ? extends Plan> join,
                IvmDeltaRewriteResult leftResult, IvmDeltaRewriteResult rightResult, boolean deltaOnLeft) {
            this.join = join;
            this.isDeltaOnLeft = deltaOnLeft;
            this.leftResult = leftResult;
            this.rightResult = rightResult;
        }

        /**
         * Return the rewrite result for the physical side carrying delta rows.
         */
        private IvmDeltaRewriteResult deltaSideResult() {
            return isDeltaOnLeft ? leftResult : rightResult;
        }

        /**
         * Return the rewrite result for the physical side not carrying delta rows.
         */
        private IvmDeltaRewriteResult nonDeltaSideResult() {
            return isDeltaOnLeft ? rightResult : leftResult;
        }

        /**
         * Return the original child plan for the branch-local delta side.
         */
        private Plan deltaSideChild() {
            return isDeltaOnLeft ? join.left() : join.right();
        }

        /**
         * Check whether an output slot belongs to the branch-local non-delta side.
         */
        private boolean isNonDeltaSideSlot(Slot slot) {
            return (isDeltaOnLeft ? join.right() : join.left()).getOutputSet().contains(slot);
        }

        /**
         * Check whether an output slot belongs to the branch-local delta side.
         */
        private boolean isDeltaSideSlot(Slot slot) {
            return (isDeltaOnLeft ? join.left() : join.right()).getOutputSet().contains(slot);
        }

        /**
         * Return the non-delta-side key expressions from physical left/right equi keys.
         */
        private List<Expression> nonDeltaSideKeyExpressions(EquiJoinKeys equiJoinKeys) {
            return isDeltaOnLeft ? equiJoinKeys.rightExpressions : equiJoinKeys.leftExpressions;
        }

        /**
         * Return the delta-side key expressions from physical left/right equi keys.
         */
        private List<Expression> deltaSideKeyExpressions(EquiJoinKeys equiJoinKeys) {
            return isDeltaOnLeft ? equiJoinKeys.leftExpressions : equiJoinKeys.rightExpressions;
        }
    }

    /**
     * Null-side event relation plus the slots needed by the final event join projection.
     */
    private static class NullSideEventPlan {
        private final Plan plan;
        private final Map<Slot, Slot> nullSideOutputMapping;
        private final List<Slot> eventKeySlots;
        private final Slot dmlFactorSlot;

        /**
         * Store the event relation and the output slots consumed by the final probe projection.
         */
        private NullSideEventPlan(Plan plan, Map<Slot, Slot> nullSideOutputMapping,
                List<Slot> eventKeySlots, Slot dmlFactorSlot) {
            this.plan = plan;
            this.nullSideOutputMapping = nullSideOutputMapping;
            this.eventKeySlots = eventKeySlots;
            this.dmlFactorSlot = dmlFactorSlot;
        }
    }

    /**
     * Aggregated null-side delta keys and flags indicating positive/negative delta existence.
     */
    private static class NullSideDeltaKeyPlan {
        private final Plan plan;
        private final List<Slot> keySlots;
        private final Slot positiveSlot;
        private final Slot negativeSlot;

        /**
         * Store the aggregated delta-key relation and its positive/negative delta flags.
         */
        private NullSideDeltaKeyPlan(Plan plan, List<Slot> keySlots, Slot positiveSlot, Slot negativeSlot) {
            this.plan = plan;
            this.keySlots = keySlots;
            this.positiveSlot = positiveSlot;
            this.negativeSlot = negativeSlot;
        }
    }

}
