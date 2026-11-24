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
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Delta rewrite handler for the restricted LEFT/RIGHT/FULL OUTER JOIN topology.
 *
 * <p>Each child subtree may contribute a delta relation. Each contribution is expanded against the corresponding
 * pre- or post-refresh snapshot, then all contributions are combined. The side carrying an individual delta
 * determines which rows can appear or disappear:
 * <ul>
 *   <li>delta side is retained by the join: keep its unmatched rows with LEFT/RIGHT OUTER JOIN</li>
 *   <li>delta side is the null side: emit joined rows, plus repair rows for old/new null-side MV rows</li>
 * </ul>
 *
 * <p>For each contribution below, B is the changing input and A is the opposite snapshot. In LEFT/RIGHT
 * joins these cases apply when B is the null-producing side. A delta on the preserved side uses the
 * original directional outer join directly. For FULL joins either side can play B.
 *
 * <pre>
 * Case 1. Pure deterministic equality. B's keys support value-preserving grouping
 *         and the equality slots prove B unique.
 *   LEFT/RIGHT, B changes. Join A once to the union of B detail and signed key repair events.
 *   FULL, B changes. Keep unmatched B rows in outer join detail. Join A separately to signed key repairs.
 *
 * Case 2. Pure deterministic equality. B's keys support value-preserving grouping
 *         and B's uniqueness is unproven.
 *   LEFT/RIGHT, B changes. Join A once to the union of B detail and two pre/post key ANTI events.
 *   FULL, B changes. Keep unmatched B rows in outer join detail. Join A to the two key ANTI events.
 *
 * Case 3. Deterministic equality plus a residual predicate. The equality slots prove B unique,
 *         A is repeatable and A's ON inputs support value-preserving grouping.
 *   LEFT/RIGHT, B changes. Inner join detail plus signed repair per A predicate tuple.
 *   FULL, B changes. Directional outer join detail plus the same tuple repair.
 *
 * Case 4. All remaining conditions, including equality keys that cannot be grouped safely.
 *   LEFT/RIGHT, B changes. Inner join detail plus pre/post SEMI/ANTI repair.
 *   FULL, B changes. Directional outer join detail plus the same SEMI/ANTI repair.
 * </pre>
 *
 * <p>Equality here means extractable, ordinary equality keys. Uniqueness must hold for direct B slots
 * on the original relation. A signed repair emits the negative change in match count, since a unique
 * key limits each A row to zero or one match in each snapshot.
 */
class IvmJoinDeltaHandler {

    private static final String NULL_SIDE_INSERT_DELTA_ALIAS = "__DORIS_IVM_NULL_SIDE_INSERT_DELTA__";
    private static final String NULL_SIDE_DELETE_DELTA_ALIAS = "__DORIS_IVM_NULL_SIDE_DELETE_DELTA__";
    private static final String NULL_SIDE_DETAIL_DELTA_ALIAS = "__DORIS_IVM_NULL_SIDE_DETAIL_DELTA__";
    private static final String NULL_SIDE_EVENT_KEY_PREFIX = "__DORIS_IVM_NULL_SIDE_EVENT_KEY_";
    private static final String NULL_SIDE_KEY_DELTA_ALIAS = "__DORIS_IVM_NULL_SIDE_KEY_DELTA__";
    private static final String NULL_SIDE_KEY_NET_COUNT_ALIAS = "__DORIS_IVM_NULL_SIDE_NET_COUNT__";
    private static final String NULL_SIDE_KEY_POSITIVE_ALIAS = "__DORIS_IVM_NULL_SIDE_KEY_POSITIVE__";
    private static final String NULL_SIDE_KEY_NEGATIVE_ALIAS = "__DORIS_IVM_NULL_SIDE_KEY_NEGATIVE__";
    private static final String NULL_SIDE_PRE_SNAPSHOT_ALIAS = "__DORIS_IVM_NULL_SIDE_PRE_SNAPSHOT__";
    private static final String NULL_SIDE_POST_SNAPSHOT_ALIAS = "__DORIS_IVM_NULL_SIDE_POST_SNAPSHOT__";

    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;

    Optional<IvmDeltaRewriteResult> rewriteJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
            IvmDeltaRewriteVisitor visitor, IvmIncrRefreshContext context) {
        IvmDeltaRewriteState rewriteState = visitor.getRewriteState();
        List<IvmDeltaRewriteResult> contributions = Lists.newArrayListWithExpectedSize(2);
        for (int i = 0; i < join.children().size(); i++) {
            Optional<IvmDeltaRewriteResult> delta = join.child(i).accept(visitor, context);
            if (delta.isPresent()) {
                contributions.add(rewriteJoinContribution(join, i == 0,
                        rebindDeltaToSource(join.child(i), delta.get()), rewriteState));
            }
        }
        return helper.combineDeltaResults(contributions, join.getOutput());
    }

    private IvmDeltaRewriteResult rebindDeltaToSource(Plan source, IvmDeltaRewriteResult delta) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                source.getOutput().size() + 2);
        for (int i = 0; i < source.getOutput().size(); i++) {
            Slot sourceOutput = source.getOutput().get(i);
            projects.add(new Alias(sourceOutput.getExprId(), delta.plan.getOutput().get(i), sourceOutput.getName()));
        }
        projects.add(delta.dmlFactorSlot);
        projects.add(delta.sequenceSlot);
        LogicalProject<?> project = new LogicalProject<>(projects.build(), delta.plan);
        return new IvmDeltaRewriteResult(project,
                helper.findSlotByName(project.getOutput(), Column.IVM_DML_FACTOR_COL),
                helper.findSlotByName(project.getOutput(), Column.SEQUENCE_COL), delta.maxDeltaIndex);
    }

    private IvmDeltaRewriteResult rewriteJoinContribution(LogicalJoin<? extends Plan, ? extends Plan> join,
            boolean deltaOnLeft, IvmDeltaRewriteResult delta, IvmDeltaRewriteState rewriteState) {
        if (join.getJoinType().isOuterJoin()) {
            SideInput leftInput = deltaOnLeft
                    ? sideInput(join.left(), delta.plan)
                    : sideInput(join.left(), helper.postSnapshot(join.left(), rewriteState));
            SideInput rightInput = deltaOnLeft
                    ? sideInput(join.right(), helper.preSnapshot(join.right(), rewriteState))
                    : sideInput(join.right(), delta.plan);
            return rewriteOuterContribution(join, deltaOnLeft, delta, leftInput, rightInput, rewriteState);
        } else if (deltaOnLeft) {
            Pair<Plan, Map<Slot, Slot>> rightPreSnapshot = helper.preSnapshot(join.right(), rewriteState);
            return rewriteInnerJoinContribution(join, delta.plan,
                    helper.mapBusinessOutputs(join.left().getOutput(), delta.plan.getOutput()),
                    rightPreSnapshot.first, rightPreSnapshot.second, delta);
        } else {
            Pair<Plan, Map<Slot, Slot>> leftPostSnapshot = helper.postSnapshot(join.left(), rewriteState);
            return rewriteInnerJoinContribution(join, leftPostSnapshot.first,
                    helper.mapBusinessOutputs(join.left().getOutput(), leftPostSnapshot.first.getOutput()), delta.plan,
                    helper.mapBusinessOutputs(join.right().getOutput(), delta.plan.getOutput()), delta);
        }
    }

    private IvmDeltaRewriteResult rewriteOuterContribution(LogicalJoin<? extends Plan, ? extends Plan> join,
            boolean deltaOnLeft, IvmDeltaRewriteResult delta, SideInput leftInput, SideInput rightInput,
            IvmDeltaRewriteState rewriteState) {
        boolean preservedSideDelta = (deltaOnLeft && join.getJoinType().isLeftOuterJoin())
                || (!deltaOnLeft && join.getJoinType().isRightOuterJoin());
        if (preservedSideDelta) {
            return rewritePreservedSideDelta(join, delta, leftInput, rightInput);
        }
        Plan deltaSideChild = deltaOnLeft ? join.left() : join.right();
        return rewriteNullSideDelta(new NullSideDeltaContext(join, deltaOnLeft, delta, leftInput, rightInput,
                deltaSideChild, rewriteState, delta.maxDeltaIndex));
    }

    private IvmDeltaRewriteResult rewriteInnerJoinContribution(LogicalJoin<? extends Plan, ? extends Plan> join,
            Plan leftPlan, Map<Slot, Slot> leftMapping, Plan rightPlan, Map<Slot, Slot> rightMapping,
            IvmDeltaRewriteResult delta) {
        Map<Slot, Slot> slotMapping = new HashMap<>(leftMapping);
        slotMapping.putAll(rightMapping);
        helper.mapJoinConjunctInputs(join, leftPlan, rightPlan, slotMapping);
        LogicalJoin<Plan, Plan> rewrittenJoin = new LogicalJoin<>(join.getJoinType(),
                ExpressionUtils.replace(join.getHashJoinConjuncts(), slotMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), slotMapping), join.getDistributeHint(),
                leftPlan, rightPlan, JoinReorderContext.EMPTY);
        Slot dmlFactorSlot = helper.findSlotByName(rewrittenJoin.getOutput(), Column.IVM_DML_FACTOR_COL);
        Slot sequenceSlot = helper.findSlotByName(rewrittenJoin.getOutput(), Column.SEQUENCE_COL);
        IvmDeltaRewriteResult joinResult = new IvmDeltaRewriteResult(rewrittenJoin,
                dmlFactorSlot, sequenceSlot, delta.maxDeltaIndex);
        return projectJoinContribution(join, joinResult, slotMapping);
    }

    private IvmDeltaRewriteResult projectJoinContribution(LogicalJoin<? extends Plan, ? extends Plan> join,
            IvmDeltaRewriteResult joinResult, Map<Slot, Slot> slotMapping) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        for (Slot output : join.getOutput()) {
            projects.add(new Alias(output.getExprId(), slotMapping.get(output), output.getName()));
        }
        projects.add(joinResult.dmlFactorSlot);
        projects.add(joinResult.sequenceSlot);
        LogicalProject<Plan> project = new LogicalProject<>(projects.build(), joinResult.plan);
        return new IvmDeltaRewriteResult(project,
                helper.findSlotByName(project.getOutput(), Column.IVM_DML_FACTOR_COL),
                helper.findSlotByName(project.getOutput(), Column.SEQUENCE_COL), joinResult.maxDeltaIndex);
    }

    private SideInput sideInput(Plan source, Plan rewritten) {
        return new SideInput(rewritten, helper.mapBusinessOutputs(source.getOutput(), rewritten.getOutput()));
    }

    private SideInput sideInput(Plan source, Pair<Plan, Map<Slot, Slot>> snapshot) {
        return new SideInput(snapshot.first, helper.mapBusinessOutputs(source.getOutput(), snapshot.first.getOutput()));
    }

    /**
     * LEFT/RIGHT OUTER JOIN non-null-side delta keeps dangling delta-side rows directly.
     *
     * <p>This path is only for LEFT JOIN left delta and RIGHT JOIN right delta. The final root-level guard handles
     * any non-deterministic MV row-id after all join contributions have been merged.
     */
    private IvmDeltaRewriteResult rewritePreservedSideDelta(LogicalJoin<? extends Plan, ? extends Plan> join,
            IvmDeltaRewriteResult delta, SideInput leftInput, SideInput rightInput) {
        Map<Slot, Slot> slotMapping = new HashMap<>(leftInput.mapping);
        slotMapping.putAll(rightInput.mapping);
        LogicalJoin<Plan, Plan> rewrittenJoin = new LogicalJoin<>(join.getJoinType(),
                ExpressionUtils.replace(join.getHashJoinConjuncts(), slotMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), slotMapping), join.getDistributeHint(),
                leftInput.plan, rightInput.plan, JoinReorderContext.EMPTY);
        IvmDeltaRewriteResult joinResult = new IvmDeltaRewriteResult(rewrittenJoin,
                helper.findSlotByName(rewrittenJoin.getOutput(), Column.IVM_DML_FACTOR_COL),
                helper.findSlotByName(rewrittenJoin.getOutput(), Column.SEQUENCE_COL), delta.maxDeltaIndex);
        return projectJoinContribution(join, joinResult, slotMapping);
    }

    /**
     * Delta from the null side may change both joined rows and null-side rows.
     *
     * <p>LEFT/RIGHT OUTER JOIN null-side delta can use the event path for pure deterministic equality with
     * value-preserving keys. FULL OUTER JOIN keeps unmatched delta rows in a separate outer-join branch
     * and probes only the repair events.
     */
    private IvmDeltaRewriteResult rewriteNullSideDelta(NullSideDeltaContext deltaContext) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        EquiJoinKeys equiJoinKeys = extractEquiJoinKeys(join);
        // Both event paths group the delta keys before probing the retained side. Grouping can merge
        // floating-point signed zeros that a hash join distinguishes.
        if (equiJoinKeys != null && !equiJoinKeys.hasResidual
                && deltaContext.deltaSideKeyExpressions(equiJoinKeys).stream()
                        .allMatch(key -> key.getDataType().supportsValuePreservingGrouping())) {
            return rewriteNullSideDeltaWithNullSideEvents(deltaContext, equiJoinKeys);
        }
        if (equiJoinKeys != null && equiJoinKeys.hasResidual) {
            Set<Slot> retainedInputs = deltaContext.isDeltaOnLeft
                    ? join.getRightConditionSlot() : join.getLeftConditionSlot();
            if (hasUniqueNullSideKeys(deltaContext, equiJoinKeys)
                    && retainedInputs.stream().allMatch(slot -> slot.getDataType().supportsValuePreservingGrouping())
                    && isRepeatableRetainedInput(deltaContext)) {
                return combineWithJoinedRows(deltaContext,
                        ImmutableList.of(buildMatchedTupleRepair(deltaContext, retainedInputs)));
            }
        }
        return combineWithJoinedRows(deltaContext, buildNullSideRepairProjects(deltaContext));
    }

    private IvmDeltaRewriteResult combineWithJoinedRows(NullSideDeltaContext deltaContext, List<Plan> repairs) {
        Plan joinedProject = rewriteNullSideJoinedRowsDelta(deltaContext);
        LogicalUnion union = helper.buildUnionAll(ImmutableList.<Plan>builder().add(joinedProject)
                .addAll(repairs).build());
        LogicalProject<Plan> outputProject = helper.projectUnionOutputs(union, joinedProject.getOutput());
        Slot dmlFactor = findSlotByName(outputProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        Slot sequence = findSlotByName(outputProject.getOutput(), Column.SEQUENCE_COL);
        return new IvmDeltaRewriteResult(outputProject, dmlFactor, sequence,
                deltaContext.deltaSideResult().maxDeltaIndex);
    }

    /**
     * Build the joined-row delta branch for the general NULL-row repair rewrite.
     *
     * <p>LEFT/RIGHT OUTER JOIN uses INNER JOIN. FULL OUTER JOIN uses LEFT/RIGHT OUTER JOIN selected by the delta
     * side, so unmatched delta rows are emitted by this first branch.
     */
    private Plan rewriteNullSideJoinedRowsDelta(NullSideDeltaContext deltaContext) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        JoinType joinType = join.getJoinType().isFullOuterJoin()
                ? (deltaContext.isDeltaOnLeft ? JoinType.LEFT_OUTER_JOIN : JoinType.RIGHT_OUTER_JOIN)
                : JoinType.INNER_JOIN;
        Map<Slot, Slot> slotMapping = new HashMap<>(deltaContext.leftInput.mapping);
        slotMapping.putAll(deltaContext.rightInput.mapping);
        LogicalJoin<Plan, Plan> newJoin = new LogicalJoin<>(joinType,
                ExpressionUtils.replace(join.getHashJoinConjuncts(), slotMapping),
                ExpressionUtils.replace(join.getOtherJoinConjuncts(), slotMapping), join.getDistributeHint(),
                deltaContext.leftInput.plan, deltaContext.rightInput.plan, JoinReorderContext.EMPTY);
        // The joined-row branch changes the join type, so its output slots/schema are not the same as the
        // original outer join output. Project it back before unioning with the repair branches.
        return projectJoinContribution(join,
                new IvmDeltaRewriteResult(newJoin,
                        helper.findSlotByName(newJoin.getOutput(), Column.IVM_DML_FACTOR_COL),
                        helper.findSlotByName(newJoin.getOutput(), Column.SEQUENCE_COL),
                        deltaContext.deltaSideResult().maxDeltaIndex), slotMapping).plan;
    }

    private boolean isRepeatableRetainedInput(NullSideDeltaContext context) {
        // ON may reference an alias such as UUID() AS tag. Its slot looks deterministic, but the
        // match aggregate and the payload lookup evaluate the retained subtree independently.
        Plan retained = context.isDeltaOnLeft ? context.join.right() : context.join.left();
        return !retained.anyMatch(node -> ((Plan) node).getExpressions().stream()
                .anyMatch(Expression::containsNondeterministic));
    }

    /**
     * Retained rows with identical ON inputs match exactly the same delta rows. For n such rows
     * the signed sum is n * (post - pre), where each endpoint is zero or one by delta-side uniqueness.
     * Its sign gives one repair per original retained row, independent of multiplicity or row-id.
     */
    private Plan buildMatchedTupleRepair(NullSideDeltaContext context, Set<Slot> retainedInputs) {
        Pair<Plan, Map<Slot, Slot>> retained = freshSideInput(context.nonDeltaSideInput());
        Pair<Plan, Map<Slot, Slot>> delta = helper.freshPlan(context.deltaSideResult().plan);
        Map<Slot, Slot> mapping = new HashMap<>(retained.second);
        // The delta contribution was rebound to the source output ExprIds before rewriting this join.
        mapping.putAll(delta.second);
        LogicalJoin<Plan, Plan> matches = new LogicalJoin<>(JoinType.INNER_JOIN,
                ExpressionUtils.replace(context.join.getHashJoinConjuncts(), mapping),
                ExpressionUtils.replace(context.join.getOtherJoinConjuncts(), mapping),
                context.join.getDistributeHint(), retained.first, delta.first, JoinReorderContext.EMPTY);
        Pair<Plan, Map<Slot, Slot>> payload = freshSideInput(context.nonDeltaSideInput());
        ImmutableList.Builder<Slot> groupBy = ImmutableList.builder();
        ImmutableList.Builder<Expression> lookup = ImmutableList.builder();
        for (Slot input : retainedInputs) {
            Slot key = retained.second.get(input);
            groupBy.add(key);
            // A residual such as COALESCE(A.threshold, 0) can match when an input is NULL.
            lookup.add(new NullSafeEqual(payload.second.get(input), key));
        }
        List<Slot> tuple = groupBy.build();
        Alias netCount = new Alias(new Sum(delta.second.get(context.deltaSideResult().dmlFactorSlot)),
                NULL_SIDE_KEY_NET_COUNT_ALIAS);
        Plan aggregate = new LogicalAggregate<>(ImmutableList.copyOf(tuple),
                ImmutableList.<NamedExpression>builder().addAll(tuple).add(netCount).build(), matches);
        Plan changedRows = new LogicalJoin<>(JoinType.INNER_JOIN, lookup.build(), payload.first,
                changedNetCounts(aggregate, netCount), JoinReorderContext.EMPTY);
        Expression factor = new If(new GreaterThan(netCount.toSlot(), new BigIntLiteral(0)),
                new TinyIntLiteral((byte) -1), new TinyIntLiteral((byte) 1));
        return projectNullSideRepairOutputs(context, changedRows, factor, payload.second);
    }

    /**
     * Build the two NULL-row repair projects for null-side delta.
     *
     * <p>The insert branch uses the null-side pre-refresh snapshot and emits {@code -1} for retained rows that move
     * from a null-side row to a matched row. The delete branch uses the post-refresh snapshot and emits {@code +1}
     * for retained rows that move from matched to a null-side row.
     */
    private List<Plan> buildNullSideRepairProjects(
            NullSideDeltaContext deltaContext) {
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
                freshSnapshot(deltaContext.deltaSideSnapshot(false)), NULL_SIDE_PRE_SNAPSHOT_ALIAS));
        Pair<Plan, Map<Slot, Slot>> nullSidePostSnapshot = helper.remapOutputs(helper.aliasPlan(
                freshSnapshot(deltaContext.deltaSideSnapshot(true)), NULL_SIDE_POST_SNAPSHOT_ALIAS));
        LogicalProject<Plan> preNullProject = buildNullSideRepairProject(deltaContext,
                freshSideInput(deltaContext.nonDeltaSideInput()),
                insertedNullSideDelta.second, nullSideInserts, nullSidePreSnapshot,
                new TinyIntLiteral((byte) -1));
        LogicalProject<Plan> postNullProject = buildNullSideRepairProject(deltaContext,
                freshSideInput(deltaContext.nonDeltaSideInput()),
                deletedNullSideDelta.second, nullSideDeletes, nullSidePostSnapshot,
                new TinyIntLiteral((byte) 1));
        return ImmutableList.of(preNullProject, postNullProject);
    }

    /**
     * Optimized null-side delta rewrite that encodes joined-row changes and NULL-row repair as key events.
     */
    private IvmDeltaRewriteResult rewriteNullSideDeltaWithNullSideEvents(
            NullSideDeltaContext deltaContext, EquiJoinKeys equiJoinKeys) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        // A one-sided outer join probes detail and repair events together. FULL must also emit
        // unmatched delta rows, so its detail branch uses an outer join and only repairs use this probe.
        boolean fullOuter = join.getJoinType().isFullOuterJoin();
        List<Slot> targetOutputs = buildOuterDeltaOutputs(deltaContext);
        Pair<Plan, Map<Slot, Slot>> retainedSnapshot = freshSideInput(deltaContext.nonDeltaSideInput());
        NullSideEventPlan nullSideEvents = buildNullSideEventPlan(deltaContext, equiJoinKeys, fullOuter);

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
        LogicalProject<Plan> outputProject = projectEventJoinOutputs(targetOutputs,
                eventJoin, retainedSnapshot.second, nullSideEvents.nullSideOutputMapping,
                nullSideEvents.dmlFactorSlot, nullSideEvents.sequenceSlot, fullOuter);
        if (fullOuter) {
            return combineWithJoinedRows(deltaContext, ImmutableList.of(outputProject));
        }
        Slot dmlFactor = findSlotByName(outputProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        Slot sequence = findSlotByName(outputProject.getOutput(), Column.SEQUENCE_COL);
        return new IvmDeltaRewriteResult(outputProject, dmlFactor, sequence,
                deltaContext.deltaSideResult().maxDeltaIndex);
    }

    private List<Slot> buildOuterDeltaOutputs(NullSideDeltaContext deltaContext) {
        List<Slot> outputs = Lists.newArrayList(deltaContext.join.getOutput());
        outputs.add(deltaContext.deltaSideResult().dmlFactorSlot);
        outputs.add(deltaContext.deltaSideResult().sequenceSlot);
        return outputs;
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
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM outer join rewrite found unknown output slot: " + slot);
            }
        }
        projects.add(new Alias(dmlFactor, Column.IVM_DML_FACTOR_COL));
        projects.add(new Alias(deltaContext.paddingSequence(dmlFactor), Column.SEQUENCE_COL));
        return new LogicalProject<>(projects.build(), source);
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
            NullSideDeltaContext deltaContext, EquiJoinKeys equiJoinKeys, boolean repairsOnly) {
        ImmutableList.Builder<Plan> events = ImmutableList.builder();
        if (!repairsOnly) {
            events.add(buildNullSideDetailEvent(deltaContext, equiJoinKeys));
        }
        if (hasUniqueNullSideKeys(deltaContext, equiJoinKeys)) {
            events.add(buildUniqueKeyRepairEvent(deltaContext, equiJoinKeys));
        } else {
            Plan preNullEvent = buildNullSideRepairEvent(deltaContext, equiJoinKeys, false,
                    new TinyIntLiteral((byte) -1));
            Plan postNullEvent = buildNullSideRepairEvent(deltaContext, equiJoinKeys, true,
                    new TinyIntLiteral((byte) 1));
            events.add(preNullEvent);
            events.add(postNullEvent);
        }
        List<Plan> branches = events.build();
        Plan eventPlan = branches.size() == 1 ? branches.get(0) : helper.buildUnionAll(branches);

        List<Slot> unionOutputs = eventPlan.getOutput();
        Map<Slot, Slot> nullSideOutputMapping = new HashMap<>();
        int nullSideOutputStart = deltaContext.deltaSideKeyExpressions(equiJoinKeys).size();
        int nullSideOutputIndex = 0;
        for (Slot slot : nullSideValueSlots(deltaContext)) {
            nullSideOutputMapping.put(slot, unionOutputs.get(nullSideOutputStart + nullSideOutputIndex));
            nullSideOutputIndex++;
        }
        List<Slot> eventKeySlots = unionOutputs.subList(0, deltaContext.deltaSideKeyExpressions(equiJoinKeys).size());
        Slot dmlFactorSlot = unionOutputs.get(unionOutputs.size() - 2);
        Slot sequenceSlot = unionOutputs.get(unionOutputs.size() - 1);
        return new NullSideEventPlan(eventPlan, nullSideOutputMapping, eventKeySlots,
                dmlFactorSlot, sequenceSlot);
    }

    /**
     * Use the snapshot relation's proof, never the incremental stream: a stream can contain several versions of
     * the same key. Only direct slots are accepted; a non-injective expression over a unique slot need not be unique.
     */
    private boolean hasUniqueNullSideKeys(NullSideDeltaContext context, EquiJoinKeys keys) {
        Set<Slot> keySlots = new HashSet<>();
        for (Expression key : context.deltaSideKeyExpressions(keys)) {
            if (!(key instanceof Slot)) {
                return false;
            }
            keySlots.add((Slot) key);
        }
        Plan nullSide = context.isDeltaOnLeft ? context.join.left() : context.join.right();
        return nullSide.getLogicalProperties().getTrait().isUnique(keySlots);
    }

    /**
     * For a unique matching key, pre/post multiplicities are in {0, 1}. Hence the NULL-row change is exactly
     * -(post - pre), without querying either snapshot. Detail changes remain in their own UNION branch.
     */
    private Plan buildUniqueKeyRepairEvent(NullSideDeltaContext context, EquiJoinKeys keys) {
        Pair<Plan, Map<Slot, Slot>> delta = helper.remapOutputs(helper.aliasPlan(
                helper.freshPlan(context.deltaSideResult().plan), NULL_SIDE_KEY_DELTA_ALIAS));
        ImmutableList.Builder<Expression> groupBy = ImmutableList.builder();
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        ImmutableSet.Builder<Expression> nonNullKeys = ImmutableSet.builder();
        List<Expression> keyExpressions = context.deltaSideKeyExpressions(keys);
        for (int i = 0; i < keyExpressions.size(); i++) {
            Expression key = ExpressionUtils.replace(keyExpressions.get(i), delta.second);
            groupBy.add(key);
            outputs.add(new Alias(key, eventKeyName(i)));
            // Ordinary equality cannot match NULL keys. Filter only repair candidates, never detail events.
            nonNullKeys.add(new Not(new IsNull(key)));
        }
        Alias netCount = new Alias(new Sum(delta.second.get(context.deltaSideResult().dmlFactorSlot)),
                NULL_SIDE_KEY_NET_COUNT_ALIAS);
        outputs.add(netCount);
        Plan aggregate = new LogicalAggregate<>(groupBy.build(), outputs.build(),
                new LogicalFilter<>(nonNullKeys.build(), delta.first));
        return projectNullSideRepairEvent(context, changedNetCounts(aggregate, netCount),
                aggregate.getOutput().subList(0, keyExpressions.size()), repairFactor(netCount));
    }

    private Plan changedNetCounts(Plan aggregate, Alias netCount) {
        return new LogicalFilter<>(ImmutableSet.of(new Not(new EqualTo(netCount.toSlot(), new BigIntLiteral(0)))),
                aggregate);
    }

    private Expression repairFactor(Alias netCount) {
        // Unique endpoints constrain the matched-row count change to -1/0/+1.
        return new Cast(new Subtract(new BigIntLiteral(0), netCount.toSlot()), TinyIntType.INSTANCE);
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
        projects.add(new Alias(nullSideDelta.second.get(deltaContext.deltaSideResult().sequenceSlot),
                Column.SEQUENCE_COL));
        return new LogicalProject<>(projects.build(), nullSideDelta.first);
    }

    /**
     * Build one NULL-row repair event branch for affected null-side keys.
     *
     * preSnapshot branch: null-side inserts with no pre-existing match emit dml_factor = -1.
     * postSnapshot branch: null-side deletes with no remaining match emit dml_factor = +1.
     */
    private Plan buildNullSideRepairEvent(
            NullSideDeltaContext deltaContext, EquiJoinKeys equiJoinKeys, boolean postSnapshot,
            Expression dmlFactor) {
        LogicalJoin<? extends Plan, ? extends Plan> join = deltaContext.join;
        NullSideDeltaKeyPlan deltaKeys = buildNullSideDeltaKeyPlan(deltaContext, equiJoinKeys);
        Slot flagSlot = postSnapshot ? deltaKeys.negativeSlot : deltaKeys.positiveSlot;
        Plan affectedKeys = new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(flagSlot, new TinyIntLiteral((byte) 0))), deltaKeys.plan);
        String snapshotAlias = postSnapshot ? NULL_SIDE_POST_SNAPSHOT_ALIAS : NULL_SIDE_PRE_SNAPSHOT_ALIAS;
        Pair<Plan, Map<Slot, Slot>> nullSideSnapshot = helper.remapOutputs(helper.aliasPlan(
                freshSnapshot(deltaContext.deltaSideSnapshot(postSnapshot)), snapshotAlias));

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

        return projectNullSideRepairEvent(deltaContext, antiJoin, deltaKeys.keySlots, dmlFactor);
    }

    private Plan projectNullSideRepairEvent(NullSideDeltaContext deltaContext, Plan source,
            List<Slot> keySlots, Expression dmlFactor) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        for (int i = 0; i < keySlots.size(); i++) {
            projects.add(new Alias(keySlots.get(i), eventKeyName(i)));
        }
        for (Slot slot : nullSideValueSlots(deltaContext)) {
            projects.add(new Alias(new NullLiteral(slot.getDataType()), slot.getName()));
        }
        projects.add(new Alias(dmlFactor, Column.IVM_DML_FACTOR_COL));
        projects.add(new Alias(deltaContext.paddingSequence(dmlFactor), Column.SEQUENCE_COL));
        return new LogicalProject<>(projects.build(), source);
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
            Map<Slot, Slot> retainedOutputMapping, Map<Slot, Slot> nullSideOutputMapping,
            Slot dmlFactorSlot, Slot sequenceSlot, boolean freshOutputs) {
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                targetOutputs.size());
        for (Slot target : targetOutputs) {
            Expression expr;
            if (Column.IVM_DML_FACTOR_COL.equals(target.getName())) {
                expr = dmlFactorSlot;
            } else if (Column.SEQUENCE_COL.equals(target.getName())) {
                expr = sequenceSlot;
            } else {
                expr = retainedOutputMapping.get(target);
                if (expr == null) {
                    expr = nullSideOutputMapping.get(target);
                }
            }
            if (expr == null) {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM outer join event rewrite lost output slot: " + target);
            }
            projects.add(freshOutputs ? new Alias(expr, target.getName()) : new Alias(target.getExprId(), expr,
                    target.getName()));
        }
        return new LogicalProject<>(projects.build(), source);
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
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM outer join rewrite lost retained output slot: " + slot);
        }
        return expr;
    }

    /**
     * Extract deterministic equi-join keys from both hash conjuncts and other conjuncts.
     *
     * Record residual predicates separately: their truth can depend on the individual retained row.
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
        if (conjuncts.stream().anyMatch(Expression::containsNondeterministic)) {
            return null;
        }
        Pair<List<Expression>, List<Expression>> extractedConjuncts = JoinUtils.extractExpressionForHashTable(
                join.left().getOutput(), join.right().getOutput(), conjuncts);
        if (extractedConjuncts.first.isEmpty()) {
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
        return new EquiJoinKeys(leftKeys.build(), rightKeys.build(), !extractedConjuncts.second.isEmpty());
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
     * Return null-side output slots carried by null-side events, excluding synthetic dml factor and op type.
     */
    private List<Slot> nullSideValueSlots(NullSideDeltaContext deltaContext) {
        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        for (Slot slot : deltaContext.deltaSideResult().plan.getOutput()) {
            if (!Column.IVM_DML_FACTOR_COL.equals(slot.getName())
                    && !Column.SEQUENCE_COL.equals(slot.getName())) {
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

    private Pair<Plan, Map<Slot, Slot>> freshSnapshot(Pair<Plan, Map<Slot, Slot>> source) {
        Pair<Plan, Map<Slot, Slot>> copied = helper.freshPlan(source.first);
        Map<Slot, Slot> mapping = new HashMap<>();
        for (Map.Entry<Slot, Slot> entry : source.second.entrySet()) {
            mapping.put(entry.getKey(), copied.second.get(entry.getValue()));
        }
        return Pair.of(copied.first, mapping);
    }

    private Pair<Plan, Map<Slot, Slot>> freshSideInput(SideInput source) {
        return freshSnapshot(Pair.of(source.plan, source.mapping));
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
        private final boolean hasResidual;

        /**
         * Store key expressions in physical join-child order.
         */
        private EquiJoinKeys(List<Expression> leftExpressions, List<Expression> rightExpressions, boolean hasResidual) {
            this.leftExpressions = leftExpressions;
            this.rightExpressions = rightExpressions;
            this.hasResidual = hasResidual;
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
        private final IvmDeltaRewriteResult deltaSideResult;
        private final SideInput leftInput;
        private final SideInput rightInput;
        private final Plan deltaSideSource;
        private final IvmDeltaRewriteState rewriteState;
        private final int maxDeltaIndex;

        /**
         * Map physical left/right results to rewrite-local delta and non-delta roles.
         */
        private NullSideDeltaContext(LogicalJoin<? extends Plan, ? extends Plan> join,
                boolean deltaOnLeft, IvmDeltaRewriteResult deltaSideResult,
                SideInput leftInput, SideInput rightInput,
                Plan deltaSideSource, IvmDeltaRewriteState rewriteState,
                int maxDeltaIndex) {
            this.join = join;
            this.isDeltaOnLeft = deltaOnLeft;
            this.deltaSideResult = deltaSideResult;
            this.leftInput = leftInput;
            this.rightInput = rightInput;
            this.deltaSideSource = deltaSideSource;
            this.rewriteState = rewriteState;
            this.maxDeltaIndex = maxDeltaIndex;
        }

        /**
         * Return the rewrite result for the physical side carrying delta rows.
         */
        private IvmDeltaRewriteResult deltaSideResult() {
            return deltaSideResult;
        }

        // Unique-key event repair does not need either null-side snapshot.
        private Pair<Plan, Map<Slot, Slot>> deltaSideSnapshot(boolean postSnapshot) {
            return postSnapshot ? IvmDeltaRewriteHelper.INSTANCE.postSnapshot(deltaSideSource, rewriteState)
                    : IvmDeltaRewriteHelper.INSTANCE.preSnapshot(deltaSideSource, rewriteState);
        }

        private Expression paddingSequence(Expression dmlFactor) {
            // A joined subtree can emit cancelling positive/negative rows absent from both snapshots.
            // Both NULL-row repairs then target the same row-id; keep the positive repair last.
            return rewriteState.toSequenceByDmlFactor(dmlFactor, maxDeltaIndex);
        }

        private SideInput nonDeltaSideInput() {
            return isDeltaOnLeft ? rightInput : leftInput;
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

    private static class SideInput {
        private final Plan plan;
        private final Map<Slot, Slot> mapping;

        private SideInput(Plan plan, Map<Slot, Slot> mapping) {
            this.plan = plan;
            this.mapping = mapping;
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
        private final Slot sequenceSlot;

        /**
         * Store the event relation and the output slots consumed by the final probe projection.
         */
        private NullSideEventPlan(Plan plan, Map<Slot, Slot> nullSideOutputMapping,
                List<Slot> eventKeySlots, Slot dmlFactorSlot, Slot sequenceSlot) {
            this.plan = plan;
            this.nullSideOutputMapping = nullSideOutputMapping;
            this.eventKeySlots = eventKeySlots;
            this.dmlFactorSlot = dmlFactorSlot;
            this.sequenceSlot = sequenceSlot;
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
