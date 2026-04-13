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
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggTarget;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.analysis.BindRelation;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Greatest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Least;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Aggregate delta rewrite strategy for IVM.
 *
 * <p>Handles single-table aggregate MVs with count/sum/avg/min/max.
 * Min/max use an assert_true guard: if a deleted row matches the current extreme,
 * execution fails and IvmRefreshManager falls back to COMPLETE refresh.
 *
 * <h3>Overall flow</h3>
 * <ol>
 *   <li><b>Delta sub-plan</b>: transforms the normalized aggregate into a signed delta aggregate
 *       where each output is weighted by {@code dml_factor} (+1 for inserts, -1 for deletes).</li>
 *   <li><b>Apply plan</b>: LEFT JOINs the delta against the MV's current state on {@code row_id},
 *       computes new hidden states (COALESCE(old,0) + delta), derives visible values, and
 *       determines the {@code __DORIS_DELETE_SIGN__}.</li>
 *   <li><b>Insert command</b>: wraps the result in an {@code InsertIntoTableCommand} that writes
 *       back to the MV via MOW upsert semantics.</li>
 * </ol>
 *
 * <h3>Visitor integration</h3>
 * <p>Inherits from {@link IvmSimpleScanDeltaStrategy} and overrides:
 * <ul>
 *   <li>{@code visitLogicalProject}: skips the normalize top-project when its child is an aggregate,
 *       since the aggregate visitor produces a complete replacement plan.</li>
 *   <li>{@code visitLogicalAggregate}: main entry point that builds delta + apply + insert.</li>
 * </ul>
 *
 * @see IvmSimpleScanDeltaStrategy
 */
public class IvmAggDeltaStrategy extends IvmSimpleScanDeltaStrategy {

    /** Set during rewrite(), used by visitor methods. Single-use per instance. */
    private IvmDeltaRewriteContext ctx;

    /**
     * Intermediate result from {@link #buildDeltaSubPlan}.
     * Carries the delta aggregate project plus slot mappings needed by {@link #buildApplyPlan}.
     */
    private static final class DeltaPlanParts {
        /** Top project above the delta aggregate: [row_id, group_keys, delta_agg_outputs...] */
        private final LogicalProject<?> topDeltaProject;
        /** Row-id slot from the top project (hash of group keys, or 0 for scalar). */
        private final Slot rowIdSlot;
        /**
         * Semantic slot map keyed by "{ordinal}:{stateType}" (e.g. "2:SUM").
         * Maps each per-target delta output to the corresponding slot in topDeltaProject.
         * Also contains the delta_group_count under key {@link Column#IVM_DELTA_GROUP_COUNT_COL}.
         */
        private final Map<String, Slot> semanticSlots;
        /** Group key slots resolved from topDeltaProject output, keyed by column name. */
        private final Map<String, Slot> groupKeySlotsByName;

        private DeltaPlanParts(LogicalProject<?> topDeltaProject, Slot rowIdSlot,
                Map<String, Slot> semanticSlots, Map<String, Slot> groupKeySlotsByName) {
            this.topDeltaProject = topDeltaProject;
            this.rowIdSlot = rowIdSlot;
            this.semanticSlots = semanticSlots;
            this.groupKeySlotsByName = groupKeySlotsByName;
        }
    }

    @Override
    public List<IvmDeltaCommandBundle> rewrite(Plan normalizedPlan, IvmDeltaRewriteContext ctx) {
        this.ctx = ctx;
        RewriteResult result = rewritePlan(normalizedPlan);
        Command insertCommand = buildInsertCommandWithDeleteSign(result.plan, ctx);
        return ImmutableList.of(new IvmDeltaCommandBundle(insertCommand));
    }

    /**
     * When the normalize top project sits above an aggregate, skip it entirely —
     * the aggregate visitor produces the complete apply plan.
     */
    @Override
    public RewriteResult visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        if (project.child() instanceof LogicalAggregate) {
            return project.child().accept(this, context);
        }
        return super.visitLogicalProject(project, context);
    }

    /**
     * Core entry point: builds the entire agg delta + apply plan.
     *
     * <p>Steps:
     * 1. Validates normalize result and agg metadata exist.
     * 2. Walks the aggregate's child subtree to inject dml_factor (via super's visitor).
     * 3. Builds the delta sub-plan (signed aggregate).
     * 4. Builds the apply plan (LEFT JOIN + state merge + visible derivation).
     * 5. Returns RewriteResult with null dmlFactorSlot (apply plan is terminal).
     */
    @Override
    public RewriteResult visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, Void context) {
        IvmNormalizeResult normalizeResult = ctx.getNormalizeResult();
        if (normalizeResult == null) {
            throw new AnalysisException("IVM agg delta rewrite requires normalize result");
        }
        IvmAggMeta aggMeta = normalizeResult.getAggMeta();
        if (aggMeta == null) {
            throw new AnalysisException("IVM agg delta rewrite requires agg metadata");
        }

        // Walk agg child to inject dml_factor
        RewriteResult childResult = agg.child().accept(this, context);

        DeltaPlanParts delta = buildDeltaSubPlan(agg, childResult, aggMeta);
        LogicalProject<?> applyProject = buildApplyPlan(delta, aggMeta, ctx);
        return new RewriteResult(applyProject, null);
    }

    /**
     * Builds the delta sub-plan: a signed aggregate over the base table's changes.
     *
     * <p>Input shape (from normalize):
     * <pre>
     *   Aggregate(normalized) → child subtree (with dml_factor injected)
     * </pre>
     *
     * <p>Output shape:
     * <pre>
     *   Project(row_id, group_keys, coalesced delta outputs...)
     *     └── Aggregate(delta: SUM(signed_expr), SUM(case_when_not_null), ...)
     *           └── child subtree with dml_factor
     * </pre>
     *
     * <p>The delta aggregate replaces each original agg function with signed delta expressions:
     * <ul>
     *   <li>COUNT(*): delta = SUM(dml_factor)</li>
     *   <li>COUNT(expr): delta = SUM(IF(expr IS NULL, 0, dml_factor))</li>
     *   <li>SUM(expr): delta_sum = SUM(IF(dml_factor > 0, expr, -expr)),
     *                  delta_count = SUM(IF(expr IS NULL, 0, dml_factor))</li>
     *   <li>AVG(expr): same as SUM (visible value derived later from hidden sum/count)</li>
     * </ul>
     *
     * <p>A top project wraps the aggregate to:
     * 1. Compute row_id (hash of group keys for grouped, 0 for scalar).
     * 2. Apply COALESCE to NULL-susceptible outputs (SUM may return NULL for all-NULL groups).
     */
    private DeltaPlanParts buildDeltaSubPlan(LogicalAggregate<?> normalizedAgg,
            RewriteResult childResult, IvmAggMeta aggMeta) {
        Plan newAggChild = childResult.plan;
        Slot dmlFactorSlot = childResult.dmlFactorSlot;

        List<NamedExpression> deltaAggOutputs = new ArrayList<>();
        int groupKeySize = aggMeta.getGroupKeySlots().size();
        for (Expression groupByExpr : normalizedAgg.getGroupByExpressions()) {
            if (!(groupByExpr instanceof NamedExpression)) {
                throw new AnalysisException("IVM agg delta rewrite requires slot-like group key, but got: "
                        + groupByExpr);
            }
            deltaAggOutputs.add((NamedExpression) groupByExpr);
        }

        Alias deltaGroupCount = new Alias(new Sum(dmlFactorSlot), Column.IVM_DELTA_GROUP_COUNT_COL);
        deltaAggOutputs.add(deltaGroupCount);

        for (AggTarget target : aggMeta.getAggTargets()) {
            switch (target.getAggType()) {
                case COUNT_STAR:
                    break;
                case COUNT_EXPR:
                    deltaAggOutputs.add(new Alias(
                            new Sum(caseWhenExprNotNull(target.getExprSlots().get(0), dmlFactorSlot)),
                            target.getHiddenStateSlot("COUNT").getName()));
                    break;
                case SUM:
                case AVG:
                    deltaAggOutputs.add(new Alias(
                            new Sum(signedExpr(target.getExprSlots().get(0), dmlFactorSlot)),
                            target.getHiddenStateSlot("SUM").getName()));
                    deltaAggOutputs.add(new Alias(
                            new Sum(caseWhenExprNotNull(target.getExprSlots().get(0), dmlFactorSlot)),
                            target.getHiddenStateSlot("COUNT").getName()));
                    break;
                case MIN:
                    // delta_min = MIN over inserts only; delta_del_min = MIN over deletes only (for guard)
                    deltaAggOutputs.add(new Alias(
                            new Min(insertOnlyExpr(target.getExprSlots().get(0), dmlFactorSlot)),
                            target.getHiddenStateSlot("MIN").getName()));
                    deltaAggOutputs.add(new Alias(
                            new Min(deleteOnlyExpr(target.getExprSlots().get(0), dmlFactorSlot)),
                            transientDelHiddenName(target, "DELMIN")));
                    deltaAggOutputs.add(new Alias(
                            new Sum(caseWhenExprNotNull(target.getExprSlots().get(0), dmlFactorSlot)),
                            target.getHiddenStateSlot("COUNT").getName()));
                    break;
                case MAX:
                    // delta_max = MAX over inserts only; delta_del_max = MAX over deletes only (for guard)
                    deltaAggOutputs.add(new Alias(
                            new Max(insertOnlyExpr(target.getExprSlots().get(0), dmlFactorSlot)),
                            target.getHiddenStateSlot("MAX").getName()));
                    deltaAggOutputs.add(new Alias(
                            new Max(deleteOnlyExpr(target.getExprSlots().get(0), dmlFactorSlot)),
                            transientDelHiddenName(target, "DELMAX")));
                    deltaAggOutputs.add(new Alias(
                            new Sum(caseWhenExprNotNull(target.getExprSlots().get(0), dmlFactorSlot)),
                            target.getHiddenStateSlot("COUNT").getName()));
                    break;
                default:
                    throw new AnalysisException("IVM agg delta rewrite does not support aggregate type: "
                            + target.getAggType());
            }
        }

        LogicalAggregate<?> deltaAgg = normalizedAgg.withAggOutputChild(deltaAggOutputs, newAggChild);
        List<NamedExpression> topOutputs = new ArrayList<>();
        Alias rowIdAlias = new Alias(
                IvmUtil.buildRowIdHash(deltaAgg.getOutput().subList(0, groupKeySize)), Column.IVM_ROW_ID_COL);
        topOutputs.add(rowIdAlias);

        for (Slot slot : deltaAgg.getOutput()) {
            if (needsCoalesceInTopProject(slot, aggMeta)) {
                topOutputs.add(new Alias(new Coalesce(slot, zeroOf(slot.getDataType())), slot.getName()));
            } else {
                topOutputs.add(slot);
            }
        }

        LogicalProject<?> topDeltaProject = new LogicalProject<>(ImmutableList.copyOf(topOutputs), deltaAgg);
        Map<String, Slot> outputByName = indexSlotsByName(topDeltaProject.getOutput());
        Map<String, Slot> semanticSlots = new LinkedHashMap<>();
        semanticSlots.put(Column.IVM_DELTA_GROUP_COUNT_COL,
                outputByName.get(Column.IVM_DELTA_GROUP_COUNT_COL));
        for (AggTarget target : aggMeta.getAggTargets()) {
            switch (target.getAggType()) {
                case COUNT_STAR:
                    semanticSlots.put(hiddenKey(target, "COUNT"),
                            outputByName.get(Column.IVM_DELTA_GROUP_COUNT_COL));
                    break;
                case COUNT_EXPR:
                    semanticSlots.put(hiddenKey(target, "COUNT"),
                            outputByName.get(target.getHiddenStateSlot("COUNT").getName()));
                    break;
                case SUM:
                case AVG:
                    semanticSlots.put(hiddenKey(target, "SUM"),
                            outputByName.get(target.getHiddenStateSlot("SUM").getName()));
                    semanticSlots.put(hiddenKey(target, "COUNT"),
                            outputByName.get(target.getHiddenStateSlot("COUNT").getName()));
                    break;
                case MIN:
                    semanticSlots.put(hiddenKey(target, "MIN"),
                            outputByName.get(target.getHiddenStateSlot("MIN").getName()));
                    semanticSlots.put(hiddenKey(target, "DELMIN"),
                            outputByName.get(transientDelHiddenName(target, "DELMIN")));
                    semanticSlots.put(hiddenKey(target, "COUNT"),
                            outputByName.get(target.getHiddenStateSlot("COUNT").getName()));
                    break;
                case MAX:
                    semanticSlots.put(hiddenKey(target, "MAX"),
                            outputByName.get(target.getHiddenStateSlot("MAX").getName()));
                    semanticSlots.put(hiddenKey(target, "DELMAX"),
                            outputByName.get(transientDelHiddenName(target, "DELMAX")));
                    semanticSlots.put(hiddenKey(target, "COUNT"),
                            outputByName.get(target.getHiddenStateSlot("COUNT").getName()));
                    break;
                default:
                    throw new AnalysisException("IVM agg delta rewrite does not support aggregate type: "
                            + target.getAggType());
            }
        }

        Map<String, Slot> groupKeySlotsByName = new LinkedHashMap<>();
        for (Slot groupKey : aggMeta.getGroupKeySlots()) {
            Slot resolved = outputByName.get(groupKey.getName());
            if (resolved == null) {
                throw new AnalysisException("IVM agg delta rewrite failed to resolve delta group key slot: "
                        + groupKey.getName());
            }
            groupKeySlotsByName.put(groupKey.getName(), resolved);
        }

        return new DeltaPlanParts(topDeltaProject, outputByName.get(Column.IVM_ROW_ID_COL),
                semanticSlots, groupKeySlotsByName);
    }

    /**
     * Builds the apply plan: merges delta into MV current state.
     *
     * <p>Plan shape:
     * <pre>
     *   Project(final sink output: [inserted_cols..., __DORIS_DELETE_SIGN__])
     *     └── Filter(net-zero)            // grouped agg only
     *         └── RightOuterJoin(mv.row_id = delta.row_id)
     *             ├── MV current-state scan (with delete-sign filter)  [large, probe side]
     *             └── delta sub-plan                                   [small, build side]
     * </pre>
     *
     * <p>For each column in the MV's inserted column list, computes:
     * <ul>
     *   <li>row_id: from delta side</li>
     *   <li>group keys: from delta side</li>
     *   <li>hidden state: COALESCE(mv_old, 0) + delta (with assert_true for non-negative counts)</li>
     *   <li>visible value: derived from new hidden state (see {@link #buildTargetExpressions})</li>
     * </ul>
     *
     * <p>Delete sign: grouped agg uses IF(new_group_count <= 0, 1, 0);
     * scalar agg always 0 (single row never deleted).
     *
     * <p>Net-zero filter (grouped only): NOT(mv.row_id IS NULL AND delta_group_count <= 0)
     * prevents inserting delete-sign rows for groups that never existed in the MV.
     */
    private LogicalProject<?> buildApplyPlan(DeltaPlanParts delta, IvmAggMeta aggMeta, IvmDeltaRewriteContext ctx) {
        LogicalOlapScan rawMvScan = buildMvScan(ctx.getMtmv(), ctx);
        LogicalPlan mvPlan = BindRelation.checkAndAddDeleteSignFilter(
                rawMvScan, ctx.getConnectContext(), ctx.getMtmv());
        Slot mvRowId = findSlotByName(rawMvScan.getOutput(), Column.IVM_ROW_ID_COL);
        // MV (large) on left as probe side, delta (small) on right as build side.
        LogicalJoin<Plan, Plan> join = new LogicalJoin<>(JoinType.RIGHT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(mvRowId, delta.rowIdSlot)),
                mvPlan, delta.topDeltaProject, JoinReorderContext.EMPTY);
        Plan joinInput = aggMeta.isScalarAgg() ? join : buildNetZeroFilter(join, delta, mvRowId);

        Map<String, Expression> finalByColumnName = new LinkedHashMap<>();
        Expression newGroupCount = assertNonNegative(
                new Add(coalesceMvSlot(rawMvScan, aggMeta.getGroupCountSlot().getName()), deltaGroupCount(delta)),
                "negative group count");
        finalByColumnName.put(Column.IVM_ROW_ID_COL, delta.rowIdSlot);
        finalByColumnName.put(aggMeta.getGroupCountSlot().getName(), newGroupCount);
        for (Slot groupKey : aggMeta.getGroupKeySlots()) {
            finalByColumnName.put(groupKey.getName(), deltaGroupKey(delta, groupKey.getName()));
        }

        for (AggTarget target : aggMeta.getAggTargets()) {
            buildTargetExpressions(finalByColumnName, rawMvScan, delta, target, newGroupCount);
        }

        Expression deleteSign = aggMeta.isScalarAgg()
                ? new TinyIntLiteral((byte) 0)
                : new If(new LessThanEqual(newGroupCount, new BigIntLiteral(0)),
                        new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0));

        List<NamedExpression> finalOutputs = new ArrayList<>();
        for (String columnName : ctx.getMtmv().getInsertedColumnNames()) {
            Expression expr = finalByColumnName.get(columnName);
            if (expr == null) {
                throw new AnalysisException("IVM agg delta rewrite missing sink expression for column: " + columnName);
            }
            finalOutputs.add(aliasIfNeeded(expr, columnName));
        }
        finalOutputs.add(new Alias(deleteSign, Column.DELETE_SIGN));
        return new LogicalProject<>(ImmutableList.copyOf(finalOutputs), joinInput);
    }

    /**
     * Computes new hidden state and visible value for one aggregate target.
     *
     * <p>State merging formula: {@code new_X = COALESCE(mv_old_X, 0) + delta_X}
     *
     * <p>Visible value derivation per type:
     * <ul>
     *   <li>COUNT(*): new_group_count (cast if needed)</li>
     *   <li>COUNT(expr): IF(new_count > 0, new_count, 0)</li>
     *   <li>SUM(expr): IF(new_count > 0, new_sum, NULL)</li>
     *   <li>AVG(expr): IF(new_count > 0, CAST(new_sum / new_count AS visible_type), NULL)</li>
     * </ul>
     *
     * <p>Count values are wrapped with {@link #assertNonNegative} to catch data corruption.
     */
    private void buildTargetExpressions(Map<String, Expression> finalByColumnName, LogicalOlapScan rawMvScan,
            DeltaPlanParts delta, AggTarget target, Expression newGroupCount) {
        switch (target.getAggType()) {
            case COUNT_STAR: {
                Expression newCount = assertNonNegative(new Add(
                        coalesceMvSlot(rawMvScan, target.getHiddenStateSlot("COUNT").getName()),
                        delta.semanticSlots.get(hiddenKey(target, "COUNT"))),
                        "negative hidden count for " + target.getVisibleSlot().getName());
                finalByColumnName.put(target.getHiddenStateSlot("COUNT").getName(), newCount);
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        castIfNeeded(newGroupCount, target.getVisibleSlot().getDataType()));
                return;
            }
            case COUNT_EXPR: {
                Expression newCount = assertNonNegative(new Add(
                        coalesceMvSlot(rawMvScan, target.getHiddenStateSlot("COUNT").getName()),
                        delta.semanticSlots.get(hiddenKey(target, "COUNT"))),
                        "negative hidden count for " + target.getVisibleSlot().getName());
                finalByColumnName.put(target.getHiddenStateSlot("COUNT").getName(), newCount);
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        new If(isPositive(newCount),
                                castIfNeeded(newCount, target.getVisibleSlot().getDataType()),
                                zeroOf(target.getVisibleSlot().getDataType())));
                return;
            }
            case SUM: {
                Expression newSum = new Add(
                        coalesceMvSlot(rawMvScan, target.getHiddenStateSlot("SUM").getName()),
                        delta.semanticSlots.get(hiddenKey(target, "SUM")));
                Expression newCount = assertNonNegative(new Add(
                        coalesceMvSlot(rawMvScan, target.getHiddenStateSlot("COUNT").getName()),
                        delta.semanticSlots.get(hiddenKey(target, "COUNT"))),
                        "negative hidden count for " + target.getVisibleSlot().getName());
                finalByColumnName.put(target.getHiddenStateSlot("SUM").getName(), newSum);
                finalByColumnName.put(target.getHiddenStateSlot("COUNT").getName(), newCount);
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        new If(isPositive(newCount),
                                castIfNeeded(newSum, target.getVisibleSlot().getDataType()),
                                new NullLiteral(target.getVisibleSlot().getDataType())));
                return;
            }
            case AVG: {
                Expression newSum = new Add(
                        coalesceMvSlot(rawMvScan, target.getHiddenStateSlot("SUM").getName()),
                        delta.semanticSlots.get(hiddenKey(target, "SUM")));
                Expression newCount = assertNonNegative(new Add(
                        coalesceMvSlot(rawMvScan, target.getHiddenStateSlot("COUNT").getName()),
                        delta.semanticSlots.get(hiddenKey(target, "COUNT"))),
                        "negative hidden count for " + target.getVisibleSlot().getName());
                finalByColumnName.put(target.getHiddenStateSlot("SUM").getName(), newSum);
                finalByColumnName.put(target.getHiddenStateSlot("COUNT").getName(), newCount);
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        new If(isPositive(newCount),
                                castIfNeeded(new Divide(newSum, newCount),
                                        target.getVisibleSlot().getDataType()),
                                new NullLiteral(target.getVisibleSlot().getDataType())));
                return;
            }
            case MIN: {
                Slot oldMin = findSlotByName(rawMvScan.getOutput(),
                        target.getHiddenStateSlot("MIN").getName());
                Expression deltaMin = delta.semanticSlots.get(hiddenKey(target, "MIN"));
                Expression deltaDelMin = delta.semanticSlots.get(hiddenKey(target, "DELMIN"));
                // Guard: assert_true(deltaDelMin IS NULL OR old_min IS NULL OR deltaDelMin > old_min)
                // If a deleted row's value <= current min, we cannot compute new min → fallback needed.
                Expression delMinGuardCond = new Or(
                        new IsNull(deltaDelMin),
                        new Or(
                                new IsNull(oldMin),
                                new GreaterThan(deltaDelMin, oldMin)));
                Expression delMinGuard = new AssertTrue(delMinGuardCond,
                        new StringLiteral("IVM: deleted row may be current MIN value, fallback to COMPLETE"));

                Expression newCount = assertNonNegative(new Add(
                        coalesceMvSlot(rawMvScan, target.getHiddenStateSlot("COUNT").getName()),
                        delta.semanticSlots.get(hiddenKey(target, "COUNT"))),
                        "negative hidden count for " + target.getVisibleSlot().getName());
                // new_min = LEAST(old_min, delta_min) — both may be NULL (NULL means no values)
                // LEAST propagates NULL if any argument is NULL in Doris, so use COALESCE guard:
                // IF(old_min IS NULL, delta_min, IF(delta_min IS NULL, old_min, LEAST(old_min, delta_min)))
                Expression newMinRaw = new If(new IsNull(oldMin), deltaMin,
                        new If(new IsNull(deltaMin), oldMin, new Least(oldMin, deltaMin)));
                // Embed guard evaluation: false branch uses NullLiteral to prevent IF constant folding.
                // assert_true either returns TRUE (pass) or throws (fail), so false branch is unreachable.
                Expression newMinGuarded = new If(delMinGuard, newMinRaw,
                        new NullLiteral(newMinRaw.getDataType()));
                finalByColumnName.put(target.getHiddenStateSlot("MIN").getName(), newMinGuarded);
                finalByColumnName.put(target.getHiddenStateSlot("COUNT").getName(), newCount);
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        new If(isPositive(newCount),
                                castIfNeeded(newMinGuarded, target.getVisibleSlot().getDataType()),
                                new NullLiteral(target.getVisibleSlot().getDataType())));
                return;
            }
            case MAX: {
                Slot oldMax = findSlotByName(rawMvScan.getOutput(),
                        target.getHiddenStateSlot("MAX").getName());
                Expression deltaMax = delta.semanticSlots.get(hiddenKey(target, "MAX"));
                Expression deltaDelMax = delta.semanticSlots.get(hiddenKey(target, "DELMAX"));
                // Guard: assert_true(deltaDelMax IS NULL OR old_max IS NULL OR deltaDelMax < old_max)
                Expression delMaxGuardCond = new Or(
                        new IsNull(deltaDelMax),
                        new Or(
                                new IsNull(oldMax),
                                new LessThan(deltaDelMax, oldMax)));
                Expression delMaxGuard = new AssertTrue(delMaxGuardCond,
                        new StringLiteral("IVM: deleted row may be current MAX value, fallback to COMPLETE"));

                Expression newCount = assertNonNegative(new Add(
                        coalesceMvSlot(rawMvScan, target.getHiddenStateSlot("COUNT").getName()),
                        delta.semanticSlots.get(hiddenKey(target, "COUNT"))),
                        "negative hidden count for " + target.getVisibleSlot().getName());
                // new_max = GREATEST(old_max, delta_max) with NULL-safe logic
                Expression newMaxRaw = new If(new IsNull(oldMax), deltaMax,
                        new If(new IsNull(deltaMax), oldMax, new Greatest(oldMax, deltaMax)));
                // Embed guard evaluation: false branch uses NullLiteral to prevent IF constant folding.
                Expression newMaxGuarded = new If(delMaxGuard, newMaxRaw,
                        new NullLiteral(newMaxRaw.getDataType()));
                finalByColumnName.put(target.getHiddenStateSlot("MAX").getName(), newMaxGuarded);
                finalByColumnName.put(target.getHiddenStateSlot("COUNT").getName(), newCount);
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        new If(isPositive(newCount),
                                castIfNeeded(newMaxGuarded, target.getVisibleSlot().getDataType()),
                                new NullLiteral(target.getVisibleSlot().getDataType())));
                return;
            }
            default:
                throw new AnalysisException("IVM agg delta rewrite does not support aggregate type: "
                        + target.getAggType());
        }
    }

    private LogicalFilter<Plan> buildNetZeroFilter(LogicalJoin<Plan, Plan> join, DeltaPlanParts delta, Slot mvRowId) {
        Expression filter = new Not(new And(new IsNull(mvRowId),
                new LessThanEqual(deltaGroupCount(delta), new BigIntLiteral(0))));
        return new LogicalFilter<>(ImmutableSet.of(filter), join);
    }

    private LogicalOlapScan buildMvScan(MTMV mtmv, IvmDeltaRewriteContext ctx) {
        return new LogicalOlapScan(
                ctx.getConnectContext().getStatementContext().getNextRelationId(),
                mtmv,
                ImmutableList.of(mtmv.getQualifiedDbName()),
                ImmutableList.of(),
                mtmv.getPartitionIds(),
                mtmv.getBaseIndexId(),
                PreAggStatus.unset(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableList.of());
    }

    /**
     * Signs an expression by dml_factor: positive factor → expr, negative → -expr.
     * Uses conditional branch (not multiplication) to avoid TinyInt × Decimal precision loss.
     */
    private Expression signedExpr(Slot exprSlot, Slot dmlFactorSlot) {
        return new If(new GreaterThan(dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                exprSlot, new Subtract(zeroOf(exprSlot.getDataType()), exprSlot));
    }

    /**
     * Produces a NULL-aware count contribution:
     * IF(expr IS NULL, 0, dml_factor).
     * Used for COUNT(expr) and hidden count of SUM/AVG targets.
     */
    private Expression caseWhenExprNotNull(Slot exprSlot, Slot dmlFactorSlot) {
        return new If(new IsNull(exprSlot), new TinyIntLiteral((byte) 0), dmlFactorSlot);
    }

    /**
     * Wraps expr in assert_true(expr >= 0, message); throws at runtime if violated.
     *
     * <p>IMPORTANT: the false branch must differ from the true branch to prevent
     * {@code FoldConstantRuleOnFE.visitIf} from collapsing {@code IF(cond, x, x)} into {@code x},
     * which would silently discard the assert_true guard. Since assert_true either returns TRUE
     * (condition satisfied) or throws (condition violated), the false branch is unreachable —
     * we use a NullLiteral as a distinct, never-reached placeholder.
     */
    private Expression assertNonNegative(Expression expr, String message) {
        return new If(new AssertTrue(new GreaterThanEqual(expr,
                new BigIntLiteral(0)), new StringLiteral(message)),
                expr, new NullLiteral(expr.getDataType()));
    }

    /** Predicate: expr > 0. Used to guard visible value derivation. */
    private Expression isPositive(Expression expr) {
        return new GreaterThan(expr, new BigIntLiteral(0));
    }

    /** Looks up the delta_group_count slot from delta plan parts. */
    private Expression deltaGroupCount(DeltaPlanParts delta) {
        return delta.semanticSlots.get(Column.IVM_DELTA_GROUP_COUNT_COL);
    }

    private Expression deltaGroupKey(DeltaPlanParts delta, String name) {
        Slot slot = delta.groupKeySlotsByName.get(name);
        if (slot == null) {
            throw new AnalysisException("IVM agg delta rewrite failed to resolve delta group key: " + name);
        }
        return slot;
    }

    /** Reads old MV hidden state with NULL-safe default: COALESCE(mv_slot, 0). */
    private Expression coalesceMvSlot(LogicalOlapScan rawMvScan, String slotName) {
        Slot slot = findSlotByName(rawMvScan.getOutput(), slotName);
        return new Coalesce(slot, zeroOf(slot.getDataType()));
    }

    /**
     * Determines whether a delta aggregate output slot needs COALESCE wrapping.
     *
     * <p>Needed when the SUM might return NULL:
     * <ul>
     *   <li>Scalar agg: all outputs can be NULL when base table is empty</li>
     *   <li>SUM/AVG hidden sum: SUM(signedExpr) returns NULL when all input exprs are NULL</li>
     * </ul>
     */
    private boolean needsCoalesceInTopProject(Slot slot, IvmAggMeta aggMeta) {
        if (aggMeta.isScalarAgg() && Column.IVM_DELTA_GROUP_COUNT_COL.equals(slot.getName())) {
            return true;
        }
        for (AggTarget target : aggMeta.getAggTargets()) {
            if (aggMeta.isScalarAgg()
                    && slot.getName().equals(target.getHiddenStateSlot("COUNT").getName())) {
                return true;
            }
            if ((target.getAggType() == AggType.SUM || target.getAggType() == AggType.AVG)
                    && slot.getName().equals(target.getHiddenStateSlot("SUM").getName())) {
                return true;
            }
            // MIN/MAX hidden slots: delta_min/max can be NULL when no inserts or no deletes
            if (target.getAggType() == AggType.MIN
                    && (slot.getName().equals(target.getHiddenStateSlot("MIN").getName())
                        || slot.getName().equals(transientDelHiddenName(target, "DELMIN")))) {
                return false; // NULL carries semantic meaning for MIN/MAX guards — do NOT coalesce
            }
            if (target.getAggType() == AggType.MAX
                    && (slot.getName().equals(target.getHiddenStateSlot("MAX").getName())
                        || slot.getName().equals(transientDelHiddenName(target, "DELMAX")))) {
                return false; // NULL carries semantic meaning for MIN/MAX guards — do NOT coalesce
            }
        }
        return false;
    }

    private Map<String, Slot> indexSlotsByName(List<Slot> slots) {
        Map<String, Slot> slotByName = new LinkedHashMap<>();
        for (Slot slot : slots) {
            slotByName.put(slot.getName(), slot);
        }
        return slotByName;
    }

    /** Semantic key for per-target delta slots: "{ordinal}:{stateType}", e.g. "2:SUM". */
    private String hiddenKey(AggTarget target, String stateType) {
        return target.getOrdinal() + ":" + stateType;
    }

    /** Produces a zero literal of the given numeric type via checked cast from TinyInt(0). */
    private Expression zeroOf(DataType dataType) {
        return new TinyIntLiteral((byte) 0).checkedCastTo(dataType);
    }

    private Expression castIfNeeded(Expression expr, DataType dataType) {
        return expr.getDataType().equals(dataType) ? expr : new Cast(expr, dataType);
    }

    private NamedExpression aliasIfNeeded(Expression expr, String name) {
        if (expr instanceof NamedExpression && name.equals(((NamedExpression) expr).getName())) {
            return (NamedExpression) expr;
        }
        return new Alias(expr, name);
    }

    /**
     * Expression for the insert-only stream: IF(dml_factor > 0, expr, NULL).
     * Used for MIN/MAX delta aggregates — only insert rows contribute to the new extreme.
     */
    private Expression insertOnlyExpr(Slot exprSlot, Slot dmlFactorSlot) {
        return new If(new GreaterThan(dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                exprSlot, new NullLiteral(exprSlot.getDataType()));
    }

    /**
     * Expression for the delete-only stream: IF(dml_factor < 0, expr, NULL).
     * Used as input to MIN/MAX over deleted values, to detect boundary violations.
     */
    private Expression deleteOnlyExpr(Slot exprSlot, Slot dmlFactorSlot) {
        return new If(new LessThan(dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                exprSlot, new NullLiteral(exprSlot.getDataType()));
    }

    /**
     * Transient column name for min/max of deleted values.
     * This column exists only in the delta sub-plan aggregate and is NOT stored in the MV.
     */
    private String transientDelHiddenName(AggTarget target, String suffix) {
        return Column.IVM_HIDDEN_COLUMN_PREFIX + "TRANSIENT_" + target.getOrdinal() + "_" + suffix + "_COL__";
    }
}
