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
import org.apache.doris.mtmv.ivm.IvmAggMeta.StateKey;
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

    /** Transient semantic key for MIN of deleted values (not stored in MV). */
    private static final String DELMIN = "DELMIN";
    /** Transient semantic key for MAX of deleted values (not stored in MV). */
    private static final String DELMAX = "DELMAX";

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
                            new Sum(caseWhenExprNotNull(target.getExprArgs().get(0), dmlFactorSlot)),
                            target.stateColumnName(StateKey.COUNT)));
                    break;
                case SUM:
                case AVG:
                    deltaAggOutputs.add(new Alias(
                            new Sum(signedExpr(target.getExprArgs().get(0), dmlFactorSlot)),
                            target.stateColumnName(StateKey.SUM)));
                    deltaAggOutputs.add(new Alias(
                            new Sum(caseWhenExprNotNull(target.getExprArgs().get(0), dmlFactorSlot)),
                            target.stateColumnName(StateKey.COUNT)));
                    break;
                case MIN:
                case MAX:
                    buildExtremalDeltaOutputs(deltaAggOutputs, target, dmlFactorSlot);
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
                    semanticSlots.put(hiddenKey(target, StateKey.COUNT),
                            outputByName.get(Column.IVM_DELTA_GROUP_COUNT_COL));
                    break;
                case COUNT_EXPR:
                    semanticSlots.put(hiddenKey(target, StateKey.COUNT),
                            outputByName.get(target.stateColumnName(StateKey.COUNT)));
                    break;
                case SUM:
                case AVG:
                    semanticSlots.put(hiddenKey(target, StateKey.SUM),
                            outputByName.get(target.stateColumnName(StateKey.SUM)));
                    semanticSlots.put(hiddenKey(target, StateKey.COUNT),
                            outputByName.get(target.stateColumnName(StateKey.COUNT)));
                    break;
                case MIN:
                case MAX:
                    putExtremalSemanticSlots(semanticSlots, outputByName, target);
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
     *   <li>COUNT(*): new_group_count (cast if needed) — no hidden columns</li>
     *   <li>COUNT(expr): IF(new_count > 0, new_count, 0) — no hidden columns,
     *       old count read from visible column</li>
     *   <li>SUM(expr): IF(new_count > 0, new_sum, NULL) — no hidden SUM,
     *       old sum read from visible column; hidden COUNT persisted</li>
     *   <li>AVG(expr): IF(new_count > 0, CAST(new_sum / new_count AS visible_type), NULL)
     *       — hidden SUM + COUNT persisted</li>
     * </ul>
     *
     * <p>Count values are wrapped with {@link #assertNonNegative} to catch data corruption.
     */
    private void buildTargetExpressions(Map<String, Expression> finalByColumnName, LogicalOlapScan rawMvScan,
            DeltaPlanParts delta, AggTarget target, Expression newGroupCount) {
        switch (target.getAggType()) {
            case COUNT_STAR: {
                // No hidden columns. Visible value equals the global group count.
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        castIfNeeded(newGroupCount, target.getVisibleSlot().getDataType()));
                return;
            }
            case COUNT_EXPR: {
                // No hidden columns. Old count read from visible column.
                Expression newCount = assertNonNegative(new Add(
                        coalesceMvSlot(rawMvScan, target.getVisibleSlot().getName()),
                        delta.semanticSlots.get(hiddenKey(target, StateKey.COUNT))),
                        "negative count for " + target.getVisibleSlot().getName());
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        new If(isPositive(newCount),
                                castIfNeeded(newCount, target.getVisibleSlot().getDataType()),
                                zeroOf(target.getVisibleSlot().getDataType())));
                return;
            }
            case SUM: {
                // No hidden SUM column. Old sum read from visible column.
                // Hidden COUNT is persisted for assertNonNegative and null-count logic.
                Expression newSum = new Add(
                        coalesceMvSlot(rawMvScan, target.getVisibleSlot().getName()),
                        delta.semanticSlots.get(hiddenKey(target, StateKey.SUM)));
                Expression newCount = buildNewCount(rawMvScan, delta, target);
                finalByColumnName.put(target.getHiddenStateSlot(StateKey.COUNT).getName(), newCount);
                Expression visibleValue = castIfNeeded(newSum, target.getVisibleSlot().getDataType());
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        new If(isPositive(newCount), visibleValue,
                                new NullLiteral(target.getVisibleSlot().getDataType())));
                return;
            }
            case AVG: {
                // Both hidden SUM and COUNT are persisted (visible is AVG ≠ SUM or COUNT).
                Expression newSum = new Add(
                        coalesceMvSlot(rawMvScan, target.getHiddenStateSlot(StateKey.SUM).getName()),
                        delta.semanticSlots.get(hiddenKey(target, StateKey.SUM)));
                Expression newCount = buildNewCount(rawMvScan, delta, target);
                finalByColumnName.put(target.getHiddenStateSlot(StateKey.SUM).getName(), newSum);
                finalByColumnName.put(target.getHiddenStateSlot(StateKey.COUNT).getName(), newCount);
                Expression divisor = castIfNeeded(newCount, newSum.getDataType());
                Expression visibleValue = castIfNeeded(new Divide(newSum, divisor),
                        target.getVisibleSlot().getDataType());
                finalByColumnName.put(target.getVisibleSlot().getName(),
                        new If(isPositive(newCount), visibleValue,
                                new NullLiteral(target.getVisibleSlot().getDataType())));
                return;
            }
            case MIN:
            case MAX:
                buildExtremalTargetExpressions(finalByColumnName, rawMvScan, delta, target);
                return;
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
    private Expression signedExpr(Expression expr, Slot dmlFactorSlot) {
        return new If(new GreaterThan(dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                expr, new Subtract(zeroOf(expr.getDataType()), expr));
    }

    /**
     * Produces a NULL-aware count contribution:
     * IF(expr IS NULL, 0, dml_factor).
     * Used for COUNT(expr) and hidden count of SUM/AVG targets.
     */
    private Expression caseWhenExprNotNull(Expression expr, Slot dmlFactorSlot) {
        return new If(new IsNull(expr), new TinyIntLiteral((byte) 0), dmlFactorSlot);
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
                    && slot.getName().equals(target.stateColumnName(StateKey.COUNT))) {
                return true;
            }
            if ((target.getAggType() == AggType.SUM || target.getAggType() == AggType.AVG)
                    && slot.getName().equals(target.stateColumnName(StateKey.SUM))) {
                return true;
            }
            // MIN/MAX visible values and their transient DELMIN/DELMAX slots carry semantic NULLs
            // and must NOT be coalesced — fall through to default false.
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

    /** Semantic key for per-target delta slots: "{ordinal}:{stateKey}", e.g. "2:SUM". */
    private String hiddenKey(AggTarget target, StateKey stateKey) {
        return target.getOrdinal() + ":" + stateKey.name();
    }

    /** Semantic key for transient delta slots: "{ordinal}:{suffix}", e.g. "0:DELMIN". */
    private String hiddenKey(AggTarget target, String transientSuffix) {
        return target.getOrdinal() + ":" + transientSuffix;
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
    private Expression insertOnlyExpr(Expression expr, Slot dmlFactorSlot) {
        return new If(new GreaterThan(dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                expr, new NullLiteral(expr.getDataType()));
    }

    /**
     * Expression for the delete-only stream: IF(dml_factor < 0, expr, NULL).
     * Used as input to MIN/MAX over deleted values, to detect boundary violations.
     */
    private Expression deleteOnlyExpr(Expression expr, Slot dmlFactorSlot) {
        return new If(new LessThan(dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                expr, new NullLiteral(expr.getDataType()));
    }

    /**
     * Transient column name for min/max of deleted values.
     * This column exists only in the delta sub-plan aggregate and is NOT stored in the MV.
     */
    private String transientDelHiddenName(AggTarget target, String suffix) {
        return Column.IVM_HIDDEN_COLUMN_PREFIX + "TRANSIENT_" + target.getOrdinal() + "_" + suffix + "_COL__";
    }

    // ---- Extracted common methods for MIN/MAX and SUM/AVG deduplication ----

    /**
     * Builds delta aggregate outputs for a MIN or MAX target.
     *
     * <p>For MIN, produces: MIN(insertOnly), MIN(deleteOnly), SUM(caseWhenNotNull).
     * For MAX, produces: MAX(insertOnly), MAX(deleteOnly), SUM(caseWhenNotNull).
     * The insert-only agg computes the new extremal from inserted rows; the delete-only
     * agg captures deleted extremal values for the boundary guard check.
     */
    private void buildExtremalDeltaOutputs(List<NamedExpression> deltaAggOutputs,
            AggTarget target, Slot dmlFactorSlot) {
        boolean isMin = target.getAggType() == AggType.MIN;
        StateKey stateKey = isMin ? StateKey.MIN : StateKey.MAX;
        String delKey = isMin ? DELMIN : DELMAX;
        Expression exprArg = target.getExprArgs().get(0);

        Expression insertAgg = isMin
                ? new Min(insertOnlyExpr(exprArg, dmlFactorSlot))
                : new Max(insertOnlyExpr(exprArg, dmlFactorSlot));
        Expression deleteAgg = isMin
                ? new Min(deleteOnlyExpr(exprArg, dmlFactorSlot))
                : new Max(deleteOnlyExpr(exprArg, dmlFactorSlot));

        deltaAggOutputs.add(new Alias(insertAgg,
                IvmUtil.ivmAggHiddenColumnName(target.getOrdinal(), stateKey.name())));
        deltaAggOutputs.add(new Alias(deleteAgg, transientDelHiddenName(target, delKey)));
        deltaAggOutputs.add(new Alias(
                new Sum(caseWhenExprNotNull(exprArg, dmlFactorSlot)),
                target.getHiddenStateSlot(StateKey.COUNT).getName()));
    }

    /**
     * Puts semantic slot mappings for a MIN or MAX target into the semantic slots map.
     *
     * <p>Maps: stateKey (MIN/MAX), delKey (DELMIN/DELMAX), and COUNT.
     */
    private void putExtremalSemanticSlots(Map<String, Slot> semanticSlots,
            Map<String, Slot> outputByName, AggTarget target) {
        boolean isMin = target.getAggType() == AggType.MIN;
        StateKey stateKey = isMin ? StateKey.MIN : StateKey.MAX;
        String delKey = isMin ? DELMIN : DELMAX;

        semanticSlots.put(hiddenKey(target, stateKey),
                outputByName.get(IvmUtil.ivmAggHiddenColumnName(target.getOrdinal(), stateKey.name())));
        semanticSlots.put(hiddenKey(target, delKey),
                outputByName.get(transientDelHiddenName(target, delKey)));
        semanticSlots.put(hiddenKey(target, StateKey.COUNT),
                outputByName.get(target.getHiddenStateSlot(StateKey.COUNT).getName()));
    }

    /**
     * Computes the new count for a target: assertNonNegative(COALESCE(old, 0) + delta).
     *
     * <p>Only called for targets that have a physical hidden COUNT column
     * (SUM, AVG, MIN, MAX). COUNT_STAR and COUNT_EXPR handle their counts
     * directly in {@link #buildTargetExpressions}.
     */
    private Expression buildNewCount(LogicalOlapScan rawMvScan, DeltaPlanParts delta, AggTarget target) {
        return assertNonNegative(new Add(
                coalesceMvSlot(rawMvScan, target.getHiddenStateSlot(StateKey.COUNT).getName()),
                delta.semanticSlots.get(hiddenKey(target, StateKey.COUNT))),
                "negative hidden count for " + target.getVisibleSlot().getName());
    }

    /**
     * Builds target expressions for a MIN or MAX aggregate target.
     *
     * <p>The structure is identical for MIN and MAX; only the comparison direction,
     * merge function (LEAST vs GREATEST), and state key differ:
     * <ul>
     *   <li><b>Guard</b>: assert_true(deltaDelExtreme IS NULL OR oldExtreme IS NULL
     *       OR deltaDelExtreme {>|<} oldExtreme) — if a deleted row matches the
     *       current extreme, incremental computation is impossible.</li>
     *   <li><b>Merge</b>: new_extreme = null-safe {LEAST|GREATEST}(old, delta_insert).</li>
     *   <li><b>Outputs</b>: visible value (guarded extreme or NULL when count=0), and count.</li>
     * </ul>
     */
    private void buildExtremalTargetExpressions(Map<String, Expression> finalByColumnName,
            LogicalOlapScan rawMvScan, DeltaPlanParts delta, AggTarget target) {
        boolean isMin = target.getAggType() == AggType.MIN;
        StateKey stateKey = isMin ? StateKey.MIN : StateKey.MAX;
        String delKey = isMin ? DELMIN : DELMAX;
        String guardMsg = isMin
                ? "IVM: deleted row may be current MIN value, fallback to COMPLETE"
                : "IVM: deleted row may be current MAX value, fallback to COMPLETE";

        Slot oldExtreme = findSlotByName(rawMvScan.getOutput(),
                target.getVisibleSlot().getName());
        Expression deltaInsert = delta.semanticSlots.get(hiddenKey(target, stateKey));
        Expression deltaDel = delta.semanticSlots.get(hiddenKey(target, delKey));

        // Guard: assert_true(deltaDel IS NULL OR old IS NULL OR deltaDel {>|<} old)
        // For MIN: deleted value must be > current min (otherwise we lose the min).
        // For MAX: deleted value must be < current max (otherwise we lose the max).
        Expression guardComparison = isMin
                ? new GreaterThan(deltaDel, oldExtreme)
                : new LessThan(deltaDel, oldExtreme);
        Expression guardCond = new Or(new IsNull(deltaDel),
                new Or(new IsNull(oldExtreme), guardComparison));
        Expression guard = new AssertTrue(guardCond, new StringLiteral(guardMsg));

        Expression newCount = buildNewCount(rawMvScan, delta, target);

        // Null-safe merge: IF(old IS NULL, deltaInsert, IF(deltaInsert IS NULL, old, {LEAST|GREATEST}))
        Expression mergeFunc = isMin
                ? new Least(oldExtreme, deltaInsert)
                : new Greatest(oldExtreme, deltaInsert);
        Expression newExtremeRaw = new If(new IsNull(oldExtreme), deltaInsert,
                new If(new IsNull(deltaInsert), oldExtreme, mergeFunc));

        // Embed guard: false branch uses NullLiteral to prevent IF constant folding.
        // assert_true either returns TRUE (pass) or throws (fail), so false branch is unreachable.
        Expression newExtremeGuarded = new If(guard, newExtremeRaw,
                new NullLiteral(newExtremeRaw.getDataType()));

        // No hidden MIN/MAX column: the visible column stores the extremal value directly.
        // When count drops to 0, visible becomes NULL; next refresh reads NULL as old,
        // which is correctly handled by the merge logic (IF old IS NULL, take deltaInsert).
        finalByColumnName.put(target.getHiddenStateSlot(StateKey.COUNT).getName(), newCount);
        finalByColumnName.put(target.getVisibleSlot().getName(),
                new If(isPositive(newCount),
                        castIfNeeded(newExtremeGuarded, target.getVisibleSlot().getDataType()),
                        new NullLiteral(target.getVisibleSlot().getDataType())));
    }
}
