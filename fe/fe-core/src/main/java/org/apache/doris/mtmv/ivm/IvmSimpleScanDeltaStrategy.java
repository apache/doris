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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Delta rewrite strategy for simple scan-only or project-scan MVs (no aggregate).
 *
 * <p>Extends {@link PlanVisitor} with return type {@link RewriteResult} that carries both
 * the rewritten plan and the propagated dml_factor Slot. The visitor injects
 * {@code dml_factor} at the OlapScan level — either derived from the base table's
 * {@code binlog_op} column ({@code IF(binlog_op = 0, 1, -1)}) when present,
 * or as the literal
 * {@code 1} (insert-only assumption) when absent — and propagates it upward through
 * Projects and Filters. Unsupported node types cause an immediate {@link AnalysisException}.
 *
 * <p>Each instance is single-use: create a fresh instance per rewrite invocation.
 *
 * @see IvmAggDeltaStrategy
 */
public class IvmSimpleScanDeltaStrategy extends PlanVisitor<IvmSimpleScanDeltaStrategy.RewriteResult, Void>
        implements IvmDeltaStrategy {

    /** Result of a visitor rewrite step: the rewritten plan plus the dml_factor Slot. */
    protected static class RewriteResult {
        protected final Plan plan;
        protected final Slot dmlFactorSlot;
        /** True only for the terminal apply plan produced by the aggregate visitor. */
        protected final boolean isTerminal;

        protected RewriteResult(Plan plan, Slot dmlFactorSlot) {
            this(plan, dmlFactorSlot, false);
        }

        protected RewriteResult(Plan plan, Slot dmlFactorSlot, boolean isTerminal) {
            this.plan = plan;
            this.dmlFactorSlot = dmlFactorSlot;
            this.isTerminal = isTerminal;
        }
    }

    private static final String NON_DET_ROW_ID_MSG_PREFIX =
            "IVM fallback: delete on non-deterministic row_id in ";

    protected final IvmDeltaRewriteContext ctx;

    public IvmSimpleScanDeltaStrategy(IvmDeltaRewriteContext ctx) {
        this.ctx = Objects.requireNonNull(ctx, "ctx can not be null");
    }

    @Override
    public List<IvmDeltaCommandBundle> rewrite(Plan normalizedPlan) {
        RewriteResult result = rewritePlan(normalizedPlan);
        Plan finalPlan = buildSinkProject(result);
        Command insertCommand = buildInsertCommandWithDeleteSign(finalPlan);
        return Collections.singletonList(new IvmDeltaCommandBundle(insertCommand));
    }

    /** Strips ResultSink and walks the plan tree via the visitor. */
    protected RewriteResult rewritePlan(Plan normalizedPlan) {
        Plan queryPlan = stripResultSink(normalizedPlan);
        return queryPlan.accept(this, null);
    }

    // ---- Visitor methods ----

    /** Unsupported node types throw immediately. */
    @Override
    public RewriteResult visit(Plan plan, Void ctx) {
        throw new AnalysisException(
                "IVM delta rewrite does not support: " + plan.getClass().getSimpleName());
    }

    /**
     * Wraps scan with Project(scan_output + dml_factor).
     *
     * <p>If the base table has a {@code binlog_op} column (following the delete-sign convention:
     * 0 = insert, 1 = delete), dml_factor is derived as {@code IF(binlog_op = 0, 1, -1)}.
     * Otherwise, falls back to the literal {@code dml_factor = 1} (insert-only assumption).
     */
    @Override
    public RewriteResult visitLogicalOlapScan(LogicalOlapScan scan, Void ctx) {
        if (!scan.isDelta()) {
            // Snapshot scan: no dml_factor injection; return the scan unchanged.
            return new RewriteResult(scan, null);
        }
        Expression factorExpr = buildDmlFactorExpr(scan);
        Alias factorAlias = new Alias(factorExpr, Column.IVM_DML_FACTOR_COL);
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builderWithExpectedSize(
                scan.getOutput().size() + 1);
        scan.getOutput().forEach(slot -> outputs.add((NamedExpression) slot));
        outputs.add(factorAlias);
        LogicalProject<?> project = new LogicalProject<>(outputs.build(), scan);
        Slot dmlFactorSlot = project.getOutput().get(project.getOutput().size() - 1);
        return new RewriteResult(project, dmlFactorSlot);
    }

    /**
     * Builds the dml_factor expression for the given scan.
     *
     * <p>If the table contains a {@code binlog_op} column, returns
     * {@code IF(binlog_op = 0, 1, -1)}.
     * Otherwise, returns the literal {@code 1} (insert-only).
     */
    private Expression buildDmlFactorExpr(LogicalOlapScan scan) {
        if (scan.getTable().getColumn(Column.BINLOG_OPERATION_COL) == null) {
            return new TinyIntLiteral((byte) 1);
        }
        Slot opSlot = findSlotByName(scan.getOutput(), Column.BINLOG_OPERATION_COL);
        return new If(
                new EqualTo(opSlot, new TinyIntLiteral((byte) 0)),
                new TinyIntLiteral((byte) 1),
                new TinyIntLiteral((byte) -1));
    }

    /** Propagates dml_factor slot by appending it to the project output, unless already present. */
    @Override
    public RewriteResult visitLogicalProject(LogicalProject<? extends Plan> project, Void ctx) {
        RewriteResult childResult = project.child().accept(this, ctx);
        // Preserve normalize-added hidden columns (for example row_id on snapshot-side projects)
        // even when there is no dml_factor to propagate.
        if (childResult.dmlFactorSlot == null) {
            LogicalProject<?> newProject = project.withProjectsAndChild(project.getProjects(), childResult.plan);
            return new RewriteResult(newProject, null);
        }
        return propagateDmlFactorThroughProject(project, childResult);
    }

    /**
     * Appends (or reuses) dml_factor in the project's output list.
     * Called when the child result carries a non-null dml_factor slot.
     */
    protected RewriteResult propagateDmlFactorThroughProject(
            LogicalProject<? extends Plan> project, RewriteResult childResult) {
        // If this project already carries dml_factor, just update the child
        for (int i = 0; i < project.getProjects().size(); i++) {
            NamedExpression expr = project.getProjects().get(i);
            if (Column.IVM_DML_FACTOR_COL.equals(expr.getName())) {
                LogicalProject<?> newProject = project.withProjectsAndChild(
                        project.getProjects(), childResult.plan);
                return new RewriteResult(newProject, newProject.getOutput().get(i));
            }
        }
        ImmutableList.Builder<NamedExpression> newOutputs = ImmutableList.builderWithExpectedSize(
                project.getProjects().size() + 1);
        newOutputs.addAll(project.getProjects());
        newOutputs.add(childResult.dmlFactorSlot);
        LogicalProject<?> newProject = project.withProjectsAndChild(newOutputs.build(), childResult.plan);
        return new RewriteResult(newProject, childResult.dmlFactorSlot);
    }

    /** Filter: recurse into child, propagate dml_factor unchanged. */
    @Override
    public RewriteResult visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void ctx) {
        RewriteResult childResult = filter.child().accept(this, ctx);
        Plan newFilter = filter.withChildren(ImmutableList.of(childResult.plan));
        return new RewriteResult(newFilter, childResult.dmlFactorSlot);
    }

    /**
     * Join: visit both children, propagate dml_factor from the delta side.
     * Only INNER_JOIN and CROSS_JOIN are supported (validated at normalize time).
     *
     * <p>At most one child has dml_factor (the delta side); the other (snapshot side)
     * has dml_factor = null. If the snapshot side's row_id is non-deterministic,
     * the dml_factor is wrapped with an assert_true guard so that delete deltas
     * trigger a runtime fallback to full refresh.
     */
    @Override
    public RewriteResult visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void ctx) {
        JoinType joinType = join.getJoinType();
        if (joinType != JoinType.INNER_JOIN && joinType != JoinType.CROSS_JOIN) {
            throw new AnalysisException(
                    "IVM delta rewrite does not support join type: " + joinType);
        }
        if (join.isMarkJoin()) {
            throw new AnalysisException(
                    "IVM delta rewrite does not support mark join (subquery with disjunction).");
        }

        RewriteResult leftResult = join.left().accept(this, ctx);
        RewriteResult rightResult = join.right().accept(this, ctx);

        if (leftResult.dmlFactorSlot != null && rightResult.dmlFactorSlot != null) {
            throw new AnalysisException(
                    "IVM: both sides of join have dml_factor — expected at most one delta side");
        }

        LogicalJoin<Plan, Plan> newJoin = (LogicalJoin<Plan, Plan>) join.withChildren(
                leftResult.plan, rightResult.plan);

        if (leftResult.dmlFactorSlot == null && rightResult.dmlFactorSlot == null) {
            return new RewriteResult(newJoin, null);
        }

        // IMPORTANT: We ONLY guard the SNAPSHOT side, NOT the delta side.
        //
        // Proof by induction that the delta side's row_id is always deterministic when
        // dml_factor < 0 (delete):
        //   Base case: dml_factor < 0 at a base scan ⟹ the table is MOW (Merge-on-Write),
        //     which always produces deterministic row_id (unique key).
        //   Inductive step: for a join, dml_factor < 0 can only come from the delta side.
        //     The delta child's row_id is deterministic (by induction hypothesis), and the
        //     snapshot side's row_id is enforced deterministic by THIS guard. So the composed
        //     row_id = hash(left_row_id, right_row_id) is also deterministic.
        //
        // Therefore, delta-side row_id non-determinism check is unnecessary and must NOT be
        // added — doing so would cause false-positive fallbacks.
        //
        // Use the ORIGINAL join children (before rewriting) because the rewritten plan may drop
        // the row_id slot (e.g., when visitLogicalProject short-circuits for null dml_factor).
        boolean deltaOnLeft = leftResult.dmlFactorSlot != null;
        Slot dmlFactorSlot = deltaOnLeft ? leftResult.dmlFactorSlot : rightResult.dmlFactorSlot;
        Plan snapshotSideOriginal = deltaOnLeft ? join.right() : join.left();

        if (needNonDetGuard(snapshotSideOriginal)) {
            return wrapDmlFactorWithNonDetGuard(new RewriteResult(newJoin, dmlFactorSlot), joinType);
        }
        return new RewriteResult(newJoin, dmlFactorSlot);
    }

    /**
     * Checks if the snapshot side's row_id slot is non-deterministic.
     * Returns true (conservatively add guard) when normalizeResult or row_id slot is unavailable.
     */
    private boolean needNonDetGuard(Plan snapshotSidePlan) {
        IvmNormalizeResult normalizeResult = this.ctx.getNormalizeResult();
        if (normalizeResult == null) {
            return true;
        }
        Slot rowIdSlot = IvmUtil.findRowIdSlotOrNull(snapshotSidePlan.getOutput());
        if (rowIdSlot == null) {
            return true;
        }
        return !normalizeResult.isDeterministic(rowIdSlot);
    }

    // ---- Helpers ----

    /**
     * Wraps the dml_factor slot with an assert_true guard that triggers a runtime exception
     * when dml_factor < 0 (i.e., delete delta), causing fallback to full refresh.
     *
     * <p>This is used when the MV's row_id is non-deterministic (e.g., join with DUP_KEYS table).
     * Insert deltas (dml_factor >= 0) pass through; delete deltas cause:
     * {@code IF(assert_true(dml_factor >= 0, msg), dml_factor, NULL)}.
     *
     * <p>The false branch (NullLiteral) must differ from the true branch to prevent
     * FoldConstantRuleOnFE from collapsing the IF.
     */
    private RewriteResult wrapDmlFactorWithNonDetGuard(RewriteResult result, JoinType joinType) {
        String msg = NON_DET_ROW_ID_MSG_PREFIX + joinType;
        Expression guardedExpr = new If(
                new AssertTrue(new GreaterThanEqual(result.dmlFactorSlot,
                        new TinyIntLiteral((byte) 0)), new StringLiteral(msg)),
                result.dmlFactorSlot, new NullLiteral(result.dmlFactorSlot.getDataType()));
        Alias guardedAlias = new Alias(guardedExpr, Column.IVM_DML_FACTOR_COL);

        ImmutableList.Builder<NamedExpression> projectOutputs = ImmutableList.builder();
        for (Slot slot : result.plan.getOutput()) {
            if (Column.IVM_DML_FACTOR_COL.equals(slot.getName())) {
                projectOutputs.add(guardedAlias);
            } else {
                projectOutputs.add(slot);
            }
        }
        LogicalProject<?> guardProject = new LogicalProject<>(projectOutputs.build(), result.plan);
        Slot newDmlFactorSlot = guardProject.getOutput().stream()
                .filter(s -> Column.IVM_DML_FACTOR_COL.equals(s.getName()))
                .findFirst()
                .orElseThrow(() -> new AnalysisException("IVM: lost dml_factor after non-det guard"));
        return new RewriteResult(guardProject, newDmlFactorSlot);
    }

    /**
     * Wraps the visitor-rewritten plan with a final project that:
     * 1. Passes through columns matching mtmv.getInsertedColumnNames() in order
     * 2. Maps dml_factor to __DORIS_DELETE_SIGN__: CASE WHEN dml_factor < 0 THEN 1 ELSE 0 END
     */
    private Plan buildSinkProject(RewriteResult result) {
        List<Slot> output = result.plan.getOutput();
        List<String> insertedColumns = ctx.getMtmv().getInsertedColumnNames();
        ImmutableList.Builder<NamedExpression> sinkOutputs = ImmutableList.builderWithExpectedSize(
                insertedColumns.size() + 1);
        for (String colName : insertedColumns) {
            sinkOutputs.add(findSlotByName(output, colName));
        }
        sinkOutputs.add(new Alias(
                new If(new LessThan(result.dmlFactorSlot, new TinyIntLiteral((byte) 0)),
                        new TinyIntLiteral((byte) 1), new TinyIntLiteral((byte) 0)),
                Column.DELETE_SIGN));
        return new LogicalProject<>(sinkOutputs.build(), result.plan);
    }

    protected Slot findSlotByName(List<Slot> slots, String name) {
        for (Slot slot : slots) {
            if (name.equals(slot.getName())) {
                return slot;
            }
        }
        throw new AnalysisException("IVM failed to find slot: " + name);
    }

    protected Plan stripResultSink(Plan plan) {
        while (plan instanceof LogicalResultSink) {
            plan = ((LogicalResultSink<?>) plan).child();
        }
        return plan;
    }

    protected Command buildInsertCommandWithDeleteSign(Plan queryPlan) {
        MTMV mtmv = ctx.getMtmv();
        List<String> sinkColumns = new ArrayList<>(mtmv.getInsertedColumnNames());
        sinkColumns.add(Column.DELETE_SIGN);
        List<String> mvNameParts = ImmutableList.of(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                mtmv.getQualifiedDbName(),
                mtmv.getName());
        UnboundTableSink<LogicalPlan> sink = new UnboundTableSink<>(
                mvNameParts, sinkColumns, ImmutableList.of(),
                false, ImmutableList.of(), false,
                TPartialUpdateNewRowPolicy.APPEND, DMLCommandType.INSERT,
                Optional.empty(), Optional.empty(), (LogicalPlan) queryPlan);
        return new InsertIntoTableCommand(sink, Optional.empty(), Optional.empty(), Optional.empty());
    }
}
