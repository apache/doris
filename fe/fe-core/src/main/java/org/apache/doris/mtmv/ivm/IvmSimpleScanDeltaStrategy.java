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
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
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
import java.util.Optional;

/**
 * Delta rewrite strategy for simple scan-only or project-scan MVs (no aggregate).
 *
 * <p>Extends {@link PlanVisitor} with return type {@link RewriteResult} that carries both
 * the rewritten plan and the propagated dml_factor Slot. The visitor always injects
 * {@code dml_factor = 1} at the OlapScan level and propagates it upward through Projects
 * and Filters. Unsupported node types cause an immediate {@link AnalysisException}.
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

        protected RewriteResult(Plan plan, Slot dmlFactorSlot) {
            this.plan = plan;
            this.dmlFactorSlot = dmlFactorSlot;
        }
    }

    @Override
    public List<DeltaCommandBundle> rewrite(Plan normalizedPlan, IvmDeltaRewriteContext ctx) {
        RewriteResult result = rewritePlan(normalizedPlan);
        Plan finalPlan = buildSinkProject(result, ctx);
        Command insertCommand = buildInsertCommandWithDeleteSign(finalPlan, ctx);
        return Collections.singletonList(new DeltaCommandBundle(insertCommand));
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

    /** Wraps scan with Project(scan_output + dml_factor=1). */
    @Override
    public RewriteResult visitLogicalOlapScan(LogicalOlapScan scan, Void ctx) {
        Alias factorAlias = new Alias(new TinyIntLiteral((byte) 1), Column.IVM_DML_FACTOR_COL);
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builderWithExpectedSize(
                scan.getOutput().size() + 1);
        scan.getOutput().forEach(slot -> outputs.add((NamedExpression) slot));
        outputs.add(factorAlias);
        LogicalProject<?> project = new LogicalProject<>(outputs.build(), scan);
        Slot dmlFactorSlot = project.getOutput().get(project.getOutput().size() - 1);
        return new RewriteResult(project, dmlFactorSlot);
    }

    /** Propagates dml_factor slot by appending it to the project output, unless already present. */
    @Override
    public RewriteResult visitLogicalProject(LogicalProject<? extends Plan> project, Void ctx) {
        RewriteResult childResult = project.child().accept(this, ctx);
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

    // ---- Helpers ----

    /**
     * Wraps the visitor-rewritten plan with a final project that:
     * 1. Passes through columns matching mtmv.getInsertedColumnNames() in order
     * 2. Maps dml_factor to __DORIS_DELETE_SIGN__: CASE WHEN dml_factor < 0 THEN 1 ELSE 0 END
     */
    private Plan buildSinkProject(RewriteResult result, IvmDeltaRewriteContext ctx) {
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

    protected Command buildInsertCommandWithDeleteSign(Plan queryPlan, IvmDeltaRewriteContext ctx) {
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
