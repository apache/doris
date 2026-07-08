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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites plan nodes whose delta propagates linearly through the plan.
 */
class IvmLinearDeltaHandler {
    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;

    /** Regular scan is a snapshot — no delta to process, return as-is with null dmlFactor. */
    IvmDeltaRewriteResult rewriteOlapScan(LogicalOlapScan scan) {
        return new IvmDeltaRewriteResult(scan, null, null);
    }

    /**
     * Wraps incremental stream scan with Project(scan_output + dml_factor).
     * Returns null dmlFactor for non-incremental stream scans.
     */
    IvmDeltaRewriteResult rewriteOlapTableStreamScan(LogicalOlapTableStreamScan scan) {
        if (!scan.isIncremental()) {
            return new IvmDeltaRewriteResult(scan, null, null);
        }
        Expression factorExpr = buildDmlFactorExpr(scan);
        Alias factorAlias = new Alias(factorExpr, Column.IVM_DML_FACTOR_COL);
        Alias baseOpAlias = new Alias(factorExpr, Column.IVM_BASE_OP_COL);
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builderWithExpectedSize(
                scan.getOutput().size() + 2);
        scan.getOutput().forEach(slot -> outputs.add((NamedExpression) slot));
        outputs.add(factorAlias);
        outputs.add(baseOpAlias);
        LogicalProject<?> project = new LogicalProject<>(outputs.build(), scan);
        int lastIdx = project.getOutput().size() - 1;
        Slot dmlFactorSlot = project.getOutput().get(lastIdx - 1);
        Slot baseOpSlot = project.getOutput().get(lastIdx);
        return new IvmDeltaRewriteResult(project, dmlFactorSlot, baseOpSlot);
    }

    /**
     * Builds the dml_factor expression from the stream change type column.
     * APPEND, UPDATE_AFTER → dml_factor = +1, DELETE, UPDATE_BEFORE → dml_factor = -1
     */
    private Expression buildDmlFactorExpr(LogicalOlapScan scan) {
        Slot opSlot = IvmDeltaRewriteHelper.INSTANCE.findSlotByName(
                scan.getOutput(), Column.STREAM_CHANGE_TYPE_COL);
        return new If(
                new Or(new EqualTo(new VarcharLiteral("APPEND"), opSlot),
                        new EqualTo(new VarcharLiteral("UPDATE_AFTER"), opSlot)),
                new TinyIntLiteral((byte) 1),
                new TinyIntLiteral((byte) -1));
    }

    IvmDeltaRewriteResult rewriteProject(LogicalProject<? extends Plan> project,
            IvmDeltaRewriteVisitor visitor, IvmRefreshContext ctx) {
        IvmDeltaRewriteResult childResult = project.child().accept(visitor, ctx);
        if (childResult.dmlFactorSlot == null) {
            LogicalProject<?> newProject = project.withProjectsAndChild(project.getProjects(), childResult.plan);
            return new IvmDeltaRewriteResult(newProject, null, null);
        }
        // IVM normalize only adds hidden row-id columns to existing projects. dml_factor and op_type are
        // introduced later while rewriting delta scans, so normalized projects need to propagate them explicitly.
        int dmlFactorIndex = -1;
        int baseOpIndex = -1;
        for (int i = 0; i < project.getProjects().size(); i++) {
            NamedExpression expr = project.getProjects().get(i);
            if (Column.IVM_DML_FACTOR_COL.equals(expr.getName())) {
                dmlFactorIndex = i;
            } else if (Column.IVM_BASE_OP_COL.equals(expr.getName())) {
                baseOpIndex = i;
            }
        }
        if (dmlFactorIndex >= 0 && baseOpIndex >= 0) {
            LogicalProject<?> newProject = project.withProjectsAndChild(project.getProjects(), childResult.plan);
            return new IvmDeltaRewriteResult(newProject,
                    newProject.getOutput().get(dmlFactorIndex), newProject.getOutput().get(baseOpIndex));
        }
        ImmutableList.Builder<NamedExpression> newOutputs = ImmutableList.builderWithExpectedSize(
                project.getProjects().size() + 2);
        newOutputs.addAll(project.getProjects());
        if (dmlFactorIndex < 0) {
            newOutputs.add(childResult.dmlFactorSlot);
        }
        if (baseOpIndex < 0) {
            newOutputs.add(childResult.baseOpSlot);
        }
        LogicalProject<?> newProject = project.withProjectsAndChild(newOutputs.build(), childResult.plan);
        Slot newDmlFactor = dmlFactorIndex >= 0 ? newProject.getOutput().get(dmlFactorIndex)
                : newProject.getOutput().get(newProject.getOutput().size() - 2);
        Slot newBaseOp = baseOpIndex >= 0 ? newProject.getOutput().get(baseOpIndex)
                : newProject.getOutput().get(newProject.getOutput().size() - 1);
        return new IvmDeltaRewriteResult(newProject, newDmlFactor, newBaseOp);
    }

    IvmDeltaRewriteResult rewriteFilter(LogicalFilter<? extends Plan> filter,
            IvmDeltaRewriteVisitor visitor, IvmRefreshContext ctx) {
        IvmDeltaRewriteResult childResult = filter.child().accept(visitor, ctx);
        Plan newFilter = filter.withChildren(ImmutableList.of(childResult.plan));
        return new IvmDeltaRewriteResult(newFilter, childResult.dmlFactorSlot, childResult.baseOpSlot);
    }

    IvmDeltaRewriteResult rewriteUnion(LogicalUnion union, IvmDeltaRewriteVisitor visitor, IvmRefreshContext ctx) {
        List<IvmDeltaRewriteResult> childResults = new ArrayList<>();
        for (Plan child : union.children()) {
            childResults.add(child.accept(visitor, ctx));
        }

        int deltaIdx = -1;
        for (int i = 0; i < childResults.size(); i++) {
            if (childResults.get(i).dmlFactorSlot != null) {
                if (deltaIdx != -1) {
                    throw new AnalysisException(
                            "IVM: multiple UNION ALL arms have dml_factor — expected at most one delta arm");
                }
                deltaIdx = i;
            }
        }

        if (deltaIdx == -1) {
            ImmutableList.Builder<Plan> newChildren = ImmutableList.builder();
            for (IvmDeltaRewriteResult result : childResults) {
                newChildren.add(result.plan);
            }
            Plan newUnion = union.withChildren(newChildren.build());
            return new IvmDeltaRewriteResult(newUnion, null, null);
        }

        IvmDeltaRewriteResult deltaChild = childResults.get(deltaIdx);
        List<SlotReference> childMapping = union.getRegularChildrenOutputs().get(deltaIdx);
        List<NamedExpression> unionOutputs = union.getOutputs();

        ImmutableList.Builder<NamedExpression> projections = ImmutableList.builder();
        for (int j = 0; j < unionOutputs.size(); j++) {
            NamedExpression unionOut = unionOutputs.get(j);
            SlotReference childSlot = childMapping.get(j);
            projections.add(new Alias(unionOut.getExprId(), childSlot, unionOut.getName()));
        }
        projections.add(deltaChild.dmlFactorSlot);
        projections.add(deltaChild.baseOpSlot);

        LogicalProject<Plan> mappedProject = new LogicalProject<>(projections.build(), deltaChild.plan);
        int lastIdx = mappedProject.getOutput().size() - 1;
        Slot newDmlFactor = mappedProject.getOutput().get(lastIdx - 1);
        Slot newBaseOp = mappedProject.getOutput().get(lastIdx);
        return new IvmDeltaRewriteResult(mappedProject, newDmlFactor, newBaseOp);
    }

    IvmDeltaRewriteResult rewriteRepeat(LogicalRepeat<? extends Plan> repeat,
            IvmDeltaRewriteVisitor visitor, IvmRefreshContext ctx) {
        IvmDeltaRewriteResult childResult = repeat.child().accept(visitor, ctx);
        if (childResult.dmlFactorSlot == null) {
            return new IvmDeltaRewriteResult(repeat.withChildren(ImmutableList.of(childResult.plan)), null, null);
        }

        List<NamedExpression> newOutputs = new ArrayList<>(repeat.getOutputExpressions());
        newOutputs.add(childResult.dmlFactorSlot);
        newOutputs.add(childResult.baseOpSlot);
        LogicalRepeat<Plan> newRepeat = repeat.withAggOutputAndChild(newOutputs, childResult.plan);
        Slot repeatDmlFactorSlot = helper.findSlotByName(newRepeat.getOutput(), Column.IVM_DML_FACTOR_COL);
        Slot repeatBaseOpSlot = helper.findSlotByName(newRepeat.getOutput(), Column.IVM_BASE_OP_COL);
        return new IvmDeltaRewriteResult(newRepeat, repeatDmlFactorSlot, repeatBaseOpSlot);
    }

}
