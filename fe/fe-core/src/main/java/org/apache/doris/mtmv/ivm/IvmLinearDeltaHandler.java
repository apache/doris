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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites plan nodes whose delta propagates linearly through the plan.
 */
class IvmLinearDeltaHandler {
    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;

    /**
     * Wraps delta scan with Project(scan_output + dml_factor).
     */
    IvmDeltaRewriteResult rewriteScan(LogicalOlapScan scan) {
        if (!scan.isDelta()) {
            return new IvmDeltaRewriteResult(scan, null);
        }
        Expression factorExpr = buildDmlFactorExpr(scan);
        Alias factorAlias = new Alias(factorExpr, Column.IVM_DML_FACTOR_COL);
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builderWithExpectedSize(
                scan.getOutput().size() + 1);
        scan.getOutput().forEach(slot -> outputs.add((NamedExpression) slot));
        outputs.add(factorAlias);
        LogicalProject<?> project = new LogicalProject<>(outputs.build(), scan);
        Slot dmlFactorSlot = project.getOutput().get(project.getOutput().size() - 1);
        return new IvmDeltaRewriteResult(project, dmlFactorSlot);
    }

    /**
     * Builds the dml_factor expression for the given scan.
     */
    private Expression buildDmlFactorExpr(LogicalOlapScan scan) {
        if (scan.getTable().getColumn(Column.IVM_MOCK_BINLOG_OPERATION_COL) == null) {
            return new TinyIntLiteral((byte) 1);
        }
        Slot opSlot = helper.findSlotByName(scan.getOutput(), Column.IVM_MOCK_BINLOG_OPERATION_COL);
        return new If(
                new EqualTo(opSlot, new TinyIntLiteral((byte) 0)),
                new TinyIntLiteral((byte) 1),
                new TinyIntLiteral((byte) -1));
    }

    IvmDeltaRewriteResult rewriteProject(LogicalProject<? extends Plan> project,
            IvmDeltaRewriteVisitor visitor, IvmRefreshContext ctx) {
        IvmDeltaRewriteResult childResult = project.child().accept(visitor, ctx);
        if (childResult.terminal) {
            return childResult;
        }
        if (childResult.dmlFactorSlot == null) {
            LogicalProject<?> newProject = project.withProjectsAndChild(project.getProjects(), childResult.plan);
            return new IvmDeltaRewriteResult(newProject, null);
        }
        return propagateDmlFactorThroughProject(project, childResult);
    }

    /**
     * Appends (or reuses) dml_factor in the project's output list.
     */
    IvmDeltaRewriteResult propagateDmlFactorThroughProject(
            LogicalProject<? extends Plan> project, IvmDeltaRewriteResult childResult) {
        for (int i = 0; i < project.getProjects().size(); i++) {
            NamedExpression expr = project.getProjects().get(i);
            if (Column.IVM_DML_FACTOR_COL.equals(expr.getName())) {
                LogicalProject<?> newProject = project.withProjectsAndChild(
                        project.getProjects(), childResult.plan);
                return new IvmDeltaRewriteResult(newProject, newProject.getOutput().get(i));
            }
        }
        ImmutableList.Builder<NamedExpression> newOutputs = ImmutableList.builderWithExpectedSize(
                project.getProjects().size() + 1);
        newOutputs.addAll(project.getProjects());
        newOutputs.add(childResult.dmlFactorSlot);
        LogicalProject<?> newProject = project.withProjectsAndChild(newOutputs.build(), childResult.plan);
        return new IvmDeltaRewriteResult(newProject, childResult.dmlFactorSlot);
    }

    IvmDeltaRewriteResult rewriteFilter(LogicalFilter<? extends Plan> filter,
            IvmDeltaRewriteVisitor visitor, IvmRefreshContext ctx) {
        IvmDeltaRewriteResult childResult = filter.child().accept(visitor, ctx);
        Plan newFilter = filter.withChildren(ImmutableList.of(childResult.plan));
        return new IvmDeltaRewriteResult(newFilter, childResult.dmlFactorSlot);
    }

    IvmDeltaRewriteResult rewriteJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
            IvmDeltaRewriteVisitor visitor, IvmRefreshContext ctx) {
        JoinType joinType = join.getJoinType();
        if (joinType != JoinType.INNER_JOIN && joinType != JoinType.CROSS_JOIN) {
            throw new AnalysisException(
                    "IVM delta rewrite does not support join type: " + joinType);
        }
        if (join.isMarkJoin()) {
            throw new AnalysisException(
                    "IVM delta rewrite does not support mark join (subquery with disjunction).");
        }

        IvmDeltaRewriteResult leftResult = join.left().accept(visitor, ctx);
        IvmDeltaRewriteResult rightResult = join.right().accept(visitor, ctx);

        if (leftResult.dmlFactorSlot != null && rightResult.dmlFactorSlot != null) {
            throw new AnalysisException(
                    "IVM: both sides of join have dml_factor — expected at most one delta side");
        }

        LogicalJoin<Plan, Plan> newJoin = (LogicalJoin<Plan, Plan>) join.withChildren(
                leftResult.plan, rightResult.plan);

        if (leftResult.dmlFactorSlot == null && rightResult.dmlFactorSlot == null) {
            return new IvmDeltaRewriteResult(newJoin, null);
        } else {
            return helper.addNonDetGuardForJoinDelta(new JoinAdapter(newJoin), leftResult, rightResult, ctx);
        }
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
            return new IvmDeltaRewriteResult(newUnion, null);
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

        LogicalProject<Plan> mappedProject = new LogicalProject<>(projections.build(), deltaChild.plan);
        Slot newDmlFactor = mappedProject.getOutput().get(mappedProject.getOutput().size() - 1);
        return new IvmDeltaRewriteResult(mappedProject, newDmlFactor);
    }

    private static class JoinAdapter implements IvmDeltaRewriteHelper.JoinPlanView {
        private final LogicalJoin<Plan, Plan> join;

        private JoinAdapter(LogicalJoin<Plan, Plan> join) {
            this.join = join;
        }

        @Override
        public Plan plan() {
            return join;
        }

        @Override
        public Plan left() {
            return join.left();
        }

        @Override
        public Plan right() {
            return join.right();
        }

        @Override
        public JoinType joinType() {
            return join.getJoinType();
        }
    }
}
