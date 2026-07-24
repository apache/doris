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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Rewrites plan nodes whose delta propagates linearly through the plan.
 */
class IvmLinearDeltaHandler {
    private final IvmDeltaRewriteHelper helper = IvmDeltaRewriteHelper.INSTANCE;

    Optional<IvmDeltaRewriteResult> rewriteOlapScan(LogicalOlapScan scan, IvmDeltaRewriteState state) {
        Optional<LogicalOlapTableStreamScan> deltaScan = state.createDeltaScan(scan);
        if (!deltaScan.isPresent()) {
            return Optional.empty();
        }
        return buildDeltaScanResult(scan, deltaScan.get(), state);
    }

    private Optional<IvmDeltaRewriteResult> buildDeltaScanResult(LogicalOlapScan originalScan,
            LogicalOlapTableStreamScan deltaScan, IvmDeltaRewriteState state) {
        long subSeqPrefix = state.nextSubSeqPrefix();
        Expression factorExpr = buildDmlFactorExpr(deltaScan);
        Alias factorAlias = new Alias(factorExpr, Column.IVM_DML_FACTOR_COL);
        Expression sequenceExpr = new If(
                new GreaterThan(factorExpr, new TinyIntLiteral((byte) 0)),
                new BigIntLiteral(state.toSequence(subSeqPrefix | 1)),
                new BigIntLiteral(state.toSequence(subSeqPrefix)));
        Alias sequenceAlias = new Alias(sequenceExpr, Column.SEQUENCE_COL);
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builderWithExpectedSize(
                deltaScan.getOutput().size() + 2);
        deltaScan.getOutput().forEach(outputs::add);
        outputs.add(factorAlias);
        outputs.add(sequenceAlias);
        LogicalProject<?> deltaProject = new LogicalProject<>(outputs.build(), deltaScan);
        ImmutableList.Builder<NamedExpression> remappedOutputs = ImmutableList.builderWithExpectedSize(
                originalScan.getOutput().size() + 2);
        for (Slot originalSlot : originalScan.getOutput()) {
            Slot deltaSlot = helper.findSlotByNameOrNull(deltaProject.getOutput(), originalSlot.getName());
            if (deltaSlot != null) {
                remappedOutputs.add(new Alias(originalSlot.getExprId(), deltaSlot, originalSlot.getName()));
            } else if (originalSlot.getName().startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
                remappedOutputs.add(new Alias(originalSlot.getExprId(),
                        helper.hiddenColumnFallbackLiteral(originalSlot), originalSlot.getName()));
            } else {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM: delta scan missing column " + originalSlot.getName());
            }
        }
        remappedOutputs.add(helper.findSlotByName(deltaProject.getOutput(), Column.IVM_DML_FACTOR_COL));
        remappedOutputs.add(helper.findSlotByName(deltaProject.getOutput(), Column.SEQUENCE_COL));
        LogicalProject<?> project = new LogicalProject<>(remappedOutputs.build(), deltaProject);
        int lastIdx = project.getOutput().size() - 1;
        Slot dmlFactorSlot = project.getOutput().get(lastIdx - 1);
        Slot sequenceSlot = project.getOutput().get(lastIdx);
        return Optional.of(new IvmDeltaRewriteResult(project, dmlFactorSlot, sequenceSlot,
                state.maxSeqSuffix(subSeqPrefix)));
    }

    private Expression buildDmlFactorExpr(LogicalOlapTableStreamScan scan) {
        Slot opSlot = IvmDeltaRewriteHelper.INSTANCE.findSlotByName(
                scan.getOutput(), Column.STREAM_CHANGE_TYPE_COL);
        return new If(
                new Or(new EqualTo(new VarcharLiteral("APPEND"), opSlot),
                        new EqualTo(new VarcharLiteral("UPDATE_AFTER"), opSlot)),
                new TinyIntLiteral((byte) 1),
                new TinyIntLiteral((byte) -1));
    }

    Optional<IvmDeltaRewriteResult> rewriteProject(LogicalProject<? extends Plan> project,
            IvmDeltaRewriteVisitor visitor, IvmIncrRefreshContext ctx) {
        Optional<IvmDeltaRewriteResult> childResult = project.child().accept(visitor, ctx);
        if (!childResult.isPresent()) {
            return Optional.empty();
        }
        IvmDeltaRewriteResult child = childResult.get();
        ImmutableList.Builder<NamedExpression> newOutputs = ImmutableList.builderWithExpectedSize(
                project.getProjects().size() + 2);
        Map<Slot, Slot> childOutputMapping = new HashMap<>();
        for (int i = 0; i < project.child().getOutput().size(); i++) {
            childOutputMapping.put(project.child().getOutput().get(i), child.plan.getOutput().get(i));
        }
        for (NamedExpression output : project.getProjects()) {
            for (Slot inputSlot : output.getInputSlots()) {
                if (!childOutputMapping.containsKey(inputSlot)) {
                    childOutputMapping.put(inputSlot, helper.findSlotByName(child.plan.getOutput(),
                            inputSlot.getName()));
                }
            }
            newOutputs.add((NamedExpression) ExpressionUtils.replace(output, childOutputMapping));
        }
        newOutputs.add(child.dmlFactorSlot);
        newOutputs.add(child.sequenceSlot);
        LogicalProject<?> newProject = project.withProjectsAndChild(newOutputs.build(), child.plan);
        Slot newDmlFactor = helper.findSlotByName(newProject.getOutput(), Column.IVM_DML_FACTOR_COL);
        Slot newSequence = helper.findSlotByName(newProject.getOutput(), Column.SEQUENCE_COL);
        return Optional.of(new IvmDeltaRewriteResult(newProject, newDmlFactor, newSequence, child.maxSeqSuffix));
    }

    Optional<IvmDeltaRewriteResult> rewriteFilter(LogicalFilter<? extends Plan> filter,
            IvmDeltaRewriteVisitor visitor, IvmIncrRefreshContext ctx) {
        return rewritePassThroughPlan(filter, visitor, ctx, false);
    }

    Optional<IvmDeltaRewriteResult> rewriteSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias,
            IvmDeltaRewriteVisitor visitor, IvmIncrRefreshContext ctx) {
        return rewritePassThroughPlan(alias, visitor, ctx, true);
    }

    private Optional<IvmDeltaRewriteResult> rewritePassThroughPlan(UnaryPlan<? extends Plan> plan,
            IvmDeltaRewriteVisitor visitor,
            IvmIncrRefreshContext ctx, boolean remapHiddenSlotsFromOutput) {
        Optional<IvmDeltaRewriteResult> childResult = plan.child().accept(visitor, ctx);
        if (!childResult.isPresent()) {
            return Optional.empty();
        }
        IvmDeltaRewriteResult deltaChild = childResult.get();
        Plan newPlan = plan.withChildren(ImmutableList.of(deltaChild.plan));
        if (!remapHiddenSlotsFromOutput) {
            return Optional.of(new IvmDeltaRewriteResult(newPlan,
                    deltaChild.dmlFactorSlot, deltaChild.sequenceSlot, deltaChild.maxSeqSuffix));
        } else {
            Slot newDmlFactorSlot = helper.findSlotByName(newPlan.getOutput(), Column.IVM_DML_FACTOR_COL);
            Slot newSequenceSlot = helper.findSlotByName(newPlan.getOutput(), Column.SEQUENCE_COL);
            return Optional.of(new IvmDeltaRewriteResult(newPlan, newDmlFactorSlot,
                    newSequenceSlot, deltaChild.maxSeqSuffix));
        }
    }

    Optional<IvmDeltaRewriteResult> rewriteUnion(LogicalUnion union,
            IvmDeltaRewriteVisitor visitor, IvmIncrRefreshContext ctx) {
        List<IvmDeltaRewriteResult> childResults = new ArrayList<>();
        for (int i = 0; i < union.children().size(); i++) {
            Optional<IvmDeltaRewriteResult> childResult = union.child(i).accept(visitor, ctx);
            if (!childResult.isPresent()) {
                continue;
            }
            IvmDeltaRewriteResult deltaChild = childResult.get();
            List<SlotReference> childMapping = union.getRegularChildrenOutputs().get(i);
            List<NamedExpression> unionOutputs = union.getOutputs();
            ImmutableList.Builder<NamedExpression> projections = ImmutableList.builder();
            for (int j = 0; j < unionOutputs.size(); j++) {
                NamedExpression unionOut = unionOutputs.get(j);
                SlotReference childSlot = childMapping.get(j);
                projections.add(new Alias(unionOut.getExprId(), childSlot, unionOut.getName()));
            }
            projections.add(deltaChild.dmlFactorSlot);
            projections.add(deltaChild.sequenceSlot);
            LogicalProject<Plan> mappedProject = new LogicalProject<>(projections.build(), deltaChild.plan);
            Slot newDmlFactor = helper.findSlotByName(mappedProject.getOutput(), Column.IVM_DML_FACTOR_COL);
            Slot newSequence = helper.findSlotByName(mappedProject.getOutput(), Column.SEQUENCE_COL);
            childResults.add(new IvmDeltaRewriteResult(mappedProject, newDmlFactor, newSequence,
                    deltaChild.maxSeqSuffix));
        }
        return helper.combineDeltaResults(childResults, union.getOutput());
    }

    Optional<IvmDeltaRewriteResult> rewriteRepeat(LogicalRepeat<? extends Plan> repeat,
            IvmDeltaRewriteVisitor visitor, IvmIncrRefreshContext ctx) {
        Optional<IvmDeltaRewriteResult> childResult = repeat.child().accept(visitor, ctx);
        if (!childResult.isPresent()) {
            return Optional.empty();
        }
        IvmDeltaRewriteResult child = childResult.get();
        List<NamedExpression> newOutputs = new ArrayList<>(repeat.getOutputExpressions());
        newOutputs.add(child.dmlFactorSlot);
        newOutputs.add(child.sequenceSlot);
        LogicalRepeat<Plan> newRepeat = repeat.withAggOutputAndChild(newOutputs, child.plan);
        // LogicalRepeat appends GROUPING_ID after outputExpressions. Restore the original output order before
        // appending delta metadata, otherwise an upper Project maps GROUPING_ID to a hidden delta slot.
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builderWithExpectedSize(
                repeat.getOutput().size() + 2);
        for (Slot output : repeat.getOutput()) {
            Slot repeatedOutput = helper.findSlotByName(newRepeat.getOutput(), output.getName());
            projects.add(new Alias(output.getExprId(), repeatedOutput, output.getName()));
        }
        Slot repeatDmlFactorSlot = helper.findSlotByName(newRepeat.getOutput(), Column.IVM_DML_FACTOR_COL);
        Slot repeatSequenceSlot = helper.findSlotByName(newRepeat.getOutput(), Column.SEQUENCE_COL);
        projects.add(repeatDmlFactorSlot);
        projects.add(repeatSequenceSlot);
        LogicalProject<Plan> project = new LogicalProject<>(projects.build(), newRepeat);
        return Optional.of(new IvmDeltaRewriteResult(project,
                helper.findSlotByName(project.getOutput(), Column.IVM_DML_FACTOR_COL),
                helper.findSlotByName(project.getOutput(), Column.SEQUENCE_COL), child.maxSeqSuffix));
    }
}
