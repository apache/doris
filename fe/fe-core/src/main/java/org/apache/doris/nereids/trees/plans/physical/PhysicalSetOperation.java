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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical SetOperation.
 */
public abstract class PhysicalSetOperation extends AbstractPhysicalPlan implements SetOperation {

    protected final Qualifier qualifier;
    protected final List<NamedExpression> outputs;
    protected final List<List<SlotReference>> regularChildrenOutputs;

    public PhysicalSetOperation(PlanType planType,
            Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> regularChildrenOutputs,
            LogicalProperties logicalProperties,
            List<Plan> children) {
        super(planType, Optional.empty(), logicalProperties, children.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
        this.regularChildrenOutputs = ImmutableList.copyOf(regularChildrenOutputs);
    }

    public PhysicalSetOperation(PlanType planType,
            Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> regularChildrenOutputs,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            List<Plan> children) {
        super(planType, groupExpression, logicalProperties, children.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
        this.regularChildrenOutputs = ImmutableList.copyOf(regularChildrenOutputs);
    }

    public PhysicalSetOperation(PlanType planType,
            Qualifier qualifier,
            List<NamedExpression> outputs,
            List<List<SlotReference>> regularChildrenOutputs,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, List<Plan> children) {
        super(planType, groupExpression, logicalProperties,
                physicalProperties, statistics, children.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
        this.regularChildrenOutputs = ImmutableList.copyOf(regularChildrenOutputs);
    }

    public List<List<SlotReference>> getRegularChildrenOutputs() {
        return regularChildrenOutputs;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalSetOperation(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalSetOperation that = (PhysicalSetOperation) o;
        return qualifier == that.qualifier && Objects.equals(outputs, that.outputs) && Objects.equals(
                regularChildrenOutputs, that.regularChildrenOutputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier, outputs, regularChildrenOutputs);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return regularChildrenOutputs.stream().flatMap(List::stream).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Qualifier getQualifier() {
        return qualifier;
    }

    @Override
    public List<SlotReference> getRegularChildOutput(int i) {
        return regularChildrenOutputs.get(i);
    }

    @Override
    public List<NamedExpression> getOutputs() {
        return getOutput().stream()
                .map(NamedExpression.class::cast)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public int getArity() {
        return children.size();
    }

    @Override
    public boolean pushDownRuntimeFilter(CascadesContext context, IdGenerator<RuntimeFilterId> generator,
            AbstractPhysicalJoin<?, ?> builderNode, Expression src, Expression probeExpr,
            TRuntimeFilterType type, long buildSideNdv, int exprOrder) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        boolean pushedDown = false;
        for (int i = 0; i < this.children().size(); i++) {
            AbstractPhysicalPlan child = (AbstractPhysicalPlan) this.child(i);
            // TODO: replace this special logic with dynamic handling and the name matching
            if (child instanceof PhysicalDistribute) {
                child = (AbstractPhysicalPlan) child.child(0);
            }
            if (child instanceof PhysicalProject) {
                PhysicalProject<?> project = (PhysicalProject<?>) child;
                int projIndex = -1;
                Slot probeSlot = RuntimeFilterGenerator.checkTargetChild(probeExpr);
                if (probeSlot == null) {
                    continue;
                }
                for (int j = 0; j < project.getProjects().size(); j++) {
                    NamedExpression expr = project.getProjects().get(j);
                    if (expr.getName().equals(probeSlot.getName())) {
                        projIndex = j;
                        break;
                    }
                }
                if (projIndex < 0 || projIndex >= project.getProjects().size()) {
                    continue;
                }
                NamedExpression newProbeExpr = project.getProjects().get(projIndex);
                if (newProbeExpr instanceof Alias) {
                    newProbeExpr = (NamedExpression) newProbeExpr.child(0);
                }
                Slot newProbeSlot = RuntimeFilterGenerator.checkTargetChild(newProbeExpr);
                if (!RuntimeFilterGenerator.checkPushDownPreconditionsForJoin(builderNode, ctx, newProbeSlot)) {
                    continue;
                }
                pushedDown |= child.pushDownRuntimeFilter(context, generator, builderNode, src,
                        newProbeExpr, type, buildSideNdv, exprOrder);
            }
        }
        return pushedDown;
    }

    @Override
    public List<Slot> computeOutput() {
        return outputs.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    public boolean isDistinct() {
        return qualifier == Qualifier.DISTINCT;
    }
}
