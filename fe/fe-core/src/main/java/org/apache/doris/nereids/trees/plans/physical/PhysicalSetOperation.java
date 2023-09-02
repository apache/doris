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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
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

    public PhysicalSetOperation(PlanType planType,
            Qualifier qualifier,
            List<NamedExpression> outputs,
            LogicalProperties logicalProperties,
            List<Plan> inputs) {
        super(planType, Optional.empty(), logicalProperties, inputs.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
    }

    public PhysicalSetOperation(PlanType planType,
            Qualifier qualifier,
            List<NamedExpression> outputs,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            List<Plan> inputs) {
        super(planType, groupExpression, logicalProperties, inputs.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
    }

    public PhysicalSetOperation(PlanType planType,
            Qualifier qualifier,
            List<NamedExpression> outputs,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, Statistics statistics, List<Plan> inputs) {
        super(planType, groupExpression, logicalProperties,
                physicalProperties, statistics, inputs.toArray(new Plan[0]));
        this.qualifier = qualifier;
        this.outputs = ImmutableList.copyOf(outputs);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalSetOperation(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalSetOperation",
                "qualifier", qualifier,
                "stats", statistics);
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
        return Objects.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Qualifier getQualifier() {
        return qualifier;
    }

    @Override
    public List<Slot> getFirstOutput() {
        return child(0).getOutput();
    }

    @Override
    public List<Slot> getChildOutput(int i) {
        return child(i).getOutput();
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
                                         AbstractPhysicalJoin builderNode,
                                         Expression src, Expression probeExpr,
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
                PhysicalProject project = (PhysicalProject) child;
                int projIndex = -1;
                Slot probeSlot = RuntimeFilterGenerator.checkTargetChild(probeExpr);
                if (probeSlot == null) {
                    continue;
                }
                for (int j = 0; j < project.getProjects().size(); j++) {
                    NamedExpression expr = (NamedExpression) project.getProjects().get(j);
                    if (expr.getName().equals(probeSlot.getName())) {
                        projIndex = j;
                        break;
                    }
                }
                if (projIndex < 0 || projIndex >= project.getProjects().size()) {
                    continue;
                }
                NamedExpression newProbeExpr = (NamedExpression) project.getProjects().get(projIndex);
                if (newProbeExpr instanceof Alias) {
                    newProbeExpr = (NamedExpression) newProbeExpr.child(0);
                }
                Slot newProbeSlot = RuntimeFilterGenerator.checkTargetChild(newProbeExpr);
                if (!RuntimeFilterGenerator.checkPushDownPreconditions(builderNode, ctx, newProbeSlot)) {
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
}
