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
import org.apache.doris.common.Pair;
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
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical project plan.
 */
public class PhysicalProject<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Project {

    private final List<NamedExpression> projects;

    public PhysicalProject(List<NamedExpression> projects, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(projects, Optional.empty(), logicalProperties, child);
    }

    public PhysicalProject(List<NamedExpression> projects, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PROJECT, groupExpression, logicalProperties, child);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
    }

    public PhysicalProject(List<NamedExpression> projects, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_PROJECT, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.projects = ImmutableList.copyOf(Objects.requireNonNull(projects, "projects can not be null"));
    }

    public List<NamedExpression> getProjects() {
        return projects;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalProject[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "projects", projects,
                "stats", statistics
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalProject that = (PhysicalProject) o;
        return projects.equals(that.projects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(projects);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalProject(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return projects;
    }

    @Override
    public PhysicalProject<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalProject<>(projects,
                groupExpression,
                getLogicalProperties(),
                physicalProperties,
                statistics,
                children.get(0)
        );
    }

    @Override
    public PhysicalProject<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalProject<>(projects, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalProject<>(projects, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalProject<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalProject<>(projects, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, child());
    }

    /**
     * replace projections and child, it is used for merge consecutive projections.
     * @param projections new projections
     * @param child new child
     * @return new project
     */
    public PhysicalProject<Plan> withProjectionsAndChild(List<NamedExpression> projections, Plan child) {
        return new PhysicalProject<>(ImmutableList.copyOf(projections),
                groupExpression,
                getLogicalProperties(),
                physicalProperties,
                statistics,
                child
        );
    }

    @Override
    public boolean pushDownRuntimeFilter(CascadesContext context, IdGenerator<RuntimeFilterId> generator,
            AbstractPhysicalJoin<?, ?> builderNode, Expression src, Expression probeExpr,
            TRuntimeFilterType type, long buildSideNdv, int exprOrder) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
        // currently, we can ensure children in the two side are corresponding to the equal_to's.
        // so right maybe an expression and left is a slot
        Slot probeSlot = RuntimeFilterGenerator.checkTargetChild(probeExpr);

        // aliasTransMap doesn't contain the key, means that the path from the scan to the join
        // contains join with denied join type. for example: a left join b on a.id = b.id
        if (!RuntimeFilterGenerator.checkPushDownPreconditionsForJoin(builderNode, ctx, probeSlot)) {
            return false;
        }
        PhysicalRelation scan = aliasTransferMap.get(probeSlot).first;
        Preconditions.checkState(scan != null, "scan is null");
        if (scan instanceof PhysicalCTEConsumer) {
            // update the probeExpr
            int projIndex = -1;
            for (int i = 0; i < getProjects().size(); i++) {
                NamedExpression expr = getProjects().get(i);
                if (expr.getName().equals(probeSlot.getName())) {
                    projIndex = i;
                    break;
                }
            }
            if (projIndex < 0 || projIndex >= getProjects().size()) {
                // the pushed down path can't contain the probe expr
                return false;
            }
            NamedExpression newProbeExpr = this.getProjects().get(projIndex);
            if (newProbeExpr instanceof Alias) {
                newProbeExpr = (NamedExpression) newProbeExpr.child(0);
            }
            Slot newProbeSlot = RuntimeFilterGenerator.checkTargetChild(newProbeExpr);
            if (!RuntimeFilterGenerator.checkPushDownPreconditionsForJoin(builderNode, ctx, newProbeSlot)) {
                return false;
            }
            scan = aliasTransferMap.get(newProbeSlot).first;
            probeExpr = newProbeExpr;
        }
        if (!RuntimeFilterGenerator.checkPushDownPreconditionsForRelation(this, scan)) {
            return false;
        }

        AbstractPhysicalPlan child = (AbstractPhysicalPlan) child(0);
        return child.pushDownRuntimeFilter(context, generator, builderNode,
                src, probeExpr, type, buildSideNdv, exprOrder);
    }

    @Override
    public List<Slot> computeOutput() {
        return projects.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public PhysicalProject<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalProject<>(projects, groupExpression, null, physicalProperties,
                statistics, child());
    }
}
