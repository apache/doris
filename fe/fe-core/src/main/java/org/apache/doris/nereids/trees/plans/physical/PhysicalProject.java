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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical project plan.
 */
public class PhysicalProject<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Project {

    private final List<NamedExpression> projects;
    //multiLayerProjects is used to extract common expressions
    // projects: (A+B) * 2, (A+B) * 3
    // multiLayerProjects:
    //            L1: A+B as x
    //            L2: x*2, x*3
    private List<List<NamedExpression>> multiLayerProjects = Lists.newArrayList();

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
        StringBuilder cse = new StringBuilder();
        for (int i = 0; i < multiLayerProjects.size(); i++) {
            List<NamedExpression> layer = multiLayerProjects.get(i);
            cse.append("l").append(i).append("(").append(layer).append(")");
        }
        return Utils.toSqlString("PhysicalProject[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "stats", statistics, "projects", projects, "multi_proj", cse.toString()
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
    public List<Slot> computeOutput() {
        List<NamedExpression> output = projects;
        if (! multiLayerProjects.isEmpty()) {
            int layers = multiLayerProjects.size();
            output = multiLayerProjects.get(layers - 1);
        }
        return output.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public PhysicalProject<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalProject<>(projects, groupExpression, null, physicalProperties,
                statistics, child());
    }

    /**
     * extract common expr, set multi layer projects
     */
    public void computeMultiLayerProjectsForCommonExpress() {
        // hard code: select (s_suppkey + s_nationkey), 1+(s_suppkey + s_nationkey), s_name from supplier;
        if (projects.size() == 3) {
            if (projects.get(2) instanceof SlotReference) {
                SlotReference sName = (SlotReference) projects.get(2);
                if (sName.getName().equals("s_name")) {
                    Alias a1 = (Alias) projects.get(0); // (s_suppkey + s_nationkey)
                    Alias a2 = (Alias) projects.get(1); // 1+(s_suppkey + s_nationkey)
                    // L1: (s_suppkey + s_nationkey) as x, s_name
                    multiLayerProjects.add(Lists.newArrayList(projects.get(0), projects.get(2)));
                    List<NamedExpression> l2 = Lists.newArrayList();
                    l2.add(a1.toSlot());
                    Alias a3 = new Alias(a2.getExprId(), new Add(a1.toSlot(), a2.child().child(1)), a2.getName());
                    l2.add(a3);
                    l2.add(sName);
                    // L2: x, (1+x) as y, s_name
                    multiLayerProjects.add(l2);
                }
            }
        }
        // hard code:
        // select (s_suppkey + n_regionkey) + 1 as x, (s_suppkey + n_regionkey) + 2 as y
        // from supplier join nation on s_nationkey=n_nationkey
        // projects: x, y
        // multi L1: s_suppkey, n_regionkey, (s_suppkey + n_regionkey) as z
        //       L2: z +1 as x, z+2 as y
        if (projects.size() == 2 && projects.get(0) instanceof Alias && projects.get(1) instanceof Alias
                && ((Alias) projects.get(0)).getName().equals("x")
                && ((Alias) projects.get(1)).getName().equals("y")) {
            Alias a0 = (Alias) projects.get(0);
            Alias a1 = (Alias) projects.get(1);
            Add common = (Add) a0.child().child(0); // s_suppkey + n_regionkey
            List<NamedExpression> l1 = Lists.newArrayList();
            common.children().stream().forEach(child -> l1.add((SlotReference) child));
            Alias aliasOfCommon = new Alias(common);
            l1.add(aliasOfCommon);
            multiLayerProjects.add(l1);
            Add add1 = new Add(common, a0.child().child(0).child(1));
            Alias aliasOfAdd1 = new Alias(a0.getExprId(), add1, a0.getName());
            Add add2 = new Add(common, a1.child().child(0).child(1));
            Alias aliasOfAdd2 = new Alias(a1.getExprId(), add2, a1.getName());
            List<NamedExpression> l2 = Lists.newArrayList(aliasOfAdd1, aliasOfAdd2);
            multiLayerProjects.add(l2);
        }
    }

    public boolean hasMultiLayerProjection() {
        return !multiLayerProjects.isEmpty();
    }

    public List<List<NamedExpression>> getMultiLayerProjects() {
        return multiLayerProjects;
    }

    public void setMultiLayerProjects(List<List<NamedExpression>> multiLayers) {
        this.multiLayerProjects = multiLayers;
    }
}
