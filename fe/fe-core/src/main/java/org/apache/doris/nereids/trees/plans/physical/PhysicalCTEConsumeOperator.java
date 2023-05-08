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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical CTE consumer operator.
 */
public class PhysicalCTEConsumeOperator extends PhysicalLeaf {
    private final int cteId;

    private List<Expression> predicates;

    private List<Expression> projections;

    private final Map<Slot, Slot> cteOutputColumnRefMap;

    public PhysicalCTEConsumeOperator(int cteId, Map<Slot, Slot> cteOutputColumnRefMap,
                                      List<Expression> predicates, List<Expression> projections,
                                      LogicalProperties logicalProperties) {
        super(PlanType.PHYSICAL_CTE_CONSUME, Optional.empty(), logicalProperties);
        this.cteId = cteId;
        this.cteOutputColumnRefMap = cteOutputColumnRefMap;
        this.predicates = predicates;
        this.projections = projections;
    }

    public PhysicalCTEConsumeOperator(int cteId, Map<Slot, Slot> cteOutputColumnRefMap,
                                      List<Expression> predicates, List<Expression> projections,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties) {
        super(PlanType.PHYSICAL_CTE_CONSUME, groupExpression, logicalProperties);
        this.cteId = cteId;
        this.cteOutputColumnRefMap = cteOutputColumnRefMap;
        this.predicates = predicates;
        this.projections = projections;
    }

    public PhysicalCTEConsumeOperator(int cteId, Map<Slot, Slot> cteOutputColumnRefMap,
                                      List<Expression> predicates, List<Expression> projections,
                                      Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
                                      Statistics statistics) {
        super(PlanType.PHYSICAL_CTE_CONSUME, groupExpression, logicalProperties, physicalProperties, statistics);
        this.cteId = cteId;
        this.cteOutputColumnRefMap = cteOutputColumnRefMap;
        this.predicates = predicates;
        this.projections = projections;
    }

    public int getCteId() {
        return cteId;
    }

    public Map<Slot, Slot> getCteOutputColumnRefMap() {
        return cteOutputColumnRefMap;
    }

    public List<Expression> getPredicates() {
        return predicates;
    }

    public List<Expression> getProjections() {
        return projections;
    }

    @Override
    public List<Expression> getExpressions() {
        return Lists.newArrayList();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalCTEConsumeOperator that = (PhysicalCTEConsumeOperator) o;
        return Objects.equals(cteId, that.cteId)
                && Objects.equals(cteOutputColumnRefMap, that.cteOutputColumnRefMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId, cteOutputColumnRefMap);
    }

    @Override
    public String toString() {
        return "PhysicalCTEConsumeOperator{"
                + "cteId='"
                + cteId
                + '\''
                + '}';
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEConsume(this, context);
    }

    @Override
    public PhysicalCTEConsumeOperator withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 0);
        return new PhysicalCTEConsumeOperator(cteId, cteOutputColumnRefMap, predicates, projections,
                                                getLogicalProperties());
    }

    @Override
    public PhysicalCTEConsumeOperator withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalCTEConsumeOperator(cteId, cteOutputColumnRefMap, predicates, projections,
                                                groupExpression, getLogicalProperties());
    }

    @Override
    public PhysicalCTEConsumeOperator withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalCTEConsumeOperator(cteId, cteOutputColumnRefMap, predicates, projections,
            Optional.empty(), logicalProperties.get());
    }

    @Override
    public PhysicalCTEConsumeOperator withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                                                 Statistics statistics) {
        return new PhysicalCTEConsumeOperator(cteId, cteOutputColumnRefMap, predicates, projections, groupExpression,
            getLogicalProperties(), physicalProperties,
            statistics);
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("cte consumer(" + cteId + ")");
        return builder.toString();
    }
}
