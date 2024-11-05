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
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical CTE producer.
 */
public class PhysicalCTEProducer<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {

    private final CTEId cteId;

    public PhysicalCTEProducer(CTEId cteId, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(cteId, Optional.empty(), logicalProperties, child);
    }

    public PhysicalCTEProducer(CTEId cteId, Optional<GroupExpression> groupExpression,
                               LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(cteId, groupExpression, logicalProperties, null, null, child);
    }

    public PhysicalCTEProducer(CTEId cteId, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_CTE_PRODUCER, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
        this.cteId = cteId;
    }

    public CTEId getCteId() {
        return cteId;
    }

    @Override
    public List<Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalCTEProducer<?> that = (PhysicalCTEProducer<?>) o;
        return cteId.equals(that.cteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cteId);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalCTEProducer[" + id.asInt() + "]",
                "stats", statistics,
                "cteId", cteId);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEProducer(this, context);
    }

    @Override
    public PhysicalCTEProducer<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalCTEProducer<>(cteId, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalCTEProducer<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalCTEProducer<>(cteId, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new PhysicalCTEProducer<>(cteId, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalCTEProducer<CHILD_TYPE> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalCTEProducer<>(cteId, groupExpression, getLogicalProperties(), physicalProperties,
            statistics, child());
    }

    @Override
    public String shapeInfo() {
        return Utils.toSqlString("PhysicalCteProducer",
                "cteId", cteId);
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public PhysicalCTEProducer<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalCTEProducer<>(cteId, groupExpression, null, physicalProperties,
                statistics, child());
    }
}
