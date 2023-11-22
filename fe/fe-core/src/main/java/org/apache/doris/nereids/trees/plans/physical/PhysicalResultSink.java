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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * result sink
 */
public class PhysicalResultSink<CHILD_TYPE extends Plan> extends PhysicalSink<CHILD_TYPE> implements Sink {

    public PhysicalResultSink(List<NamedExpression> outputExprs, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(outputExprs, groupExpression, logicalProperties, PhysicalProperties.GATHER, null, child);
    }

    public PhysicalResultSink(List<NamedExpression> outputExprs, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, @Nullable PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_RESULT_SINK, outputExprs, groupExpression,
                logicalProperties, physicalProperties, statistics, child);
    }

    public List<NamedExpression> getOutputExprs() {
        return outputExprs;
    }

    @Override
    public PhysicalResultSink<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalResultSink's children size must be 1, but real is %s", children.size());
        return new PhysicalResultSink<>(outputExprs, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalResultSink(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return outputExprs;
    }

    @Override
    public PhysicalResultSink<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalResultSink<>(outputExprs, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, child());
    }

    @Override
    public PhysicalResultSink<Plan> withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "PhysicalResultSink's children size must be 1, but real is %s", children.size());
        return new PhysicalResultSink<>(outputExprs, groupExpression, logicalProperties.get(),
                physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalResultSink<Plan> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalResultSink<>(outputExprs, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, child());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalResultSink<?> that = (PhysicalResultSink<?>) o;
        return outputExprs.equals(that.outputExprs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputExprs);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalResultSink[" + id.asInt() + "]",
                "outputExprs", outputExprs);
    }

    @Override
    public PhysicalResultSink<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalResultSink<>(outputExprs, groupExpression,
                null, physicalProperties, statistics, child());
    }
}
