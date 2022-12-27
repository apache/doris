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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical SetOperation.
 */
public abstract class PhysicalSetOperation extends AbstractPhysicalPlan implements SetOperation {
    protected final Qualifier qualifier;

    public PhysicalSetOperation(PlanType planType,
            Qualifier qualifier,
            LogicalProperties logicalProperties,
            List<Plan> inputs) {
        super(planType, Optional.empty(), logicalProperties, inputs.toArray(new Plan[0]));
        this.qualifier = qualifier;
    }

    public PhysicalSetOperation(PlanType planType,
            Qualifier qualifier,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            List<Plan> inputs) {
        super(planType, groupExpression, logicalProperties, inputs.toArray(new Plan[0]));
        this.qualifier = qualifier;
    }

    public PhysicalSetOperation(PlanType planType,
            Qualifier qualifier,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, List<Plan> inputs) {
        super(planType, groupExpression, logicalProperties,
                physicalProperties, statsDeriveResult, inputs.toArray(new Plan[0]));
        this.qualifier = qualifier;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalSetOperation(this, context);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalSetOperation",
                "qualifier", qualifier,
                "stats", statsDeriveResult);
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

}
