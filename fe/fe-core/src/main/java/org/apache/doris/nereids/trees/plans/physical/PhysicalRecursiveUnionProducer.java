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
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * PhysicalRecursiveUnionProducer is sentinel plan for must_shuffle
 */
public class PhysicalRecursiveUnionProducer<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {
    private final String cteName;

    public PhysicalRecursiveUnionProducer(String cteName, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(cteName, Optional.empty(), logicalProperties, child);
    }

    public PhysicalRecursiveUnionProducer(String cteName, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(cteName, groupExpression, logicalProperties, PhysicalProperties.ANY, null, child);
    }

    public PhysicalRecursiveUnionProducer(String cteName, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, @Nullable PhysicalProperties physicalProperties, Statistics statistics,
            CHILD_TYPE child) {
        super(PlanType.PHYSICAL_RECURSIVE_CTE_RECURSIVE_CHILD, groupExpression, logicalProperties, physicalProperties,
                statistics, child);
        this.cteName = cteName;
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("PhysicalRecursiveUnionProducer",
                "cteName", cteName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalRecursiveUnionProducer that = (PhysicalRecursiveUnionProducer) o;
        return cteName.equals(that.cteName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cteName);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalRecursiveUnionProducer<>(cteName, groupExpression, getLogicalProperties(),
                children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalRecursiveUnionProducer(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalRecursiveUnionProducer<>(cteName, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalRecursiveUnionProducer<>(cteName, groupExpression, logicalProperties.get(), child());
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {

    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {

    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {

    }

    @Override
    public void computeFd(DataTrait.Builder builder) {

    }

    @Override
    public PhysicalPlan withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalRecursiveUnionProducer<>(cteName, groupExpression, getLogicalProperties(),
                physicalProperties, statistics, child());
    }
}
