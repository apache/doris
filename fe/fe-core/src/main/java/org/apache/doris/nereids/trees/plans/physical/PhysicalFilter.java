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
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Physical filter plan.
 */
public class PhysicalFilter<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Filter {

    private final Set<Expression> conjuncts;

    public PhysicalFilter(Set<Expression> conjuncts, LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(conjuncts, Optional.empty(), logicalProperties, child);
    }

    public PhysicalFilter(Set<Expression> conjuncts, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_FILTER, groupExpression, logicalProperties, child);
        this.conjuncts = ImmutableSet.copyOf(Objects.requireNonNull(conjuncts, "conjuncts can not be null"));
    }

    public PhysicalFilter(Set<Expression> conjuncts, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_FILTER, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.conjuncts = ImmutableSet.copyOf(Objects.requireNonNull(conjuncts, "conjuncts can not be null"));
    }

    @Override
    public Set<Expression> getConjuncts() {
        return conjuncts;
    }

    public List<Expression> getExpressions() {
        return ImmutableList.of(getPredicate());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalFilter[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                "predicates", getPredicate(),
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
        PhysicalFilter that = (PhysicalFilter) o;
        return conjuncts.equals(that.conjuncts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conjuncts);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalFilter(this, context);
    }

    @Override
    public PhysicalFilter<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalFilter<>(conjuncts, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, children.get(0));
    }

    @Override
    public PhysicalFilter<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalFilter<>(conjuncts, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalFilter<>(conjuncts, groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalFilter<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalFilter<>(conjuncts, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, child());
    }

    public PhysicalFilter<Plan> withConjunctsAndChild(Set<Expression> conjuncts, Plan child) {
        return new PhysicalFilter<>(conjuncts, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, child);
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("filter");
        builder.append(
                conjuncts.stream().map(conjunct -> conjunct.shapeInfo())
                        .sorted()
                        .collect(Collectors.joining(" and ", "(", ")")));
        // List<String> strConjuncts = Lists.newArrayList();
        // conjuncts.forEach(conjunct -> strConjuncts.add(conjunct.shapeInfo()));
        // builder.append(strConjuncts.stream().sorted().collect(Collectors.joining(" and ", "(", ")")));
        return builder.toString();
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public PhysicalFilter<Plan> resetLogicalProperties() {
        return new PhysicalFilter<>(conjuncts, groupExpression, null, physicalProperties,
                statistics, child());
    }
}
