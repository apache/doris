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
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Generate;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * PhysicalGenerate
 */
public class PhysicalGenerate<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements Generate {

    private final List<Function> generators;
    private final List<Slot> generatorOutput;

    public PhysicalGenerate(List<Function> generators, List<Slot> generatorOutput,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(generators, generatorOutput, Optional.empty(), logicalProperties, child);
    }

    /**
     * constructor
     */
    public PhysicalGenerate(List<Function> generators, List<Slot> generatorOutput,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_GENERATE, groupExpression, logicalProperties, child);
        this.generators = ImmutableList.copyOf(Objects.requireNonNull(generators, "predicates can not be null"));
        this.generatorOutput = ImmutableList.copyOf(Objects.requireNonNull(generatorOutput,
                "generatorOutput can not be null"));
    }

    /**
     * constructor
     */
    public PhysicalGenerate(List<Function> generators, List<Slot> generatorOutput,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_FILTER, groupExpression, logicalProperties, physicalProperties, statistics,
                child);
        this.generators = ImmutableList.copyOf(Objects.requireNonNull(generators, "predicates can not be null"));
        this.generatorOutput = ImmutableList.copyOf(Objects.requireNonNull(generatorOutput,
                "generatorOutput can not be null"));
    }

    @Override
    public List<Function> getGenerators() {
        return generators;
    }

    @Override
    public List<Slot> getGeneratorOutput() {
        return generatorOutput;
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return generators;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalGenerate",
                "generators", generators,
                "generatorOutput", generatorOutput
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
        PhysicalGenerate<?> that = (PhysicalGenerate<?>) o;
        return generators.equals(that.generators)
                && generatorOutput.equals(that.generatorOutput);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generators, generatorOutput);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalGenerate(this, context);
    }

    @Override
    public PhysicalGenerate<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalGenerate<>(generators, generatorOutput, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, children.get(0));
    }

    @Override
    public PhysicalGenerate<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalGenerate<>(generators, generatorOutput,
                groupExpression, getLogicalProperties(), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalGenerate<>(generators, generatorOutput,
                groupExpression, logicalProperties.get(), children.get(0));
    }

    @Override
    public PhysicalGenerate<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalGenerate<>(generators, generatorOutput,
                Optional.empty(), getLogicalProperties(), physicalProperties,
                statistics, child());
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(child().getOutput())
                .addAll(generatorOutput)
                .build();
    }

    @Override
    public PhysicalGenerate<CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalGenerate<>(generators, generatorOutput,
                Optional.empty(), null, physicalProperties,
                statistics, child());
    }
}
