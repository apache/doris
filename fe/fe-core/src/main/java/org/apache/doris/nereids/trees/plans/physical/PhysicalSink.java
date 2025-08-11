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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** abstract physical sink */
public abstract class PhysicalSink<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {

    protected final List<NamedExpression> outputExprs;

    public PhysicalSink(PlanType type,
                        List<NamedExpression> outputExprs,
                        Optional<GroupExpression> groupExpression,
                        LogicalProperties logicalProperties,
                        @Nullable PhysicalProperties physicalProperties,
                        Statistics statistics, CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, physicalProperties, statistics, child);
        this.outputExprs = ImmutableList.copyOf(Objects.requireNonNull(outputExprs, "outputExprs should not null"));
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExprs.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    public List<NamedExpression> getOutputExprs() {
        return outputExprs;
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        // should not be invoked
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        // should not be invoked
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        // should not be invoked
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        // should not be invoked
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName());
        ConnectContext context = ConnectContext.get();
        if (context != null
                && (context.getSessionVariable().getDetailShapePlanNodesSet().contains(getClass().getSimpleName()))
                    || context.getSessionVariable().getDetailShapePlanNodesSet().contains("PhysicalSink")) {
            builder.append(getOutputExprs().stream().map(Expression::shapeInfo)
                    .collect(Collectors.joining(", ", "[", "]")));
        }
        return builder.toString();
    }
}
