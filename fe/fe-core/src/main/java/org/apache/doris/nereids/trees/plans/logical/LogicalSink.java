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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** abstract logical sink */
public abstract class LogicalSink<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {

    protected final List<NamedExpression> outputExprs;

    public LogicalSink(PlanType type, List<NamedExpression> outputExprs, CHILD_TYPE child) {
        super(type, child);
        this.outputExprs = ImmutableList.copyOf(Objects.requireNonNull(outputExprs, "outputExprs should not null"));
    }

    public LogicalSink(PlanType type, List<NamedExpression> outputExprs,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
        this.outputExprs = ImmutableList.copyOf(Objects.requireNonNull(outputExprs, "outputExprs should not null"));
    }

    public List<NamedExpression> getOutputExprs() {
        return outputExprs;
    }

    public abstract LogicalSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs);

    @Override
    public List<? extends Expression> getExpressions() {
        return outputExprs;
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExprs.stream()
                .map(NamedExpression::toSlot)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalSink<?> that = (LogicalSink<?>) o;
        return Objects.equals(outputExprs, that.outputExprs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), outputExprs);
    }
}
