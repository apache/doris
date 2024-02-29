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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * result sink
 */
public class LogicalResultSink<CHILD_TYPE extends Plan> extends LogicalSink<CHILD_TYPE>
        implements Sink, PropagateFuncDeps {

    public LogicalResultSink(List<NamedExpression> outputExprs, CHILD_TYPE child) {
        super(PlanType.LOGICAL_RESULT_SINK, outputExprs, child);
    }

    public LogicalResultSink(List<NamedExpression> outputExprs,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_RESULT_SINK, outputExprs, groupExpression, logicalProperties, child);
    }

    @Override
    public LogicalResultSink<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "LogicalResultSink's children size must be 1, but real is %s", children.size());
        return new LogicalResultSink<>(outputExprs, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalResultSink(this, context);
    }

    @Override
    public LogicalResultSink<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalResultSink<>(outputExprs, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public LogicalResultSink<Plan> withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalResultSink only accepts one child");
        return new LogicalResultSink<>(outputExprs, groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public LogicalResultSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalResultSink<>(outputExprs, child());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalResultSink[" + id.asInt() + "]",
                "outputExprs", outputExprs);
    }
}
