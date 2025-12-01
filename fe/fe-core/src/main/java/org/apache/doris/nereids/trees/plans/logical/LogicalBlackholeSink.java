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
 * logical blackhole sink
 * The blackhole sink is currently used in "warm up select" SQL statements to preload file block caches into tables.
 * It is planned as a terminal sink (like /dev/null),
 * meaning a "black hole" at the end of the execution plan that discards all incoming data.
 */
public class LogicalBlackholeSink<CHILD_TYPE extends Plan> extends LogicalSink<CHILD_TYPE>
        implements Sink, PropagateFuncDeps {

    public LogicalBlackholeSink(List<NamedExpression> outputExprs, CHILD_TYPE child) {
        super(PlanType.LOGICAL_BLACKHOLE_SINK, outputExprs, child);
    }

    public LogicalBlackholeSink(List<NamedExpression> outputExprs,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_BLACKHOLE_SINK, outputExprs, groupExpression, logicalProperties, child);
    }

    @Override
    public LogicalBlackholeSink<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "LogicalBlackholeSink's children size must be 1, but real is %s", children.size());
        return new LogicalBlackholeSink<>(outputExprs, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalBlackholeSink(this, context);
    }

    @Override
    public LogicalBlackholeSink<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalBlackholeSink<>(outputExprs, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public LogicalBlackholeSink<Plan> withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "LogicalBlackholeSink only accepts one child");
        return new LogicalBlackholeSink<>(outputExprs, groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public LogicalBlackholeSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        return new LogicalBlackholeSink<>(outputExprs, child());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalBlackholeSink[" + id.asInt() + "]",
                "outputExprs", outputExprs);
    }
}
