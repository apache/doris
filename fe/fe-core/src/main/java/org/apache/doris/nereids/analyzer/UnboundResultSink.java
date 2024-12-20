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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * unbound result sink
 */
public class UnboundResultSink<CHILD_TYPE extends Plan> extends LogicalSink<CHILD_TYPE>
        implements Unbound, Sink, BlockFuncDepsPropagation {

    public UnboundResultSink(CHILD_TYPE child) {
        super(PlanType.LOGICAL_UNBOUND_RESULT_SINK, ImmutableList.of(), child);
    }

    public UnboundResultSink(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_UNBOUND_RESULT_SINK, ImmutableList.of(), groupExpression, logicalProperties, child);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "UnboundResultSink only accepts one child");
        return new UnboundResultSink<>(groupExpression, Optional.empty(), children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitUnboundResultSink(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UnboundResultSink<>(groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1, "UnboundResultSink only accepts one child");
        return new UnboundResultSink<>(groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public UnboundResultSink<CHILD_TYPE> withOutputExprs(List<NamedExpression> outputExprs) {
        throw new UnboundException("could not call withOutputExprs on UnboundResultSink");
    }

    @Override
    public List<Slot> computeOutput() {
        throw new UnboundException("output");
    }

    @Override
    public String toString() {
        return Utils.toSqlString("UnboundResultSink[" + id.asInt() + "]");
    }
}
