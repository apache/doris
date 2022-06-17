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
import org.apache.doris.nereids.operators.plans.logical.LogicalUnaryOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all logical plan that have one child.
 */
public class LogicalUnaryPlan<OP_TYPE extends LogicalUnaryOperator, CHILD_TYPE extends Plan>
        extends AbstractLogicalPlan<OP_TYPE>
        implements UnaryPlan<CHILD_TYPE> {

    public LogicalUnaryPlan(OP_TYPE operator, CHILD_TYPE child) {
        super(NodeType.LOGICAL, operator, child);
    }

    public LogicalUnaryPlan(OP_TYPE operator, Optional<GroupExpression> groupExpression,
                            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(NodeType.LOGICAL, operator, groupExpression, logicalProperties, child);
    }

    @Override
    public LogicalUnaryPlan<OP_TYPE, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalUnaryPlan(operator, groupExpression, Optional.empty(), children.get(0));
    }

    @Override
    public LogicalUnaryPlan<OP_TYPE, CHILD_TYPE> withOutput(List<Slot> output) {
        return new LogicalUnaryPlan<>(operator, groupExpression,
            Optional.of(logicalProperties.withOutput(output)), child());
    }
}
