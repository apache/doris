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
import org.apache.doris.nereids.operators.plans.logical.LogicalLeafOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all logical plan that have no child.
 */
public class LogicalLeafPlan<OP_TYPE extends LogicalLeafOperator>
        extends AbstractLogicalPlan<OP_TYPE>
        implements LeafPlan {

    public LogicalLeafPlan(OP_TYPE operator) {
        super(NodeType.LOGICAL, operator);
    }

    public LogicalLeafPlan(OP_TYPE operator, Optional<LogicalProperties> logicalProperties) {
        super(NodeType.LOGICAL, operator, logicalProperties);
    }

    public LogicalLeafPlan(OP_TYPE operator, Optional<GroupExpression> groupExpression,
                           Optional<LogicalProperties> logicalProperties) {
        super(NodeType.LOGICAL, operator, groupExpression, logicalProperties);
    }

    @Override
    public LogicalLeafPlan<OP_TYPE> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 0);
        return new LogicalLeafPlan(operator, groupExpression, Optional.empty());
    }

    @Override
    public LogicalLeafPlan<OP_TYPE> withOutput(List<Slot> output) {
        return new LogicalLeafPlan<>(operator, groupExpression,
            Optional.of(logicalProperties.withOutput(output)));
    }
}
