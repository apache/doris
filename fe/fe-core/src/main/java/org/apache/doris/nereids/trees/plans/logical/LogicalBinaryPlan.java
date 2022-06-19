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
import org.apache.doris.nereids.operators.plans.logical.LogicalBinaryOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BinaryPlan;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all logical plan that have two children.
 */
public class LogicalBinaryPlan<
            OP_TYPE extends LogicalBinaryOperator,
            LEFT_CHILD_TYPE extends Plan,
            RIGHT_CHILD_TYPE extends Plan>
        extends AbstractLogicalPlan<OP_TYPE>
        implements BinaryPlan<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    public LogicalBinaryPlan(OP_TYPE operator, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(NodeType.LOGICAL, operator, Optional.empty(), leftChild, rightChild);
    }

    public LogicalBinaryPlan(OP_TYPE operator, Optional<LogicalProperties> logicalProperties,
                             LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(NodeType.LOGICAL, operator, logicalProperties, leftChild, rightChild);
    }

    public LogicalBinaryPlan(OP_TYPE operator, Optional<GroupExpression> groupExpression,
             Optional<LogicalProperties> logicalProperties, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(NodeType.LOGICAL, operator, groupExpression, logicalProperties, leftChild, rightChild);
    }

    @Override
    public LogicalBinaryPlan<OP_TYPE, Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalBinaryPlan(operator, groupExpression, Optional.empty(),
            children.get(0), children.get(1));
    }

    @Override
    public LogicalBinaryPlan<OP_TYPE, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withOutput(List<Slot> output) {
        return new LogicalBinaryPlan<>(operator, groupExpression,
            Optional.of(logicalProperties.withOutput(output)), left(), right());
    }
}
