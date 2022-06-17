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
import org.apache.doris.nereids.operators.plans.physical.PhysicalUnaryOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all physical plan that have one child.
 */
public class PhysicalUnaryPlan<OP_TYPE extends PhysicalUnaryOperator, CHILD_TYPE extends Plan>
        extends AbstractPhysicalPlan<OP_TYPE>
        implements UnaryPlan<CHILD_TYPE> {

    public PhysicalUnaryPlan(OP_TYPE operator, LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(NodeType.PHYSICAL, operator, logicalProperties, child);
    }

    public PhysicalUnaryPlan(OP_TYPE operator, Optional<GroupExpression> groupExpression,
                             LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(NodeType.PHYSICAL, operator, groupExpression, logicalProperties, child);
    }

    @Override
    public PhysicalUnaryPlan<OP_TYPE, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalUnaryPlan(operator, groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public PhysicalUnaryPlan<OP_TYPE, CHILD_TYPE> withOutput(List<Slot> output) {
        LogicalProperties logicalProperties = new LogicalProperties(output);
        return new PhysicalUnaryPlan<>(operator, groupExpression, logicalProperties, child());
    }
}
