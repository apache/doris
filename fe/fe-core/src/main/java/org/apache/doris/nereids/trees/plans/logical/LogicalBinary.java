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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BinaryPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all logical plan that have two children.
 */
public abstract class LogicalBinary<
            LEFT_CHILD_TYPE extends Plan,
            RIGHT_CHILD_TYPE extends Plan>
        extends AbstractLogicalPlan
        implements BinaryPlan<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    public LogicalBinary(PlanType type, Optional<GroupExpression> groupExpression,
                             Optional<LogicalProperties> logicalProperties, LEFT_CHILD_TYPE leftChild,
                             RIGHT_CHILD_TYPE rightChild) {
        super(type, groupExpression, logicalProperties, leftChild, rightChild);
    }

    public LogicalBinary(PlanType type, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        super(type, groupExpression, logicalProperties, children);
    }

    public abstract List<Slot> computeOutput();
}
