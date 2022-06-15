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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.operators.plans.logical.LogicalBinaryOperator;
import org.apache.doris.nereids.operators.plans.logical.LogicalLeafOperator;
import org.apache.doris.nereids.operators.plans.logical.LogicalUnaryOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalBinaryOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalLeafOperator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalUnaryOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnaryPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLeafPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnaryPlan;

/**
 * An interface provided some builder of Plan.
 * Child Interface(PlanRuleFactory) can use to build some plan for transform rule.
 * You can simply use the override plan function to build a plan by the operator type.
 */
public interface Plans {
    default <OP_TYPE extends LogicalLeafOperator> LogicalLeafPlan<OP_TYPE> plan(OP_TYPE op) {
        return new LogicalLeafPlan(op);
    }

    default <OP_TYPE extends LogicalUnaryOperator, CHILD_TYPE extends Plan> LogicalUnaryPlan<OP_TYPE, CHILD_TYPE>
            plan(OP_TYPE op, CHILD_TYPE child) {
        return new LogicalUnaryPlan(op, child);
    }

    default <OP_TYPE extends LogicalBinaryOperator, LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
            LogicalBinaryPlan<OP_TYPE, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>
            plan(OP_TYPE op, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        return new LogicalBinaryPlan(op, leftChild, rightChild);
    }

    default <OP_TYPE extends PhysicalLeafOperator> PhysicalLeafPlan<OP_TYPE>
            plan(OP_TYPE op, LogicalProperties logicalProperties) {
        return new PhysicalLeafPlan(op, logicalProperties);
    }

    default <OP_TYPE extends PhysicalUnaryOperator, CHILD_TYPE extends Plan> PhysicalUnaryPlan<OP_TYPE, CHILD_TYPE>
            plan(OP_TYPE op, LogicalProperties logicalProperties, CHILD_TYPE child) {
        return new PhysicalUnaryPlan(op, logicalProperties, child);
    }

    default <OP_TYPE extends PhysicalBinaryOperator, LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
            PhysicalBinaryPlan<OP_TYPE, LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE>
            plan(OP_TYPE op, LogicalProperties logicalProperties,
                LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        return new PhysicalBinaryPlan(op, logicalProperties, leftChild, rightChild);
    }
}
