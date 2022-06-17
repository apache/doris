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

package org.apache.doris.nereids.operators.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.operators.AbstractOperator;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.UnaryPlanOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.PlaceHolderPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnaryPlan;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all logical operator that have one input.
 */
public abstract class LogicalUnaryOperator extends AbstractOperator
        implements LogicalOperator, UnaryPlanOperator {

    public LogicalUnaryOperator(OperatorType type) {
        super(type);
    }

    @Override
    public LogicalProperties computeLogicalProperties(Plan... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        return new LogicalProperties(computeOutput(inputs[0]));
    }

    public abstract List<Slot> computeOutput(Plan input);

    @Override
    public LogicalUnaryPlan toTreeNode(GroupExpression groupExpression) {
        LogicalProperties logicalProperties = groupExpression.getParent().getLogicalProperties();
        LogicalProperties childProperties = groupExpression.child(0).getLogicalProperties();
        return new LogicalUnaryPlan(this, Optional.of(groupExpression),
            Optional.of(logicalProperties), new PlaceHolderPlan(childProperties)
        );
    }
}
