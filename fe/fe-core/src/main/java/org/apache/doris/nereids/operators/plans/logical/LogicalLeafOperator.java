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
import org.apache.doris.nereids.operators.plans.LeafPlanOperator;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;

import java.util.List;

/**
 * Abstract class for all logical operator that have no input.
 */
public abstract class LogicalLeafOperator extends AbstractOperator
        implements LogicalOperator, LeafPlanOperator {

    public LogicalLeafOperator(OperatorType type) {
        super(type);
    }

    @Override
    public final List<Slot> computeOutput(Plan... inputs) {
        return doComputeOutput();
    }

    public abstract List<Slot> doComputeOutput();

    @Override
    public LogicalLeafPlan toTreeNode(GroupExpression groupExpression) {
        return new LogicalLeafPlan(this, groupExpression, groupExpression.getParent().getLogicalProperties());
    }
}
