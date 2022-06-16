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
import org.apache.doris.nereids.operators.plans.logical.LogicalOperator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all concrete logical plan.
 */
public abstract class AbstractLogicalPlan<OP_TYPE extends LogicalOperator>
        extends AbstractPlan<OP_TYPE> implements LogicalPlan {

    public AbstractLogicalPlan(NodeType type, OP_TYPE operator, Plan... children) {
        super(type, operator, operator.computeLogicalProperties(children), children);
    }

    public AbstractLogicalPlan(NodeType type, OP_TYPE operator,
                               Optional<LogicalProperties> logicalProperties, Plan... children) {
        super(type, operator, logicalProperties.orElseGet(() -> operator.computeLogicalProperties(children)), children);
    }

    public AbstractLogicalPlan(NodeType type, OP_TYPE operator, Optional<GroupExpression> groupExpression,
                               Optional<LogicalProperties> logicalProperties, Plan... children) {
        super(type, operator, groupExpression,
                logicalProperties.orElseGet(() -> operator.computeLogicalProperties(children)), children);
    }

    @Override
    public LogicalProperties getLogicalProperties() {
        return logicalProperties;
    }

    @Override
    public List<Slot> getOutput() {
        return logicalProperties.getOutput();
    }
}
