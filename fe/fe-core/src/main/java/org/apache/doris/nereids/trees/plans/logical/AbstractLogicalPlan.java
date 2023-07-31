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
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Abstract class for all concrete logical plan.
 */
public abstract class AbstractLogicalPlan extends AbstractPlan implements LogicalPlan, Explainable {

    private final Supplier<Boolean> hasUnboundExpressions = super::hasUnboundExpression;

    public AbstractLogicalPlan(PlanType type, Plan... children) {
        super(type, children);
    }

    public AbstractLogicalPlan(PlanType type, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, Plan... children) {
        super(type, groupExpression, logicalProperties, null, children);
    }

    public AbstractLogicalPlan(PlanType type, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        super(type, groupExpression, logicalProperties, null, children);
    }

    @Override
    public boolean hasUnboundExpression() {
        return hasUnboundExpressions.get();
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return this;
    }
}
