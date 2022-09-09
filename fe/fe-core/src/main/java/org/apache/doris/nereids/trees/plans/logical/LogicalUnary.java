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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.UnaryPlan;

import java.util.List;
import java.util.Optional;

/**
 * Abstract class for all logical plan that have one child.
 */
public abstract class LogicalUnary<CHILD_TYPE extends Plan>
        extends AbstractLogicalPlan
        implements UnaryPlan<CHILD_TYPE> {

    public LogicalUnary(PlanType type, CHILD_TYPE child) {
        super(type, child);
    }

    public LogicalUnary(PlanType type, Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(type, logicalProperties, child);
    }

    public LogicalUnary(PlanType type, Optional<GroupExpression> groupExpression,
                            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
    }

    public abstract List<Slot> computeOutput();
}
