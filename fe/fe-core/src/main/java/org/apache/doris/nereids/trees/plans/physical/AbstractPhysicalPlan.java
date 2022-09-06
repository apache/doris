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
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;

import java.util.Optional;

/**
 * Abstract class for all concrete physical plan.
 */
public abstract class AbstractPhysicalPlan extends AbstractPlan implements PhysicalPlan {

    protected final PhysicalProperties physicalProperties;

    /**
     * create physical plan by op, logicalProperties and children.
     */
    public AbstractPhysicalPlan(PlanType type, LogicalProperties logicalProperties, Plan... children) {
        super(type, Optional.empty(), Optional.of(logicalProperties), children);
        // TODO: compute physical properties
        this.physicalProperties = PhysicalProperties.ANY;
    }

    /**
     * create physical plan by groupExpression, logicalProperties and children.
     *
     * @param type node type
     * @param groupExpression group expression contains plan
     * @param logicalProperties logical properties of this plan
     * @param children children of this plan
     */
    public AbstractPhysicalPlan(PlanType type, Optional<GroupExpression> groupExpression,
                                LogicalProperties logicalProperties, Plan... children) {
        super(type, groupExpression, Optional.of(logicalProperties), children);
        // TODO: compute physical properties
        this.physicalProperties = PhysicalProperties.ANY;
    }

    @Override
    public LogicalProperties getLogicalProperties() {
        return logicalProperties;
    }

    @Override
    public PhysicalProperties getPhysicalProperties() {
        return physicalProperties;
    }

}
