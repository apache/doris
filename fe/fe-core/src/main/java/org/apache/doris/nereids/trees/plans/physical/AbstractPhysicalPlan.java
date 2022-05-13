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

import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.List;

/**
 * Abstract class for all concrete physical plan.
 */
public abstract class AbstractPhysicalPlan<PLAN_TYPE extends AbstractPhysicalPlan<PLAN_TYPE>>
        extends AbstractPlan<PLAN_TYPE>
        implements PhysicalPlan<PLAN_TYPE> {

    protected LogicalProperties logicalProperties;
    protected PhysicalProperties physicalProperties;

    public AbstractPhysicalPlan(NodeType type, Plan... children) {
        super(type, children);
    }

    @Override
    public void setLogicalProperties(LogicalProperties logicalProperties) {
        this.logicalProperties = logicalProperties;
    }

    @Override
    public List<Slot> getOutput() {
        return logicalProperties.getOutput();
    }
}
