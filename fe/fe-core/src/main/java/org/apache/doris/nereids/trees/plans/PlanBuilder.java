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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;

import java.util.List;
import java.util.Optional;

/**
 * plan builder
 */
public class PlanBuilder {
    private Optional<GroupExpression> groupExpression;
    private List<Plan> children;
    private Optional<LogicalProperties> logicalProperties;

    public PlanBuilder() {
    }

    public PlanBuilder setChildren(List<Plan> children) {
        this.children = children;
        return this;
    }

    public PlanBuilder setGroupExpression(GroupExpression groupExpression) {
        this.groupExpression = Optional.ofNullable(groupExpression);
        return this;
    }

    public PlanBuilder setLogicalProperties(LogicalProperties logicalProperties) {
        this.logicalProperties = Optional.ofNullable(logicalProperties);
        return this;
    }

    public Plan build(Plan plan) {
        return plan.withGroupExpression(groupExpression).withLogicalProperties(logicalProperties)
                .withChildren(children);
    }
}
