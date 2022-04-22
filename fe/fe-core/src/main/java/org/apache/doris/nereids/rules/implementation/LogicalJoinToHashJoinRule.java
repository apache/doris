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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBroadcastHashJoin;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Implementation rule that convert logical join to physical hash join.
 */
public class LogicalJoinToHashJoinRule extends ImplementationRule {
    /**
     * Constructor for LogicalJoinToHashJoinRule.
     */
    public LogicalJoinToHashJoinRule() {
        super(RuleType.LOGICAL_JOIN_TO_HASH_JOIN_RULE,
                new Pattern(NodeType.LOGICAL_JOIN,
                        Pattern.PATTERN_LEAF_INSTANCE,
                        Pattern.PATTERN_LEAF_INSTANCE));
    }

    @Override
    public List<Plan<?>> transform(Plan<?> plan, PlannerContext context) {
        LogicalJoin originPlan = (LogicalJoin) plan;
        PhysicalBroadcastHashJoin physicalBroadcastHashJoin = new PhysicalBroadcastHashJoin(
                originPlan.getJoinType(),
                originPlan.getOnClause());
        return Lists.newArrayList(physicalBroadcastHashJoin);
    }
}
