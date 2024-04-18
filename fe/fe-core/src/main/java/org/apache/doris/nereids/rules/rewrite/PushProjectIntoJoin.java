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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Push the project in the projection to the join.
 */
public class PushProjectIntoJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(logicalJoin())
                .when(p -> p.isAllSlots())
                .then(p -> {
                    LogicalJoin<Plan, Plan> join = p.child();
                    Set<ExprId> exprIds = p.getProjects().stream().map(e -> e.getExprId()).collect(Collectors.toSet());
                    join.setProjectExprIds(exprIds);
                    return join;
                }).toRule(RuleType.PUSH_FILTER_INSIDE_JOIN);
    }
}
