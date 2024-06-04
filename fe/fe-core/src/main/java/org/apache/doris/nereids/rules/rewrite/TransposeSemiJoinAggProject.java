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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

/**
 * Pushdown semi-join through agg
 */
public class TransposeSemiJoinAggProject extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalJoin(logicalProject(logicalAggregate()), any())
                .whenNot(join -> ConnectContext.get().getSessionVariable().isDisableJoinReorder())
                .when(join -> join.getJoinType().isLeftSemiOrAntiJoin())
                .whenNot(join -> join.isMarkJoin())
                .when(join -> join.left().isAllSlots())
                .then(join -> {
                    LogicalProject<LogicalAggregate<Plan>> project = join.left();
                    LogicalAggregate<Plan> aggregate = project.child();
                    if (!TransposeSemiJoinAgg.canTranspose(aggregate, join)) {
                        return null;
                    }
                    Plan newPlan = aggregate.withChildren(join.withChildren(aggregate.child(), join.right()));
                    return project.withChildren(newPlan);
                }).toRule(RuleType.TRANSPOSE_LOGICAL_SEMI_JOIN_AGG_PROJECT);
    }
}
