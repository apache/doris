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
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * limit ->agg(without agg functions) -> any(not limit)
 * =>
 * limit -> agg -> limit -> any
 */
public class PushLimitThroughDistinctAgg implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalLimit(logicalAggregate().when(LogicalAggregate::isDistinct))
                        .then(limit -> {
                            LogicalAggregate<? extends Plan> aggregate = limit.child();
                            Plan aggChild = aggregate.child();
                            if (aggChild instanceof LogicalLimit) {
                                return null;
                            }
                            LogicalLimit childLimit = new LogicalLimit(limit.getLimit() + limit.getOffset(),
                                    0, limit.getPhase(), aggChild);
                            aggregate = aggregate.withChildren(ImmutableList.of(childLimit));
                            return limit.withChildren(ImmutableList.of(aggregate));
                        }).toRule(RuleType.PUSH_DOWN_LIMIT_THROUGH_DISTINCT_AGG),
                logicalLimit(logicalProject(logicalAggregate().when(LogicalAggregate::isDistinct)))
                        .then(limit -> {
                            LogicalProject<? extends Plan> project = limit.child();
                            LogicalAggregate<? extends Plan> aggregate = (LogicalAggregate) project.child();
                            Plan aggChild = aggregate.child();
                            if (aggChild instanceof LogicalLimit) {
                                return null;
                            }
                            LogicalLimit childLimit = new LogicalLimit(limit.getLimit() + limit.getOffset(),
                                    0, limit.getPhase(), aggChild);
                            aggregate = aggregate.withChildren(ImmutableList.of(childLimit));
                            project = project.withChildren(ImmutableList.of(aggregate));
                            return limit.withChildren(ImmutableList.of(project));
                        }).toRule(RuleType.PUSH_DOWN_LIMIT_THROUGH_DISTINCT_AGG)
        );
    }
}
