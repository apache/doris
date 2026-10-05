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
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * rule to eliminate limit node by replace to other nodes.
 */
public class LimitSortToTopN implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // limit -> sort ==> topN
                logicalLimit(logicalSort())
                        .then(limit -> {
                            LogicalSort<Plan> sort = limit.child();
                            return new LogicalTopN<>(sort.getOrderKeys(),
                                    limit.getLimit(),
                                    limit.getOffset(),
                                    sort.child(0));
                        }).toRule(RuleType.LIMIT_SORT_TO_TOP_N),
                // limit -> proj -> sort ==> proj -> topN
                logicalLimit(logicalProject(logicalSort()))
                        .then(limit -> {
                            LogicalProject<LogicalSort<Plan>> project = limit.child();
                            LogicalSort<Plan> sort = limit.child().child();
                            LogicalTopN<Plan> topN = new LogicalTopN<>(sort.getOrderKeys(),
                                    limit.getLimit(),
                                    limit.getOffset(),
                                    sort.child(0));
                            return project.withChildren(Lists.newArrayList(topN));
                        }).toRule(RuleType.LIMIT_SORT_TO_TOP_N)
        );
    }
}
