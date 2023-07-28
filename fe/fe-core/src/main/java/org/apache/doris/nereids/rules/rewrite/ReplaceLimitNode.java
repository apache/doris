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
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * rule to eliminate limit node by replace to other nodes.
 */
public class ReplaceLimitNode implements RewriteRuleFactory {
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
                        }).toRule(RuleType.PUSH_LIMIT_INTO_SORT),
                //limit->proj->sort ==> proj->topN
                logicalLimit(logicalProject(logicalSort()))
                        .then(limit -> {
                            LogicalProject project = limit.child();
                            LogicalSort sort = limit.child().child();
                            LogicalTopN topN = new LogicalTopN(sort.getOrderKeys(),
                                    limit.getLimit(),
                                    limit.getOffset(),
                                    sort.child(0));
                            return project.withChildren(Lists.newArrayList(topN));
                        }).toRule(RuleType.PUSH_LIMIT_INTO_SORT),
                logicalLimit(logicalOneRowRelation())
                        .then(limit -> limit.getLimit() > 0 && limit.getOffset() == 0
                                ? limit.child() : new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(),
                                limit.child().getOutput()))
                        .toRule(RuleType.ELIMINATE_LIMIT_ON_ONE_ROW_RELATION),
                logicalLimit(logicalEmptyRelation())
                        .then(UnaryNode::child)
                        .toRule(RuleType.ELIMINATE_LIMIT_ON_EMPTY_RELATION)
        );
    }
}
