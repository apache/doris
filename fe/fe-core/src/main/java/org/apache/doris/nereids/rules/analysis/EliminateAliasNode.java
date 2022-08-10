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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Eliminate the logical sub query and alias node after analyze and before rewrite
 * If we match the alias node and return its child node, in the execute() of the job
 * <p>
 * TODO: refactor group merge strategy to support the feature above
 */
public class EliminateAliasNode implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.PROJECT_ELIMINATE_ALIAS_NODE.build(
                        logicalProject(logicalSubQueryAlias())
                                .then(project -> project.withChildren(ImmutableList.of(project.child().child())))
                ),
                RuleType.FILTER_ELIMINATE_ALIAS_NODE.build(
                        logicalFilter(logicalSubQueryAlias())
                                .then(filter -> filter.withChildren(ImmutableList.of(filter.child().child())))
                ),
                RuleType.AGGREGATE_ELIMINATE_ALIAS_NODE.build(
                        logicalAggregate(logicalSubQueryAlias())
                                .then(agg -> agg.withChildren(ImmutableList.of(agg.child().child())))
                ),
                RuleType.JOIN_ELIMINATE_ALIAS_NODE.build(
                        logicalJoin(logicalSubQueryAlias(), logicalSubQueryAlias())
                                .then(join -> join.withChildren(
                                        ImmutableList.of(join.left().child(), join.right().child())))
                ),
                RuleType.JOIN_LEFT_CHILD_ELIMINATE_ALIAS_NODE.build(
                        logicalJoin(logicalSubQueryAlias(), group())
                                .then(join -> join.withChildren(
                                        ImmutableList.of(join.left().child(), join.right())))
                ),
                RuleType.JOIN_RIGHT_CHILD_ELIMINATE_ALIAS_NODE.build(
                        logicalJoin(group(), logicalSubQueryAlias())
                                .then(join -> join.withChildren(
                                        ImmutableList.of(join.left(), join.right().child())))
                )
        );
    }
}
