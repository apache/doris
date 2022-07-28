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
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

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
                                .then(project -> eliminateSubQueryAliasNode(project, project.children()))
                ),
                RuleType.FILTER_ELIMINATE_ALIAS_NODE.build(
                        logicalFilter(logicalSubQueryAlias())
                                .then(filter -> eliminateSubQueryAliasNode(filter, filter.children()))
                ),
                RuleType.AGGREGATE_ELIMINATE_ALIAS_NODE.build(
                        logicalAggregate(logicalSubQueryAlias())
                                .then(agg -> eliminateSubQueryAliasNode(agg, agg.children()))
                ),
                RuleType.JOIN_ELIMINATE_ALIAS_NODE.build(
                        logicalJoin().then(join -> joinEliminateSubQueryAliasNode(join, join.children()))
                )
        );
    }

    private LogicalPlan eliminateSubQueryAliasNode(LogicalPlan node, List<Plan> aliasNodes) {
        List<Plan> nodes = aliasNodes.stream()
                .map(this::getPlan)
                .collect(Collectors.toList());
        return (LogicalPlan) node.withChildren(nodes);
    }

    private LogicalPlan joinEliminateSubQueryAliasNode(LogicalPlan node, List<Plan> aliasNode) {
        List<Plan> nodes = aliasNode.stream()
                .map(child -> {
                    if (checkIsSubQueryAliasNode((GroupPlan) child)) {
                        return ((GroupPlan) child).getGroup()
                                .getLogicalExpression()
                                .child(0)
                                .getLogicalExpression()
                                .getPlan();
                    }
                    return child;
                })
                .collect(Collectors.toList());
        return (LogicalPlan) node.withChildren(nodes);
    }

    private boolean checkIsSubQueryAliasNode(GroupPlan node) {
        return node.getGroup().getLogicalExpression().getPlan().getType()
                == PlanType.LOGICAL_SUBQUERY_ALIAS;
    }

    private Plan getPlan(Plan node) {
        return ((GroupPlan) node.child(0)).getGroup().getLogicalExpression().getPlan();
    }
}
