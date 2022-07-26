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
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Eliminate the logical sub query and alias node after analyze and before rewrite
 * If we match the alias node and return its child node, in the execute() of the job
 *
 * TODO: refactor group merge strategy to support the feature above
 */
public class EliminateAliasNode implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.PROJECT_ELIMINATE_ALIAS_NODE.build(
                        logicalProject().then(project -> eliminateSubQueryAliasNode(project, project.children()))
                ),
                RuleType.FILTER_ELIMINATE_ALIAS_NODE.build(
                        logicalFilter().then(filter -> eliminateSubQueryAliasNode(filter, filter.children()))
                ),
                RuleType.JOIN_ELIMINATE_ALIAS_NODE.build(
                        logicalJoin().then(join -> eliminateSubQueryAliasNode(join, join.children()))
                ),
                RuleType.AGGREGATE_ELIMINATE_ALIAS_NODE.build(
                        logicalAggregate().then(agg -> eliminateSubQueryAliasNode(agg, agg.children()))
                )
        );
    }

    private LogicalPlan eliminateSubQueryAliasNode(LogicalPlan node, List<Plan> aliasNodes) {
        ArrayList<Plan> nodes = Lists.newArrayList();
        aliasNodes.forEach(child -> {
                    if (checkIsSubQueryAliasNode(child)) {
                        nodes.add(getPlan(child));
                    } else {
                        nodes.add(child);
                    }
                }
        );
        return (LogicalPlan) node.withChildren(nodes);
    }

    private boolean checkIsSubQueryAliasNode(Plan node) {
        return ((GroupPlan) node).getGroup().getLogicalExpression().getPlan().getType()
                == PlanType.LOGICAL_SUBQUERY_ALIAS;
    }

    private Plan getPlan(Plan node) {
        return ((GroupPlan) node).getGroup().getLogicalExpression().child(0).getLogicalExpression().getPlan();
    }
}
