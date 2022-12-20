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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.analysis.AnalysisRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * 1. Include oneRowRelation in the union, hide the oneRowRelation, and reduce the generation of union nodes.
 * eg: select k1, k2 from t1 union select 1, 2 union select d1, d2 from t2;
 * before:
 *                          logicalUnion()
 *                      /                    \
 *              logicalUnion()           logicalProject
 *              /           \
 *    logicalProject  logicalOneRowRelation(BuildUnionNode:true)
 * eg: select k1, k2 from t1 union select 1, 2 union select d1, d2 from t2;
 *
 * after:
 *                          logicalUnion()
 *                      /                    \
 *              logicalUnion()           logicalProject
 *              /           \
 *    logicalProject  logicalOneRowRelation(BuildUnionNode:false)
 */
public class HideOneRowRelationUnderUnion implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.HIDE_ONE_ROW_RELATION_UNDER_UNION.build(
                logicalUnion(logicalOneRowRelation().when(LogicalOneRowRelation::buildUnionNode), group())
                    .then(union -> {
                        List<Plan> newChildren = new ImmutableList.Builder<Plan>()
                                .add(((LogicalOneRowRelation) union.child(0)).withBuildUnionNode(false))
                                .add(union.child(1))
                                .build();
                        return union.withChildren(newChildren);
                    })
            ),
            RuleType.HIDE_ONE_ROW_RELATION_UNDER_UNION.build(
                logicalUnion(group(), logicalOneRowRelation().when(LogicalOneRowRelation::buildUnionNode))
                    .then(union -> {
                        List<Plan> children = new ImmutableList.Builder<Plan>()
                                .add(union.child(0))
                                .add(((LogicalOneRowRelation) union.child(1)).withBuildUnionNode(false))
                                .build();
                        return union.withChildren(children);
                    })
            )
        );
    }
}
