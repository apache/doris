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
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Extract All condition From CrossJoin.
 */
public class ExtractFilterFromCrossJoin extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return crossLogicalJoin()
                .then(join -> {
                    LogicalJoin<GroupPlan, GroupPlan> newJoin = new LogicalJoin<>(JoinType.CROSS_JOIN,
                            ExpressionUtils.EMPTY_CONDITION, ExpressionUtils.EMPTY_CONDITION, join.getHint(),
                            join.left(), join.right());
                    Set<Expression> predicates = Stream.concat(join.getHashJoinConjuncts().stream(),
                                    join.getOtherJoinConjuncts().stream())
                            .collect(Collectors.toSet());
                    return PlanUtils.filterOrSelf(predicates, newJoin);
                }).toRule(RuleType.EXTRACT_FILTER_FROM_JOIN);
    }
}
