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
import org.apache.doris.nereids.trees.plans.JoinType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Convert invalid inner join to cross join.
 * Like: A inner join B on true -> A cross join B on true;
 * Or
 * Convert invalid cross join to inner join.
 * Like: A cross join B on A.id > 1 -> A inner join B on A.id > 1;
 */
public class ConvertInnerOrCrossJoin implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            innerLogicalJoin()
                .when(join -> join.getHashJoinConjuncts().isEmpty() && join.getOtherJoinConjuncts().isEmpty()
                        && join.getMarkJoinConjuncts().isEmpty())
                .then(join -> join.withJoinTypeAndContext(JoinType.CROSS_JOIN, join.getJoinReorderContext()))
                .toRule(RuleType.INNER_TO_CROSS_JOIN),
            crossLogicalJoin()
                .when(join -> !join.getHashJoinConjuncts().isEmpty() || !join.getOtherJoinConjuncts().isEmpty()
                        || !join.getMarkJoinConjuncts().isEmpty())
                .then(join -> join.withJoinTypeAndContext(JoinType.INNER_JOIN, join.getJoinReorderContext()))
                .toRule(RuleType.CROSS_TO_INNER_JOIN)
        );
    }
}
