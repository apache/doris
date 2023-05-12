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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * CheckPolicy.
 */
public class CheckPolicy implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.CHECK_ROW_POLICY.build(
                logicalCheckPolicy(any().when(child -> !(child instanceof UnboundRelation))).thenApply(ctx -> {
                    LogicalCheckPolicy<Plan> checkPolicy = ctx.root;
                    Plan child = checkPolicy.child();
                    if (!(child instanceof LogicalRelation)
                            || ctx.connectContext.getSessionVariable().isPlayNereidsDump()) {
                        return child;
                    }
                    LogicalRelation relation = (LogicalRelation) child;
                    Optional<Expression> filter = checkPolicy.getFilter(relation, ctx.connectContext);
                    if (!filter.isPresent()) {
                        return relation;
                    }
                    return new LogicalFilter(ExpressionUtils.extractConjunctionToSet(filter.get()), relation);
                })
            )
        );
    }
}
