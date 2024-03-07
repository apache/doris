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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import java.util.Set;

/**
 * InferNotNull from Filter.
 * Like:
 * filter a + b > 0
 * ->
 * filter a + b > 0 and a is not null and b is not null
 * <p>
 * This rule will cooperate with PushDownFilter and EliminateNotNull.
 */
public class InferFilterNotNull extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter()
            .when(filter -> filter.getConjuncts().stream()
                    .filter(Not.class::isInstance)
                    .map(Not.class::cast)
                    .noneMatch(Not::isGeneratedIsNotNull))
            .thenApply(ctx -> {
                LogicalFilter<Plan> filter = ctx.root;
                Set<Expression> predicates = filter.getConjuncts();
                Set<Expression> isNotNulls = ExpressionUtils.inferNotNull(predicates, ctx.cascadesContext);
                ImmutableSet.Builder<Expression> needGenerateNotNullsBuilder = ImmutableSet.builder();
                for (Expression isNotNull : isNotNulls) {
                    if (!predicates.contains(isNotNull)) {
                        needGenerateNotNullsBuilder.add(((Not) isNotNull).withGeneratedIsNotNull(true));
                    }
                }
                Set<Expression> needGenerateNotNulls = needGenerateNotNullsBuilder.build();
                if (needGenerateNotNulls.isEmpty()) {
                    return null;
                }
                Set<Expression> conjuncts = Streams.concat(predicates.stream(), needGenerateNotNulls.stream())
                        .collect(ImmutableSet.toImmutableSet());
                return PlanUtils.filter(conjuncts, filter.child()).get();
            }).toRule(RuleType.INFER_FILTER_NOT_NULL);
    }
}
