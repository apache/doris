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
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Eliminate filter which is FALSE or TRUE.
 */
public class EliminateFilter extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter()
                .when(filter -> filter.getConjuncts().stream().anyMatch(BooleanLiteral.class::isInstance))
                .thenApply(ctx -> {
                    LogicalFilter<Plan> filter = ctx.root;
                    Set<Expression> newConjuncts = Sets.newHashSetWithExpectedSize(filter.getConjuncts().size());
                    for (Expression expression : filter.getConjuncts()) {
                        if (expression == BooleanLiteral.FALSE) {
                            return new LogicalEmptyRelation(ctx.statementContext.getNextRelationId(),
                                    filter.getOutput());
                        } else if (expression != BooleanLiteral.TRUE) {
                            newConjuncts.add(expression);
                        }
                    }
                    if (newConjuncts.isEmpty()) {
                        return filter.child();
                    } else {
                        return new LogicalFilter<>(newConjuncts, filter.child());
                    }
                })
                .toRule(RuleType.ELIMINATE_FILTER);
    }
}
