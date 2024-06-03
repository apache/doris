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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import java.util.Set;

/**
 * CollectFilterOnConsumer
 */
public class CollectFilterAboveConsumer extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalCTEConsumer()).thenApply(ctx -> {
            LogicalFilter<LogicalCTEConsumer> filter = ctx.root;
            LogicalCTEConsumer cteConsumer = filter.child();
            Set<Expression> exprs = filter.getConjuncts();
            for (Expression expr : exprs) {
                Expression rewrittenExpr = expr.rewriteUp(e -> {
                    if (e instanceof Slot) {
                        return cteConsumer.getProducerSlot((Slot) e);
                    }
                    return e;
                });
                ctx.cascadesContext.putConsumerIdToFilter(cteConsumer.getRelationId(), rewrittenExpr);
            }
            return ctx.root;
        }).toRule(RuleType.COLLECT_FILTER_ABOVE_CTE_CONSUMER);
    }
}
