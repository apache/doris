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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Collect Projects Above CTE Consumer.
 */
public class CollectProjectAboveConsumer extends OneRewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(RuleType.COLLECT_PROJECT_ABOVE_CONSUMER
                .build(logicalProject(logicalCTEConsumer()).thenApply(ctx -> {
                    LogicalProject<LogicalCTEConsumer> project = ctx.root;
                    List<NamedExpression> namedExpressions = project.getProjects();
                    LogicalCTEConsumer cteConsumer = project.child();
                    collectProject(ctx, namedExpressions, cteConsumer);
                    return ctx.root;
                })),
                RuleType.COLLECT_PROJECT_ABOVE_FILTER_CONSUMER
                        .build(logicalProject(logicalFilter(logicalCTEConsumer())).thenApply(ctx -> {
                            LogicalProject<LogicalFilter<LogicalCTEConsumer>> project = ctx.root;
                            List<NamedExpression> namedExpressions = project.getProjects();
                            LogicalCTEConsumer cteConsumer = project.child().child();
                            collectProject(ctx, namedExpressions, cteConsumer);
                            return ctx.root;
                        })));
    }

    private static void collectProject(MatchingContext ctx,
            List<NamedExpression> namedExpressions, LogicalCTEConsumer cteConsumer) {
        for (Expression expr : namedExpressions) {
            expr.foreach(node -> {
                if (!(node instanceof Slot)) {
                    return;
                }
                Slot slot = cteConsumer.findProducerSlot((Slot) node);
                ctx.cascadesContext.putCTEIdToProject(cteConsumer.getCteId(), slot);
            });
        }
    }
}
