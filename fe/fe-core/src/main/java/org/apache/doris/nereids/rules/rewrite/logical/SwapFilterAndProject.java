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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionReplacer;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Rewrite filter -> project to project -> filter.
 */
public class SwapFilterAndProject extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter(logicalProject()).thenApply(ctx -> {
            LogicalFilter<LogicalProject<GroupPlan>> filter = ctx.root;
            LogicalProject<GroupPlan> project = filter.child();
            List<NamedExpression> namedExpressionList = project.getProjects();
            Map<Expression, Expression> slotToAlias = new HashMap<>();
            namedExpressionList.stream().filter(Alias.class::isInstance).forEach(s -> {
                slotToAlias.put(s.toSlot(), ((Alias) s).child());
            });
            Expression rewrittenPredicate = ExpressionReplacer.INSTANCE.visit(filter.getPredicates(), slotToAlias);
            LogicalFilter<LogicalPlan> rewrittenFilter =
                    new LogicalFilter<LogicalPlan>(rewrittenPredicate, project.child());
            return new LogicalProject(project.getProjects(), rewrittenFilter);
        }).toRule(RuleType.SWAP_FILTER_AND_PROJECT);
    }
}
