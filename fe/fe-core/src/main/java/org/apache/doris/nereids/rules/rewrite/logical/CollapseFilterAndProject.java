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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.visitor.UpdateAliasVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Do collapse.
 */
public class CollapseFilterAndProject extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalProject(logicalFilter(logicalProject(any()))).thenApply(ctx -> {
            LogicalProject topProject = ctx.root;
            LogicalFilter filter = (LogicalFilter) topProject.child(0);
            LogicalProject bottomProject = (LogicalProject) filter.child(0);
            List<NamedExpression> namedExpressionList = bottomProject.getProjects();
            Map<Slot, Alias> slotToAlias = new HashMap<>();
            namedExpressionList
                    .stream()
                    .filter(Alias.class::isInstance)
                    .forEach(s -> {
                        slotToAlias.put(s.toSlot(), (Alias) s);
                    });
            UpdateAliasVisitor updateAliasVisitor = new UpdateAliasVisitor();
            Expression rewrittenPredicate = updateAliasVisitor.visit(filter.getPredicates(), slotToAlias);
            LogicalFilter rewrittenFilter =
                    new LogicalFilter<LogicalPlan>(rewrittenPredicate, (LogicalPlan) bottomProject.child(0));
            List<NamedExpression> projectList = topProject.getProjects();
            List<NamedExpression> rewrittenProjectList = new ArrayList<>();
            for (NamedExpression expression : projectList) {
                rewrittenProjectList.add((NamedExpression) updateAliasVisitor.visit(expression, slotToAlias));
            }
            return new LogicalProject(rewrittenProjectList, rewrittenFilter);
        }
        ).toRule(RuleType.REWRITE_COLLAPSE_FILTER_PROJECT);
    }
}
