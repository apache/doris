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
import org.apache.doris.nereids.trees.expressions.LeafExpression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Do collapse.
 */
public class CollapseFilterAndProject extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalProject(logicalFilter(logicalProject())).thenApply(ctx -> {
            LogicalProject topProject = ctx.root;
            LogicalFilter filter = (LogicalFilter) topProject.child(0);
            LogicalProject bottomProject = (LogicalProject) filter.child(0);
            rewriteExpression(filter.getPredicates(), bottomProject);
            List<NamedExpression> projectList = topProject.getProjects();
            for (NamedExpression namedExpression : projectList) {
                if (namedExpression instanceof Alias) {
                    Alias alias = (Alias) namedExpression;
                    rewriteExpression(alias, bottomProject);
                }
            }
            topProject.children().remove(0);
            topProject.children().add(filter);
            filter.children().remove(0);
            filter.children().add(bottomProject.child(0));
            return topProject;
        }
        ).toRule(RuleType.REWRITE_COLLAPSE_FILTER_PROJECT);
    }

    private void rewriteExpression(Expression expression, LogicalProject project) {
        List<NamedExpression> namedExpressionList = project.getProjects();
        Map<Slot, Alias> slotToAlias = new HashMap<>();
        namedExpressionList
                .stream()
                .filter(Alias.class::isInstance)
                .forEach(s -> {
                    slotToAlias.put(s.toSlot(), (Alias) s);
                });
        doRewriteExpression(expression, slotToAlias);
    }

    private void doRewriteExpression(Expression expr, Map<Slot, Alias> slotToAlias) {
        if (expr instanceof LeafExpression) {
            return;
        }
        List<Expression> children = expr.children();
        int childSize = children.size();
        if (childSize > 0) {
            Expression leftChild = expr.child(0);
            if (leftChild instanceof Slot) {
                Slot slot = (Slot) leftChild;
                Alias alias = slotToAlias.get(slot);
                if (alias != null) {
                    children.remove(0);
                    children.add(0, alias.child(0));
                }
            } else if (!(leftChild instanceof LeafExpression)) {
                doRewriteExpression(leftChild, slotToAlias);
            }
        }

        if (childSize > 1) {
            Expression rightChild = expr.child(1);
            if (rightChild instanceof Slot) {
                Slot slot = (Slot) rightChild;
                Alias alias = slotToAlias.get(slot);
                if (alias != null) {
                    children.remove(1);
                    children.add(1, alias.child(0));
                }
            } else if (!(rightChild instanceof LeafExpression)) {
                doRewriteExpression(rightChild, slotToAlias);
            }
        }
    }
}
