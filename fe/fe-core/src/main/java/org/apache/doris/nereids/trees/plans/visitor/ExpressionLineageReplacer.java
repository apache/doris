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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer.ExpressionReplaceContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ExpressionLineageReplacer
 * Get from rewrite plan and can also get from plan struct info, if from plan struct info it depends on
 * the nodes from graph.
 */
public class ExpressionLineageReplacer extends DefaultPlanVisitor<Expression, ExpressionReplaceContext> {

    public static final Logger LOG = LogManager.getLogger(ExpressionLineageReplacer.class);
    public static final ExpressionLineageReplacer INSTANCE = new ExpressionLineageReplacer();

    @Override
    public Expression visit(Plan plan, ExpressionReplaceContext context) {
        Set<ExprId> usedExprIdSet = context.getUsedExprIdSet();
        // Filter the namedExpression used by target and collect the alias and used named expression
        for (Expression expression : plan.getExpressions()) {
            if (!(expression instanceof NamedExpression)) {
                continue;
            }
            if (!usedExprIdSet.contains(((NamedExpression) expression).getExprId())) {
                continue;
            }
            // Get used named expression by used expr id set
            expression.accept(NamedExpressionCollector.INSTANCE, context);
        }
        return super.visit(plan, context);
    }

    @Override
    public Expression visitGroupPlan(GroupPlan groupPlan, ExpressionReplaceContext context) {
        LOG.debug("ExpressionLineageReplacer should not meet groupPlan, plan is {}", groupPlan.toString());
        return null;
    }

    /**
     * Replace the expression with lineage according the exprIdExpressionMap
     */
    public static class ExpressionReplacer extends DefaultExpressionRewriter<Map<ExprId, Expression>> {

        public static final ExpressionReplacer INSTANCE = new ExpressionReplacer();

        @Override
        public Expression visitNamedExpression(NamedExpression namedExpression,
                Map<ExprId, Expression> exprIdExpressionMap) {
            Expression childExpr = exprIdExpressionMap.get(namedExpression.getExprId());
            if (childExpr != null) {
                // remove alias
                return visit(childExpr, exprIdExpressionMap);
            }
            return visit(namedExpression, exprIdExpressionMap);
        }

        @Override
        public Expression visit(Expression expr, Map<ExprId, Expression> exprIdExpressionMap) {
            if (expr instanceof NamedExpression
                    && expr.arity() == 0
                    && exprIdExpressionMap.containsKey(((NamedExpression) expr).getExprId())) {
                expr = exprIdExpressionMap.get(((NamedExpression) expr).getExprId());
            }
            List<Expression> newChildren = new ArrayList<>(expr.arity());
            boolean hasNewChildren = false;
            for (Expression child : expr.children()) {
                Expression newChild = child.accept(this, exprIdExpressionMap);
                if (newChild != child) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }
            return hasNewChildren ? expr.withChildren(newChildren) : expr;
        }
    }

    /**
     * The Collector for named expressions in the whole plan, and will be used to
     * replace the target expression later
     */
    public static class NamedExpressionCollector
            extends DefaultExpressionVisitor<Void, ExpressionReplaceContext> {

        public static final NamedExpressionCollector INSTANCE = new NamedExpressionCollector();

        @Override
        public Void visitNamedExpression(NamedExpression namedExpression, ExpressionReplaceContext context) {
            context.getUsedExprIdSet().add(namedExpression.getExprId());
            return super.visitNamedExpression(namedExpression, context);
        }

        @Override
        public Void visitAlias(Alias alias, ExpressionReplaceContext context) {
            // remove the alias
            if (context.getUsedExprIdSet().contains(alias.getExprId())) {
                context.getExprIdExpressionMap().put(alias.getExprId(), alias.child());
            }
            return super.visitAlias(alias, context);
        }
    }

    /**
     * The context for replacing the expression with lineage
     */
    public static class ExpressionReplaceContext {
        private final List<? extends Expression> targetExpressions;
        private final Set<ExprId> usedExprIdSet = new HashSet<>();
        // The key is alias exprId, the value is alias child
        private final Map<ExprId, Expression> exprIdExpressionMap = new HashMap<>();
        private List<Expression> replacedExpressions;

        /**
         * ExpressionReplaceContext
         */
        public ExpressionReplaceContext(List<? extends Expression> targetExpressions) {
            this.targetExpressions = targetExpressions;
            // collect the named expressions used in target expression and will be replaced later
            for (Expression expression : targetExpressions) {
                for (Object namedExpression : expression.collectToList(NamedExpression.class::isInstance)) {
                    this.usedExprIdSet.add(((NamedExpression) namedExpression).getExprId());
                }
            }
        }

        public Map<ExprId, Expression> getExprIdExpressionMap() {
            return exprIdExpressionMap;
        }

        public Set<ExprId> getUsedExprIdSet() {
            return usedExprIdSet;
        }

        /**
         * getReplacedExpressions
         */
        public List<? extends Expression> getReplacedExpressions() {
            if (this.replacedExpressions == null) {
                this.replacedExpressions = targetExpressions.stream()
                        .map(original -> original.accept(ExpressionReplacer.INSTANCE, getExprIdExpressionMap()))
                        .collect(Collectors.toList());
            }
            return this.replacedExpressions;
        }
    }
}
