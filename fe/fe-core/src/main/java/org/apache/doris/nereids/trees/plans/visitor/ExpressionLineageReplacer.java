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

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer.ExpressionReplaceContext;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ExpressionLineageReplacer
 * Get from rewrite plan and can also get from plan struct info, if from plan struct info it depends on
 * the nodes from graph.
 */
public class ExpressionLineageReplacer extends DefaultPlanVisitor<Expression, ExpressionReplaceContext> {

    public static final ExpressionLineageReplacer INSTANCE = new ExpressionLineageReplacer();

    @Override
    public Expression visit(Plan plan, ExpressionReplaceContext context) {
        List<? extends Expression> expressions = plan.getExpressions();
        Map<ExprId, Expression> targetExpressionMap = context.getExprIdExpressionMap();
        // Filter the namedExpression used by target and collect the namedExpression
        expressions.stream()
                .filter(expression -> expression instanceof NamedExpression
                        && targetExpressionMap.containsKey(((NamedExpression) expression).getExprId()))
                .forEach(expression -> expression.accept(NamedExpressionCollector.INSTANCE, context));
        return super.visit(plan, context);
    }

    @Override
    public Expression visitGroupPlan(GroupPlan groupPlan, ExpressionReplaceContext context) {
        Group group = groupPlan.getGroup();
        if (group == null) {
            return visit(groupPlan, context);
        }
        Collection<StructInfo> structInfos = group.getstructInfoMap().getStructInfos();
        if (structInfos.isEmpty()) {
            return visit(groupPlan, context);
        }
        // Find first info which the context's bitmap contains all to make sure that
        // the expression lineage is correct
        Optional<StructInfo> structInfoOptional = structInfos.stream()
                .filter(info -> (context.getTableBitSet().isEmpty()
                        || StructInfo.containsAll(context.getTableBitSet(), info.getTableBitSet()))
                        && !info.getNamedExprIdAndExprMapping().isEmpty())
                .findFirst();
        if (!structInfoOptional.isPresent()) {
            return visit(groupPlan, context);
        }
        context.getExprIdExpressionMap().putAll(structInfoOptional.get().getNamedExprIdAndExprMapping());
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
            if (exprIdExpressionMap.containsKey(namedExpression.getExprId())) {
                return visit(exprIdExpressionMap.get(namedExpression.getExprId()), exprIdExpressionMap);
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
     * TODO Collect named expression by targetTypes, tableIdentifiers
     */
    public static class NamedExpressionCollector
            extends DefaultExpressionVisitor<Void, ExpressionReplaceContext> {

        public static final NamedExpressionCollector INSTANCE = new NamedExpressionCollector();

        @Override
        public Void visitSlot(Slot slot, ExpressionReplaceContext context) {
            context.getExprIdExpressionMap().put(slot.getExprId(), slot);
            return super.visit(slot, context);
        }

        @Override
        public Void visitAlias(Alias alias, ExpressionReplaceContext context) {
            // remove the alias
            if (context.getExprIdExpressionMap().containsKey(alias.getExprId())) {
                context.getExprIdExpressionMap().put(alias.getExprId(), alias.child());
            }
            return super.visitAlias(alias, context);
        }
    }

    /**
     * The context for replacing the expression with lineage
     */
    public static class ExpressionReplaceContext {
        private final List<Expression> targetExpressions;
        private final Set<TableType> targetTypes;
        private final Set<String> tableIdentifiers;
        private final Map<ExprId, Expression> exprIdExpressionMap;
        private final BitSet tableBitSet;
        private List<Expression> replacedExpressions;

        /**
         * ExpressionReplaceContext
         */
        public ExpressionReplaceContext(List<Expression> targetExpressions,
                Set<TableType> targetTypes,
                Set<String> tableIdentifiers,
                BitSet tableBitSet) {
            this.targetExpressions = targetExpressions;
            this.targetTypes = targetTypes;
            this.tableIdentifiers = tableIdentifiers;
            this.tableBitSet = tableBitSet;
            // collect the named expressions used in target expression and will be replaced later
            this.exprIdExpressionMap = targetExpressions.stream()
                    .map(each -> each.collectToList(NamedExpression.class::isInstance))
                    .flatMap(Collection::stream)
                    .map(NamedExpression.class::cast)
                    .distinct()
                    .collect(Collectors.toMap(NamedExpression::getExprId, expr -> expr));
        }

        public List<Expression> getTargetExpressions() {
            return targetExpressions;
        }

        public Set<TableType> getTargetTypes() {
            return targetTypes;
        }

        public Set<String> getTableIdentifiers() {
            return tableIdentifiers;
        }

        public Map<ExprId, Expression> getExprIdExpressionMap() {
            return exprIdExpressionMap;
        }

        public BitSet getTableBitSet() {
            return tableBitSet;
        }

        /**
         * getReplacedExpressions
         */
        public List<Expression> getReplacedExpressions() {
            if (this.replacedExpressions == null) {
                this.replacedExpressions = targetExpressions.stream()
                        .map(original -> original.accept(ExpressionReplacer.INSTANCE, getExprIdExpressionMap()))
                        .collect(Collectors.toList());
            }
            return this.replacedExpressions;
        }
    }
}
