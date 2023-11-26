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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.ExpressionLineageReplacer.ExpressionReplaceContext;

import java.util.Collection;
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

    /**
     * Replace the expression with lineage according the exprIdExpressionMap
     */
    public static class ExpressionReplacer extends DefaultExpressionRewriter<Map<ExprId, Expression>> {

        public static final ExpressionReplacer INSTANCE = new ExpressionReplacer();

        @Override
        public Expression visitNamedExpression(NamedExpression namedExpression,
                Map<ExprId, Expression> exprIdExpressionMap) {
            if (exprIdExpressionMap.containsKey(namedExpression.getExprId())) {
                return super.visit(exprIdExpressionMap.get(namedExpression.getExprId()), exprIdExpressionMap);
            }
            return super.visitNamedExpression(namedExpression, exprIdExpressionMap);
        }
    }

    /**
     * The Collector for target named expressions
     * TODO Collect named expression by targetTypes, tableIdentifiers
     */
    public static class NamedExpressionCollector
            extends DefaultExpressionVisitor<Void, ExpressionReplaceContext> {

        public static final NamedExpressionCollector INSTANCE = new NamedExpressionCollector();

        @Override
        public Void visitSlotReference(SlotReference slotReference, ExpressionReplaceContext context) {
            context.getExprIdExpressionMap().put(slotReference.getExprId(), slotReference);
            return super.visitSlotReference(slotReference, context);
        }

        @Override
        public Void visitArrayItemSlot(ArrayItemSlot arrayItemSlot, ExpressionReplaceContext context) {
            context.getExprIdExpressionMap().put(arrayItemSlot.getExprId(), arrayItemSlot);
            return super.visitArrayItemSlot(arrayItemSlot, context);
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
        private Map<ExprId, Expression> exprIdExpressionMap;
        private List<Expression> replacedExpressions;

        /**ExpressionReplaceContext*/
        public ExpressionReplaceContext(List<Expression> targetExpressions,
                Set<TableType> targetTypes,
                Set<String> tableIdentifiers) {
            this.targetExpressions = targetExpressions;
            this.targetTypes = targetTypes;
            this.tableIdentifiers = tableIdentifiers;
            // collect only named expressions and replace them with linage identifier later
            this.exprIdExpressionMap = targetExpressions.stream()
                    .map(each -> each.collectToList(NamedExpression.class::isInstance))
                    .flatMap(Collection::stream)
                    .map(NamedExpression.class::cast)
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
