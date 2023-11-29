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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteRule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Transform element_at function to SlotReference for variant sub-column access.
 * This optimization will help query engine to prune as many sub columns as possible
 * to speed up query.
 * eg: element_at(element_at(v, "a"), "b") -> SlotReference(column=v, subColLabels=["a", "b"])
 */
public class ElementAtToSlot extends DefaultExpressionRewriter<ExpressionRewriteContext> implements
        ExpressionRewriteRule<ExpressionRewriteContext> {

    public static final ElementAtToSlot INSTANCE = new ElementAtToSlot();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        return expr.accept(this, ctx);
    }

    /**
     * Rewrites an {@link ElementAt} instance to a {@link SlotReference}.
     * This method is used to transform an ElementAt expr into a SlotReference,
     * based on the provided topColumnSlot and the context of the statement.
     *
     * @param elementAt The {@link ElementAt} instance to be rewritten.
     * @param topColumnSlot The {@link SlotReference} that represents the top column slot.
     * @return A {@link SlotReference} that represents the rewritten element.
     *         If a target column slot is found in the context, it is returned to avoid duplicates.
     *         Otherwise, a new SlotReference is created and added to the context.
     */
    public static Expression rewriteToSlot(ElementAt elementAt, SlotReference topColumnSlot) {
        // rewrite to slotRef
        StatementContext ctx = ConnectContext.get().getStatementContext();
        List<String> fullPaths = elementAt.collectToList(node -> node instanceof VarcharLiteral).stream()
                .map(node -> ((VarcharLiteral) node).getValue())
                .collect(Collectors.toList());
        String qualifiedName = topColumnSlot.getQualifiedName();
        SlotReference targetColumnSlot = ctx.getPathSlot(qualifiedName, fullPaths);
        if (targetColumnSlot != null) {
            // avoid duplicated slots
            return targetColumnSlot;
        }
        SlotReference slotRef = new SlotReference(StatementScopeIdGenerator.newExprId(),
                topColumnSlot.getName(), topColumnSlot.getDataType(),
                topColumnSlot.nullable(), topColumnSlot.getQualifier(),
                topColumnSlot.getColumn().get(), Optional.of(topColumnSlot.getInternalName()),
                fullPaths);
        ctx.addPathSlotRef(qualifiedName, fullPaths, slotRef);
        ctx.addSlotToRelation(slotRef, ctx.getRelationBySlot(topColumnSlot));

        return slotRef;
    }

    @Override
    public Expression visitElementAt(ElementAt elementAt, ExpressionRewriteContext context) {
        // todo
        return elementAt;
    }
}
