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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Function that could be rewritten and pushed down to projection
 */
public abstract class PushDownToProjectionFunction extends ScalarFunction {
    public PushDownToProjectionFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    /**
     * check if specified function could be pushed down to project
     * @param pushDownExpr expr to check
     * @return if it is valid to push down input expr
     */
    public static boolean validToPushDown(Expression pushDownExpr) {
        // Currently only element at for variant type could be pushed down
        return pushDownExpr != null && pushDownExpr.anyMatch(expr ->
            expr instanceof PushDownToProjectionFunction && ((Expression) expr).getDataType().isVariantType()
        );
    }

    /**
     * Rewrites an {@link PushDownToProjectionFunction} instance to a {@link SlotReference}.
     * This method is used to transform an PushDownToProjectionFunction expr into a SlotReference,
     * based on the provided topColumnSlot and the context of the statement.
     *
     * @param pushedFunction The {@link PushDownToProjectionFunction} instance to be rewritten.
     * @param topColumnSlot The {@link SlotReference} that represents the top column slot.
     * @return A {@link SlotReference} that represents the rewritten element.
     *         If a target column slot is found in the context, it is returned to avoid duplicates.
     *         Otherwise, a new SlotReference is created and added to the context.
     */
    public static Expression rewriteToSlot(PushDownToProjectionFunction pushedFunction, SlotReference topColumnSlot) {
        // push down could not work well with variant that not belong to table, so skip it.
        if (!topColumnSlot.getColumn().isPresent() || !topColumnSlot.getTable().isPresent()) {
            return pushedFunction;
        }
        // rewrite to slotRef
        StatementContext ctx = ConnectContext.get().getStatementContext();
        List<String> fullPaths = pushedFunction.collectToList(node -> node instanceof VarcharLiteral).stream()
                .map(node -> ((VarcharLiteral) node).getValue())
                .collect(Collectors.toList());
        SlotReference targetColumnSlot = ctx.getPathSlot(topColumnSlot, fullPaths);
        if (targetColumnSlot != null) {
            // avoid duplicated slots
            return targetColumnSlot;
        }
        boolean nullable = true; // always nullable at present
        SlotReference slotRef = new SlotReference(StatementScopeIdGenerator.newExprId(),
                topColumnSlot.getName(), topColumnSlot.getDataType(),
                nullable, topColumnSlot.getQualifier(), topColumnSlot.getTable().get(),
                topColumnSlot.getColumn().get(), Optional.of(topColumnSlot.getInternalName()),
                fullPaths);
        ctx.addPathSlotRef(topColumnSlot, fullPaths, slotRef, pushedFunction);
        ctx.addSlotToRelation(slotRef, ctx.getRelationBySlot(topColumnSlot));

        return slotRef;
    }
}
