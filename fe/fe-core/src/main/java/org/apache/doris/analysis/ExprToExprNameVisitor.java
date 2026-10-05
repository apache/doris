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

package org.apache.doris.analysis;

import org.apache.doris.common.NameFormatUtils;

/**
 * Visitor that computes the "expr name" for an {@link Expr} tree.
 *
 * <p>The expr name is a short, normalized identifier used for column labels
 * and other metadata — it is <b>not</b> a full SQL representation.
 *
 * <ul>
 *   <li>{@link SlotRef} → raw column name</li>
 *   <li>{@link FunctionCallExpr} → normalized function name</li>
 *   <li>{@link ColumnRefExpr} → normalized column name</li>
 *   <li>{@link LiteralExpr} (all subtypes) → {@code "literal"}</li>
 *   <li>Everything else → normalized simple class name</li>
 * </ul>
 */
public class ExprToExprNameVisitor extends ExprVisitor<String, Void> {

    public static final ExprToExprNameVisitor INSTANCE = new ExprToExprNameVisitor();

    private ExprToExprNameVisitor() {
    }

    @Override
    public String visit(Expr expr, Void context) {
        if (expr instanceof LiteralExpr) {
            return "literal";
        }
        return NameFormatUtils.normalizeName(expr.getClass().getSimpleName(), Expr.DEFAULT_EXPR_NAME);
    }

    @Override
    public String visitSlotRef(SlotRef expr, Void context) {
        return expr.getCol();
    }

    @Override
    public String visitFunctionCallExpr(FunctionCallExpr expr, Void context) {
        return NameFormatUtils.normalizeName(expr.getFnName().getFunction(), Expr.DEFAULT_EXPR_NAME);
    }

    @Override
    public String visitColumnRefExpr(ColumnRefExpr expr, Void context) {
        return NameFormatUtils.normalizeName(expr.getName(), Expr.DEFAULT_EXPR_NAME);
    }
}
