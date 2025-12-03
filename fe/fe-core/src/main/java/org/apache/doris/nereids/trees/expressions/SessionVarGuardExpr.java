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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.AutoCloseSessionVariable;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A transparent wrapper expression that temporarily applies given session variables
 * during type computation and SQL rendering of its child expression. This is used
 * to preserve precision/scale decisions for generated columns and alias functions
 * that depend on affectQueryResult session variables (e.g. enable_decimal256, decimalOverflowScale).
 */
public class SessionVarGuardExpr extends Expression implements UnaryExpression {

    private final Map<String, String> sessionVars;

    public SessionVarGuardExpr(Expression child, Map<String, String> sessionVars) {
        super(ImmutableList.of(child));
        Preconditions.checkNotNull(sessionVars, "sessionVars cannot be null in SessionVarGuardExpr");
        this.sessionVars = ImmutableMap.copyOf(sessionVars);
    }

    public Map<String, String> getSessionVars() {
        return sessionVars;
    }

    @Override
    public boolean nullable() {
        return child().nullable();
    }

    @Override
    public DataType getDataType() throws UnboundException {
        try (AutoCloseSessionVariable ignored = openGuard()) {
            return child().getDataType();
        }
    }

    @Override
    public String computeToSql() {
        try (AutoCloseSessionVariable ignored = openGuard()) {
            return child().toSql();
        }
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1, "SessionVarGuardExpr must have exactly one child");
        // Rebuild the wrapped expression with provided children, then wrap again
        return new SessionVarGuardExpr(children.get(0), sessionVars);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        try (AutoCloseSessionVariable ignored = openGuard()) {
            return visitor.visitSessionVarGuardExpr(this, context);
        }
    }

    private AutoCloseSessionVariable openGuard() {
        return new AutoCloseSessionVariable(ConnectContext.get(), sessionVars);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionVarGuardExpr that = (SessionVarGuardExpr) o;
        return Objects.equals(child(), that.child()) && Objects.equals(sessionVars, that.sessionVars);
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(super.computeHashCode(), child(), sessionVars);
    }

    @Override
    public String toString() {
        try (AutoCloseSessionVariable ignored = openGuard()) {
            return "svGuard:" + child().toString();
        }
    }

    /**
     * input: guard(guard(guard(child))) -> return child
     * input: child -> return child
     * */
    public static Expression getSessionVarGuardChild(Expression expr) {
        while (expr instanceof SessionVarGuardExpr) {
            expr = expr.child(0);
        }
        return expr;
    }

    /**
     * Wrap expressions with SessionVarGuardExpr if needed.
     */
    public static List<Expression> getExprWithGuard(Map<? extends Expression, Map<String, String>> varMap) {
        List<Expression> exprs = new ArrayList<>(varMap.size());
        for (Map.Entry<? extends Expression, Map<String, String>> entry : varMap.entrySet()) {
            Expression expr;
            Map<String, String> sessionVarMap = entry.getValue();
            if (sessionVarMap != null) {
                expr = new SessionVarGuardExpr(entry.getKey(), sessionVarMap);
            } else {
                expr = entry.getKey();
            }
            exprs.add(expr);
        }
        return exprs;
    }
}


