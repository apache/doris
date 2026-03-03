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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Snowflake-compatible function 'to_varchar'.
 * TO_VARCHAR(expr) converts expr to VARCHAR, equivalent to CAST(expr AS VARCHAR).
 * TO_VARCHAR(date_expr, fmt) formats a date/timestamp using the format string,
 * equivalent to date_format(date_expr, fmt).
 *
 * Note: Snowflake format specifiers (e.g., 'YYYY-MM-DD') differ from Doris
 * format specifiers (e.g., '%Y-%m-%d'). Complex format conversions require
 * manual adaptation.
 */
public class ToVarcharFn extends ScalarFunction
        implements CustomSignature, AlwaysNullable, RewriteWhenAnalyze {

    /**
     * constructor with 1 argument: TO_VARCHAR(expr)
     */
    public ToVarcharFn(Expression arg) {
        super("to_varchar", arg);
    }

    /**
     * constructor with 2 arguments: TO_VARCHAR(expr, fmt)
     */
    public ToVarcharFn(Expression arg0, Expression arg1) {
        super("to_varchar", arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    private ToVarcharFn(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        if (arity() == 1) {
            return FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(getArgumentType(0));
        } else {
            return FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(getArgumentType(0), VarcharType.SYSTEM_DEFAULT);
        }
    }

    /**
     * withChildren.
     */
    @Override
    public ToVarcharFn withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
        return new ToVarcharFn(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitToVarcharFn(this, context);
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        if (arity() == 2) {
            // TO_VARCHAR(expr, fmt) => date_format(expr, fmt)
            return new DateFormat(child(0), child(1));
        } else {
            // TO_VARCHAR(expr) => CAST(expr AS VARCHAR)
            return new Cast(child(0), VarcharType.SYSTEM_DEFAULT);
        }
    }
}
