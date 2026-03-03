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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Snowflake-compatible function 'nullifzero'.
 * NULLIFZERO(x) returns NULL if x is 0, otherwise returns x.
 * Rewrites to NULLIF(x, 0) during analysis.
 */
public class Nullifzero extends ScalarFunction
        implements UnaryExpression, CustomSignature, AlwaysNullable, RewriteWhenAnalyze {

    /**
     * constructor with 1 argument.
     */
    public Nullifzero(Expression arg) {
        super("nullifzero", arg);
    }

    /** constructor for withChildren and reuse signature */
    private Nullifzero(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.ret(getArgumentType(0)).args(getArgumentType(0));
    }

    /**
     * withChildren.
     */
    @Override
    public Nullifzero withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Nullifzero(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNullifzero(this, context);
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        return new NullIf(child(0), new Cast(new IntegerLiteral(0), child(0).getDataType()));
    }
}
