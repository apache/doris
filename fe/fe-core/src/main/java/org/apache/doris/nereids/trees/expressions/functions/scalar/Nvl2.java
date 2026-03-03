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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Snowflake-compatible function 'nvl2'.
 * NVL2(expr1, expr2, expr3): if expr1 IS NOT NULL, return expr2; else return expr3.
 * Rewrites to IF(expr1 IS NOT NULL, expr2, expr3) during analysis.
 */
public class Nvl2 extends ScalarFunction
        implements TernaryExpression, CustomSignature, RewriteWhenAnalyze {

    /**
     * constructor with 3 arguments.
     */
    public Nvl2(Expression arg0, Expression arg1, Expression arg2) {
        super("nvl2", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private Nvl2(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public boolean nullable() {
        return child(1).nullable() || child(2).nullable();
    }

    @Override
    public FunctionSignature customSignature() {
        Optional<DataType> commonType;
        try {
            commonType = TypeCoercionUtils.findWiderTypeForTwoByVariable(
                    getArgumentType(1), getArgumentType(2), false, true);
        } catch (Exception e) {
            SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
            return null;
        }
        if (commonType.isPresent()) {
            return FunctionSignature.ret(commonType.get())
                    .args(getArgumentType(0), commonType.get(), commonType.get());
        } else {
            SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
            return null;
        }
    }

    /**
     * withChildren.
     */
    @Override
    public Nvl2 withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new Nvl2(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitNvl2(this, context);
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        // NVL2(e1, e2, e3) => IF(e1 IS NOT NULL, e2, e3)
        return new If(new Not(new IsNull(child(0))), child(1), child(2));
    }
}
