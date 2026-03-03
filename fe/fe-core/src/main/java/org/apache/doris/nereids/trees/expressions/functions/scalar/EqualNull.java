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
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.RewriteWhenAnalyze;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Optional;

/**
 * Snowflake-compatible function 'equal_null'.
 * EQUAL_NULL(a, b) returns TRUE if a and b are both NULL, or if a = b; FALSE otherwise.
 * Rewrites to (a <=> b) during analysis, leveraging Doris's NullSafeEqual operator.
 */
public class EqualNull extends ScalarFunction
        implements BinaryExpression, CustomSignature, AlwaysNotNullable, RewriteWhenAnalyze {

    /**
     * constructor with 2 arguments.
     */
    public EqualNull(Expression arg0, Expression arg1) {
        super("equal_null", arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    private EqualNull(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        Optional<DataType> commonType;
        try {
            commonType = TypeCoercionUtils.findWiderTypeForTwoByVariable(
                    getArgumentType(0), getArgumentType(1), false, true);
        } catch (Exception e) {
            SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
            return null;
        }
        if (commonType.isPresent()) {
            return FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(commonType.get(), commonType.get());
        } else {
            SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
            return null;
        }
    }

    /**
     * withChildren.
     */
    @Override
    public EqualNull withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new EqualNull(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitEqualNull(this, context);
    }

    @Override
    public Expression rewriteWhenAnalyze() {
        // EQUAL_NULL(a, b) => a <=> b
        return new NullSafeEqual(child(0), child(1));
    }
}
