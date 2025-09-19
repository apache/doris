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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_enumerate_uniq'.
 *  more than 0 array as args
 */
public class ArrayEnumerateUniq extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(ArrayType.of(BigIntType.INSTANCE))
                    .varArgs(ArrayType.of(AnyDataType.INSTANCE_WITHOUT_INDEX))
    );

    /**
     * constructor with more than 0 arguments.
     */
    public ArrayEnumerateUniq(Expression arg, Expression ...varArgs) {
        super("array_enumerate_uniq", ExpressionUtils.mergeArguments(arg, varArgs));
    }

    /** constructor for withChildren and reuse signature */
    private ArrayEnumerateUniq(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * array_enumerate_uniq needs to compare whether the sub-elements in the array are equal.
     * so the sub-elements must be comparable. but now map and struct type is not comparable.
     */
    @Override
    public void checkLegalityBeforeTypeCoercion() {
        for (Expression arg : children()) {
            DataType argType = arg.getDataType();
            if (argType.isArrayType()) {
                DataType itemType = ((ArrayType) argType).getItemType();
                if (itemType.isComplexType()) {
                    throw new AnalysisException("array_enumerate_uniq does not support types: " + toSql());
                }
            }
        }
    }

    /**
     * withChildren.
     */
    @Override
    public ArrayEnumerateUniq withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        return new ArrayEnumerateUniq(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayEnumerateUniq(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

}
