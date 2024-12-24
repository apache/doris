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
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.FollowToAnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * scalar function array_apply
 */
public class ArrayApply extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, PropagateNullable {
    public static final List<FunctionSignature> FOLLOW_DATATYPE_SIGNATURE = ImmutableList.of(
            FunctionSignature.retArgType(0)
                    .args(ArrayType.of(new AnyDataType(0)), VarcharType.SYSTEM_DEFAULT,
                            new FollowToAnyDataType(0)));

    public static final List<FunctionSignature> MIN_COMMON_TYPE_SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0)
                    .args(ArrayType.of(new AnyDataType(0)), VarcharType.SYSTEM_DEFAULT,
                            new AnyDataType(0)));

    /**
     * constructor
     */
    public ArrayApply(Expression arg0, Expression arg1, Expression arg2) {
        super("array_apply", arg0, arg1, arg2);
        if (!(arg1 instanceof StringLikeLiteral)) {
            throw new AnalysisException(
                    "array_apply(arr, op, val): op support const value only.");
        } else {
            String op = ((StringLikeLiteral) arg1).getStringValue();
            if (! "=".equals(op) && !">".equals(op) && !"<".equals(op)
                    && !">=".equals(op) && !"<=".equals(op) && !"!=".equals(op)) {
                throw new AnalysisException(
                        "array_apply(arr, op, val): op support =, >=, <=, >, <, !=, but we get " + op);
            }
        }
        if (!(arg2.isConstant())) {
            throw new AnalysisException(
                    "array_apply(arr, op, val): val support const value only.");
        }
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType argType = ((ArrayType) child(0).getDataType()).getItemType();
        if (!(argType.isIntegralType() || argType.isFloatLikeType() || argType.isDecimalLikeType()
                || argType.isDateLikeType() || argType.isBooleanType())) {
            throw new AnalysisException("array_apply does not support type: " + toSql());
        }
    }

    @Override
    public ArrayApply withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3,
                "array_apply accept 3 args, but got %s (%s)",
                children.size(),
                children);
        return new ArrayApply(children.get(0), children.get(1), children.get(2));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayApply(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (getArgument(0).getDataType().isArrayType()
                &&
                ((ArrayType) getArgument(0).getDataType()).getItemType()
                        .isSameTypeForComplexTypeParam(getArgument(2).getDataType())) {
            // return least common type
            return MIN_COMMON_TYPE_SIGNATURES;
        }
        return FOLLOW_DATATYPE_SIGNATURE;
    }
}
