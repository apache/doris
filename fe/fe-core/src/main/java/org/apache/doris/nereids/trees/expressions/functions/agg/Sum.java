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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** sum agg function. */
public class Sum extends NullableAggregateFunction implements UnaryExpression, CustomSignature {
    public Sum(Expression child) {
        this(false, false, child);
    }

    public Sum(boolean isDistinct, Expression arg) {
        this(isDistinct, false, arg);
    }

    private Sum(boolean isDistinct, boolean isAlwaysNullable, Expression arg) {
        super("sum", isAlwaysNullable, isDistinct, arg);
    }

    @Override
    public FunctionSignature customSignature() {
        DataType originDataType = getArgument(0).getDataType();
        DataType implicitCastType = implicitCast(originDataType);
        return FunctionSignature.ret(implicitCastType).args(originDataType);
    }

    @Override
    protected List<DataType> intermediateTypes() {
        return ImmutableList.of(getDataType());
    }

    @Override
    public Sum withDistinctAndChildren(boolean isDistinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Sum(isDistinct, isAlwaysNullable, children.get(0));
    }

    @Override
    public Sum withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Sum(isDistinct, isAlwaysNullable, children.get(0));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean isAlwaysNullable) {
        return new Sum(isDistinct, isAlwaysNullable, children.get(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSum(this, context);
    }

    private DataType implicitCast(DataType dataType) {
        if (dataType instanceof LargeIntType) {
            return dataType;
        } else if (dataType instanceof DecimalV2Type) {
            return DecimalV2Type.SYSTEM_DEFAULT;
        } else if (dataType instanceof IntegralType) {
            return BigIntType.INSTANCE;
        } else if (dataType instanceof NumericType) {
            return DoubleType.INSTANCE;
        } else {
            throw new AnalysisException("sum requires a numeric parameter: " + dataType);
        }
    }
}
