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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.ForbiddenMetricTypeArguments;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** min agg function. */
public class Min extends NullableAggregateFunction implements UnaryExpression, CustomSignature,
        ForbiddenMetricTypeArguments {

    public Min(Expression child) {
        this(false, false, child);
    }

    public Min(boolean isDistinct, Expression arg) {
        this(isDistinct, false, arg);
    }

    private Min(boolean isDistinct, boolean isAlwaysNullable, Expression arg) {
        super("min", isAlwaysNullable, isDistinct, arg);
    }

    @Override
    public FunctionSignature customSignature() {
        DataType dataType = getArgument(0).getDataType();
        return FunctionSignature.ret(dataType).args(dataType);
    }

    @Override
    protected List<DataType> intermediateTypes() {
        return ImmutableList.of(getDataType());
    }

    @Override
    public Min withDistinctAndChildren(boolean isDistinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Min(isDistinct, isAlwaysNullable, children.get(0));
    }

    @Override
    public Min withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Min(children.get(0));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean isAlwaysNullable) {
        return new Min(isDistinct, isAlwaysNullable, children.get(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMin(this, context);
    }
}
