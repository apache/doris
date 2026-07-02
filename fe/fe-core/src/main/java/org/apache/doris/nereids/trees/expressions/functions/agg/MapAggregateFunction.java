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
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;

import java.util.LinkedHashMap;
import java.util.List;

/** Base class for aggregate functions that aggregate Map values by key. */
public abstract class MapAggregateFunction extends NotNullableAggregateFunction
        implements UnaryExpression, CustomSignature {

    protected MapAggregateFunction(String name, Expression arg) {
        this(name, false, arg);
    }

    protected MapAggregateFunction(String name, boolean distinct, Expression arg) {
        super(name, distinct, arg);
    }

    protected MapAggregateFunction(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        MapType mapType = getMapType();
        DataType keyType = mapType.getKeyType();
        DataType argumentValueType = argumentValueType(mapType.getValueType());
        DataType returnValueType = returnValueType(mapType.getValueType());
        return FunctionSignature.ret(MapType.of(keyType, returnValueType))
                .args(MapType.of(keyType, argumentValueType));
    }

    protected MapType getMapType() {
        DataType dataType = child().getDataType();
        if (!(dataType instanceof MapType)) {
            throw new AnalysisException(getName() + " requires a MAP argument");
        }
        return (MapType) dataType;
    }

    protected DataType argumentValueType(DataType valueType) {
        return valueType;
    }

    protected void checkMinMaxValueType() {
        DataType valueType = getMapType().getValueType();
        if (valueType.isOnlyMetricType() && !valueType.isArrayType()) {
            throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
        }
    }

    protected abstract DataType returnValueType(DataType valueType);

    @Override
    public Expression resultForEmptyInput() {
        return new MapLiteral(new LinkedHashMap<>(), getDataType());
    }

    @Override
    public abstract MapAggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children);
}
