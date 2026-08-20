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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;

import com.google.common.base.Preconditions;

import java.util.List;

/** AggregateFunction 'max_map'. */
public class MaxMap extends MapAggregateFunction {

    public MaxMap(Expression arg) {
        this(false, arg);
    }

    public MaxMap(boolean distinct, Expression arg) {
        super("max_map", distinct, arg);
    }

    private MaxMap(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    protected DataType argumentValueType(DataType valueType) {
        return normalizeValueType(valueType);
    }

    @Override
    protected DataType returnValueType(DataType valueType) {
        return normalizeValueType(valueType);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        checkMinMaxValueType();
    }

    @Override
    public void checkLegalityAfterRewrite() {
        checkLegalityBeforeTypeCoercion();
    }

    private static DataType normalizeValueType(DataType valueType) {
        if (valueType instanceof DecimalV2Type) {
            return DecimalV3Type.forType(valueType);
        }
        return valueType;
    }

    @Override
    public MaxMap withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new MaxMap(getFunctionParams(distinct, children));
    }
}
