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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

import java.util.List;

/** AggregateFunction 'avg_map'. */
public class AvgMap extends MapAggregateFunction {

    public AvgMap(Expression arg) {
        this(false, arg);
    }

    public AvgMap(boolean distinct, Expression arg) {
        super("avg_map", distinct, arg);
    }

    private AvgMap(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    protected DataType argumentValueType(DataType valueType) {
        if (valueType instanceof FloatType) {
            return DoubleType.INSTANCE;
        }
        if (valueType instanceof NullType) {
            return TinyIntType.INSTANCE;
        }
        if (valueType instanceof DecimalV2Type) {
            return DecimalV2Type.SYSTEM_DEFAULT;
        }
        return valueType;
    }

    @Override
    protected DataType returnValueType(DataType valueType) {
        if (valueType instanceof DecimalV3Type) {
            boolean enableDecimal256 = false;
            ConnectContext connectContext = ConnectContext.get();
            if (connectContext != null) {
                enableDecimal256 = connectContext.getSessionVariable().isEnableDecimal256();
            }
            DecimalV3Type decimalType = DecimalV3Type.forType(valueType);
            int scale = decimalType.getScale();
            if (scale < ScalarType.DEFAULT_MIN_AVG_DECIMAL128_SCALE) {
                scale = ScalarType.DEFAULT_MIN_AVG_DECIMAL128_SCALE;
            }
            return DecimalV3Type.createDecimalV3Type(
                    enableDecimal256 ? DecimalV3Type.MAX_DECIMAL256_PRECISION
                            : DecimalV3Type.MAX_DECIMAL128_PRECISION,
                    scale);
        }
        if (valueType instanceof DecimalV2Type) {
            return DecimalV2Type.SYSTEM_DEFAULT;
        }
        if (valueType.isNumericType() || valueType instanceof NullType) {
            return DoubleType.INSTANCE;
        }
        throw new AnalysisException("avg_map requires a numeric MAP value type");
    }

    @Override
    public AvgMap withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new AvgMap(getFunctionParams(distinct, children));
    }
}
