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
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared implementation of {@code topk_min_count(x, k)} and {@code topk_max_count(x, k)}.
 *
 * <p>Both aggregates return {@code STRUCT<values ARRAY<STRUCT<value T, count BIGINT>>, bound T>}.
 * {@code values} holds at most k distinct non-NULL values ordered by value (ascending for MIN,
 * descending for MAX) with the number of rows equal to each value. {@code bound} is NULL when the
 * group was not truncated; otherwise it is the k-th value and {@code bound IS NOT NULL} is the
 * truncation flag. With {@code k = 0} no list is built and {@code bound} is the extreme value seen
 * on this side (NULL when no non-NULL input was seen).
 */
public abstract class TopKCountFunction extends NotNullableAggregateFunction
        implements CustomSignature, NullIgnoringAggregateFunction {

    /** Upper bound of k, shared with the BE implementation. */
    public static final long MAX_TOPK_K = 65536;

    protected TopKCountFunction(String name, boolean distinct, List<Expression> children) {
        super(name, distinct, false, children);
    }

    /** constructor for withChildren and reuse signature */
    protected TopKCountFunction(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (distinct) {
            throw new AnalysisException(getName() + " does not support DISTINCT");
        }
        if (!getArgument(1).isConstant()) {
            throw new AnalysisException(
                    getName() + " requires second parameter must be a constant: " + toSql());
        }
        if (!isSupportedValueType(getArgumentType(0))) {
            throw new AnalysisException(getName() + " does not support type "
                    + getArgumentType(0).toSql());
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        checkLegalityBeforeTypeCoercion();
        Expression count = getArgument(1);
        // A constant FE cannot fold is left to BE, which reads the value from the count column.
        if (!(count instanceof Literal)) {
            return;
        }
        if (count instanceof NullLiteral) {
            throw new AnalysisException(getName() + " requires a non-null constant count: " + toSql());
        }
        double value = ((Literal) count).getDouble();
        if (value < 0 || value > MAX_TOPK_K || value != Math.floor(value)) {
            throw new AnalysisException(getName() + " requires 0 <= count <= " + MAX_TOPK_K
                    + ", but got " + count.toSql());
        }
    }

    @Override
    public FunctionSignature customSignature() {
        DataType valueType = normalizeValueType(getArgumentType(0));
        return FunctionSignature.ret(resultType(valueType)).args(valueType, BigIntType.INSTANCE);
    }

    @Override
    public Expression resultForEmptyInput() {
        ArrayType valuesType = (ArrayType) ((StructType) getDataType()).getField("values").getDataType();
        return new StructLiteral(ImmutableList.of(
                new ArrayLiteral(new ArrayList<>(), valuesType),
                NullLiteral.INSTANCE), getDataType());
    }

    /** The value type stored in the list and in bound; DECIMALV2 is replaced by its V3 form. */
    public static DataType normalizeValueType(DataType dataType) {
        if (dataType instanceof DecimalV2Type) {
            return DecimalV3Type.forType(dataType);
        }
        return dataType;
    }

    /** The pair element shared by the state column, the aggregate result and topk_merge_*. */
    public static StructType pairType(DataType valueType) {
        return new StructType(ImmutableList.of(
                new StructField("value", valueType, false, ""),
                new StructField("count", BigIntType.INSTANCE, false, "")));
    }

    /**
     * Builds {@code STRUCT<values ARRAY<STRUCT<value T, count BIGINT>>, bound T NULL>}.
     */
    public static StructType resultType(DataType valueType) {
        return new StructType(ImmutableList.of(
                new StructField("values", ArrayType.of(pairType(valueType)), false, ""),
                new StructField("bound", valueType, true, "")));
    }

    /**
     * The supported types are the MIN/MAX types minus VARBINARY/JSONB/VARIANT and the complex
     * types: the values live in a struct/array state and must be losslessly comparable.
     */
    public static boolean isSupportedValueType(DataType dataType) {
        return dataType.isBooleanType() || dataType.isIntegerLikeType() || dataType.isLargeIntType()
                || dataType.isFloatLikeType() || dataType.isDecimalLikeType()
                || dataType.isStringLikeType()
                || dataType.isDateLikeType() || dataType.isTimeType()
                || dataType.isIPType() || dataType.isUuidType();
    }
}
