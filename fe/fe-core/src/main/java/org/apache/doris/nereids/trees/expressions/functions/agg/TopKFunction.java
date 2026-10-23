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
 * Shared implementation of {@code min_topk(x, k)} and {@code max_topk(x, k)}.
 *
 * <p>Both aggregates return {@code ARRAY<STRUCT<value T, count BIGINT>>}: at most k distinct
 * non-NULL values ordered by value (ascending for MIN, descending for MAX) with the number of rows
 * equal to each value. There is no truncation flag; the caller infers it from the length (a list of
 * exactly k entries is conservatively treated as possibly truncated). With {@code k = 0} the array
 * holds at most the extreme value and its row count.
 *
 * <p>NULL values of x are skipped by the implementation, but this class deliberately does not
 * implement {@code NullIgnoringAggregateFunction}: a count expression the FE cannot fold is
 * validated by BE on every input row, so an all-NULL input must still reach the aggregate to
 * report a NULL count. Filtering those rows away (as {@code InferAggNotNull} would) turns that
 * error into an empty result, which breaks the null-ignoring result-invariance contract.
 */
public abstract class TopKFunction extends NotNullableAggregateFunction
        implements CustomSignature {

    /** Upper bound of k, shared with the BE implementation. */
    public static final long MAX_TOPK_K = 65536;

    protected TopKFunction(String name, boolean distinct, List<Expression> children) {
        super(name, distinct, false, children);
    }

    /** constructor for withChildren and reuse signature */
    protected TopKFunction(AggregateFunctionParams functionParams) {
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
        return new ArrayLiteral(new ArrayList<>(), getDataType());
    }

    /** The value type stored in the list; DECIMALV2 is replaced by its V3 form. */
    public static DataType normalizeValueType(DataType dataType) {
        if (dataType instanceof DecimalV2Type) {
            return DecimalV3Type.forType(dataType);
        }
        return dataType;
    }

    /** The pair element shared by the state column and the aggregate result. */
    public static StructType pairType(DataType valueType) {
        return new StructType(ImmutableList.of(
                new StructField("value", valueType, false, ""),
                new StructField("count", BigIntType.INSTANCE, false, "")));
    }

    /** The aggregate returns the sorted list itself; truncation is inferred by the caller. */
    public static ArrayType resultType(DataType valueType) {
        return ArrayType.of(pairType(valueType));
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
