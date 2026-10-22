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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.agg.TopKCountFunction;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Shared implementation of {@code topk_merge_min} and {@code topk_merge_max}, the apply-side
 * merge of a MIN/MAX top-K state with the insert/delete top-K lists of one delta batch:
 *
 * <pre>
 * topk_merge_min(S, b, I, b_i, D, b_d, n, dn, k)
 *   -> STRUCT&lt;values ARRAY&lt;STRUCT&lt;value T, count BIGINT&gt;&gt;, bound T, n BIGINT,
 *             exhausted BOOLEAN&gt;
 * </pre>
 *
 * <p>A NULL list is an empty list; a NULL {@code b}/{@code b_i}/{@code b_d} means that side is
 * complete/untruncated. The result struct itself is never NULL, only its {@code bound} field can
 * be: the merge only trusts values in {@code v <= min(b, b_i, b_d)}, so {@code bound} is the
 * effective upper bound (for MIN; the lower bound for MAX) of the resulting state.
 * {@code exhausted} is true when the value range is used up: the merge found no value to keep but
 * {@code n + dn > 0}, which requires a fallback to a complete refresh.
 */
public abstract class TopKMergeFunction extends ScalarFunction
        implements CustomSignature, AlwaysNotNullable {

    protected TopKMergeFunction(String name, List<Expression> children) {
        super(name, children);
    }

    /** constructor for withChildren and reuse signature */
    protected TopKMergeFunction(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (arity() != 9) {
            throw new AnalysisException(getName() + " requires 9 arguments, but got " + arity());
        }
        DataType valueType = inferValueType();
        if (valueType == null) {
            throw new AnalysisException(getName() + " can not infer the value type from arguments: "
                    + toSql());
        }
        if (!TopKCountFunction.isSupportedValueType(valueType)) {
            throw new AnalysisException(getName() + " does not support type " + valueType.toSql());
        }
        for (int index : new int[] {0, 2, 4}) {
            DataType type = getArgumentType(index);
            if (type.isNullType()) {
                continue;
            }
            if (!(type instanceof ArrayType)
                    || !(((ArrayType) type).getItemType() instanceof StructType)) {
                throw new AnalysisException(getName() + " requires argument " + (index + 1)
                        + " to be ARRAY<STRUCT<value, count>>, but got " + type.toSql());
            }
        }
    }

    @Override
    public FunctionSignature customSignature() {
        DataType valueType = inferValueType();
        if (valueType == null) {
            throw new AnalysisException(getName() + " can not infer the value type from arguments: "
                    + toSql());
        }
        ArrayType listType = ArrayType.of(TopKCountFunction.pairType(valueType));
        return FunctionSignature.ret(resultType(valueType)).args(
                listType, valueType,
                listType, valueType,
                listType, valueType,
                BigIntType.INSTANCE, BigIntType.INSTANCE, BigIntType.INSTANCE);
    }

    /**
     * The value type is read from the first argument that carries it: the per-value type of S/I/D
     * (argument positions 1, 3, 5) or, when all lists are untyped NULL literals, the bound type
     * (positions 2, 4, 6).
     */
    private DataType inferValueType() {
        for (int index : new int[] {0, 2, 4}) {
            DataType type = getArgumentType(index);
            if (!(type instanceof ArrayType)) {
                continue;
            }
            DataType itemType = ((ArrayType) type).getItemType();
            if (!(itemType instanceof StructType)) {
                continue;
            }
            List<StructField> fields = ((StructType) itemType).getFields();
            if (fields.size() != 2) {
                continue;
            }
            return TopKCountFunction.normalizeValueType(fields.get(0).getDataType());
        }
        for (int index : new int[] {1, 3, 5}) {
            DataType type = getArgumentType(index);
            if (!type.isNullType()) {
                return TopKCountFunction.normalizeValueType(type);
            }
        }
        return null;
    }

    /**
     * Builds {@code STRUCT<values ARRAY<STRUCT<value T, count BIGINT>>, bound T NULL, n BIGINT,
     * exhausted BOOLEAN>}.
     */
    private static StructType resultType(DataType valueType) {
        return new StructType(ImmutableList.of(
                new StructField("values", ArrayType.of(TopKCountFunction.pairType(valueType)), false, ""),
                new StructField("bound", valueType, true, ""),
                new StructField("n", BigIntType.INSTANCE, false, ""),
                new StructField("exhausted", BooleanType.INSTANCE, false, "")));
    }
}
