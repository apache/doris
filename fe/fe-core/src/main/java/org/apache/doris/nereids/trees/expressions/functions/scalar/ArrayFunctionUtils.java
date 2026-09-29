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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;

/** Argument validation and element type support checks shared by array functions. */
final class ArrayFunctionUtils {
    private ArrayFunctionUtils() {
    }

    /** Check the physical element type used by array/scalar equality functions. */
    static void checkArrayScalarEqualityArguments(ScalarFunction function) {
        checkNoVarBinaryArguments(function);
        DataType arrayType = function.getArgument(0).getDataType();
        if (arrayType.isArrayType()) {
            DataType itemType = ((ArrayType) arrayType).getItemType();
            if (itemType.isNullType()) {
                // Indexed ANY resolves an all-NULL array using the scalar argument.
                itemType = function.getArgument(1).getDataType();
            }
            if (!isSupportedByArrayEqualityFunctions(itemType)) {
                throw new AnalysisException(function.getName() + " does not support element type "
                        + itemType.toSql());
            }
        }
    }

    static void checkNoVarBinaryArguments(ScalarFunction function) {
        // Inspect original arguments before coercion can hide unsupported binary comparison/hash inputs.
        for (Expression argument : function.getArguments()) {
            DataType type = argument.getDataType();
            while (type instanceof ArrayType) {
                type = ((ArrayType) type).getItemType();
            }
            if (type.isVarBinaryType()) {
                throw new AnalysisException(function.getName() + " does not support VARBINARY arguments");
            }
        }
    }

    /** Whether the element type is supported by hash-based array set functions. */
    static boolean isSupportedByArraySetFunctions(DataType dataType) {
        return dataType.isNumericType() || dataType.isBooleanType() || dataType.isStringLikeType()
                || dataType.isDateLikeType() || dataType.isIPType() || dataType.isNullType();
    }

    /** Whether the element type is supported by array equality and hash functions. */
    static boolean isSupportedByArrayEqualityFunctions(DataType dataType) {
        return isSupportedByArraySetFunctions(dataType) || dataType.isTimeType();
    }

    /** Whether array comparison functions can compare this element type. */
    static boolean isSupportedByArrayComparisonFunctions(DataType dataType) {
        if (dataType.isArrayType()) {
            return isSupportedByArrayComparisonFunctions(((ArrayType) dataType).getItemType());
        }
        return !dataType.isOnlyMetricType();
    }

    /** Whether the element type is supported by the lambda array_sort implementation. */
    static boolean isSupportedByArraySortLambdaFunction(DataType dataType) {
        return dataType.isNumericType() || dataType.isBooleanType() || dataType.isStringLikeType()
                || dataType.isVarBinaryType() || dataType.isArrayType() || dataType.isIPType()
                || (dataType.isDateLikeType() && !dataType.isTimeStampTzType())
                || dataType.isTimeType() || dataType.isNullType();
    }

    /** Whether the element type supports the serialized-key path used by variadic array functions. */
    static boolean isSupportedByArraySerializedKeyFunctions(DataType dataType) {
        return isSupportedByArrayEqualityFunctions(dataType) || dataType.isVarBinaryType()
                || dataType.isJsonType();
    }

    /** Whether the element type is supported by array_min and array_max. */
    static boolean isSupportedByArrayMinMaxFunctions(DataType dataType) {
        return (dataType.isNumericType() && !dataType.isDecimalV2Type())
                || dataType.isBooleanType() || dataType.isStringLikeType()
                || dataType.isDateV2Type() || dataType.isDateTimeV2Type()
                || dataType.isTimeStampNsType() || dataType.isTimeStampTzType()
                || dataType.isIPType() || dataType.isNullType();
    }
}
