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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;

/**
 * Element type checks that are specific to array function implementations.
 * These restrictions are checked against the original argument types before type coercion.
 */
public final class ArrayFunctionTypeChecker {

    private ArrayFunctionTypeChecker() {
    }

    /** Whether the element type is supported by hash-based array set functions. */
    public static boolean isSupportedByArraySetFunctions(DataType dataType) {
        return dataType.isNumericType() || dataType.isBooleanType() || dataType.isStringLikeType()
                || dataType.isDateLikeType() || dataType.isIPType() || dataType.isNullType();
    }

    /** Whether the element type is supported by array equality and hash functions. */
    public static boolean isSupportedByArrayEqualityFunctions(DataType dataType) {
        return isSupportedByArraySetFunctions(dataType) || dataType.isTimeType();
    }

    /** Whether array comparison functions can compare this element type. */
    public static boolean isSupportedByArrayComparisonFunctions(DataType dataType) {
        if (dataType.isArrayType()) {
            return isSupportedByArrayComparisonFunctions(((ArrayType) dataType).getItemType());
        }
        return !dataType.isOnlyMetricType();
    }

    /** Whether the element type is supported by the lambda array_sort implementation. */
    public static boolean isSupportedByArraySortLambdaFunction(DataType dataType) {
        return dataType.isNumericType() || dataType.isBooleanType() || dataType.isStringLikeType()
                || dataType.isVarBinaryType() || dataType.isArrayType() || dataType.isIPType()
                || dataType.isDateLikeType() || dataType.isTimeType() || dataType.isNullType();
    }

    /** Whether the element type supports the serialized-key path used by variadic array functions. */
    public static boolean isSupportedByArraySerializedKeyFunctions(DataType dataType) {
        return isSupportedByArrayEqualityFunctions(dataType) || dataType.isVarBinaryType()
                || dataType.isJsonType();
    }

    /** Whether the element type is supported by array_min and array_max. */
    public static boolean isSupportedByArrayMinMaxFunctions(DataType dataType) {
        return isSupportedByArraySetFunctions(dataType);
    }
}
