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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;

/**
 * Marker interface for expressions that can convert NULL input into a non-NULL output.
 *
 * For example: Coalesce(NULL, 2) → 2, Nvl(NULL, 0) → 0, NullOrEmpty(NULL) → true.
 *
 * This is significant for outer-join push-down safety: when an aggregate function contains
 * a NullToNonNull expression wrapping a column from the nullable side of an outer join,
 * the aggregation must NOT be pushed down. Null-extended rows (produced by the join for
 * unmatched rows) have NULL for all nullable-side columns. The NullToNonNull expression
 * would convert those NULLs to non-NULL values, and the pre-aggregation would miss those
 * contributions because null-extended rows do not exist in the base table.
 *
 * <p>Note: {@link AlwaysNotNullable} expressions with input slots (e.g. Array, JsonArray,
 * JsonObject, CreateStruct, CreateMap) are also blocked from being pushed to the nullable
 * side of outer joins via a separate check in {@link #canConvertNullToNonNull(Expression)}.
 */
public interface NullToNonNullFunction {

    /**
     * Check whether an expression can convert NULL input to non-NULL output.
     * This covers both {@link NullToNonNullFunction} (e.g. Coalesce, Nvl, NullOrEmpty)
     * and {@link AlwaysNotNullable} expressions with input slots (e.g. Array, JsonArray,
     * CreateStruct, CreateMap), which always produce non-NULL output regardless of NULL inputs.
     *
     * <p>In outer-join push-down safety checks, any expression matching this predicate
     * must NOT be pushed to the nullable side, because null-extended rows (produced by the
     * join for unmatched rows) would produce non-NULL values that get aggregated, but the
     * pre-aggregation on the base table cannot see those rows — resulting in wrong results.
     */
    static boolean canConvertNullToNonNull(Expression e) {
        return e instanceof NullToNonNullFunction
                || (e instanceof AlwaysNotNullable
                        && !e.getInputSlots().isEmpty());
    }
}
