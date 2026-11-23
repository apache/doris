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
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;

/**
 * Marker interface for expressions that can convert NULL input into a non-NULL output.
 *
 * For example: Coalesce(NULL, 2) → 2, ArrayPosition([1], NULL) → 0, NullOrEmpty(NULL) → true.
 * A function can have this behavior for specific arguments even if it propagates NULL in others.
 *
 * An outer join supplies NULL for columns on the unmatched side. These expressions can turn
 * those NULLs into values that a pushed-down aggregation would miss. A pushed-down TopN filter
 * can also remove a match and cause the join to generate a row with a better sort key.
 *
 * <p>See {@link #canConvertNullToNonNull(Expression)} for the full push-down safety check.
 */
public interface NullToNonNullFunction {

    /** Whether this expression's resolved overload can return a non-NULL value for a NULL input. */
    default boolean canConvertNullToNonNull() {
        return true;
    }

    /**
     * Check whether an expression can convert NULL input to non-NULL output.
     * This covers {@link NullToNonNullFunction} markers and input-dependent
     * {@link AlwaysNotNullable} expressions. A scalar {@link Udf} may also return a value
     * for NULL input because its declared return nullability does not guarantee NULL propagation.
     *
     * <p>In outer-join push-down safety checks, any expression matching this predicate
     * must NOT be pushed to the nullable side. Evaluate it after the join has supplied the
     * NULLs for unmatched rows.
     */
    static boolean canConvertNullToNonNull(Expression e) {
        return (e instanceof NullToNonNullFunction function && function.canConvertNullToNonNull())
                || ((e instanceof AlwaysNotNullable || (e instanceof ScalarFunction && e instanceof Udf))
                        && !e.getInputSlots().isEmpty());
    }
}
