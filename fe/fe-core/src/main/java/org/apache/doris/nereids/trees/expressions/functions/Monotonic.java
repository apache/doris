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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.util.Optional;

/** monotonicity of expressions */
public interface Monotonic extends ExpressionTrait {
    default boolean isMonotonic(Literal lower, Literal upper) {
        return true;
    }

    // true means that the function is an increasing function
    boolean isPositive();

    // return the range input child index
    // e.g. date_trunc(dt,'xxx') return 0
    int getMonotonicFunctionChildIndex();

    // return the function with the arguments replaced by literal
    // e.g. date_trunc(dt, 'day'), dt in range ['2020-01-01 10:00:00', '2020-01-03 10:00:00']
    // return date_trunc('2020-01-01 10:00:00', 'day')
    Expression withConstantArgs(Expression literal);

    // whether the function is a floor: f(col) <= col in its DEFAULT (no-origin) form (e.g. date
    // truncation). Used to derive col >= c from f(col) >= c without an inverse. Default false.
    // CONTRACT: this declares the rounding SHAPE of the plain form; callers that reverse it into a
    // bare-column bound are responsible for argument-shape gating. For the period/origin *_floor
    // family a user origin is still safe (floor always rounds to a boundary <= col), but a
    // non-positive / non-literal integer period must be gated out (BE rejects period < 1 at runtime)
    // -- see InferPredicateFromMonotonicFunction.floorHasValidPeriod. Implementations return a
    // per-class constant (shape), not a per-argument soundness verdict.
    default boolean isFloor() {
        return false;
    }

    // for a floor function f, return c raised by exactly one floor bucket as an (unfolded)
    // expression, so that f(col) = c  <=>  col in [c, thisBound). e.g. date(col): DaysAdd(c, 1).
    // Empty = no preimage upper bound available (caller keeps only the lower bound). The caller
    // constant-folds the returned expression and fails open (lower bound only) if it cannot.
    default Optional<Expression> floorUpperBound(Literal c) {
        return Optional.empty();
    }

    // whether the function is a ceil: f(col) >= col in its DEFAULT (no-origin) form (e.g. date
    // round-up). Used to derive col <= c from f(col) <= c without an inverse. Default false.
    // Mirror of isFloor(); the same CONTRACT applies: this declares the rounding SHAPE of the plain
    // form, NOT a per-argument guarantee. Unlike floor, a user ORIGIN can make ceil round DOWN below
    // col (the day-of-month clamp in month/quarter/year add fights ceil), breaking f(col) >= col, so
    // callers reversing this into a bare-column bound MUST gate out origin / non-positive /
    // non-literal period forms -- see InferPredicateFromMonotonicFunction.ceilHasNoOriginAndValidPeriod.
    // Implementations return a per-class constant (shape), not a per-argument soundness verdict.
    default boolean isCeil() {
        return false;
    }

    // for a ceil function f, return c lowered by exactly one ceil bucket as an (unfolded) expression,
    // so that f(col) = c  <=>  col in (thisBound, c]. e.g. day_ceil(col): DaysSub(c, 1). Empty = no
    // preimage lower bound available (caller keeps only the upper bound col <= c). Mirror of
    // floorUpperBound. The caller constant-folds it and fails open (upper bound only) if it cannot.
    default Optional<Expression> ceilLowerBound(Literal c) {
        return Optional.empty();
    }
}
