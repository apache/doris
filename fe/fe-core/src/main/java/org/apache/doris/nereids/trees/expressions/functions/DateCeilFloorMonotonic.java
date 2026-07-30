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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.util.Optional;
import java.util.function.BiFunction;

/** monotonicity of XX_CEIL and XX_FLOOR */
public interface DateCeilFloorMonotonic extends Monotonic {
    @Override
    default boolean isMonotonic(Literal lower, Literal upper) {
        switch (arity()) {
            case 1:
                return true;
            case 2:
                return !(child(0) instanceof Literal) && child(1) instanceof Literal;
            case 3:
                return !(child(0) instanceof Literal) && child(1) instanceof Literal && child(2) instanceof Literal;
            default:
                return false;
        }
    }

    @Override
    default boolean isPositive() {
        return true;
    }

    @Override
    default int getMonotonicFunctionChildIndex() {
        return 0;
    }

    /**
     * Shared preimage upper bound for the FLOOR subclasses (NOT ceil): floor(col) = c &lt;=&gt; col in
     * [c, c + period units). The period is the second arg (default 1 when absent); addFn builds the
     * unit-specific "+n" expression (e.g. HoursAdd::new). Returns empty when the period is not an
     * integer literal or an origin (3rd arg) is present -- an origin shifts bucket alignment and no
     * engine reverse-solves through it, so we conservatively skip the upper bound (lower bound stays
     * sound). Only meaningful when the subclass also overrides isFloor() to true.
     */
    default Optional<Expression> floorUpperBoundByPeriod(Literal c,
            BiFunction<Expression, Expression, Expression> addFn) {
        switch (arity()) {
            case 1:
                return Optional.of(addFn.apply(c, new IntegerLiteral(1)));
            case 2:
                Expression period = child(1);
                // BE rejects period < 1 at runtime (function_datetime_floor_ceil.cpp), so a non-positive
                // period is not a valid bucket width -- skip the preimage rather than build a bogus bound.
                if (period instanceof IntegerLikeLiteral && ((IntegerLikeLiteral) period).getIntValue() > 0) {
                    return Optional.of(addFn.apply(c, period));
                }
                return Optional.empty();
            default:
                // 3-arg (with origin) or unexpected arity: only the lower bound is derived
                return Optional.empty();
        }
    }

    /**
     * Shared preimage lower bound for the CEIL subclasses (NOT floor): ceil(col) = c &lt;=&gt; col in
     * (c - period units, c]. Mirror of {@link #floorUpperBoundByPeriod}. subFn builds the
     * unit-specific "-n" expression (e.g. HoursSub::new). Returns empty when the period is not an
     * integer literal or an origin (3rd arg) is present. IMPORTANT: an origin can also make the ceil
     * property itself fail (the month/quarter/year day-of-month clamp fights ceil), so the caller
     * must additionally gate isCeil() to the no-origin forms; this method only builds the =-preimage.
     * Only meaningful when the subclass also overrides isCeil() to true.
     */
    default Optional<Expression> ceilLowerBoundByPeriod(Literal c,
            BiFunction<Expression, Expression, Expression> subFn) {
        switch (arity()) {
            case 1:
                return Optional.of(subFn.apply(c, new IntegerLiteral(1)));
            case 2:
                Expression period = child(1);
                // BE rejects period < 1 at runtime (function_datetime_floor_ceil.cpp), so a non-positive
                // period is not a valid bucket width -- skip the preimage rather than build a bogus bound.
                if (period instanceof IntegerLikeLiteral && ((IntegerLikeLiteral) period).getIntValue() > 0) {
                    return Optional.of(subFn.apply(c, period));
                }
                return Optional.empty();
            default:
                // 3-arg (with origin) or unexpected arity: only the upper bound is derived
                return Optional.empty();
        }
    }
}
