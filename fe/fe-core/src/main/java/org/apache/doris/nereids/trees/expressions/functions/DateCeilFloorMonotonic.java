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
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.util.Optional;

/** Monotonicity and rounding relation of date/time ceil and floor functions. */
public interface DateCeilFloorMonotonic extends RoundingMonotonic {
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

    @Override
    default boolean isRoundingRelationGuaranteed() {
        if (arity() == 1) {
            return true;
        }
        if (arity() == 2 && getArgument(1).getDataType().isDateLikeType()) {
            return true;
        }
        return (arity() == 2 || arity() == 3) && isPositiveIntegerLiteral(getArgument(1));
    }

    /**
     * Return the period when adjacent bucket boundaries can be obtained by adding or subtracting
     * that period from the rounded value. Only the canonical origin is accepted here; in particular,
     * custom origins for calendar units can have irregular boundaries after month-end clamping.
     */
    default Optional<Expression> regularBucketPeriod() {
        if (arity() == 1) {
            return Optional.of(new IntegerLiteral(1));
        }
        Expression secondArgument = getArgument(1);
        if (arity() == 2) {
            if (secondArgument.getDataType().isIntegerLikeType()) {
                return isPositiveIntegerLiteral(secondArgument)
                        ? Optional.of(secondArgument)
                        : Optional.empty();
            }
            return isDefaultOrigin(secondArgument)
                    ? Optional.of(new IntegerLiteral(1))
                    : Optional.empty();
        }
        if (arity() == 3 && isPositiveIntegerLiteral(secondArgument)
                && isDefaultOrigin(getArgument(2))) {
            return Optional.of(secondArgument);
        }
        return Optional.empty();
    }

    private boolean isPositiveIntegerLiteral(Expression expression) {
        return expression instanceof IntegerLikeLiteral
                && ((IntegerLikeLiteral) expression).getBigDecimalValue().signum() > 0;
    }

    private boolean isDefaultOrigin(Expression expression) {
        return expression instanceof DateLiteral
                && ((DateLiteral) expression).compareTo(DateTimeV2Literal.USE_IN_FLOOR_CEIL) == 0;
    }
}
