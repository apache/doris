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

/** A monotonic function whose result is guaranteed to be on one side of its input. */
public interface RoundingMonotonic extends Monotonic {
    /** Direction in which the function rounds its input. */
    enum RoundingType {
        FLOOR,
        CEIL
    }

    RoundingType getRoundingType();

    /** Whether the current arguments preserve the declared relation between the result and input. */
    default boolean isRoundingRelationGuaranteed() {
        return true;
    }

    /**
     * Return the next bucket boundary for an equality predicate on a floor function. For example,
     * {@code date_trunc(dt, 'day') = c} has the preimage {@code [c, nextBoundary(c))}.
     */
    default Optional<Expression> nextBucketBoundary(Literal value) {
        return Optional.empty();
    }

    /**
     * Return the previous bucket boundary for an equality predicate on a ceil function. For
     * example, {@code day_ceil(dt) = c} has the preimage {@code (previousBoundary(c), c]}.
     */
    default Optional<Expression> previousBucketBoundary(Literal value) {
        return Optional.empty();
    }
}
