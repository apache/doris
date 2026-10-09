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

/**
 * Monotonicity of year, quarter, and month ceil functions. Calendar arithmetic can clamp the
 * origin's day at month end, so custom origins do not universally guarantee ceil(x) >= x. For
 * partition pruning, only the canonical first-day origin is accepted.
 */
public interface CalendarCeilMonotonic extends DateCeilFloorMonotonic {
    @Override
    default boolean isRoundingRelationGuaranteed() {
        if (!DateCeilFloorMonotonic.super.isRoundingRelationGuaranteed()) {
            return false;
        }
        if (arity() == 1 || (arity() == 2 && getArgument(1) instanceof IntegerLikeLiteral)) {
            return true;
        }
        Expression origin = getArgument(arity() - 1);
        return origin instanceof DateLiteral
                && ((DateLiteral) origin).compareTo(DateTimeV2Literal.USE_IN_FLOOR_CEIL) == 0;
    }
}
