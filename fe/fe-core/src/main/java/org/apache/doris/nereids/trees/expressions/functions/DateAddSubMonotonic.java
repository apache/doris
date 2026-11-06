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

import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysSub;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.util.DateUtils;

/** monotonicity for XX_ADD XX_SUB */
public interface DateAddSubMonotonic extends Monotonic {
    @Override
    default boolean isMonotonic(Literal lower, Literal upper) {
        // At a fall-back, 01:50-04 precedes 01:10-05, but adding one local day
        // reverses their UTC order. Fixed-offset sessions retain the original guarantee.
        if ((this instanceof DaysAdd || this instanceof DaysSub)
                && child(0).getDataType() instanceof TimeStampTzType
                && !DateUtils.getTimeZone().getRules().isFixedOffset()) {
            return false;
        }
        return MonotonicityUtils.hasConstantOtherArguments(this, 0);
    }

    @Override
    default boolean isPositive() {
        return true;
    }

    @Override
    default int getMonotonicFunctionChildIndex() {
        return 0;
    }
}
