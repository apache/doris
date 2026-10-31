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

import org.apache.doris.nereids.trees.expressions.literal.Literal;

/** Monotonicity of date arithmetic that can clamp the day to the end of a month. */
public interface CalendarAddSubMonotonic extends DateAddSubMonotonic {
    @Override
    default boolean isMonotonic(Literal lower, Literal upper) {
        // Clamping consecutive dates to the same date resets the time at midnight. DATE has no
        // time part, and within one input day the calendar adjustment preserves the time order.
        return DateAddSubMonotonic.super.isMonotonic(lower, upper)
                && (child(0).getDataType().isDateV2Type() || MonotonicityUtils.isWithinSameDay(lower, upper));
    }
}
