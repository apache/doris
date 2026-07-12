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

#pragma once

#include <cctz/time_zone.h>

#include <cstdint>

#include "common/check.h"

namespace doris::format {

inline int64_t floor_epoch_seconds(int64_t value, int64_t units_per_second) {
    DORIS_CHECK(units_per_second > 0);
    auto seconds = value / units_per_second;
    if (value < 0 && value % units_per_second != 0) {
        --seconds;
    }
    return seconds;
}

// UTC instants are ordered, but converting them to civil DATETIMEV2 values is not monotonic when
// a timezone moves its clock backward. Metadata min/max converted across such a transition cannot
// safely form a ZoneMap: the true civil minimum or maximum may occur inside the UTC interval.
inline bool utc_timestamp_range_is_monotonic(int64_t min_seconds, int64_t max_seconds,
                                             const cctz::time_zone& timezone) {
    DORIS_CHECK(min_seconds <= max_seconds);
    auto current = cctz::time_point<cctz::seconds>(cctz::seconds(min_seconds));
    const auto range_end = cctz::time_point<cctz::seconds>(cctz::seconds(max_seconds));
    cctz::time_zone::civil_transition transition;
    while (timezone.next_transition(current, &transition)) {
        const auto transition_time = timezone.lookup(transition.to).trans;
        if (transition_time > range_end) {
            return true;
        }
        if (transition.to < transition.from) {
            return false;
        }
        // Move past the transition that was just inspected. Some cctz implementations return the
        // same transition again when queried at its exact instant, which would otherwise prevent
        // us from seeing a later rollback in the requested range.
        current = transition_time + cctz::seconds(1);
    }
    return true;
}

} // namespace doris::format
