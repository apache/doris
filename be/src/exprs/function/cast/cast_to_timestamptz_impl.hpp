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

#include "core/value/timestamptz_value.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"

namespace doris {

/**
 * CastToTimestampTz wraps CastToDatetimeV2 with DataTimeCastEnumType::TIMESTAMP_TZ hardcoded,
 * so external callers never need to specify DataTimeCastEnumType explicitly.
 *
 * The difference from CastToDatetimeV2:
 * - When timezone info is present in the string, TIMESTAMP_TZ converts to UTC;
 *   DATE_TIME converts to local_time_zone.
 * - When timezone info is absent, TIMESTAMP_TZ treats input as local time and converts to UTC;
 *   DATE_TIME keeps the time as-is.
 */
struct CastToTimestampTz {
    template <DatelikeParseMode ParseMode>
    static inline bool from_string_strict_mode(const StringRef& str, TimestampTzValue& res,
                                               CastParameters& params,
                                               const cctz::time_zone* local_time_zone,
                                               uint32_t to_scale) {
        if (!CastToDatetimeV2::from_string_strict_mode_internal<ParseMode,
                                                                DataTimeCastEnumType::TIMESTAMP_TZ>(
                    str, res.mutable_utc_dt(), local_time_zone, to_scale, params)) {
            return false;
        }
        return true;
    }

    static inline bool from_string_non_strict_mode_impl(const StringRef& str, TimestampTzValue& res,
                                                        CastParameters& params,
                                                        const cctz::time_zone* local_time_zone,
                                                        uint32_t to_scale) {
        if (!CastToDatetimeV2::from_string_non_strict_mode_internal<
                    DataTimeCastEnumType::TIMESTAMP_TZ>(str, res.mutable_utc_dt(), local_time_zone,
                                                        to_scale, params)) {
            return false;
        }
        return true;
    }

    static inline bool from_string_non_strict_mode(const StringRef& str, TimestampTzValue& res,
                                                   CastParameters& params,
                                                   const cctz::time_zone* local_time_zone,
                                                   uint32_t to_scale) {
        return from_string_strict_mode<DatelikeParseMode::NON_STRICT>(str, res, params,
                                                                      local_time_zone, to_scale) ||
               from_string_non_strict_mode_impl(str, res, params, local_time_zone, to_scale);
    }

    // Auto dispatch based on params.is_strict
    static inline bool from_string(const StringRef& str, TimestampTzValue& res,
                                   CastParameters& params, const cctz::time_zone* local_time_zone,
                                   uint32_t to_scale) {
        if (params.is_strict) {
            return from_string_strict_mode<DatelikeParseMode::STRICT>(str, res, params,
                                                                      local_time_zone, to_scale);
        } else {
            return from_string_non_strict_mode(str, res, params, local_time_zone, to_scale);
        }
    }
};

} // namespace doris
