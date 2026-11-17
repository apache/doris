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
//

#pragma once

#include <cctz/time_zone.h>

#include <cstdint>
#include <string>

namespace doris {

// When BE start, we call load_timezones_to_cache to fill lower_zone_cache_ with lower case timezone name as key
// for compatibility. then when we `find_cctz_time_zone`, just convert to lower case and find in cache. if miss,
// use parse_tz_offset_string to try to parse as offset format string.
// The whole timezone function is powered by system tzdata, which offered by TZDIR or `/usr/share/zoneinfo`
class TimezoneUtils {
public:
    // Use the offset before a transition: gap input moves forward by the gap length,
    // and repeated local times use their first occurrence. For example, New York
    // 2024-03-10 02:30 becomes 03:30-04:00 (07:30 UTC), rather than 03:00.
    // Callers handle fractional seconds separately.
    static cctz::time_point<cctz::seconds> convert_local_to_instant(
            const cctz::civil_second& local, const cctz::time_zone& timezone) {
        return timezone.lookup(local).pre;
    }

    static void load_timezones_to_cache();

    [[nodiscard]] static bool find_cctz_time_zone(const std::string& timezone,
                                                  cctz::time_zone& ctz);

    // The caller validates the offset according to its parsing policy.
    static cctz::time_zone create_fixed_offset_time_zone(int32_t offset_seconds);

    static bool try_get_fixed_offset_seconds(const cctz::time_zone& timezone,
                                             int32_t* offset_seconds);

    static const std::string default_time_zone;

private:
    // for ut only
    static void clear_timezone_caches();
    static size_t cache_size();

    static void load_offsets_to_cache();

    static bool normalize_timezone_name(const std::string& timezone, std::string* normalized);

    static bool parse_tz_offset_string(const std::string& timezone, cctz::time_zone& ctz);
};
} // namespace doris
