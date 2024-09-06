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

#include <string>

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {

// When BE start, we call load_timezones_to_cache to fill lower_zone_cache_ with lower case timezone name as key
// for compatibility. then when we `find_cctz_time_zone`, just convert to lower case and find in cache. if miss,
// use parse_tz_offset_string to try to parse as offset format string.
// The whole timezone function is powered by system tzdata, which offered by TZDIR or `/usr/share/zoneinfo`
class TimezoneUtils {
public:
    static void load_timezones_to_cache();

    static bool find_cctz_time_zone(const std::string& timezone, cctz::time_zone& ctz);

    static const std::string default_time_zone;

private:
    // for ut only
    static void clear_timezone_caches();

    static bool parse_tz_offset_string(const std::string& timezone, cctz::time_zone& ctz);
};
} // namespace doris
