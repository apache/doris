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

#include "util/timezone_utils.h"

#include <cctz/time_zone.h>
#include <re2/stringpiece.h>

#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <string>

namespace doris {

RE2 TimezoneUtils::time_zone_offset_format_reg("^[+-]{1}\\d{2}\\:\\d{2}$");
std::unordered_map<std::string, std::string> TimezoneUtils::timezone_names_map_;
bool TimezoneUtils::inited_ = false;

const std::string TimezoneUtils::default_time_zone = "+08:00";

void TimezoneUtils::load_timezone_names() {
    if (inited_) {
        return;
    }

    inited_ = true;
    std::string path;
    const char* tzdir = "/usr/share/zoneinfo";
    char* tzdir_env = std::getenv("TZDIR");
    if (tzdir_env && *tzdir_env) {
        tzdir = tzdir_env;
    }
    path += tzdir;
    path += '/';

    auto path_prefix_len = path.size();
    for (auto const& dir_entry : std::filesystem::recursive_directory_iterator {path}) {
        if (dir_entry.is_regular_file()) {
            auto timezone_full_name = dir_entry.path().string().substr(path_prefix_len);
            timezone_names_map_[boost::algorithm::to_lower_copy(timezone_full_name)] =
                    timezone_full_name;
        }
    }
}
bool TimezoneUtils::find_cctz_time_zone(const std::string& timezone, cctz::time_zone& ctz) {
    auto timezone_lower = boost::algorithm::to_lower_copy(timezone);
    re2::StringPiece value;
    if (time_zone_offset_format_reg.Match(timezone, 0, timezone.size(), RE2::UNANCHORED, &value,
                                          1)) {
        bool positive = value[0] != '-';

        //Regular expression guarantees hour and minute must be int
        int hour = std::stoi(value.substr(1, 2).as_string());
        int minute = std::stoi(value.substr(4, 2).as_string());

        // timezone offsets around the world extended from -12:00 to +14:00
        if (!positive && hour > 12) {
            return false;
        } else if (positive && hour > 14) {
            return false;
        }
        int offset = hour * 60 * 60 + minute * 60;
        offset *= positive ? 1 : -1;
        ctz = cctz::fixed_time_zone(cctz::seconds(offset));
        return true;
    } else if (timezone_lower == "cst") {
        // Supports offset and region timezone type, "CST" use here is compatibility purposes.
        ctz = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));
        return true;
    } else {
        auto it = timezone_names_map_.find(timezone_lower);
        if (it == timezone_names_map_.end()) {
            return false;
        }
        return cctz::load_time_zone(it->second, &ctz);
    }
}

} // namespace doris
