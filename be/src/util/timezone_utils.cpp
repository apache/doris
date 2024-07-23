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

#include "util/timezone_utils.h"

#include <cctz/civil_time.h>
#include <cctz/time_zone.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <filesystem>
#include <memory>
#include <string>

#include "common/logging.h"
#include "common/status.h"

using boost::algorithm::to_lower_copy;

namespace fs = std::filesystem;

namespace doris {

namespace vectorized {
using ZoneList = std::unordered_map<std::string, cctz::time_zone>;
}

RE2 time_zone_offset_format_reg(R"(^[+-]{1}\d{2}\:\d{2}$)"); // visiting is thread-safe

// for ut, make it never nullptr.
std::unique_ptr<vectorized::ZoneList> lower_zone_cache_ = std::make_unique<vectorized::ZoneList>();

const std::string TimezoneUtils::default_time_zone = "+08:00";
static const char* tzdir = "/usr/share/zoneinfo"; // default value, may change by TZDIR env var

void TimezoneUtils::clear_timezone_caches() {
    lower_zone_cache_->clear();
}

static bool parse_save_name_tz(const std::string& tz_name) {
    cctz::time_zone tz;
    PROPAGATE_FALSE(cctz::load_time_zone(tz_name, &tz));
    lower_zone_cache_->emplace(to_lower_copy(tz_name), tz);
    return true;
}

void TimezoneUtils::load_timezones_to_cache() {
    std::string base_str;
    // try get from system
    char* tzdir_env = std::getenv("TZDIR");
    if (tzdir_env && *tzdir_env) {
        tzdir = tzdir_env;
    }

    base_str = tzdir;
    base_str += '/';

    const auto root_path = fs::path {base_str};
    if (!exists(root_path)) {
        LOG(FATAL) << "Cannot find system tzfile. Doris exiting!";
        __builtin_unreachable();
    }

    std::set<std::string> ignore_paths = {"posix", "right"}; // duplications. ignore them.

    for (fs::recursive_directory_iterator it {base_str}; it != end(it); it++) {
        const auto& dir_entry = *it;
        if (dir_entry.is_regular_file() ||
            (dir_entry.is_symlink() && is_regular_file(read_symlink(dir_entry)))) {
            auto tz_name = dir_entry.path().string().substr(base_str.length());
            if (!parse_save_name_tz(tz_name)) {
                LOG(WARNING) << "Meet illegal tzdata file: " << tz_name << ". skipped";
            }
        } else if (dir_entry.is_directory() && ignore_paths.contains(dir_entry.path().filename())) {
            it.disable_recursion_pending();
        }
    }
    // some special cases. Z = Zulu. CST = Asia/Shanghai
    if (auto it = lower_zone_cache_->find("zulu"); it != lower_zone_cache_->end()) {
        lower_zone_cache_->emplace("z", it->second);
    }
    if (auto it = lower_zone_cache_->find("asia/shanghai"); it != lower_zone_cache_->end()) {
        lower_zone_cache_->emplace("cst", it->second);
    }

    lower_zone_cache_->erase("lmt"); // local mean time for every timezone
    LOG(INFO) << "Read " << lower_zone_cache_->size() << " timezones.";
}

bool TimezoneUtils::find_cctz_time_zone(const std::string& timezone, cctz::time_zone& ctz) {
    if (auto it = lower_zone_cache_->find(to_lower_copy(timezone));
        it != lower_zone_cache_->end()) {
        ctz = it->second;
        return true;
    }
    // offset format or just illegal
    return parse_tz_offset_string(timezone, ctz);
}

bool TimezoneUtils::parse_tz_offset_string(const std::string& timezone, cctz::time_zone& ctz) {
    // like +08:00, which not in timezone_names_map_
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
        // try to push the result time offset of "+08:00" need lock. now it's harmful for performance.
        // maybe we can use rcu of hazard-pointer to opt it.
        return true;
    }
    return false;
}

} // namespace doris
