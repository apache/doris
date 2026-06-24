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

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"

using boost::algorithm::to_lower_copy;

namespace fs = std::filesystem;

namespace doris {

using ZoneList = std::unordered_map<std::string, cctz::time_zone>;

RE2 time_zone_offset_format_reg(R"(^[+-]{1}\d{2}\:\d{2}$)"); // visiting is thread-safe

// for ut, make it never nullptr.
std::unique_ptr<ZoneList> lower_zone_cache_ = std::make_unique<ZoneList>();

const std::string TimezoneUtils::default_time_zone = "+08:00";
static const char* tzdir = "/usr/share/zoneinfo"; // default value, may change by TZDIR env var

void TimezoneUtils::clear_timezone_caches() {
    lower_zone_cache_->clear();
}
size_t TimezoneUtils::cache_size() {
    return lower_zone_cache_->size();
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
        throw Exception(Status::FatalError("Cannot find system tzfile. Doris exiting!"));
    }

    std::set<std::string> ignore_paths = {"posix", "right"}; // duplications. ignore them.

    for (fs::recursive_directory_iterator it {base_str}; it != end(it); it++) {
        const auto& dir_entry = *it;
        try {
            if (dir_entry.is_regular_file() ||
                (dir_entry.is_symlink() && is_regular_file(read_symlink(dir_entry)))) {
                auto tz_name = dir_entry.path().string().substr(base_str.length());
                if (!parse_save_name_tz(tz_name)) {
                    LOG(WARNING) << "Meet illegal tzdata file: " << tz_name << ". skipped";
                }
            } else if (dir_entry.is_directory() &&
                       ignore_paths.contains(dir_entry.path().filename())) {
                it.disable_recursion_pending();
            }
        } catch (const fs::filesystem_error& e) {
            // maybe symlink loop or to nowhere...
            LOG(WARNING) << "filesystem error when loading timezone file from " << dir_entry.path()
                         << ": " << e.what();
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

    load_offsets_to_cache();
    LOG(INFO) << "Preloaded" << lower_zone_cache_->size() << " timezones.";
}

static std::string to_hour_string(int arg) {
    if (arg < 0 && arg > -10) { // -9 to -1
        return std::string {"-0"} + std::to_string(std::abs(arg));
    } else if (arg >= 0 && arg < 10) { //0 to 9
        return std::string {"0"} + std::to_string(arg);
    }
    return std::to_string(arg);
}

void TimezoneUtils::load_offsets_to_cache() {
    static constexpr int supported_minutes[] = {0, 30, 45};
    for (int hour = -12; hour <= +14; hour++) {
        for (int minute : supported_minutes) {
            char min_str[3];
            snprintf(min_str, sizeof(min_str), "%02d", minute);
            std::string offset_str = (hour >= 0 ? "+" : "") + to_hour_string(hour) + ':' + min_str;
            cctz::time_zone result;
            parse_tz_offset_string(offset_str, result);
            lower_zone_cache_->emplace(offset_str, result);
        }
    }
    // -00 for hour is also valid
    std::string offset_str = "-00:00";
    cctz::time_zone result;
    parse_tz_offset_string(offset_str, result);
    lower_zone_cache_->emplace(offset_str, result);
    offset_str = "-00:30";
    parse_tz_offset_string(offset_str, result);
    lower_zone_cache_->emplace(offset_str, result);
    offset_str = "-00:45";
    parse_tz_offset_string(offset_str, result);
    lower_zone_cache_->emplace(offset_str, result);
}

bool TimezoneUtils::find_cctz_time_zone(const std::string& timezone, cctz::time_zone& ctz) {
    if (auto it = lower_zone_cache_->find(to_lower_copy(timezone)); it != lower_zone_cache_->end())
            [[likely]] {
        ctz = it->second;
        return true;
    }

    std::string normalized;
    if (!normalize_timezone_name(timezone, &normalized)) {
        return false;
    }
    if (auto it = lower_zone_cache_->find(to_lower_copy(normalized));
        it != lower_zone_cache_->end()) [[likely]] {
        ctz = it->second;
        return true;
    }
    return parse_tz_offset_string(normalized, ctz);
}

bool TimezoneUtils::try_get_fixed_offset_seconds(const cctz::time_zone& timezone,
                                                 int32_t* offset_seconds) {
    const std::string& timezone_name = timezone.name();
    if (timezone_name == "UTC" || timezone_name == "Etc/UTC" || timezone_name == "Etc/GMT") {
        *offset_seconds = 0;
        return true;
    }

    // cctz names fixed_time_zone() instances with the "Fixed/" prefix. TZDB's Etc/GMT*
    // zones are fixed offsets too; cctz handles their POSIX-style reversed sign in lookup_offset().
    // If this naming convention changes, falling through to the generic path remains correct.
    static const auto epoch = std::chrono::time_point_cast<cctz::sys_seconds>(
            std::chrono::system_clock::from_time_t(0));
    if (timezone_name.compare(0, 6, "Fixed/") == 0 || timezone_name.compare(0, 7, "Etc/GMT") == 0) {
        *offset_seconds = timezone.lookup_offset(epoch).offset;
        return true;
    }
    return false;
}

static bool normalize_offset_string(const std::string& timezone, bool allow_hour_only,
                                    std::string* normalized) {
    if (timezone.size() < 2 || (timezone[0] != '+' && timezone[0] != '-')) {
        return false;
    }

    const bool positive = timezone[0] == '+';
    const std::string_view rest(timezone.data() + 1, timezone.size() - 1);
    int hour = 0;
    int minute = 0;

    const auto parse_digit = [](char c) -> int { return c - '0'; };
    const auto is_two_digits = [](std::string_view value) -> bool {
        return value.size() == 2 && std::isdigit(static_cast<unsigned char>(value[0])) &&
               std::isdigit(static_cast<unsigned char>(value[1]));
    };
    const auto is_one_or_two_digits = [](std::string_view value) -> bool {
        return (value.size() == 1 || value.size() == 2) &&
               std::all_of(value.begin(), value.end(),
                           [](char c) { return std::isdigit(static_cast<unsigned char>(c)); });
    };

    auto colon_pos = rest.find(':');
    if (colon_pos != std::string_view::npos) {
        std::string_view hour_part = rest.substr(0, colon_pos);
        std::string_view minute_part = rest.substr(colon_pos + 1);
        if (!is_one_or_two_digits(hour_part) || !is_two_digits(minute_part)) {
            return false;
        }
        hour = std::stoi(std::string(hour_part));
        minute = parse_digit(minute_part[0]) * 10 + parse_digit(minute_part[1]);
    } else {
        if (!allow_hour_only || !is_one_or_two_digits(rest)) {
            return false;
        }
        hour = std::stoi(std::string(rest));
        minute = 0;
    }

    if ((!positive && hour > 12) || (positive && hour > 14) || minute >= 60) {
        return false;
    }

    *normalized = std::string(1, positive ? '+' : '-') + (hour < 10 ? "0" : "") +
                  std::to_string(hour) + ":" + (minute < 10 ? "0" : "") + std::to_string(minute);
    return true;
}

bool TimezoneUtils::normalize_timezone_name(const std::string& timezone, std::string* normalized) {
    const std::string lower = to_lower_copy(timezone);
    if (lower == "utc" || lower == "etc/utc" || lower == "zulu") {
        *normalized = "UTC";
        return true;
    }

    if (lower.rfind("utc", 0) == 0 || lower.rfind("gmt", 0) == 0) {
        if (timezone.size() <= 3) {
            return false;
        }
        return normalize_offset_string(timezone.substr(3), true, normalized);
    }

    if (!timezone.empty() && (timezone[0] == '+' || timezone[0] == '-')) {
        return normalize_offset_string(timezone, false, normalized);
    }

    return false;
}

bool TimezoneUtils::parse_tz_offset_string(const std::string& timezone, cctz::time_zone& ctz) {
    std::string normalized;
    if (!normalize_timezone_name(timezone, &normalized)) {
        return false;
    }
    if (normalized == "UTC") {
        ctz = cctz::utc_time_zone();
        return true;
    }

    re2::StringPiece value;
    if (time_zone_offset_format_reg.Match(normalized, 0, normalized.size(), RE2::UNANCHORED, &value,
                                          1)) [[likely]] {
        const bool positive = value[0] != '-';
        const int hour = std::stoi(value.substr(1, 2).as_string());
        const int minute = std::stoi(value.substr(4, 2).as_string());
        int offset = hour * 60 * 60 + minute * 60;
        offset *= positive ? 1 : -1;
        ctz = cctz::fixed_time_zone(cctz::seconds(offset));
        return true;
    }
    return false;
}

} // namespace doris
