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
#include <re2/stringpiece.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <cctype>
#include <exception>
#include <filesystem>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "common/exception.h"
#include "common/logging.h"

namespace doris {

namespace vectorized {
using ZoneList = std::unordered_map<std::string, cctz::time_zone>;
}

RE2 TimezoneUtils::time_zone_offset_format_reg("^[+-]{1}\\d{2}\\:\\d{2}$");

std::unordered_map<std::string, std::string> TimezoneUtils::timezone_names_map_;
bool TimezoneUtils::inited_ = false;
// for ut, make it never nullptr.
std::unique_ptr<vectorized::ZoneList> zone_cache = std::make_unique<vectorized::ZoneList>();
std::shared_mutex zone_cache_rw_lock;

const std::string TimezoneUtils::default_time_zone = "+08:00";
static const char* tzdir = "/usr/share/zoneinfo"; // default value, may change by TZDIR env var

void TimezoneUtils::clear_timezone_caches() {
    zone_cache->clear();
    timezone_names_map_.clear();
    inited_ = false;
}

void TimezoneUtils::load_timezone_names() {
    if (inited_) {
        return;
    }

    inited_ = true;
    std::string path;
    char* tzdir_env = std::getenv("TZDIR");
    if (tzdir_env && *tzdir_env) {
        tzdir = tzdir_env;
    }
    path += tzdir;
    path += '/';

    if (!std::filesystem::exists(path)) {
        LOG_WARNING("Cannot find system tzfile. Abandon to preload timezone name cache.");
        return;
    }

    auto path_prefix_len = path.size();
    for (auto const& dir_entry : std::filesystem::recursive_directory_iterator {path}) {
        if (dir_entry.is_regular_file()) {
            auto timezone_full_name = dir_entry.path().string().substr(path_prefix_len);
            timezone_names_map_[boost::algorithm::to_lower_copy(timezone_full_name)] =
                    timezone_full_name;
        }
    }
}

namespace { // functions use only in this file

template <typename T>
T swapEndianness(T value) {
    constexpr int numBytes = sizeof(T);
    T result = 0;
    for (int i = 0; i < numBytes; ++i) {
        result = (result << 8) | ((value >> (8 * i)) & 0xFF);
    }
    return result;
}

template <typename T>
T next_from_charstream(int8_t*& src) {
    T value = *reinterpret_cast<T*>(src);
    src += sizeof(T) / sizeof(int8_t);
    if constexpr (std::endian::native == std::endian::little) {
        return swapEndianness(
                value); // timezone information files use network endianess, which is big-endian
    } else if (std::endian::native == std::endian::big) {
        return value;
    } else {
        LOG(FATAL) << "Unknown endianess";
    }
    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

std::pair<int8_t*, int> load_file_to_memory(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    int len = lseek(fd, 0, SEEK_END); // bytes

    int8_t* addr = (int8_t*)mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
    int8_t* data = new int8_t[len];
    memcpy(data, addr, len);
    close(fd);
    munmap(addr, len);

    return {data, len};
}

struct alignas(alignof(uint8_t)) ttinfo {
    uint8_t tt_utoff[4]; // need force cast to int32_t
    uint8_t tt_isdst;
    uint8_t tt_desigidx;
};
constexpr static int TTINFO_SIZE = sizeof(ttinfo);
static_assert(TTINFO_SIZE == 6);

struct real_ttinfo {
    [[maybe_unused]] real_ttinfo() = default; // actually it's used. how stupid compiler!
    real_ttinfo(const ttinfo& arg) {
        diff_seconds = *reinterpret_cast<const int32_t*>(arg.tt_utoff + 0);
        is_dst = arg.tt_isdst;
        name_index = arg.tt_desigidx;
    }

    int32_t diff_seconds; // to UTC
    bool is_dst;
    uint8_t name_index;
};

template <>
ttinfo next_from_charstream<ttinfo>(int8_t*& src) {
    ttinfo value = *reinterpret_cast<ttinfo*>(src);
    src += TTINFO_SIZE;
    if constexpr (std::endian::native == std::endian::little) {
        std::swap(value.tt_utoff[0], value.tt_utoff[3]);
        std::swap(value.tt_utoff[1], value.tt_utoff[2]);
    }
    return value;
}

/* 
 * follow the rule of tzfile(5) which defined in https://man7.org/linux/man-pages/man5/tzfile.5.html.
 * should change when it changes.
 */
bool parse_load_timezone(vectorized::ZoneList& zone_list, int8_t* data, int len,
                         bool first_time = true) {
    int8_t* begin_pos = data;
    /* HEADERS */
    if (memcmp(data, "TZif", 4) != 0) [[unlikely]] { // magic number
        return false;
    }
    data += 4;

    // if version = 2, the whole header&data will repeat itself one time.
    int8_t version = next_from_charstream<int8_t>(data) - '0';
    data += 15; // null bits
    int32_t ut_count = next_from_charstream<int32_t>(data);
    int32_t wall_count = next_from_charstream<int32_t>(data);
    int32_t leap_count = next_from_charstream<int32_t>(data);
    int32_t trans_time_count = next_from_charstream<int32_t>(data);
    int32_t type_count = next_from_charstream<int32_t>(data);
    int32_t char_count = next_from_charstream<int32_t>(data);

    /* HEADERS end, FIELDS begin*/
    // transaction time points, which we don't need
    data += (first_time ? 5 : 9) * trans_time_count;

    // timezones
    std::vector<real_ttinfo> timezones(type_count);
    for (int i = 0; i < type_count; i++) {
        ttinfo tz_data = next_from_charstream<ttinfo>(data);
        timezones[i] = tz_data; // cast by c'tor
    }

    // timezone names
    const char* name_zone = (char*)data;
    data += char_count;

    // concate names
    for (auto& tz : timezones) {
        int len = strlen(name_zone + tz.name_index);
        zone_list.emplace(std::string {name_zone + tz.name_index, name_zone + tz.name_index + len},
                          cctz::fixed_time_zone(cctz::seconds(tz.diff_seconds)));
    }

    // the second part.
    if (version == 2 && first_time) {
        // leap seconds, standard/wall indicators, UT/local indicators, which we don't need
        data += 4 * leap_count + wall_count + ut_count;

        return (data < begin_pos + len) &&
               parse_load_timezone(zone_list, data, len - (data - begin_pos), false);
    }

    return true;
}

} // namespace

void TimezoneUtils::load_timezones_to_cache() {
    (*zone_cache)["CST"] = cctz::fixed_time_zone(cctz::seconds(8 * 3600));

    std::string base_str;
    // try get from System
    char* tzdir_env = std::getenv("TZDIR");
    if (tzdir_env && *tzdir_env) {
        tzdir = tzdir_env;
    }

    base_str += tzdir;
    base_str += '/';

    const auto root_path = std::filesystem::path {base_str};
    if (!std::filesystem::exists(root_path)) {
        LOG_WARNING("Cannot find system tzfile. Abandon to preload timezone cache.");
        return;
    }

    std::set<std::string> ignore_paths = {"posix", "right"}; // duplications

    for (std::filesystem::recursive_directory_iterator it {base_str}; it != end(it); it++) {
        const auto& dir_entry = *it;
        if (dir_entry.is_regular_file()) {
            auto tz_name = relative(dir_entry, base_str);

            auto tz_path = dir_entry.path().string();
            auto [handle, length] = load_file_to_memory(tz_path);

            parse_load_timezone(*zone_cache, handle, length);

            delete[] handle;
        } else if (dir_entry.is_directory() && ignore_paths.contains(dir_entry.path().filename())) {
            it.disable_recursion_pending();
        }
    }

    zone_cache->erase("LMT"); // local mean time for every timezone
    LOG(INFO) << "Read " << zone_cache->size() << " timezones.";
}

bool TimezoneUtils::find_cctz_time_zone(const std::string& timezone, cctz::time_zone& ctz) {
    zone_cache_rw_lock.lock_shared();
    if (auto it = zone_cache->find(timezone); it != zone_cache->end()) {
        ctz = it->second;
        zone_cache_rw_lock.unlock_shared();
        return true;
    }
    zone_cache_rw_lock.unlock_shared();
    return find_cctz_time_zone_impl(timezone, ctz);
}

bool TimezoneUtils::find_cctz_time_zone_impl(const std::string& timezone, cctz::time_zone& ctz) {
    // now timezone is not in zone_cache

    auto timezone_lower = boost::algorithm::to_lower_copy(timezone);
    re2::StringPiece value;
    // +08:00
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
        std::unique_lock<std::shared_mutex> l(zone_cache_rw_lock);
        zone_cache->emplace(timezone, ctz);
        return true;
    } else { // not only offset, GMT or GMT+8
        // split tz_name and offset
        int split = timezone_lower.find('+') != std::string::npos ? timezone_lower.find('+')
                                                                  : timezone_lower.find('-');
        cctz::time_zone offset;
        bool have_both = split != std::string::npos && split + 1 < timezone_lower.length() &&
                         std::isdigit(timezone_lower[split + 1]);
        if (have_both) {
            auto offset_str = timezone_lower.substr(split);
            timezone_lower = timezone_lower.substr(0, split);
            int offset_hours = 0;
            try {
                offset_hours = std::stoi(offset_str);
            } catch ([[maybe_unused]] std::exception& e) {
                VLOG_DEBUG << "Unable to cast " << timezone << " as timezone";
                return false;
            }
            offset = cctz::fixed_time_zone(cctz::seconds(offset_hours * 60 * 60));
        }

        bool tz_parsed = false;
        if (timezone_lower == "cst") {
            // Supports offset and region timezone type, "CST" use here is compatibility purposes.
            ctz = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));
            tz_parsed = true;
        } else if (timezone_lower == "z") {
            ctz = cctz::utc_time_zone();
            tz_parsed = true;
        } else {
            auto it = timezone_names_map_.find(timezone_lower);
            if (it != timezone_names_map_.end()) {
                tz_parsed = cctz::load_time_zone(it->second, &ctz);
            } else {
                tz_parsed = cctz::load_time_zone(timezone, &ctz);
            }
        }
        if (tz_parsed) {
            if (!have_both) { // GMT only
                std::unique_lock<std::shared_mutex> l(zone_cache_rw_lock);
                zone_cache->emplace(timezone, ctz);
                return true;
            }
            // GMT+8
            auto tz = (cctz::convert(cctz::civil_second {}, ctz) -
                       cctz::time_point<cctz::seconds>()) -
                      (cctz::convert(cctz::civil_second {}, offset) -
                       cctz::time_point<cctz::seconds>());
            ctz = cctz::fixed_time_zone(std::chrono::duration_cast<std::chrono::seconds>(tz));
            std::unique_lock<std::shared_mutex> l(zone_cache_rw_lock);
            zone_cache->emplace(timezone, ctz);
            return true;
        }
    }
    return false;
}

} // namespace doris
