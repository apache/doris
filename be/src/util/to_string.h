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

#include <absl/strings/ascii.h>
#include <fmt/compile.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <cfloat>
#include <map>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

namespace doris {

template <typename T>
std::string to_string(const T& t) {
    return fmt::format("{}", t);
}

template <typename K, typename V>
std::string to_string(const std::map<K, V>& m);

template <typename T>
std::string to_string(const std::set<T>& s);

template <typename T>
std::string to_string(const std::vector<T>& t);

template <typename K, typename V>
std::string to_string(const typename std::pair<K, V>& v) {
    return fmt::format("{}: {}", to_string(v.first), to_string(v.second));
}

template <typename T>
std::string to_string(const T& beg, const T& end) {
    std::string out;
    for (T it = beg; it != end; ++it) {
        if (it != beg) out += ", ";
        out += to_string(*it);
    }
    return out;
}

template <typename T>
std::string to_string(const std::vector<T>& t) {
    return "[" + to_string(t.begin(), t.end()) + "]";
}

template <typename K, typename V>
std::string to_string(const std::map<K, V>& m) {
    return "{" + to_string(m.begin(), m.end()) + "}";
}

template <typename T>
std::string to_string(const std::set<T>& s) {
    return "{" + to_string(s.begin(), s.end()) + "}";
}

// refer to: https://en.cppreference.com/w/cpp/types/numeric_limits/max_digits10.html
template <typename T>
inline int fast_to_buffer(T value, char* buffer) {
    char* end = nullptr;
    // output NaN and Infinity to be compatible with most of the implementations
    if (std::isnan(value)) {
        static constexpr char nan_str[] = "NaN";
        static constexpr int nan_str_len = sizeof(nan_str) - 1;
        memcpy(buffer, nan_str, nan_str_len);
        end = buffer + nan_str_len;
    } else if (std::isinf(value)) {
        static constexpr char inf_str[] = "Infinity";
        static constexpr int inf_str_len = sizeof(inf_str) - 1;
        static constexpr char neg_inf_str[] = "-Infinity";
        static constexpr int neg_inf_str_len = sizeof(neg_inf_str) - 1;
        if (value > 0) {
            memcpy(buffer, inf_str, inf_str_len);
            end = buffer + inf_str_len;
        } else {
            memcpy(buffer, neg_inf_str, neg_inf_str_len);
            end = buffer + neg_inf_str_len;
        }
    } else {
        end = fmt::format_to(buffer, FMT_COMPILE("{}"), value);
    }
    *end = '\0';
    return end - buffer;
}

template <typename T>
int to_buffer(const T& value, int width, char* buffer) {
    constexpr int DIG = (std::is_same_v<T, double> ? DBL_DIG : FLT_DIG);
    int snprintf_result = snprintf(buffer, width, "%.*g", DIG, value);
    // The snprintf should never overflow because the buffer is significantly
    // larger than the precision we asked for.
    DCHECK(snprintf_result > 0 && snprintf_result < width);

    bool need_reformat = false;
    if constexpr (std::is_same_v<T, double>) {
        need_reformat = (strtod(buffer, nullptr) != value);
    } else {
        auto safe_strtof = [](const char* str, float* value) {
            char* endptr;
            *value = strtof(str, &endptr);
            if (endptr != str) {
                while (absl::ascii_isspace(*endptr)) {
                    ++endptr;
                }
            }
            // Ignore range errors from strtod/strtof.
            // The values it returns on underflow and
            // overflow are the right fallback in a
            // robust setting.
            return *str != '\0' && *endptr == '\0';
        };

        if (float parsed_value; !safe_strtof(buffer, &parsed_value) || parsed_value != value) {
            need_reformat = true;
        }
    }

    if (need_reformat) {
        snprintf_result = snprintf(buffer, width, "%.*g", DIG + 2, value);
        // Should never overflow; see above.
        DCHECK(snprintf_result > 0 && snprintf_result < width);
    }
    return snprintf_result;
}
} // namespace doris
