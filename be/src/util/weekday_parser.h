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

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

namespace doris {

namespace detail {
constexpr size_t weekday_hash(std::string_view weekday) {
    const auto first = static_cast<unsigned char>(weekday[0]) | 0x20U;
    const auto second = static_cast<unsigned char>(weekday[1]) | 0x20U;
    return (first ^ (second * 26U) ^ (weekday.size() * 30U)) & 31U;
}

template <typename T>
bool weekday_chunk_equal(const char* input, const char* expected) {
    T actual;
    T target;
    std::memcpy(&actual, input, sizeof(T));
    std::memcpy(&target, expected, sizeof(T));
    return (actual | static_cast<T>(0x2020202020202020ULL)) == target;
}
} // namespace detail

// Return ISO weekday 1..7, or 0 for anything except a supported English alias.
inline int parse_weekday(std::string_view weekday) {
    struct Entry {
        std::string_view name;
        int day = 0;
    };
    static constexpr std::array<Entry, 21> names = {{
            {.name = "mo", .day = 1}, {.name = "mon", .day = 1}, {.name = "monday", .day = 1},
            {.name = "tu", .day = 2}, {.name = "tue", .day = 2}, {.name = "tuesday", .day = 2},
            {.name = "we", .day = 3}, {.name = "wed", .day = 3}, {.name = "wednesday", .day = 3},
            {.name = "th", .day = 4}, {.name = "thu", .day = 4}, {.name = "thursday", .day = 4},
            {.name = "fr", .day = 5}, {.name = "fri", .day = 5}, {.name = "friday", .day = 5},
            {.name = "sa", .day = 6}, {.name = "sat", .day = 6}, {.name = "saturday", .day = 6},
            {.name = "su", .day = 7}, {.name = "sun", .day = 7}, {.name = "sunday", .day = 7},
    }};
    // The hash constants distinguish all 21 aliases in 32 slots: "mon" hashes to 17,
    // "monday" to 31. Verify every placement at compile time instead of handling collisions.
    static constexpr auto table = [] {
        std::array<Entry, 32> result {};
        for (const auto& entry : names) {
            result[detail::weekday_hash(entry.name)] = entry;
        }
        return result;
    }();
    static_assert(std::ranges::all_of(names, [](const auto& entry) {
        return table[detail::weekday_hash(entry.name)].name == entry.name;
    }));

    // Empty/single-byte input cannot supply the hash characters; no alias exceeds 9 bytes.
    if (weekday.size() < 2 || weekday.size() > 9) {
        return 0;
    }
    const auto& entry = table[detail::weekday_hash(weekday)];
    if (entry.name.size() != weekday.size()) {
        return 0;
    }
    // A perfect hash is collision-free only for stored keys. For example, "monddy" must
    // still be rejected even though its first two characters and length match "monday".
    const char* input = weekday.data();
    const char* expected = entry.name.data();
    const auto length = weekday.size();
    // Fixed-size loads fold ASCII case a word at a time without reading outside the view.
    // The two 4-byte loads overlap for 6/7-byte names; a 9-byte name needs 8 bytes plus its tail.
    bool matches;
    if (length <= 3) {
        matches = detail::weekday_chunk_equal<uint16_t>(input, expected) &&
                  (static_cast<unsigned char>(input[length - 1]) | 0x20U) == expected[length - 1];
    } else if (length <= 8) {
        matches = detail::weekday_chunk_equal<uint32_t>(input, expected) &&
                  detail::weekday_chunk_equal<uint32_t>(input + length - 4, expected + length - 4);
    } else {
        matches = detail::weekday_chunk_equal<uint64_t>(input, expected) &&
                  (static_cast<unsigned char>(input[8]) | 0x20U) == expected[8];
    }
    return matches ? entry.day : 0;
}

} // namespace doris
