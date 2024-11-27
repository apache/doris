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
#include <fmt/compile.h>
#include <fmt/format.h>
#include <stdint.h>

#include <cstddef>
#include <iostream>
#include <string>

#include "olap/olap_common.h"

namespace doris {

inline const __int128 MAX_INT128 = ~((__int128)0x01 << 127);
inline const __int128 MIN_INT128 = ((__int128)0x01 << 127);

class LargeIntValue {
public:
    static int32_t to_buffer(__int128 value, char* buffer) {
        return fmt::format_to(buffer, FMT_COMPILE("{}"), value) - buffer;
    }

    static std::string to_string(__int128 value) { return fmt::format(FMT_COMPILE("{}"), value); }
    static std::string to_string(__uint128_t value) {
        return fmt::format(FMT_COMPILE("{}"), value);
    }
};

std::ostream& operator<<(std::ostream& os, __int128 const& value);

std::istream& operator>>(std::istream& is, __int128& value);

std::size_t hash_value(LargeIntValue const& value);

} // namespace doris

// Thirdparty printers like gtest needs operator<< to be exported into global namespace, so that ADL will work.
inline std::ostream& operator<<(std::ostream& os, __int128 const& value) {
    return doris::operator<<(os, value);
}
inline std::istream& operator>>(std::istream& is, __int128& value) {
    return doris::operator>>(is, value);
}
