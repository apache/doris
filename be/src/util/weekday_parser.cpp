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

#include "util/weekday_parser.h"

#include <perfect_hash.h>

#include <array>

namespace doris {

int parse_weekday(std::string_view weekday) {
    // No supported alias is shorter than two bytes or longer than WEDNESDAY.
    if (weekday.size() < 2 || weekday.size() > 9) {
        return 0;
    }
    static constexpr auto days = ConstexprCore::make_perfect_map<
            ConstexprCore::kv<"mo", 1>, ConstexprCore::kv<"mon", 1>, ConstexprCore::kv<"monday", 1>,
            ConstexprCore::kv<"tu", 2>, ConstexprCore::kv<"tue", 2>,
            ConstexprCore::kv<"tuesday", 2>, ConstexprCore::kv<"we", 3>,
            ConstexprCore::kv<"wed", 3>, ConstexprCore::kv<"wednesday", 3>,
            ConstexprCore::kv<"th", 4>, ConstexprCore::kv<"thu", 4>,
            ConstexprCore::kv<"thursday", 4>, ConstexprCore::kv<"fr", 5>,
            ConstexprCore::kv<"fri", 5>, ConstexprCore::kv<"friday", 5>, ConstexprCore::kv<"sa", 6>,
            ConstexprCore::kv<"sat", 6>, ConstexprCore::kv<"saturday", 6>,
            ConstexprCore::kv<"su", 7>, ConstexprCore::kv<"sun", 7>,
            ConstexprCore::kv<"sunday", 7>>();
    // The lookup compares up to 16 bytes. Copy only the view's bytes into padded
    // stack storage: an exact-size input such as {'M', 'O'} need not have padding.
    std::array<char, 16> lowercase {};
    for (size_t i = 0; i < weekday.size(); ++i) {
        lowercase[i] = static_cast<char>(static_cast<unsigned char>(weekday[i]) | 0x20U);
    }
    return days.lookup(std::string_view(lowercase.data(), weekday.size())).value_or(0);
}

} // namespace doris
