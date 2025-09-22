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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/wide_integer_to_string.h
// and modified by Doris
#pragma once

#include <fmt/format.h>

#include <string>

namespace wide {
template <size_t Bits, typename Signed>
class integer;
}

using Int128 = wide::integer<128, signed>;
using UInt128 = wide::integer<128, unsigned>;
using Int256 = wide::integer<256, signed>;
using UInt256 = wide::integer<256, unsigned>;

namespace wide {

template <size_t Bits, typename Signed>
std::string to_string(const integer<Bits, Signed>& n);

extern template std::string to_string(const Int128& n);
extern template std::string to_string(const UInt128& n);
extern template std::string to_string(const Int256& n);
extern template std::string to_string(const UInt256& n);
} // namespace wide

template <size_t Bits, typename Signed>
std::ostream& operator<<(std::ostream& out, const wide::integer<Bits, Signed>& value);

extern std::ostream& operator<<(std::ostream& out, const Int128& value);
extern std::ostream& operator<<(std::ostream& out, const UInt128& value);
extern std::ostream& operator<<(std::ostream& out, const Int256& value);
extern std::ostream& operator<<(std::ostream& out, const UInt256& value);

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <size_t Bits, typename Signed>
struct fmt::formatter<wide::integer<Bits, Signed>> {
    constexpr auto parse(format_parse_context& ctx) {
        const auto* it = ctx.begin();
        const auto* end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}') {
            throw format_error("invalid format");
        }

        return it;
    }

    template <typename FormatContext>
    auto format(const wide::integer<Bits, Signed>& value, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", to_string(value));
    }
};

extern template struct fmt::formatter<Int128>;
extern template struct fmt::formatter<UInt128>;
extern template struct fmt::formatter<Int256>;
extern template struct fmt::formatter<UInt256>;
