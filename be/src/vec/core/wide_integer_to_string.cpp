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
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/wide_integer_to_string.cpp
// and modified by Doris

#include <fmt/format.h>
#include <vec/core/wide_integer.h>
#include <vec/core/wide_integer_to_string.h>

#include <string>

namespace wide {

template <size_t Bits, typename Signed>
inline std::string to_string(const integer<Bits, Signed>& n) {
    std::string res;
    if (integer<Bits, Signed>::_impl::operator_eq(n, 0U)) return "0";

    integer<Bits, unsigned> t;
    bool is_neg = integer<Bits, Signed>::_impl::is_negative(n);
    if (is_neg)
        t = integer<Bits, Signed>::_impl::operator_unary_minus(n);
    else
        t = n;

    while (!integer<Bits, unsigned>::_impl::operator_eq(t, 0U)) {
        res.insert(res.begin(),
                   '0' + char(integer<Bits, unsigned>::_impl::operator_percent(t, 10U)));
        t = integer<Bits, unsigned>::_impl::operator_slash(t, 10U);
    }

    if (is_neg) res.insert(res.begin(), '-');
    return res;
}

template std::string to_string(const integer<128, signed>& n);
template std::string to_string(const integer<128, unsigned>& n);
template std::string to_string(const integer<256, signed>& n);
template std::string to_string(const integer<256, unsigned>& n);

} // namespace wide

template <size_t Bits, typename Signed>
std::ostream& operator<<(std::ostream& out, const wide::integer<Bits, Signed>& value) {
    return out << to_string(value);
}

std::ostream& operator<<(std::ostream& out, const wide::integer<128, signed>& value);
std::ostream& operator<<(std::ostream& out, const wide::integer<128, unsigned>& value);
std::ostream& operator<<(std::ostream& out, const wide::integer<256, signed>& value);
std::ostream& operator<<(std::ostream& out, const wide::integer<256, unsigned>& value);

template struct fmt::formatter<wide::integer<128, signed>>;
template struct fmt::formatter<wide::integer<128, unsigned>>;
template struct fmt::formatter<wide::integer<256, signed>>;
template struct fmt::formatter<wide::integer<256, unsigned>>;