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

#include "runtime/large_int_value.h"

#include <string>

#include "util/string_parser.hpp"

namespace doris {

std::ostream& operator<<(std::ostream& os, __int128 const& value) {
    std::ostream::sentry s(os);
    if (s) {
        std::string value_str = fmt::format("{}", value);
        if (os.rdbuf()->sputn(value_str.data(), value_str.size()) != value_str.size()) {
            os.setstate(std::ios_base::badbit);
        }
    }
    return os;
}

std::istream& operator>>(std::istream& is, __int128& value) {
    std::string str;
    is >> str;
    StringParser::ParseResult result;
    value = StringParser::string_to_int<__int128>(str.c_str(), str.size(), &result);
    if (result != StringParser::PARSE_SUCCESS) {
        is.setstate(std::ios_base::failbit);
    }
    return is;
}

std::size_t hash_value(__int128 const& value) {
    return HashUtil::hash(&value, sizeof(value), 0);
}

} // namespace doris

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
