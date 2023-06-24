
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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <limits>
#include <memory>
#include <random>
#include <type_traits>

namespace doris::vectorized {
static constexpr const char END_SYMBOL = '\0';

static void rtrim(std::string& s) {
    if (int pos = s.find_last_not_of(END_SYMBOL); pos != std::string::npos) {
        s = s.substr(0, pos + 1);
    }
}
static constexpr const char alphanum[] =
        "0123456789"
        "!@#$%^&*"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
static std::default_random_engine random {static_cast<unsigned>(time(0))};
static std::mt19937 random_generator(random());
[[maybe_unused]] static std::string generate(size_t length, const std::string& charset = "") {
    // use default charset if no charset is specified
    std::string str = charset.empty() ? std::string(alphanum) : charset;
    // double string length until it is at least as long as the requested length
    while (length > str.length()) str += str;
    // shuffle string
    std::shuffle(str.begin(), str.end(), random_generator);
    // return substring with specified length
    return str.substr(0, length);
}
} // namespace doris::vectorized
