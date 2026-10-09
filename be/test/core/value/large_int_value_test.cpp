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

#include "core/value/large_int_value.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <sstream>
#include <string>

namespace doris {

TEST(LargeIntValueTest, StreamOutputUsesDecimalText) {
    const __int128 value = -((static_cast<__int128>(1) << 100) + 12345);

    std::ostringstream out;
    out << value;

    EXPECT_EQ(out.str(), LargeIntValue::to_string(value));
}

TEST(LargeIntValueTest, StreamInputParsesDecimalText) {
    const __int128 expected = (static_cast<__int128>(1) << 100) + 54321;

    std::istringstream in(LargeIntValue::to_string(expected));
    __int128 actual = 0;
    in >> actual;

    EXPECT_FALSE(in.fail());
    EXPECT_EQ(actual, expected);
}

TEST(LargeIntValueTest, StreamInputFailsOnInvalidText) {
    std::istringstream in("not-a-large-int");
    __int128 value = 123;
    in >> value;

    EXPECT_TRUE(in.fail());
    EXPECT_EQ(value, 0);
}

TEST(LargeIntValueTest, StringToLargeInt) {
    {
        std::string str("1024");
        std::stringstream ss;
        ss << str;
        __int128 v;
        ss >> v;
        EXPECT_EQ(v, 1024);
    }

    {
        std::string str("170141183460469231731687303715884105727");
        std::stringstream ss;
        ss << str;
        __int128 v;
        ss >> v;
        EXPECT_TRUE(v == MAX_INT128);
    }

    {
        std::string str("-170141183460469231731687303715884105728");
        std::stringstream ss;
        ss << str;
        __int128 v;
        ss >> v;
        EXPECT_TRUE(v == MIN_INT128);
    }
}

TEST(LargeIntValueTest, LargeIntToString) {
    {
        __int128 v1 = std::numeric_limits<int64_t>::max();
        std::stringstream ss;
        ss << v1;
        EXPECT_EQ(ss.str(), "9223372036854775807");
    }

    {
        __int128 v2 = MAX_INT128;
        std::stringstream ss;
        ss << v2;
        EXPECT_EQ(ss.str(), "170141183460469231731687303715884105727");
    }

    {
        __int128 v2 = MIN_INT128;
        std::stringstream ss;
        ss << v2;
        EXPECT_EQ(ss.str(), "-170141183460469231731687303715884105728");
    }
}

} // namespace doris
