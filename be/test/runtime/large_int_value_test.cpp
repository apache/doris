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

#include <gtest/gtest.h>

#include <iostream>
#include <sstream>
#include <string>

#include "common/configbase.h"
#include "common/logging.h"

namespace doris {

class LargeIntValueTest : public testing::Test {
public:
    LargeIntValueTest() {}

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}
};

TEST_F(LargeIntValueTest, string_to_largeint) {
    {
        std::string str("1024");
        std::stringstream ss;
        ss << str;
        __int128 v;
        ss >> v;
        ASSERT_EQ(v, 1024);
    }

    {
        std::string str("170141183460469231731687303715884105727");
        std::stringstream ss;
        ss << str;
        __int128 v;
        ss >> v;
        ASSERT_TRUE(v == MAX_INT128);
    }

    {
        std::string str("-170141183460469231731687303715884105728");
        std::stringstream ss;
        ss << str;
        __int128 v;
        ss >> v;
        ASSERT_TRUE(v == MIN_INT128);
    }
}

TEST_F(LargeIntValueTest, largeint_to_string) {
    {
        __int128 v1 = std::numeric_limits<int64_t>::max();
        std::stringstream ss;
        ss << v1;
        ASSERT_EQ(ss.str(), "9223372036854775807");
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

} // end namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
