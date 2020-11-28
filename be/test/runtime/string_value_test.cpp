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

#include "runtime/string_value.hpp"

#include <gtest/gtest.h>

#include <string>

#include "util/cpu_info.h"

using std::string;

namespace doris {

StringValue FromStdString(const string& str) {
    char* ptr = const_cast<char*>(str.c_str());
    int len = str.size();
    return StringValue(ptr, len);
}

TEST(StringValueTest, TestCompare) {
    std::string empty_str = "";
    std::string str1_str = "abc";
    std::string str2_str = "abcdef";
    std::string str3_str = "xyz";

    const int NUM_STRINGS = 4;

    // Must be in lexical order
    StringValue svs[NUM_STRINGS];
    svs[0] = FromStdString(empty_str);
    svs[1] = FromStdString(str1_str);
    svs[2] = FromStdString(str2_str);
    svs[3] = FromStdString(str3_str);

    for (int i = 0; i < NUM_STRINGS; ++i) {
        for (int j = 0; j < NUM_STRINGS; ++j) {
            if (i == j) {
                // Same string
                EXPECT_TRUE(svs[i].eq(svs[j]));
                EXPECT_FALSE(svs[i].ne(svs[j]));
                EXPECT_FALSE(svs[i].lt(svs[j]));
                EXPECT_FALSE(svs[i].gt(svs[j]));
                EXPECT_TRUE(svs[i].le(svs[j]));
                EXPECT_TRUE(svs[i].ge(svs[j]));
                EXPECT_TRUE(svs[i].compare(svs[j]) == 0);
            } else if (i < j) {
                // svs[i] < svs[j]
                EXPECT_FALSE(svs[i].eq(svs[j]));
                EXPECT_TRUE(svs[i].ne(svs[j]));
                EXPECT_TRUE(svs[i].lt(svs[j]));
                EXPECT_FALSE(svs[i].gt(svs[j]));
                EXPECT_TRUE(svs[i].le(svs[j]));
                EXPECT_FALSE(svs[i].gt(svs[j]));
                EXPECT_TRUE(svs[i].compare(svs[j]) < 0);
            } else {
                // svs[i] > svs[j]
                EXPECT_FALSE(svs[i].eq(svs[j]));
                EXPECT_TRUE(svs[i].ne(svs[j]));
                EXPECT_FALSE(svs[i].lt(svs[j]));
                EXPECT_TRUE(svs[i].gt(svs[j]));
                EXPECT_FALSE(svs[i].le(svs[j]));
                EXPECT_TRUE(svs[i].gt(svs[j]));
                EXPECT_TRUE(svs[i].compare(svs[j]) > 0);
            }
        }
    }
}

} // namespace doris

int main(int argc, char** argv) {
#if 0
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    doris::CpuInfo::Init();
#endif
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
