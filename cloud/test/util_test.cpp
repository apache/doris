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

#include <string>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "common/string_util.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

int main(int argc, char** argv) {
    doris::cloud::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(StringUtilTest, test_string_strip) {
    // clang-format off
    //                       str         expect          to_drop
    std::vector<std::tuple<std::string, std::string, std::string>> leading_inputs {
        {""       , ""      , ""    },
        {""       , ""      , "/"   },
        {"/"      , ""      , "/"   },
        {"\t////" , ""      , "/ \t"},
        {"/a///"  , "a///"  , "/"   },
        {"/a/b/c/", "a/b/c/", "/"   },
        {"a/b/c/" , "a/b/c/", "/"   },
        {"a/b/c/" , "/b/c/" , "a"   },
    };
    int idx = 0;
    for (auto&& i : leading_inputs) {
        doris::cloud::strip_leading(std::get<0>(i), std::get<2>(i));
        EXPECT_EQ(std::get<0>(i), std::get<1>(i)) << " index=" << idx;
        ++idx;
    }

    idx = 0;
    std::vector<std::tuple<std::string, std::string, std::string>> trailing_inputs {
        {""       , ""      , ""    },
        {"/"      , ""      , "/"   },
        {"////\t" , ""      , "/ \t"},
        {"/a///"  , "/a"    ,  "/"  },
        {"/a/b/c/", "/a/b/c", "/"   },
        {"a/b/c/" , "a/b/c" , "/"   },
        {"a/b/c"  , "a/b/c" , "/"   },
        {"a/b/c"  , "a/b/"  , "c"   },
    };
    for (auto&& i : trailing_inputs) {
        doris::cloud::strip_trailing(std::get<0>(i), std::get<2>(i));
        EXPECT_EQ(std::get<0>(i), std::get<1>(i)) << " index=" << idx;
        ++idx;
    }

    idx = 0;
    std::vector<std::tuple<std::string, std::string>> trim_inputs {
        {""              , ""     },
        {""              , ""     },
        {"/"             , ""     },
        {"\t////"        , ""     },
        {"/a ///"        , "a"   },
        {"/a/b/c/"       , "a/b/c"},
        {"a/b/c/"        , "a/b/c"},
        {"a/b/c"         , "a/b/c"},
        {"\t/bbc///"     , "bbc"  },
        {"ab c"          , "ab c" },
        {"\t  /a/b/c \t/", "a/b/c"},
    };
    for (auto&& i : trim_inputs) {
        doris::cloud::trim(std::get<0>(i));
        EXPECT_EQ(std::get<0>(i), std::get<1>(i)) << " index=" << idx;
        ++idx;
    }

    // clang-format on
}
