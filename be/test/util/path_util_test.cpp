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

#include "util/path_util.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/config.h"
#include "util/logging.h"

using std::string;
using std::vector;

namespace doris {

TEST(TestPathUtil, JoinPathSegments) {
    ASSERT_EQ("a", path_util::join_path_segments("a", ""));
    ASSERT_EQ("b", path_util::join_path_segments("", "b"));
    ASSERT_EQ("a/b", path_util::join_path_segments("a", "b"));
    ASSERT_EQ("a/b", path_util::join_path_segments("a/", "b"));
    ASSERT_EQ("a/b", path_util::join_path_segments("a", "/b"));
    ASSERT_EQ("a/b", path_util::join_path_segments("a/", "/b"));
}

TEST(TestPathUtil, BaseNameTest) {
    ASSERT_EQ(".", path_util::base_name(""));
    ASSERT_EQ(".", path_util::base_name("."));
    ASSERT_EQ("..", path_util::base_name(".."));
    ASSERT_EQ("/", path_util::base_name("/"));
    ASSERT_EQ("/", path_util::base_name("//"));
    ASSERT_EQ("a", path_util::base_name("a"));
    ASSERT_EQ("ab", path_util::base_name("ab"));
    ASSERT_EQ("ab", path_util::base_name("ab/"));
    ASSERT_EQ("cd", path_util::base_name("ab/cd"));
    ASSERT_EQ("ab", path_util::base_name("/ab"));
    ASSERT_EQ("ab", path_util::base_name("/ab///"));
    ASSERT_EQ("cd", path_util::base_name("/ab/cd"));
}

TEST(TestPathUtil, DirNameTest) {
    ASSERT_EQ(".", path_util::dir_name(""));
    ASSERT_EQ(".", path_util::dir_name("."));
    ASSERT_EQ(".", path_util::dir_name(".."));
    ASSERT_EQ("/", path_util::dir_name("/"));
    ASSERT_EQ("//", path_util::dir_name("//"));
    ASSERT_EQ(".", path_util::dir_name("a"));
    ASSERT_EQ(".", path_util::dir_name("ab"));
    ASSERT_EQ(".", path_util::dir_name("ab/"));
    ASSERT_EQ("ab", path_util::dir_name("ab/cd"));
    ASSERT_EQ("/", path_util::dir_name("/ab"));
    ASSERT_EQ("/", path_util::dir_name("/ab///"));
    ASSERT_EQ("/ab", path_util::dir_name("/ab/cd"));
}

TEST(TestPathUtil, SplitPathTest) {
    using Vec = std::vector<string>;
    ASSERT_EQ(Vec({"/"}), path_util::split_path("/"));
    ASSERT_EQ(Vec({"/", "a", "b"}), path_util::split_path("/a/b"));
    ASSERT_EQ(Vec({"/", "a", "b"}), path_util::split_path("/a/b/"));
    ASSERT_EQ(Vec({"/", "a", "b"}), path_util::split_path("/a//b/"));
    ASSERT_EQ(Vec({"a", "b"}), path_util::split_path("a/b"));
    ASSERT_EQ(Vec({"."}), path_util::split_path("."));
    ASSERT_EQ(Vec(), path_util::split_path(""));
}

TEST(TestPathUtil, file_extension_test) {
    ASSERT_EQ("", path_util::file_extension(""));
    ASSERT_EQ("", path_util::file_extension("."));
    ASSERT_EQ("", path_util::file_extension(".."));
    ASSERT_EQ("", path_util::file_extension("/"));
    ASSERT_EQ("", path_util::file_extension("//"));
    ASSERT_EQ("", path_util::file_extension("///"));
    ASSERT_EQ("", path_util::file_extension("a"));
    ASSERT_EQ("", path_util::file_extension("ab"));
    ASSERT_EQ("", path_util::file_extension("ab/"));
    ASSERT_EQ("", path_util::file_extension("ab/cd"));
    ASSERT_EQ("", path_util::file_extension("/ab"));
    ASSERT_EQ("", path_util::file_extension("/ab/"));
    ASSERT_EQ("", path_util::file_extension("///ab///"));
    ASSERT_EQ("", path_util::file_extension("/ab/cd"));
    ASSERT_EQ("", path_util::file_extension("../ab/cd"));

    ASSERT_EQ(".a", path_util::file_extension(".a"));
    ASSERT_EQ("", path_util::file_extension("a.b/c"));
    ASSERT_EQ(".d", path_util::file_extension("a.b/c.d"));
    ASSERT_EQ(".c", path_util::file_extension("a/b.c"));
    ASSERT_EQ(".", path_util::file_extension("a/b."));
    ASSERT_EQ(".c", path_util::file_extension("a.b.c"));
    ASSERT_EQ(".", path_util::file_extension("a.b.c."));
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
