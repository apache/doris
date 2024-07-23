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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"

using std::string;
using std::vector;

namespace doris {

TEST(TestPathUtil, JoinPathSegments) {
    EXPECT_EQ("a", path_util::join_path_segments("a", ""));
    EXPECT_EQ("b", path_util::join_path_segments("", "b"));
    EXPECT_EQ("a/b", path_util::join_path_segments("a", "b"));
    EXPECT_EQ("a/b", path_util::join_path_segments("a/", "b"));
    EXPECT_EQ("a/b", path_util::join_path_segments("a", "/b"));
    EXPECT_EQ("a/b", path_util::join_path_segments("a/", "/b"));
}

TEST(TestPathUtil, BaseNameTest) {
    EXPECT_EQ(".", path_util::base_name(""));
    EXPECT_EQ(".", path_util::base_name("."));
    EXPECT_EQ("..", path_util::base_name(".."));
    EXPECT_EQ("/", path_util::base_name("/"));
    EXPECT_EQ("/", path_util::base_name("//"));
    EXPECT_EQ("a", path_util::base_name("a"));
    EXPECT_EQ("ab", path_util::base_name("ab"));
    EXPECT_EQ("ab", path_util::base_name("ab/"));
    EXPECT_EQ("cd", path_util::base_name("ab/cd"));
    EXPECT_EQ("ab", path_util::base_name("/ab"));
    EXPECT_EQ("ab", path_util::base_name("/ab///"));
    EXPECT_EQ("cd", path_util::base_name("/ab/cd"));
}

TEST(TestPathUtil, DirNameTest) {
    EXPECT_EQ(".", path_util::dir_name(""));
    EXPECT_EQ(".", path_util::dir_name("."));
    EXPECT_EQ(".", path_util::dir_name(".."));
    EXPECT_EQ("/", path_util::dir_name("/"));
#ifndef __APPLE__
    EXPECT_EQ("//", path_util::dir_name("//"));
#else
    EXPECT_EQ("/", path_util::dir_name("//"));
#endif
    EXPECT_EQ(".", path_util::dir_name("a"));
    EXPECT_EQ(".", path_util::dir_name("ab"));
    EXPECT_EQ(".", path_util::dir_name("ab/"));
    EXPECT_EQ("ab", path_util::dir_name("ab/cd"));
    EXPECT_EQ("/", path_util::dir_name("/ab"));
    EXPECT_EQ("/", path_util::dir_name("/ab///"));
    EXPECT_EQ("/ab", path_util::dir_name("/ab/cd"));
}

TEST(TestPathUtil, file_extension_test) {
    EXPECT_EQ("", path_util::file_extension(""));
    EXPECT_EQ("", path_util::file_extension("."));
    EXPECT_EQ("", path_util::file_extension(".."));
    EXPECT_EQ("", path_util::file_extension("/"));
    EXPECT_EQ("", path_util::file_extension("//"));
    EXPECT_EQ("", path_util::file_extension("///"));
    EXPECT_EQ("", path_util::file_extension("a"));
    EXPECT_EQ("", path_util::file_extension("ab"));
    EXPECT_EQ("", path_util::file_extension("ab/"));
    EXPECT_EQ("", path_util::file_extension("ab/cd"));
    EXPECT_EQ("", path_util::file_extension("/ab"));
    EXPECT_EQ("", path_util::file_extension("/ab/"));
    EXPECT_EQ("", path_util::file_extension("///ab///"));
    EXPECT_EQ("", path_util::file_extension("/ab/cd"));
    EXPECT_EQ("", path_util::file_extension("../ab/cd"));

    EXPECT_EQ(".a", path_util::file_extension(".a"));
    EXPECT_EQ("", path_util::file_extension("a.b/c"));
    EXPECT_EQ(".d", path_util::file_extension("a.b/c.d"));
    EXPECT_EQ(".c", path_util::file_extension("a/b.c"));
    EXPECT_EQ(".", path_util::file_extension("a/b."));
    EXPECT_EQ(".c", path_util::file_extension("a.b.c"));
    EXPECT_EQ(".", path_util::file_extension("a.b.c."));
}

} // namespace doris
