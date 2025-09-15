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

#include <cstdlib>
#include <filesystem>
#include <fstream>
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

// Helpers for plugin URL tests
namespace {

std::string create_temp_home() {
    namespace fs = std::filesystem;
    fs::path base = fs::temp_directory_path() / "doris_path_util_test";
    fs::create_directories(base);
    // ensure unique subdir per test run
    fs::path home = base / std::to_string(reinterpret_cast<uintptr_t>(&base));
    fs::create_directories(home);
    // set DORIS_HOME
#ifdef __APPLE__
    setenv("DORIS_HOME", home.string().c_str(), 1);
#else
    setenv("DORIS_HOME", home.string().c_str(), 1);
#endif
    return home.string();
}

void touch_file(const std::string& dir, const std::string& filename) {
    namespace fs = std::filesystem;
    fs::create_directories(dir);
    std::ofstream ofs(fs::path(dir) / filename, std::ios::binary);
    ofs << "x";
}

} // namespace

TEST(TestPathUtil, get_real_plugin_url_absolute_passthrough) {
    std::string _home = create_temp_home();
    // absolute style URLs containing ":/" should be returned as-is
    EXPECT_EQ(
            "http://example.com/a.jar",
            path_util::get_real_plugin_url("http://example.com/a.jar", "/any/dir", "jdbc_drivers"));
    EXPECT_EQ(
            "file:///opt/driver/a.jar",
            path_util::get_real_plugin_url("file:///opt/driver/a.jar", "/any/dir", "jdbc_drivers"));
}

TEST(TestPathUtil, check_and_return_default_plugin_url_prefers_new_default_when_exists) {
    namespace fs = std::filesystem;
    std::string home = create_temp_home();
    std::string plugin_name = "jdbc_drivers";
    std::string default_new = home + "/plugins/" + plugin_name;
    std::string default_old = home + "/" + plugin_name;
    std::string fname = "drv.jar";

    touch_file(default_new, fname);

    std::string expected = "file://" + default_new + "/" + fname;
    EXPECT_EQ(expected,
              path_util::check_and_return_default_plugin_url(fname, default_new, plugin_name));
}

TEST(TestPathUtil, check_and_return_default_plugin_url_falls_back_to_old_default) {
    std::string home = create_temp_home();
    std::string plugin_name = "jdbc_drivers";
    std::string default_new = home + "/plugins/" + plugin_name;
    std::string default_old = home + "/" + plugin_name;
    std::string fname = "drv.jar";

    // create only old default file
    touch_file(default_old, fname);

    std::string expected = "file://" + default_old + "/" + fname;
    EXPECT_EQ(expected,
              path_util::check_and_return_default_plugin_url(fname, default_new, plugin_name));
}

TEST(TestPathUtil, check_and_return_default_plugin_url_old_even_if_missing) {
    std::string home = create_temp_home();
    std::string plugin_name = "jdbc_drivers";
    std::string default_new = home + "/plugins/" + plugin_name;
    std::string default_old = home + "/" + plugin_name;
    std::string fname = "drv.jar";

    // neither new nor old has the file; should still point to old default
    std::string expected = "file://" + default_old + "/" + fname;
    EXPECT_EQ(expected,
              path_util::check_and_return_default_plugin_url(fname, default_new, plugin_name));
}

TEST(TestPathUtil, check_and_return_default_plugin_url_custom_config_dir) {
    std::string home = create_temp_home();
    std::string plugin_name = "jdbc_drivers";
    std::string custom_dir = home + "/custom/plugins";
    std::string fname = "drv.jar";
    touch_file(custom_dir, fname);

    std::string expected = "file://" + custom_dir + "/" + fname;
    EXPECT_EQ(expected,
              path_util::check_and_return_default_plugin_url(fname, custom_dir, plugin_name));
}

TEST(TestPathUtil, get_real_plugin_url_relative_paths) {
    std::string home = create_temp_home();
    std::string plugin_name = "jdbc_drivers";
    std::string default_new = home + "/plugins/" + plugin_name;
    std::string default_old = home + "/" + plugin_name;
    std::string fname = "drv.jar";

    // When new default exists
    touch_file(default_new, fname);
    std::string expected_new = "file://" + default_new + "/" + fname;
    EXPECT_EQ(expected_new, path_util::get_real_plugin_url(fname, default_new, plugin_name));

    // When only old default exists
    std::string home2 = create_temp_home();
    std::string default_new2 = home2 + "/plugins/" + plugin_name;
    std::string default_old2 = home2 + "/" + plugin_name;
    touch_file(default_old2, fname);
    std::string expected_old = "file://" + default_old2 + "/" + fname;
    EXPECT_EQ(expected_old, path_util::get_real_plugin_url(fname, default_new2, plugin_name));

    // When using a custom configured dir (not equal to default new path)
    std::string custom_dir = home + "/custom";
    touch_file(custom_dir, fname);
    std::string expected_custom = "file://" + custom_dir + "/" + fname;
    EXPECT_EQ(expected_custom, path_util::get_real_plugin_url(fname, custom_dir, plugin_name));
}

} // namespace doris
