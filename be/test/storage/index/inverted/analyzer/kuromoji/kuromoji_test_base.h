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

#include <gtest/gtest.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <system_error>
#include <utility>

namespace doris::segment_v2::inverted_index::kuromoji {

// Each test owns a fresh directory, independent of previous runs and other users.
class KuromojiTestBase : public ::testing::Test {
protected:
    void SetUp() override {
        std::string dir =
                (std::filesystem::path(::testing::TempDir()) / "kuromoji_XXXXXX").string();
        const auto* created = ::mkdtemp(dir.data());
        const int error = errno;
        ASSERT_NE(created, nullptr) << "cannot create " << dir << ": " << std::strerror(error);
        _dir = std::move(dir);
    }

    void TearDown() override {
        // GTest also calls TearDown when directory creation fails in SetUp.
        if (_dir.empty()) {
            return;
        }
        std::error_code error;
        std::filesystem::remove_all(_dir, error);
        EXPECT_FALSE(error) << "cannot remove " << _dir << ": " << error.message();
    }

    std::string _dir;
};

} // namespace doris::segment_v2::inverted_index::kuromoji
