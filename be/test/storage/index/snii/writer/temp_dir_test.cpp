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

#include "storage/index/snii/writer/temp_dir.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

namespace doris::snii::writer {
namespace {

// ExecEnv's tmp_file_dirs are initialized by the global SniiTmpDirEnvironment
// (be/test/storage/index/snii/snii_test_env.cpp).
TEST(SniiTempDir, ResolvesIntoSniiSubdirOfConfiguredTmpDir) {
    const std::string dir = resolve_temp_dir();
    ASSERT_FALSE(dir.empty());
    // SNII gets its own "snii" subdirectory so it does not crowd the tmp root.
    ASSERT_GE(dir.size(), 5U);
    EXPECT_EQ(0, dir.compare(dir.size() - 5, 5, "/snii")) << dir;
    // resolve_temp_dir() creates the directory, so it stats successfully.
    EXPECT_NE(temp_dir_available_bytes(dir), UINT64_MAX) << dir;
}

TEST(SniiTempDir, AvailableBytesStatsRealDirAndSentinelsOnFailure) {
    EXPECT_NE(temp_dir_available_bytes("/tmp"), UINT64_MAX); // real dir -> some free
    EXPECT_EQ(temp_dir_available_bytes("/snii_no_such_dir_xyzzy"), UINT64_MAX);
}

} // namespace
} // namespace doris::snii::writer
