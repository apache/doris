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

#include "snii/writer/temp_dir.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <string>

#include "common/status.h"

namespace snii::writer {
namespace {

// Saves an env var on construction and restores it on destruction, so these tests
// never leak SNII_TEMP_DIR / TMPDIR into the rest of the suite (which would redirect
// other tests' spill/section temp files to a possibly non-existent directory).
struct EnvGuard {
    std::string name;
    bool had = false;
    std::string old;
    explicit EnvGuard(const char* n) : name(n) {
        const char* v = std::getenv(n);
        had = (v != nullptr);
        if (had) {
            old = v;
        }
    }
    ~EnvGuard() {
        if (had) {
            ::setenv(name.c_str(), old.c_str(), 1);
        } else {
            ::unsetenv(name.c_str());
        }
    }
};

TEST(SniiTempDir, SniiTempDirTakesPrecedenceOverTmpdir) {
    EnvGuard g1("SNII_TEMP_DIR");
    EnvGuard g2("TMPDIR");
    ::setenv("SNII_TEMP_DIR", "/mnt/nvme/scratch", 1);
    ::setenv("TMPDIR", "/var/tmp", 1);
    EXPECT_EQ(resolve_temp_dir(), "/mnt/nvme/scratch");
}

TEST(SniiTempDir, FallsBackTmpdirThenTmp) {
    EnvGuard g1("SNII_TEMP_DIR");
    EnvGuard g2("TMPDIR");
    ::unsetenv("SNII_TEMP_DIR");
    ::setenv("TMPDIR", "/var/tmp/", 1); // trailing slash stripped
    EXPECT_EQ(resolve_temp_dir(), "/var/tmp");
    ::unsetenv("TMPDIR");
    EXPECT_EQ(resolve_temp_dir(), "/tmp");
}

TEST(SniiTempDir, EmptyEnvIsIgnored) {
    EnvGuard g1("SNII_TEMP_DIR");
    EnvGuard g2("TMPDIR");
    ::setenv("SNII_TEMP_DIR", "", 1); // empty -> ignored, falls through
    ::unsetenv("TMPDIR");
    EXPECT_EQ(resolve_temp_dir(), "/tmp");
}

TEST(SniiTempDir, AvailableBytesStatsRealDirAndSentinelsOnFailure) {
    EXPECT_NE(temp_dir_available_bytes("/tmp"), UINT64_MAX); // real dir -> some free
    EXPECT_EQ(temp_dir_available_bytes("/snii_no_such_dir_xyzzy"), UINT64_MAX);
}

} // namespace
} // namespace snii::writer
