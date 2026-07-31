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

#include "util/adbc_driver_registry.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "testutil/adbc_sqlite_driver.h"

namespace doris {

namespace {

// Skipping is only correct when thirdparty predates arrow-adbc; it must say so out loud rather
// than surface as an unrelated-looking dlopen failure.
#define SKIP_WITHOUT_SQLITE_DRIVER()                                                              \
    do {                                                                                          \
        if (!adbc_sqlite_driver_available()) {                                                    \
            GTEST_SKIP() << "ADBC SQLite driver not found at " << adbc_sqlite_driver_path()       \
                         << "; run 'cd thirdparty && ./build-thirdparty.sh arrow_adbc' to build " \
                            "it. ADBC coverage is NOT being exercised.";                          \
        }                                                                                         \
    } while (0)

} // namespace

// Loading a real third-party driver is the only way to prove the driver manager is linked in and
// that the entrypoint search works.
TEST(AdbcDriverRegistryTest, LoadsSqliteDriver) {
    SKIP_WITHOUT_SQLITE_DRIVER();
    const AdbcDriver* drv = nullptr;
    Status st = AdbcDriverRegistry::instance().get_or_load(adbc_sqlite_driver_path(), "", &drv);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(drv, nullptr);
    EXPECT_NE(drv->DatabaseNew, nullptr);
}

// The second call must not dlopen again -- that is the whole premise of never dlclosing.
TEST(AdbcDriverRegistryTest, SamePathReturnsCachedInstance) {
    SKIP_WITHOUT_SQLITE_DRIVER();
    const AdbcDriver* a = nullptr;
    const AdbcDriver* b = nullptr;
    ASSERT_TRUE(AdbcDriverRegistry::instance().get_or_load(adbc_sqlite_driver_path(), "", &a).ok());
    const size_t after_first = AdbcDriverRegistry::instance().loaded_count();
    ASSERT_TRUE(AdbcDriverRegistry::instance().get_or_load(adbc_sqlite_driver_path(), "", &b).ok());
    EXPECT_EQ(a, b);
    EXPECT_EQ(after_first, AdbcDriverRegistry::instance().loaded_count());
}

// Without the path in the message the user cannot tell where the driver was expected.
TEST(AdbcDriverRegistryTest, MissingFileFailsWithPathInMessage) {
    const AdbcDriver* drv = nullptr;
    Status st = AdbcDriverRegistry::instance().get_or_load("/nonexistent/libnope.so", "", &drv);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("/nonexistent/libnope.so"), std::string::npos);
}

// Remembering the failure keeps a bad path from retrying dlopen once per scan range.
TEST(AdbcDriverRegistryTest, LoadFailureIsCachedNotRetried) {
    const AdbcDriver* drv = nullptr;
    ASSERT_FALSE(AdbcDriverRegistry::instance().get_or_load("/nonexistent/x.so", "", &drv).ok());
    const size_t after_first = AdbcDriverRegistry::instance().loaded_count();
    ASSERT_FALSE(AdbcDriverRegistry::instance().get_or_load("/nonexistent/x.so", "", &drv).ok());
    EXPECT_EQ(after_first, AdbcDriverRegistry::instance().loaded_count());
}

// An empty path would otherwise reach the driver manager and come back as a confusing dlopen error.
TEST(AdbcDriverRegistryTest, EmptyPathIsRejectedWithoutCaching) {
    const AdbcDriver* drv = nullptr;
    const size_t before = AdbcDriverRegistry::instance().loaded_count();
    EXPECT_FALSE(AdbcDriverRegistry::instance().get_or_load("", "", &drv).ok());
    EXPECT_EQ(before, AdbcDriverRegistry::instance().loaded_count());
}

} // namespace doris
