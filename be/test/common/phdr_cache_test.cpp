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

#if defined(__linux__) && !defined(THREAD_SANITIZER) && !defined(USE_MUSL)

#include "common/phdr_cache.h"

#include <dlfcn.h>
#include <link.h>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>

namespace {

constexpr std::string_view TEST_DSO_NAME = "libphdr_cache_test_dso";

int find_test_dso(dl_phdr_info* info, size_t /*size*/, void* data) {
    auto* found = reinterpret_cast<bool*>(data);
    if (info->dlpi_name != nullptr &&
        std::string_view(info->dlpi_name).find(TEST_DSO_NAME) != std::string_view::npos) {
        *found = true;
        return 1;
    }
    return 0;
}

bool phdr_contains_test_dso() {
    bool found = false;
    dl_iterate_phdr(find_test_dso, &found);
    return found;
}

bool unwind_phdr_cache_contains_test_dso(uintptr_t ip) {
    bool found = false;
    doris_unwind_iterate_phdr(find_test_dso, &found, ip);
    return found;
}

std::string test_dso_path() {
    const char* doris_home = std::getenv("DORIS_HOME");
    std::filesystem::path path = doris_home == nullptr ? "." : doris_home;
    path /= "libphdr_cache_test_dso.so";
    return path.string();
}

} // namespace

// Covers the exact late-dlopen risk of PHDR caching. Normal callers of dl_iterate_phdr must keep
// seeing the live loader list, while the stack-trace signal handler can explicitly opt in to the
// cached snapshot to avoid re-entering glibc's loader lock from an interrupted thread.
TEST(PhdrCacheTest, DefaultLoaderViewIsLiveWhileScopedViewUsesSnapshot) {
    updatePHDRCache();
    ASSERT_TRUE(hasPHDRCache());
    ASSERT_FALSE(phdr_contains_test_dso()) << "test DSO was loaded before the snapshot";

    void* handle = dlopen(test_dso_path().c_str(), RTLD_NOW | RTLD_LOCAL);
    ASSERT_NE(nullptr, handle) << dlerror();
    using MarkerFunction = int (*)();
    auto* marker = reinterpret_cast<MarkerFunction>(dlsym(handle, "phdr_cache_test_dso_marker"));
    ASSERT_NE(nullptr, marker) << dlerror();
    // Keep the handle open. Several sanitizer configurations are known to be fragile around
    // dlclose(), and this test only needs a late-loaded object in the process loader list.
    (void)handle;

    EXPECT_TRUE(phdr_contains_test_dso())
            << "default dl_iterate_phdr must use glibc's live loader list";

    {
        ScopedPHDRCacheRead cache_scope;
        EXPECT_FALSE(phdr_contains_test_dso())
                << "scoped PHDR cache should read the pre-dlopen snapshot";
    }
    EXPECT_FALSE(unwind_phdr_cache_contains_test_dso(reinterpret_cast<uintptr_t>(marker)))
            << "libunwind PHDR hook should also read the pre-dlopen snapshot";

    updatePHDRCache();
    EXPECT_TRUE(unwind_phdr_cache_contains_test_dso(reinterpret_cast<uintptr_t>(marker)))
            << "libunwind PHDR hook should use the refreshed snapshot without scoped opt-in";
    {
        ScopedPHDRCacheRead cache_scope;
        EXPECT_TRUE(phdr_contains_test_dso())
                << "refreshed scoped PHDR cache should include the late-loaded DSO";
    }
}

#endif
