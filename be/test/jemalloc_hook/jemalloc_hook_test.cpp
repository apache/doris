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

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <thread>

namespace doris {

class JemallocHookTest : public testing::Test {
protected:
    using MemalignFunction = void* (*)(size_t, size_t);

    void SetUp() override {
        void* symbol = dlsym(RTLD_DEFAULT, "__libc_memalign");
        ASSERT_NE(nullptr, symbol) << dlerror();
        _memalign = reinterpret_cast<MemalignFunction>(symbol);
        // Exercise the legacy allocation/free pair even on newer glibc versions
        // whose loader no longer uses __libc_memalign for TLS. A subprocess turns
        // an allocator-mismatch crash into a test failure instead of aborting UT.
        ASSERT_EXIT(
                {
                    void* ptr = _memalign(16, 32);
                    const int result = (ptr == nullptr);
                    std::free(ptr);
                    std::_Exit(result);
                },
                testing::ExitedWithCode(0), "");
    }

    MemalignFunction _memalign = nullptr;
};

TEST_F(JemallocHookTest, AlignedAllocationsCanBeFreed) {
    for (size_t alignment : {1, 8, 16, 64, 4096}) {
        for (size_t size : {0, 1, 17, 4097}) {
            void* ptr = _memalign(alignment, size);
            ASSERT_NE(nullptr, ptr);
            EXPECT_EQ(0, reinterpret_cast<uintptr_t>(ptr) % alignment);
            std::memset(ptr, 0x5a, size);
            std::free(ptr);
        }
    }
}

TEST_F(JemallocHookTest, AllocationErrorsPreserveMemalignSemantics) {
    errno = 0;
    void* ptr = _memalign(3, 16);
    EXPECT_EQ(nullptr, ptr);
    EXPECT_EQ(EINVAL, errno);
    std::free(ptr);

    errno = 0;
    ptr = _memalign(64, std::numeric_limits<size_t>::max());
    EXPECT_EQ(nullptr, ptr);
    EXPECT_EQ(ENOMEM, errno);
    std::free(ptr);
}

TEST_F(JemallocHookTest, AllocationsCanBeFreedAfterAllocatingThreadsExit) {
    std::array<void*, 64> pointers = {};
    std::array<std::thread, 64> threads;
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([&, i] { pointers[i] = _memalign(64, 1031); });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (void* ptr : pointers) {
        ASSERT_NE(nullptr, ptr);
        std::memset(ptr, 0x5a, 1031);
        std::free(ptr);
    }
}

TEST_F(JemallocHookTest, LateLoadedDynamicTlsCanBeFreedAfterThreadsExit) {
    std::unique_ptr<void, decltype(&dlclose)> handle(
            dlopen(JEMALLOC_HOOK_TEST_DSO, RTLD_NOW | RTLD_LOCAL), &dlclose);
    ASSERT_NE(nullptr, handle) << dlerror();
    using TouchTlsFunction = int (*)();
    auto touch_tls =
            reinterpret_cast<TouchTlsFunction>(dlsym(handle.get(), "jemalloc_hook_test_touch_tls"));
    ASSERT_NE(nullptr, touch_tls) << dlerror();

    // Exceed glibc's default stack cache to exercise TLS deallocation instead
    // of only keeping exited threads' stacks cached.
    for (int round = 0; round < 8; ++round) {
        std::array<int, 64> results = {};
        std::array<std::thread, 64> threads;
        for (size_t i = 0; i < threads.size(); ++i) {
            threads[i] = std::thread([&, i] {
                for (int call = 0; call < 8; ++call) {
                    results[i] = touch_tls();
                }
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        for (int result : results) {
            EXPECT_EQ(8, result);
        }
    }
}

} // namespace doris
