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

extern "C" void* doris_memalign(size_t alignment, size_t size) noexcept;

namespace doris {

class JemallocHookTest : public testing::Test {
protected:
    using MemalignFunction = void* (*)(size_t, size_t);

    void SetUp() override {
        ASSERT_NO_FATAL_FAILURE(check_executable_interposition());
        ASSERT_NO_FATAL_FAILURE(check_legacy_allocation_pair());
    }

    void check_executable_interposition() {
        // Anchor the lookup to the linked production hook. A libc allocation
        // paired with a libc free could otherwise pass without testing the hook.
        Dl_info hook_image = {};
        ASSERT_NE(0, dladdr(reinterpret_cast<void*>(&doris_memalign), &hook_image));
        constexpr std::array<const char*, 3> symbol_names = {"__libc_memalign", "malloc", "free"};
        std::array<void*, 3> symbols = {};
        for (size_t i = 0; i < symbol_names.size(); ++i) {
            symbols[i] = dlsym(RTLD_DEFAULT, symbol_names[i]);
            ASSERT_NE(nullptr, symbols[i]) << symbol_names[i] << ": " << dlerror();
            Dl_info symbol_image = {};
            ASSERT_NE(0, dladdr(symbols[i], &symbol_image)) << symbol_names[i];
            ASSERT_EQ(hook_image.dli_fbase, symbol_image.dli_fbase)
                    << symbol_names[i] << " resolved to " << symbol_image.dli_fname
                    << " instead of " << hook_image.dli_fname;
        }
        _memalign = reinterpret_cast<MemalignFunction>(symbols.front());
    }

    void check_legacy_allocation_pair() {
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
    using GetAllocatorSymbols = void (*)(void**, void**);
    auto get_allocator_symbols = reinterpret_cast<GetAllocatorSymbols>(
            dlsym(handle.get(), "jemalloc_hook_test_allocator_symbols"));
    ASSERT_NE(nullptr, get_allocator_symbols) << dlerror();
    void* memalign_symbol = nullptr;
    void* free_symbol = nullptr;
    get_allocator_symbols(&memalign_symbol, &free_symbol);
    // Check the DSO's actual relocations before exercising dynamic TLS.
    ASSERT_EQ(reinterpret_cast<void*>(_memalign), memalign_symbol);
    ASSERT_EQ(reinterpret_cast<void*>(&std::free), free_symbol);

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
