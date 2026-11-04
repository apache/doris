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
#include <malloc.h>
#include <unistd.h>

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
    using AllocationFunction = void* (*)(size_t);
    using CallocFunction = void* (*)(size_t, size_t);
    using FreeFunction = void (*)(void*);
    using ReallocationFunction = void* (*)(void*, size_t);
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
        constexpr std::array<const char*, 9> symbol_names = {
                "__libc_memalign", "malloc",        "free",
                "__libc_malloc",   "__libc_free",   "__libc_calloc",
                "__libc_realloc",  "__libc_valloc", "__libc_pvalloc"};
        for (size_t i = 0; i < symbol_names.size(); ++i) {
            _allocator_symbols[i] = dlsym(RTLD_DEFAULT, symbol_names[i]);
            ASSERT_NE(nullptr, _allocator_symbols[i]) << symbol_names[i] << ": " << dlerror();
            Dl_info symbol_image = {};
            ASSERT_NE(0, dladdr(_allocator_symbols[i], &symbol_image)) << symbol_names[i];
            ASSERT_EQ(hook_image.dli_fbase, symbol_image.dli_fbase)
                    << symbol_names[i] << " resolved to " << symbol_image.dli_fname
                    << " instead of " << hook_image.dli_fname;
        }
        _memalign = reinterpret_cast<MemalignFunction>(_allocator_symbols[0]);
        _libc_malloc = reinterpret_cast<AllocationFunction>(_allocator_symbols[3]);
        _libc_free = reinterpret_cast<FreeFunction>(_allocator_symbols[4]);
        _libc_calloc = reinterpret_cast<CallocFunction>(_allocator_symbols[5]);
        _libc_realloc = reinterpret_cast<ReallocationFunction>(_allocator_symbols[6]);
        _libc_valloc = reinterpret_cast<AllocationFunction>(_allocator_symbols[7]);
        _libc_pvalloc = reinterpret_cast<AllocationFunction>(_allocator_symbols[8]);
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

    std::array<void*, 9> _allocator_symbols = {};
    MemalignFunction _memalign = nullptr;
    AllocationFunction _libc_malloc = nullptr;
    FreeFunction _libc_free = nullptr;
    CallocFunction _libc_calloc = nullptr;
    ReallocationFunction _libc_realloc = nullptr;
    AllocationFunction _libc_valloc = nullptr;
    AllocationFunction _libc_pvalloc = nullptr;
};

TEST_F(JemallocHookTest, LibcAllocationsCanBeFreedThroughPublicEntry) {
    for (size_t size : {0, 17, 4097}) {
        void* ptr = _libc_malloc(size);
        ASSERT_NE(nullptr, ptr);
        std::memset(ptr, 0x5a, size);
        std::free(ptr);
    }
}

TEST_F(JemallocHookTest, PublicAllocationsCanBeFreedThroughLibcEntry) {
    _libc_free(nullptr);
    for (size_t size : {0, 17, 4097}) {
        std::unique_ptr<void, decltype(&std::free)> ptr(std::malloc(size), &std::free);
        ASSERT_NE(nullptr, ptr.get());
        std::memset(ptr.get(), 0x5a, size);
        errno = EDOM;
        _libc_free(ptr.release());
        const int free_errno = errno;
        EXPECT_EQ(EDOM, free_errno);
    }
}

TEST_F(JemallocHookTest, LibcCallocInitializesMemoryAndRejectsOverflow) {
    for (size_t count : {0, 17}) {
        constexpr size_t element_size = 13;
        void* ptr = _libc_calloc(count, element_size);
        ASSERT_NE(nullptr, ptr);
        for (size_t i = 0; i < count * element_size; ++i) {
            EXPECT_EQ(0, static_cast<unsigned char*>(ptr)[i]);
        }
        std::free(ptr);
    }

    errno = 0;
    EXPECT_EQ(nullptr, _libc_calloc(std::numeric_limits<size_t>::max(), 2));
    EXPECT_EQ(ENOMEM, errno);
}

TEST_F(JemallocHookTest, ReallocCanCrossLibcAndPublicEntryPoints) {
    struct AllocationPair {
        AllocationFunction allocate;
        ReallocationFunction reallocate;
        FreeFunction release;
    };
    const std::array<AllocationPair, 3> pairs = {{{&std::malloc, _libc_realloc, &std::free},
                                                  {_libc_malloc, &std::realloc, _libc_free},
                                                  {_libc_malloc, _libc_realloc, &std::free}}};
    for (const auto& pair : pairs) {
        void* ptr = pair.allocate(17);
        ASSERT_NE(nullptr, ptr);
        std::memset(ptr, 0x5a, 17);
        void* resized = pair.reallocate(ptr, 1031);
        ASSERT_NE(nullptr, resized);
        for (size_t i = 0; i < 17; ++i) {
            EXPECT_EQ(0x5a, static_cast<unsigned char*>(resized)[i]);
        }
        std::memset(resized, 0x5a, 1031);
        pair.release(resized);
    }
}

TEST_F(JemallocHookTest, LibcReallocFailurePreservesTheOriginalAllocation) {
    void* ptr = _libc_realloc(nullptr, 17);
    ASSERT_NE(nullptr, ptr);
    std::memset(ptr, 0x5a, 17);

    errno = 0;
    void* resized = _libc_realloc(ptr, std::numeric_limits<size_t>::max());
    const int allocation_errno = errno;
    ASSERT_EQ(nullptr, resized);
    EXPECT_EQ(ENOMEM, allocation_errno);
    for (size_t i = 0; i < 17; ++i) {
        EXPECT_EQ(0x5a, static_cast<unsigned char*>(ptr)[i]);
    }
    std::free(ptr);
}

TEST_F(JemallocHookTest, LibcReallocZeroSizeIsNotReportedAsOutOfMemory) {
    std::unique_ptr<void, decltype(&std::free)> ptr(std::malloc(17), &std::free);
    ASSERT_NE(nullptr, ptr.get());
    errno = 0;
    void* resized = _libc_realloc(ptr.release(), 0);
    const int allocation_errno = errno;
    EXPECT_NE(ENOMEM, allocation_errno);
    std::free(resized);
}

TEST_F(JemallocHookTest, LibcPageAlignedAllocationsCanBeFreed) {
    const long page_size = sysconf(_SC_PAGESIZE);
    ASSERT_GT(page_size, 0);
    for (auto allocate : {_libc_valloc, _libc_pvalloc}) {
        for (size_t size : {size_t {0}, size_t {1}, static_cast<size_t>(page_size) + 1}) {
            void* ptr = allocate(size);
            ASSERT_NE(nullptr, ptr);
            EXPECT_EQ(0, reinterpret_cast<uintptr_t>(ptr) % page_size) << "size: " << size;
            std::memset(ptr, 0x5a, size);
            std::free(ptr);
        }
        errno = 0;
        void* ptr = allocate(std::numeric_limits<size_t>::max());
        const int allocation_errno = errno;
        EXPECT_EQ(nullptr, ptr);
        EXPECT_EQ(ENOMEM, allocation_errno);
    }
}

TEST_F(JemallocHookTest, LibcPvallocRoundsAllocationSizeToPages) {
    const size_t page_size = getpagesize();
    for (size_t size : {size_t {0}, size_t {1}, page_size + 1}) {
        void* ptr = _libc_pvalloc(size);
        ASSERT_NE(nullptr, ptr);
        const size_t rounded_size = (size + page_size - 1) / page_size * page_size;
        ASSERT_GE(malloc_usable_size(ptr), rounded_size);
        std::memset(ptr, 0x5a, rounded_size);
        std::free(ptr);
    }
}

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
    using GetAllocatorSymbols = void (*)(void**);
    auto get_allocator_symbols = reinterpret_cast<GetAllocatorSymbols>(
            dlsym(handle.get(), "jemalloc_hook_test_allocator_symbols"));
    ASSERT_NE(nullptr, get_allocator_symbols) << dlerror();
    std::array<void*, 9> allocator_symbols = {};
    get_allocator_symbols(allocator_symbols.data());
    // Check the DSO's actual relocations for every glibc allocation entry point
    // before exercising dynamic TLS.
    ASSERT_EQ(_allocator_symbols, allocator_symbols);

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
