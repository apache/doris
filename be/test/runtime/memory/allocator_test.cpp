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

#include "core/allocator.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>

#include "common/exception.h"
#include "core/allocator_fwd.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/resource_context.h"

namespace doris {

template <typename T>
void test_allocator(T allocator) {
    auto ptr = allocator.alloc(4096);
    EXPECT_NE(nullptr, ptr);
    ptr = allocator.realloc(ptr, 4096, 4096 * 1024);
    EXPECT_NE(nullptr, ptr);
    allocator.free(ptr, 4096 * 1024);

    ptr = allocator.alloc(100);
    EXPECT_NE(nullptr, ptr);
    ptr = allocator.realloc(ptr, 100, 100 * 1024);
    EXPECT_NE(nullptr, ptr);
    allocator.free(ptr, 100 * 1024);
}

void test_normal() {
    {
        test_allocator(Allocator<false, false, false>());
        test_allocator(Allocator<false, false, true>());
        test_allocator(Allocator<false, true, false>());
        test_allocator(Allocator<false, true, true>());
        test_allocator(Allocator<true, false, false>());
        test_allocator(Allocator<true, false, true>());
        test_allocator(Allocator<true, true, false>());
        test_allocator(Allocator<true, true, true>());
    }
}

TEST(AllocatorTest, TestNormal) {
    test_normal();
}

// Guards realloc's tracker accounting: growing a buffer must charge only the
// delta (new-old) to the current MemTracker, never new+old with a transient
// double-count of the old region. A regression here surfaces as spurious
// MEM_LIMIT_EXCEEDED when a hash table doubles at the memory-limit boundary
// in queries without spill fallback.
//
// Coverage note: these cases exercise the malloc realloc path (use_mmap=false)
// and the big-alloc copy path (old<threshold<new). The mremap path
// (old_size and new_size both >= mmap_threshold, default 128 MiB) is not
// covered on purpose: forcing it needs a >=128 MiB allocation per case, which
// is uneconomical on CI. Its accounting is byte-for-byte identical to the
// malloc grow/shrink branch already tested here (grow ? consume(delta) :
// release(delta)); the only branch-specific difference is whether the kernel
// resizes via mremap or realloc, which is libc/kernel behavior, not this fix's
// accounting logic.
class AllocatorTrackerTest : public ::testing::Test {
protected:
    void SetUp() override {
        _tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER,
                                                    "UT-AllocatorTrackerTest");
        _rc = ResourceContext::create_shared();
        _rc->memory_context()->set_mem_tracker(_tracker);
    }

    // ThreadMemTrackerMgr batches small allocations in `_untracked_mem` and
    // only flushes to the tracker at `mem_tracker_consume_min_size_bytes`.
    // Force a flush so single-alloc tests see deterministic `consumption()`.
    static void flush_thread_tracker() {
        thread_context()->thread_mem_tracker_mgr->flush_untracked_mem();
    }

    std::shared_ptr<MemTrackerLimiter> _tracker;
    std::shared_ptr<ResourceContext> _rc;
};

TEST_F(AllocatorTrackerTest, GrowChargesDeltaOnly) {
    SCOPED_ATTACH_TASK(_rc);
    flush_thread_tracker();
    const int64_t base = _tracker->consumption();

    Allocator<false, false, false> allocator;
    // Both sizes are >= mem_tracker_consume_min_size_bytes (default 1 MiB)
    // to guarantee the thread-local batch flushes on each call.
    constexpr size_t kOld = 4 * 1024 * 1024;  // 4 MiB
    constexpr size_t kNew = 16 * 1024 * 1024; // 16 MiB

    void* buf = allocator.alloc(kOld);
    ASSERT_NE(nullptr, buf);
    flush_thread_tracker();
    const int64_t after_alloc = _tracker->consumption();
    EXPECT_EQ(after_alloc, base + static_cast<int64_t>(kOld));

    buf = allocator.realloc(buf, kOld, kNew);
    ASSERT_NE(nullptr, buf);
    flush_thread_tracker();
    const int64_t after_grow = _tracker->consumption();
    // Net delta must equal (kNew - kOld); the old_size must NOT be
    // transiently added on top of new_size before being released.
    EXPECT_EQ(after_grow, base + static_cast<int64_t>(kNew));
    EXPECT_LT(after_grow, base + static_cast<int64_t>(kOld + kNew));

    allocator.free(buf, kNew);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base);
}

TEST_F(AllocatorTrackerTest, ShrinkReleasesDeltaOnly) {
    SCOPED_ATTACH_TASK(_rc);
    flush_thread_tracker();
    const int64_t base = _tracker->consumption();

    Allocator<false, false, false> allocator;
    constexpr size_t kOld = 16 * 1024 * 1024;
    constexpr size_t kNew = 4 * 1024 * 1024;

    void* buf = allocator.alloc(kOld);
    ASSERT_NE(nullptr, buf);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base + static_cast<int64_t>(kOld));

    buf = allocator.realloc(buf, kOld, kNew);
    ASSERT_NE(nullptr, buf);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base + static_cast<int64_t>(kNew));

    allocator.free(buf, kNew);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base);
}

TEST_F(AllocatorTrackerTest, GrowDoesNotTripLimitOnBoundary) {
    // Simulates the OOM boundary observed in production: tracker consumption
    // is close to the query mem_limit and a hash table doubles. Under delta
    // accounting the check charges only (new-old), so realloc succeeds; a
    // regression to `charge new_size` would fail the check.
    SCOPED_ATTACH_TASK(_rc);
    flush_thread_tracker();

    Allocator<false, false, false> allocator;
    constexpr size_t kOld = 4 * 1024 * 1024;    // 4 MiB
    constexpr size_t kNew = 6 * 1024 * 1024;    // 6 MiB
    constexpr int64_t kLimit = 9 * 1024 * 1024; // 9 MiB

    void* buf = allocator.alloc(kOld);
    ASSERT_NE(nullptr, buf);
    flush_thread_tracker();

    // Set limit AFTER the initial alloc so consumption() sits at 4 MiB with
    // 5 MiB headroom. A regression that charges new_size=6MiB during realloc
    // would exceed 9 MiB (4 + 6 > 9). Delta accounting charges only 2 MiB
    // (4 + 2 <= 9), which passes.
    _tracker->set_limit(kLimit);
    ASSERT_TRUE(_tracker->check_limit(static_cast<int64_t>(kNew - kOld)).ok());

    void* new_buf = nullptr;
    try {
        new_buf = allocator.realloc(buf, kOld, kNew);
    } catch (const Exception& e) {
        _tracker->set_limit(-1);
        allocator.free(buf, kOld);
        FAIL() << "realloc must not double-count old_size against mem_limit; got: " << e.what();
    }
    _tracker->set_limit(-1);
    ASSERT_NE(nullptr, new_buf);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), static_cast<int64_t>(kNew));

    allocator.free(new_buf, kNew);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), 0);
}

TEST_F(AllocatorTrackerTest, SameSizeReallocIsNoOp) {
    SCOPED_ATTACH_TASK(_rc);
    flush_thread_tracker();
    const int64_t base = _tracker->consumption();

    Allocator<false, false, false> allocator;
    constexpr size_t kSize = 4 * 1024 * 1024;

    void* buf = allocator.alloc(kSize);
    ASSERT_NE(nullptr, buf);
    flush_thread_tracker();
    const int64_t after_alloc = _tracker->consumption();

    void* same = allocator.realloc(buf, kSize, kSize);
    EXPECT_EQ(same, buf);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), after_alloc);

    allocator.free(same, kSize);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base);
}

TEST_F(AllocatorTrackerTest, GrowThenShrinkReturnsToBase) {
    // Chained realloc must keep the tracker balanced end-to-end regardless of
    // which realloc path each hop takes.
    SCOPED_ATTACH_TASK(_rc);
    flush_thread_tracker();
    const int64_t base = _tracker->consumption();

    Allocator<false, false, false> allocator;
    constexpr size_t kA = 4 * 1024 * 1024;
    constexpr size_t kB = 32 * 1024 * 1024;
    constexpr size_t kC = 8 * 1024 * 1024;

    void* buf = allocator.alloc(kA);
    ASSERT_NE(nullptr, buf);
    buf = allocator.realloc(buf, kA, kB);
    ASSERT_NE(nullptr, buf);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base + static_cast<int64_t>(kB));

    buf = allocator.realloc(buf, kB, kC);
    ASSERT_NE(nullptr, buf);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base + static_cast<int64_t>(kC));

    allocator.free(buf, kC);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base);
}

TEST_F(AllocatorTrackerTest, GrowAcrossMmapThresholdChargesDeltaOnly) {
    // old_size below the mmap threshold and new_size above it forces the
    // copy path (`alloc(new)+memcpy+free(old)`). Inner alloc/free self-track;
    // the outer realloc must not add a second layer of consume/release.
    SCOPED_ATTACH_TASK(_rc);
    flush_thread_tracker();
    const int64_t base = _tracker->consumption();

    Allocator<false, false, true> allocator; // use_mmap = true
    const size_t threshold = static_cast<size_t>(doris::config::mmap_threshold);
    const size_t kOld = 4 * 1024 * 1024;
    const size_t kNew = threshold + 16 * 1024 * 1024; // comfortably above threshold

    void* buf = allocator.alloc(kOld);
    ASSERT_NE(nullptr, buf);

    buf = allocator.realloc(buf, kOld, kNew);
    ASSERT_NE(nullptr, buf);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base + static_cast<int64_t>(kNew));

    allocator.free(buf, kNew);
    flush_thread_tracker();
    EXPECT_EQ(_tracker->consumption(), base);
}

} // namespace doris
