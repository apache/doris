// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/lance/lance_memory.h"

#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <limits>
#include <thread>
#include <vector>

#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/workload_group/workload_group.h"
#include "util/mem_info.h"

namespace doris::format::lance {

class LanceMemoryTest : public testing::Test {
protected:
    void SetUp() override {
        _soft_limit = MemInfo::_s_soft_mem_limit.exchange(int64_t {1} << 60);
        _available = MemInfo::_s_sys_mem_available.exchange(int64_t {1} << 60);
        _watermark = MemInfo::_s_sys_mem_available_warning_water_mark;
        MemInfo::_s_sys_mem_available_warning_water_mark = 0;
        _process_reserved = GlobalMemoryArbitrator::process_reserved_memory();
    }

    void TearDown() override {
        EXPECT_EQ(_process_reserved, GlobalMemoryArbitrator::process_reserved_memory());
        MemInfo::_s_soft_mem_limit = _soft_limit;
        MemInfo::_s_sys_mem_available = _available;
        MemInfo::_s_sys_mem_available_warning_water_mark = _watermark;
    }

    static std::shared_ptr<MemTrackerLimiter> tracker(int64_t limit = -1) {
        return MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY, "LanceMemoryTest",
                                                limit);
    }

    static std::shared_ptr<WorkloadGroup> group(int64_t limit) {
        return std::make_shared<WorkloadGroup>(
                WorkloadGroupInfo {.id = 1, .memory_limit = limit, .memory_high_watermark = 100});
    }

    int64_t _soft_limit = 0;
    int64_t _available = 0;
    int64_t _watermark = 0;
    int64_t _process_reserved = 0;
};

TEST_F(LanceMemoryTest, OrdinaryAllocationRecordsAndLatchesQueryLimit) {
    auto t = tracker(1024);
    LanceMemoryAccount account(t);
    const auto o = account.options();
    ASSERT_EQ(1, o.abi_version);
    ASSERT_EQ(sizeof(o), o.struct_size);
    ASSERT_NE(nullptr, o.context);
    ASSERT_NE(nullptr, o.record);
    EXPECT_EQ(0, o.check(o.context));
    EXPECT_NO_THROW(o.record(o.context, 1024));
    EXPECT_EQ(0, o.check(o.context));
    EXPECT_NO_THROW(o.record(o.context, 1));
    EXPECT_EQ(1025, t->consumption());
    EXPECT_EQ(1, o.check(o.context));
    o.record(o.context, -1025);
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(1, o.check(o.context));
    EXPECT_EQ(1, o.reserve(o.context, 512));
    EXPECT_EQ(0, t->reserved_consumption());
}

TEST_F(LanceMemoryTest, AllocationOwnerOutlivesRegistrationAndFreesOnAnotherThread) {
    auto t = tracker();
    std::weak_ptr<MemTrackerLimiter> weak = t;
    LanceHostMemoryAccountOptionsV1 o;
    {
        LanceMemoryAccount account(t);
        o = account.options();
        o.retain_context(o.context);
        o.record(o.context, 8192);
    }
    t.reset();
    ASSERT_FALSE(weak.expired());
    std::thread cleanup([o] {
        o.record(o.context, -8192);
        o.release_context(o.context);
    });
    cleanup.join();
    EXPECT_TRUE(weak.expired());
}

TEST_F(LanceMemoryTest, ReservationConvertsToOneLiveChargeAcrossThreads) {
    auto t = tracker(1024);
    auto wg = group(2048);
    LanceMemoryAccount account(t, wg);
    const auto o = account.options();
    ASSERT_EQ(0, o.reserve(o.context, 1024));
    EXPECT_EQ(1024, t->consumption());
    EXPECT_EQ(1024, t->reserved_consumption());
    EXPECT_EQ(1024, wg->wg_refresh_interval_memory_growth());
    EXPECT_EQ(_process_reserved + 1024, GlobalMemoryArbitrator::process_reserved_memory());
    std::thread complete([o] { o.finish_reserve(o.context, 1024, true); });
    complete.join();
    EXPECT_EQ(1024, t->consumption());
    EXPECT_EQ(0, t->reserved_consumption());
    EXPECT_EQ(1024, wg->wg_refresh_interval_memory_growth());
    EXPECT_EQ(_process_reserved, GlobalMemoryArbitrator::process_reserved_memory());
    o.record(o.context, -1024);
    EXPECT_EQ(0, t->consumption());
}

TEST_F(LanceMemoryTest, FailedAllocationRefundsQueryAndProcessQuota) {
    auto t = tracker(1024);
    auto wg = group(2048);
    LanceMemoryAccount account(t, wg);
    const auto o = account.options();
    ASSERT_EQ(0, o.reserve(o.context, 1024));
    std::thread complete([o] { o.finish_reserve(o.context, 1024, false); });
    complete.join();
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(0, t->reserved_consumption());
    EXPECT_EQ(1024, wg->wg_refresh_interval_memory_growth());
    EXPECT_EQ(_process_reserved, GlobalMemoryArbitrator::process_reserved_memory());
}

TEST_F(LanceMemoryTest, FailedAllocationAfterRefreshPreservesTheNewGrowthInterval) {
    for (const int64_t new_growth : {0, 256}) {
        auto t = tracker(4096);
        auto wg = group(4096);
        LanceMemoryAccount account(t, wg);
        const auto o = account.options();
        ASSERT_EQ(0, o.reserve(o.context, 1024));
        // A WG refresh replaces the interval counter before another allocation.
        wg->_wg_refresh_interval_memory_growth = 0;
        o.record(o.context, new_growth);
        o.finish_reserve(o.context, 1024, false);
        EXPECT_EQ(new_growth, t->consumption());
        EXPECT_EQ(0, t->reserved_consumption());
        EXPECT_EQ(new_growth, wg->wg_refresh_interval_memory_growth());
        EXPECT_EQ(_process_reserved, GlobalMemoryArbitrator::process_reserved_memory());
        o.record(o.context, -new_growth);
    }
}

TEST_F(LanceMemoryTest, QueryRejectionDoesNotReserveGroupOrProcess) {
    auto t = tracker(1023);
    auto wg = group(2048);
    LanceMemoryAccount account(t, wg);
    const auto o = account.options();
    EXPECT_EQ(1, o.reserve(o.context, 1024));
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(0, t->reserved_consumption());
    EXPECT_EQ(0, wg->wg_refresh_interval_memory_growth());
    EXPECT_EQ(_process_reserved, GlobalMemoryArbitrator::process_reserved_memory());
    EXPECT_EQ(0, o.check(o.context));
    ASSERT_EQ(0, o.reserve(o.context, 512));
    o.finish_reserve(o.context, 512, false);
}

TEST_F(LanceMemoryTest, GroupRejectionRollsBackQueryReservation) {
    auto t = tracker(4096);
    auto wg = group(1023);
    LanceMemoryAccount account(t, wg);
    const auto o = account.options();
    EXPECT_EQ(2, o.reserve(o.context, 1024));
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(0, t->reserved_consumption());
    EXPECT_EQ(0, wg->wg_refresh_interval_memory_growth());
    EXPECT_EQ(_process_reserved, GlobalMemoryArbitrator::process_reserved_memory());
}

TEST_F(LanceMemoryTest, ProcessRejectionRefundsQueryAndKeepsConservativeGroupGrowth) {
    auto t = tracker(4096);
    auto wg = group(2048);
    LanceMemoryAccount account(t, wg);
    const auto o = account.options();
    MemInfo::_s_soft_mem_limit = GlobalMemoryArbitrator::process_memory_usage() + 1024;
    EXPECT_EQ(3, o.reserve(o.context, 1024));
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(0, t->reserved_consumption());
    EXPECT_EQ(1024, wg->wg_refresh_interval_memory_growth());
    EXPECT_EQ(_process_reserved, GlobalMemoryArbitrator::process_reserved_memory());
}

TEST_F(LanceMemoryTest, FreeDoesNotSubtractGrowthAfterGroupRefresh) {
    auto t = tracker();
    auto wg = group(4096);
    LanceMemoryAccount account(t, wg);
    const auto o = account.options();
    o.record(o.context, 1024);
    EXPECT_EQ(1024, wg->wg_refresh_interval_memory_growth());
    wg->_wg_refresh_interval_memory_growth = 0;
    o.record(o.context, -1024);
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(0, wg->wg_refresh_interval_memory_growth());
}

TEST_F(LanceMemoryTest, SafeBoundaryChecksGroupAndProcessLimits) {
    auto t = tracker();
    auto wg = group(1024);
    LanceMemoryAccount account(t, wg);
    const auto o = account.options();
    o.record(o.context, 1025);
    EXPECT_EQ(2, o.check(o.context));
    o.record(o.context, -1025);
    wg->_wg_refresh_interval_memory_growth = 0;
    MemInfo::_s_soft_mem_limit = GlobalMemoryArbitrator::process_memory_usage();
    EXPECT_EQ(3, o.check(o.context));
}

TEST_F(LanceMemoryTest, ExpiredWorkloadGroupDoesNotInvalidateAllocationOwner) {
    auto t = tracker();
    auto wg = group(4096);
    std::weak_ptr<WorkloadGroup> weak = wg;
    LanceMemoryAccount account(t, weak);
    wg.reset();
    ASSERT_TRUE(weak.expired());
    const auto o = account.options();
    ASSERT_EQ(0, o.reserve(o.context, 1024));
    o.finish_reserve(o.context, 1024, true);
    o.record(o.context, -1024);
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(0, o.check(o.context));
}

TEST_F(LanceMemoryTest, SharedCacheAndBackgroundAccountsDoNotChargeQuery) {
    auto query = tracker(1);
    LanceMemoryAccount query_account(query);
    for (auto type : {MemTrackerLimiter::Type::GLOBAL, MemTrackerLimiter::Type::CACHE,
                      MemTrackerLimiter::Type::METADATA}) {
        auto t = MemTrackerLimiter::create_shared(type, "LanceMemoryServiceTest");
        LanceMemoryAccount account(t);
        const auto o = account.options();
        o.record(o.context, 8192);
        EXPECT_EQ(8192, t->consumption());
        EXPECT_EQ(0, query->consumption());
        EXPECT_EQ(0, query_account.options().check(query_account.options().context));
        o.record(o.context, -8192);
    }
}

TEST_F(LanceMemoryTest, ConcurrentAllocationAndReservationKeepExactBalances) {
    auto t = tracker(1 << 20);
    LanceMemoryAccount first(t);
    LanceMemoryAccount second(t);
    const auto first_options = first.options();
    const auto second_options = second.options();
    std::atomic<int> failures {0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&, o = i % 2 == 0 ? first_options : second_options] {
            for (int iteration = 0; iteration < 1000; ++iteration) {
                o.record(o.context, 64);
                if (o.reserve(o.context, 128) != 0) {
                    ++failures;
                } else {
                    const bool allocated = iteration % 2 == 0;
                    o.finish_reserve(o.context, 128, allocated);
                    if (allocated) o.record(o.context, -128);
                }
                o.record(o.context, -64);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(0, failures);
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(0, t->reserved_consumption());
    EXPECT_EQ(_process_reserved, GlobalMemoryArbitrator::process_reserved_memory());
}

TEST_F(LanceMemoryTest, GroupAdmissionAcrossAccountsDoesNotOversubscribe) {
    auto t = tracker();
    auto wg = group(1024);
    LanceMemoryAccount first(t, wg);
    LanceMemoryAccount second(t, wg);
    const auto first_options = first.options();
    const auto second_options = second.options();
    std::barrier admitted(16);
    std::atomic<int> successes {0};
    std::atomic<int> unexpected_errors {0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([&, o = i % 2 == 0 ? first_options : second_options] {
            const auto result = o.reserve(o.context, 256);
            if (result == 0) ++successes;
            if (result != 0 && result != 2) ++unexpected_errors;
            admitted.arrive_and_wait();
            if (result == 0) o.finish_reserve(o.context, 256, false);
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(4, successes);
    EXPECT_EQ(0, unexpected_errors);
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(0, t->reserved_consumption());
    EXPECT_EQ(1024, wg->wg_refresh_interval_memory_growth());
}

TEST_F(LanceMemoryTest, ZeroAndUnrepresentableReservationsLeaveCountersUntouched) {
    auto t = tracker();
    LanceMemoryAccount account(t);
    const auto o = account.options();
    EXPECT_EQ(0, o.reserve(o.context, 0));
    o.finish_reserve(o.context, 0, true);
    o.finish_reserve(o.context, 0, false);
    EXPECT_EQ(3, o.reserve(o.context, std::numeric_limits<size_t>::max()));
    EXPECT_EQ(3, o.reserve(o.context, static_cast<size_t>(std::numeric_limits<int64_t>::max())));
    EXPECT_EQ(0, t->consumption());
    EXPECT_EQ(0, t->reserved_consumption());
    EXPECT_EQ(_process_reserved, GlobalMemoryArbitrator::process_reserved_memory());
}

} // namespace doris::format::lance
