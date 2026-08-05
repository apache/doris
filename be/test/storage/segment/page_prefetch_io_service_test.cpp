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

#include "storage/segment/page_prefetch_io_service.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <future>
#include <memory>
#include <string_view>
#include <vector>

#include "cpp/sync_point.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/defer_op.h"

namespace doris::segment_v2 {
namespace {

constexpr PagePrefetchBudgetLimits kWideLimits {.max_ranges = 16, .max_bytes = 1 << 20};

struct RangeFixture {
    std::shared_ptr<PagePrefetchQueryContext> query;
    std::shared_ptr<PagePrefetchGlobalBudget> global;
    std::shared_ptr<MemTrackerLimiter> tracker;
    std::shared_ptr<PrefetchRange> range;
};

PageFetchRangeSpec make_spec(size_t size = 64) {
    return PageFetchRangeSpec {
            .offset = 100,
            .size = size,
            .requested_page_bytes = 24,
            .coalesced_gap_bytes = size - 24,
            .block_fill_bytes = 0,
            .pages = {{.page_index = 7, .page_offset = 108, .page_size = 16, .buffer_offset = 8}},
            .complete_blocks = {},
    };
}

RangeFixture make_range() {
    RangeFixture fixture;
    fixture.query = std::make_shared<PagePrefetchQueryContext>(kWideLimits);
    fixture.global = std::make_shared<PagePrefetchGlobalBudget>(kWideLimits);
    fixture.tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::CACHE,
                                                       "PagePrefetchIOServiceTest");
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto reservation = PagePrefetchReservation::try_reserve_range(fixture.query, fixture.global, 64,
                                                                  &reject_reason);
    DORIS_CHECK(reservation.has_value());
    DORIS_CHECK(reject_reason == PagePrefetchRejectReason::NONE);
    std::shared_ptr<PagePrefetchBuffer> buffer;
    DORIS_CHECK(
            PagePrefetchBuffer::create(64, fixture.tracker, std::move(*reservation), &buffer).ok());
    std::memset(buffer->data(), 'p', buffer->size());
    fixture.range = std::make_shared<PrefetchRange>(make_spec(), std::move(buffer));
    return fixture;
}

TEST(PagePrefetchAdmissionTest, QueryRangeAndByteLimitsRollbackCompletely) {
    auto global = std::make_shared<PagePrefetchGlobalBudget>(kWideLimits);
    auto range_limited_query = std::make_shared<PagePrefetchQueryContext>(
            PagePrefetchBudgetLimits {.max_ranges = 1, .max_bytes = 64});
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto first = PagePrefetchReservation::try_reserve_range(range_limited_query, global, 16,
                                                            &reject_reason);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::NONE);
    auto rejected = PagePrefetchReservation::try_reserve_range(range_limited_query, global, 16,
                                                               &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::QUERY_RANGE_LIMIT);
    EXPECT_EQ(range_limited_query->inflight_ranges(), 1);
    EXPECT_EQ(range_limited_query->resident_bytes(), 16);
    EXPECT_EQ(global->inflight_ranges(), 1);
    EXPECT_EQ(global->resident_bytes(), 16);
    first.reset();
    EXPECT_EQ(range_limited_query->inflight_ranges(), 0);
    EXPECT_EQ(range_limited_query->resident_bytes(), 0);
    EXPECT_EQ(global->inflight_ranges(), 0);
    EXPECT_EQ(global->resident_bytes(), 0);

    auto byte_limited_query = std::make_shared<PagePrefetchQueryContext>(
            PagePrefetchBudgetLimits {.max_ranges = 2, .max_bytes = 8});
    auto within_limit = PagePrefetchReservation::try_reserve_range(byte_limited_query, global, 6,
                                                                   &reject_reason);
    ASSERT_TRUE(within_limit.has_value());
    rejected = PagePrefetchReservation::try_reserve_range(byte_limited_query, global, 3,
                                                          &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::QUERY_BYTE_LIMIT);
    EXPECT_EQ(byte_limited_query->inflight_ranges(), 1);
    EXPECT_EQ(byte_limited_query->resident_bytes(), 6);
    EXPECT_EQ(global->inflight_ranges(), 1);
    EXPECT_EQ(global->resident_bytes(), 6);
}

TEST(PagePrefetchAdmissionTest, GlobalRangeAndByteLimitsRollbackQueryReservation) {
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto query = std::make_shared<PagePrefetchQueryContext>(kWideLimits);
    auto range_limited_global = std::make_shared<PagePrefetchGlobalBudget>(
            PagePrefetchBudgetLimits {.max_ranges = 1, .max_bytes = 64});
    auto first = PagePrefetchReservation::try_reserve_range(query, range_limited_global, 16,
                                                            &reject_reason);
    ASSERT_TRUE(first.has_value());
    auto rejected = PagePrefetchReservation::try_reserve_range(query, range_limited_global, 16,
                                                               &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::GLOBAL_RANGE_LIMIT);
    EXPECT_EQ(query->inflight_ranges(), 1);
    EXPECT_EQ(query->resident_bytes(), 16);

    first.reset();
    auto byte_limited_global = std::make_shared<PagePrefetchGlobalBudget>(
            PagePrefetchBudgetLimits {.max_ranges = 2, .max_bytes = 8});
    auto within_limit = PagePrefetchReservation::try_reserve_range(query, byte_limited_global, 6,
                                                                   &reject_reason);
    ASSERT_TRUE(within_limit.has_value());
    rejected = PagePrefetchReservation::try_reserve_range(query, byte_limited_global, 3,
                                                          &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::GLOBAL_BYTE_LIMIT);
    EXPECT_EQ(query->inflight_ranges(), 1);
    EXPECT_EQ(query->resident_bytes(), 6);
    EXPECT_EQ(byte_limited_global->inflight_ranges(), 1);
    EXPECT_EQ(byte_limited_global->resident_bytes(), 6);
}

TEST(PagePrefetchAdmissionTest, MoveOnlyReservationSeparatesRangeAndResidentLifetimes) {
    auto query = std::make_shared<PagePrefetchQueryContext>(kWideLimits);
    auto global = std::make_shared<PagePrefetchGlobalBudget>(kWideLimits);
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto reservation =
            PagePrefetchReservation::try_reserve_range(query, global, 32, &reject_reason);
    ASSERT_TRUE(reservation.has_value());
    PagePrefetchReservation moved = std::move(*reservation);
    EXPECT_TRUE(moved.valid());
    EXPECT_FALSE(reservation->valid());
    EXPECT_EQ(query->inflight_ranges(), 1);
    EXPECT_EQ(query->resident_bytes(), 32);

    moved.release_range_slot();
    EXPECT_EQ(query->inflight_ranges(), 0);
    EXPECT_EQ(global->inflight_ranges(), 0);
    EXPECT_EQ(query->resident_bytes(), 32);
    EXPECT_EQ(global->resident_bytes(), 32);

    auto replacement =
            PagePrefetchReservation::try_reserve_writeback(query, global, 8, &reject_reason);
    ASSERT_TRUE(replacement.has_value());
    moved = std::move(*replacement);
    EXPECT_EQ(query->resident_bytes(), 8);
    EXPECT_EQ(global->resident_bytes(), 8);
}

TEST(PagePrefetchAdmissionTest, BufferAllocationFailureRollsBackEveryReservation) {
    auto query = std::make_shared<PagePrefetchQueryContext>(kWideLimits);
    auto global = std::make_shared<PagePrefetchGlobalBudget>(kWideLimits);
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::CACHE,
                                                    "PagePrefetchAllocationFailureTest");
    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto reservation =
            PagePrefetchReservation::try_reserve_range(query, global, 64, &reject_reason);
    ASSERT_TRUE(reservation.has_value());

    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "PagePrefetchBuffer::create:inject_failure",
            [](auto&& values) {
                *try_any_cast<Status*>(values.back()) =
                        Status::MemoryAllocFailed("injected page prefetch allocation failure");
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    std::shared_ptr<PagePrefetchBuffer> buffer;
    Status status = PagePrefetchBuffer::create(64, tracker, std::move(*reservation), &buffer);
    EXPECT_TRUE(status.is<ErrorCode::MEM_ALLOC_FAILED>());
    EXPECT_EQ(buffer, nullptr);
    EXPECT_EQ(query->inflight_ranges(), 0);
    EXPECT_EQ(query->resident_bytes(), 0);
    EXPECT_EQ(global->inflight_ranges(), 0);
    EXPECT_EQ(global->resident_bytes(), 0);
    EXPECT_EQ(tracker->consumption(), 0);
}

TEST(PagePrefetchRangeTest, ReadyWakesAllWaitersAndExposesOwnedPageSlice) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    ASSERT_TRUE(fixture.range->mark_running());

    std::vector<std::future<Status>> waiters;
    for (size_t i = 0; i < 8; ++i) {
        waiters.emplace_back(std::async(std::launch::async, [range = fixture.range]() {
            return range->wait_for_consume();
        }));
    }
    RangeReadStats stats {.cache_or_inflight_bytes = 16,
                          .remote_bytes = 48,
                          .remote_io_time_ns = 1234,
                          .self_heal_count = 0};
    fixture.range->publish_ready(stats);
    for (auto& waiter : waiters) {
        EXPECT_TRUE(waiter.get().ok());
    }
    EXPECT_TRUE(fixture.range->wait_for_consume().ok());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::READY);
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
    EXPECT_EQ(fixture.query->resident_bytes(), 64);

    Slice page = fixture.range->page_slice(0);
    ASSERT_EQ(page.size, 16);
    EXPECT_EQ(std::string_view(page.data, page.size), std::string_view("pppppppppppppppp"));
    RangeReadStats merged;
    EXPECT_TRUE(fixture.range->take_read_stats_once(&merged));
    EXPECT_EQ(merged.remote_bytes, 48);
    EXPECT_FALSE(fixture.range->take_read_stats_once(&merged));

    fixture.range->request_cancel();
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::READY);
    EXPECT_TRUE(fixture.range->wait_for_consume().ok());
    fixture.range.reset();
    EXPECT_EQ(fixture.query->resident_bytes(), 0);
    EXPECT_EQ(fixture.global->resident_bytes(), 0);
    EXPECT_EQ(fixture.tracker->consumption(), 0);
}

TEST(PagePrefetchRangeTest, RunningCancellationWakesWaiterBeforeWorkerFinishes) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    ASSERT_TRUE(fixture.range->mark_running());
    auto waiter = std::async(std::launch::async,
                             [range = fixture.range]() { return range->wait_for_consume(); });

    fixture.range->request_cancel();
    ASSERT_EQ(waiter.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_TRUE(waiter.get().is<ErrorCode::CANCELLED>());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::RUNNING);
    EXPECT_EQ(fixture.query->inflight_ranges(), 1);

    fixture.range->publish_ready({.cache_or_inflight_bytes = 0,
                                  .remote_bytes = 64,
                                  .remote_io_time_ns = 0,
                                  .self_heal_count = 0});
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::CANCELLED);
    EXPECT_TRUE(fixture.range->wait_for_consume().is<ErrorCode::CANCELLED>());
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
    EXPECT_EQ(fixture.query->resident_bytes(), 64);
}

TEST(PagePrefetchRangeTest, QueuedCancellationFinalizesWhenWorkerTakesTask) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    fixture.range->request_cancel();
    EXPECT_TRUE(fixture.range->wait_for_consume().is<ErrorCode::CANCELLED>());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::QUEUED);
    EXPECT_FALSE(fixture.range->mark_running());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::CANCELLED);
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
}

TEST(PagePrefetchRangeTest, FailedRangePublishesStableStatusAndStats) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    ASSERT_TRUE(fixture.range->mark_running());
    fixture.range->publish_failed(Status::IOError("injected prefetch read failure"),
                                  {.cache_or_inflight_bytes = 16,
                                   .remote_bytes = 0,
                                   .remote_io_time_ns = 55,
                                   .self_heal_count = 1});
    Status first = fixture.range->wait_for_consume();
    Status second = fixture.range->wait_for_consume();
    EXPECT_TRUE(first.is<ErrorCode::IO_ERROR>());
    EXPECT_EQ(first, second);
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::FAILED);
    EXPECT_EQ(fixture.range->read_stats().self_heal_count, 1);
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
}

TEST(PagePrefetchRangeTest, QueryCancellationCancelsRegisteredRangeAndNewReservations) {
    auto fixture = make_range();
    fixture.range->mark_queued();
    ASSERT_TRUE(fixture.range->mark_running());
    fixture.query->register_range(fixture.range);
    fixture.query->cancel();
    EXPECT_TRUE(fixture.range->wait_for_consume().is<ErrorCode::CANCELLED>());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::RUNNING);

    PagePrefetchRejectReason reject_reason = PagePrefetchRejectReason::NONE;
    auto rejected = PagePrefetchReservation::try_reserve_range(fixture.query, fixture.global, 1,
                                                               &reject_reason);
    EXPECT_FALSE(rejected.has_value());
    EXPECT_EQ(reject_reason, PagePrefetchRejectReason::QUERY_CANCELLED);
    fixture.range->publish_cancelled();
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::CANCELLED);
}

TEST(PagePrefetchRangeTest, RejectedRangeReleasesActiveSlotAndKeepsBufferUntilDestruction) {
    auto fixture = make_range();
    fixture.range->mark_rejected(Status::TooManyTasks("thread pool rejected range"));
    Status status = fixture.range->wait_for_consume();
    EXPECT_TRUE(status.is<ErrorCode::TOO_MANY_TASKS>());
    EXPECT_EQ(fixture.range->state(), PrefetchRange::State::REJECTED);
    EXPECT_EQ(fixture.query->inflight_ranges(), 0);
    EXPECT_EQ(fixture.query->resident_bytes(), 64);
    fixture.range.reset();
    EXPECT_EQ(fixture.query->resident_bytes(), 0);
    EXPECT_EQ(fixture.global->resident_bytes(), 0);
}

} // namespace
} // namespace doris::segment_v2
