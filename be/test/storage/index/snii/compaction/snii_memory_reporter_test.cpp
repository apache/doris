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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <thread>
#include <type_traits>
#include <utility>

#include "common/status.h"
#include "storage/index/snii/writer/memory_reporter.h"

namespace {

namespace ErrorCode = doris::ErrorCode;
using doris::snii::writer::MemoryReporter;

static_assert(!std::is_copy_constructible_v<MemoryReporter::Reservation>);
static_assert(!std::is_copy_assignable_v<MemoryReporter::Reservation>);
static_assert(std::is_nothrow_move_constructible_v<MemoryReporter::Reservation>);
static_assert(std::is_nothrow_move_assignable_v<MemoryReporter::Reservation>);

TEST(SniiMemoryReporterTest, ReservationGrowthFailureLeavesStateUnchanged) {
    int64_t mirrored_bytes = 0;
    MemoryReporter reporter([&](int64_t delta) { mirrored_bytes += delta; }, 100);

    {
        auto first = reporter.make_reservation();
        ASSERT_TRUE(first.set_bytes(60).ok());
        EXPECT_EQ(first.bytes(), 60);
        EXPECT_EQ(reporter.current_bytes(), 60);
        EXPECT_EQ(mirrored_bytes, 60);

        const auto rejected = first.set_bytes(101);
        EXPECT_TRUE(rejected.is<ErrorCode::MEM_LIMIT_EXCEEDED>());
        EXPECT_EQ(first.bytes(), 60);
        EXPECT_EQ(reporter.current_bytes(), 60);
        EXPECT_EQ(mirrored_bytes, 60);

        auto second = reporter.make_reservation();
        ASSERT_TRUE(second.set_bytes(40).ok());
        EXPECT_EQ(reporter.current_bytes(), 100);
        EXPECT_EQ(mirrored_bytes, 100);

        ASSERT_TRUE(first.set_bytes(20).ok());
        EXPECT_EQ(reporter.current_bytes(), 60);
        EXPECT_EQ(mirrored_bytes, 60);
        second.reset();
        EXPECT_EQ(reporter.current_bytes(), 20);
        EXPECT_EQ(mirrored_bytes, 20);
    }

    EXPECT_EQ(reporter.current_bytes(), 0);
    EXPECT_EQ(mirrored_bytes, 0);
}

TEST(SniiMemoryReporterTest, ReservationIncludesLegacyReportedBytesAtAcquireTime) {
    MemoryReporter reporter(nullptr, 100);
    reporter.report(40);

    auto reservation = reporter.make_reservation();
    const auto rejected = reservation.set_bytes(61);
    EXPECT_TRUE(rejected.is<ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_EQ(reservation.bytes(), 0);
    EXPECT_EQ(reporter.current_bytes(), 40);

    ASSERT_TRUE(reservation.set_bytes(60).ok());
    EXPECT_EQ(reporter.current_bytes(), 100);
    reservation.reset();
    reporter.report(-40);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiMemoryReporterTest, SpillThresholdReservationsRemainAccountedAboveCap) {
    MemoryReporter reporter(nullptr, 100, MemoryReporter::CapPolicy::kSpillThreshold);
    reporter.report(120);
    EXPECT_TRUE(reporter.over_cap());

    auto reservation = reporter.make_reservation();
    ASSERT_TRUE(reservation.set_bytes(60).ok());
    EXPECT_EQ(reservation.bytes(), 60);
    EXPECT_EQ(reporter.current_bytes(), 180);
    EXPECT_TRUE(reporter.over_cap());

    reservation.reset();
    reporter.report(-120);
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiMemoryReporterTest, ReservationMoveTransfersChargeAndUnlimitedCapStillCounts) {
    int64_t mirrored_bytes = 0;
    MemoryReporter reporter([&](int64_t delta) { mirrored_bytes += delta; });

    auto source = reporter.make_reservation();
    ASSERT_TRUE(source.set_bytes(512).ok());
    auto destination = std::move(source);
    EXPECT_EQ(source.bytes(), 0);
    EXPECT_EQ(destination.bytes(), 512);
    EXPECT_EQ(reporter.current_bytes(), 512);

    ASSERT_TRUE(destination.set_bytes(1024).ok());
    EXPECT_EQ(reporter.current_bytes(), 1024);
    ASSERT_TRUE(destination.set_bytes(17).ok());
    EXPECT_EQ(reporter.current_bytes(), 17);
    EXPECT_EQ(mirrored_bytes, 17);

    auto replacement = reporter.make_reservation();
    ASSERT_TRUE(replacement.set_bytes(23).ok());
    destination = std::move(replacement);
    EXPECT_EQ(reporter.current_bytes(), 23);
    EXPECT_EQ(destination.bytes(), 23);
    destination.reset();
    EXPECT_EQ(reporter.current_bytes(), 0);
    EXPECT_EQ(mirrored_bytes, 0);
}

TEST(SniiMemoryReporterTest, ConcurrentReservationsCannotOversubscribeCap) {
    std::atomic<int64_t> mirrored_bytes {0};
    MemoryReporter reporter(
            [&mirrored_bytes](int64_t delta) {
                mirrored_bytes.fetch_add(delta, std::memory_order_relaxed);
            },
            100);
    std::array<bool, 2> succeeded {};
    std::array<bool, 2> rejected_by_limit {};
    std::atomic<uint32_t> ready {0};
    std::atomic<uint32_t> attempted {0};
    std::atomic<bool> start {false};
    std::array<std::thread, 2> threads;

    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([&, i] {
            auto reservation = reporter.make_reservation();
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto status = reservation.set_bytes(60);
            succeeded[i] = status.ok();
            rejected_by_limit[i] = status.is<ErrorCode::MEM_LIMIT_EXCEEDED>();
            attempted.fetch_add(1, std::memory_order_release);
            while (attempted.load(std::memory_order_acquire) != threads.size()) {
                std::this_thread::yield();
            }
        });
    }

    while (ready.load(std::memory_order_acquire) != threads.size()) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (std::thread& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(std::count(succeeded.begin(), succeeded.end(), true), 1);
    EXPECT_EQ(std::count(rejected_by_limit.begin(), rejected_by_limit.end(), true), 1);
    EXPECT_EQ(reporter.current_bytes(), 0);
    EXPECT_EQ(mirrored_bytes.load(std::memory_order_relaxed), 0);
}

} // namespace
