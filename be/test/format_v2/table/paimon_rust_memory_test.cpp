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

#include "format_v2/table/paimon_rust_memory.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <cstring>
#include <thread>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_profile.h"
#include "runtime/thread_context.h"

namespace doris::format::paimon {
namespace {
constexpr int64_t MIB = 1024 * 1024;

struct ArrowFixture {
    std::shared_ptr<arrow::Buffer> allocation;
    std::shared_ptr<arrow::RecordBatch> batch;
};

ArrowFixture aliased_batch(bool known_parent) {
    std::shared_ptr<arrow::Buffer> root = arrow::AllocateBuffer(1024).ValueOrDie();
    std::memset(root->mutable_data(), 0, 1024);
    std::shared_ptr<arrow::Buffer> left;
    std::shared_ptr<arrow::Buffer> right;
    if (known_parent) {
        left = arrow::SliceBuffer(root, 0, 32);
        right = arrow::SliceBuffer(root, 16, 32);
    } else {
        // C imports can expose overlapping extents without a parent/capacity relationship.
        left = std::make_shared<arrow::Buffer>(root->data(), 32);
        right = std::make_shared<arrow::Buffer>(root->data() + 16, 32);
    }
    auto a = arrow::MakeArray(arrow::ArrayData::Make(arrow::int32(), 4, {nullptr, left}));
    auto b = arrow::MakeArray(arrow::ArrayData::Make(arrow::int32(), 4, {nullptr, right}));
    return {root, arrow::RecordBatch::Make(arrow::schema({arrow::field("a", arrow::int32()),
                                                          arrow::field("b", arrow::int32())}),
                                           4, {a, b})};
}
} // namespace

TEST(PaimonRustMemoryBudgetTest, ConcurrentScannersShareQueryAdmission) {
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "rust-admission", 8 * MIB);
    // Other query operators already own half the query allowance.
    tracker->consume(4 * MIB);
    std::barrier gate(17);
    std::atomic<int> admitted = 0;
    std::array<std::thread, 16> scanners;
    for (auto& scanner : scanners) {
        scanner = std::thread([&] {
            SCOPED_INIT_THREAD_CONTEXT();
            PaimonRustMemoryBudget budget(tracker, nullptr);
            auto status = budget.acquire();
            if (status.ok()) {
                admitted.fetch_add(1);
                EXPECT_EQ(budget.credit_bytes(), MIB / 2);
                EXPECT_EQ(budget.sdk_limit_bytes(), MIB / 4);
            } else {
                EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
            }
            gate.arrive_and_wait();
            gate.arrive_and_wait();
        });
    }
    gate.arrive_and_wait();
    EXPECT_EQ(admitted.load(), 8);
    EXPECT_EQ(tracker->reserved_consumption(), 4 * MIB);
    EXPECT_EQ(tracker->consumption(), 8 * MIB);
    gate.arrive_and_wait();
    for (auto& scanner : scanners) {
        scanner.join();
    }
    EXPECT_EQ(tracker->reserved_consumption(), 0);
    EXPECT_EQ(tracker->consumption(), 4 * MIB);
    tracker->release(4 * MIB);
    EXPECT_EQ(tracker->consumption(), 0);
}

TEST(PaimonRustMemoryBudgetTest, SharedAndSlicedBuffersUseExposedExtents) {
    auto fixture = aliased_batch(true);
    auto batch = fixture.batch;
    EXPECT_EQ(PaimonRustMemoryBudget::estimate_pending_bytes(*batch), 1024);
    auto exposed = aliased_batch(false);
    EXPECT_EQ(PaimonRustMemoryBudget::estimate_pending_bytes(*exposed.batch), 48);
    auto empty = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("empty", arrow::int32())}), 0,
            {arrow::MakeArray(arrow::ArrayData::Make(
                    arrow::int32(), 0, {nullptr, arrow::SliceBuffer(fixture.allocation, 0, 0)}))});
    EXPECT_EQ(PaimonRustMemoryBudget::estimate_pending_bytes(*empty), 1024);
    // A child reachable through both STRUCT and a root column must not be counted twice.
    auto child = batch->column(0)->data();
    auto type = arrow::struct_({arrow::field("child", arrow::int32())});
    auto parent = arrow::ArrayData::Make(type, 4, {nullptr});
    parent->child_data.push_back(child);
    auto nested = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("s", type), arrow::field("a", arrow::int32())}), 4,
            {arrow::MakeArray(parent), batch->column(0)});
    EXPECT_EQ(PaimonRustMemoryBudget::estimate_pending_bytes(*nested), 1024);
    auto dictionary_type = arrow::dictionary(arrow::int32(), arrow::int32());
    auto dictionary = arrow::ArrayData::Make(dictionary_type, 4, child->buffers);
    dictionary->dictionary = child;
    auto dictionaries = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("d", dictionary_type)}), 4, {arrow::MakeArray(dictionary)});
    EXPECT_EQ(PaimonRustMemoryBudget::estimate_pending_bytes(*dictionaries), 1024);
}

TEST(PaimonRustMemoryBudgetTest, RejectionAndCrossThreadReleaseBalanceCredit) {
    RuntimeProfile profile("rust-memory");
    ADD_TIMER(&profile, "PaimonRustReader");
    auto tracker = MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::QUERY,
                                                    "rust-small-query", 16 * 1024);
    auto fixture = aliased_batch(true);
    auto batch = fixture.batch;
    {
        PaimonRustMemoryBudget budget(tracker, &profile);
        ASSERT_TRUE(budget.acquire().ok());
        auto status = budget.retain_pending(*batch);
        EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
        EXPECT_EQ(budget.pending_bytes(), 0);
        EXPECT_EQ(profile.get_counter("MemoryBudgetRejections")->value(), 1);
    }
    EXPECT_EQ(tracker->consumption(), 0);
    EXPECT_EQ(tracker->reserved_consumption(), 0);
    tracker->set_limit(8 * MIB);
    auto budget = std::make_unique<PaimonRustMemoryBudget>(tracker, &profile);
    ASSERT_TRUE(budget->acquire().ok());
    ASSERT_TRUE(budget->retain_pending(*batch).ok());
    EXPECT_EQ(budget->pending_bytes(), 1024);
    // The estimate and SDK counters are not added on top of the fixed admission credit.
    EXPECT_EQ(tracker->consumption(), budget->credit_bytes());
    std::thread close_thread([owned = std::move(budget)]() mutable {
        SCOPED_INIT_THREAD_CONTEXT();
        owned.reset();
    });
    close_thread.join();
    EXPECT_EQ(tracker->consumption(), 0);
    EXPECT_EQ(tracker->reserved_consumption(), 0);
    EXPECT_EQ(profile.get_counter("ReaderCreditBytes")->value(), 0);
    EXPECT_EQ(profile.get_counter("PendingArrowEstimatedBytes")->value(), 0);
    EXPECT_EQ(profile.get_counter("SdkReservedBytes")->value(), 0);
    EXPECT_EQ(profile.get_counter("PeakPendingArrowEstimatedBytes")->value(), 1024);
}

} // namespace doris::format::paimon
