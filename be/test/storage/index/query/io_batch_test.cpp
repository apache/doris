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

#include "storage/index/query/spi/io_batch.h"

#include <gtest/gtest.h>

#include <limits>
#include <numeric>

namespace doris::index_query {
namespace {

class CountingIoReader final : public IoReader {
public:
    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        ++calls;
        if (calls == fail_call) {
            return Status::IOError("Injected wave reader failure");
        }
        if (offset > size() || len > size() - offset) {
            return Status::IOError("Read exceeds the test file");
        }
        out->resize(len);
        std::iota(out->begin(), out->end(), static_cast<uint8_t>(seed + offset));
        return Status::OK();
    }
    uint64_t size() const override { return 64; }
    size_t calls = 0;
    size_t fail_call = 0;
    uint8_t seed = 0;
};

TEST(IndexQueryIoBatch, RejectsTheWholeWaveBeforeReadingWhenMemoryIsInsufficient) {
    CountingIoReader first;
    CountingIoReader second;
    MemoryBudget budget(12);
    IoBatch batch(budget, {.bytes = 32, .ranges = 2});
    bool accepted = false;
    size_t handle = 0;
    ASSERT_TRUE(batch.try_add(first, 0, 8, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.try_add(second, 0, 8, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    EXPECT_TRUE(batch.fetch().is<ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_EQ(first.calls, 0U);
    EXPECT_EQ(second.calls, 0U);
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(IndexQueryIoBatch, ChargesOverlappingRangesOnceAndSharesTheirBytes) {
    CountingIoReader reader;
    MemoryBudget budget(12);
    IoBatch batch(budget, {.bytes = 12, .ranges = 1});
    bool accepted = false;
    size_t first = 0;
    size_t second = 0;
    ASSERT_TRUE(batch.try_add(reader, 0, 8, &accepted, &first).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.try_add(reader, 4, 8, &accepted, &second).ok());
    ASSERT_TRUE(accepted);
    EXPECT_EQ(reader.calls, 0U);
    EXPECT_EQ(batch.pending(), 2U);
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(reader.calls, 1U);
    EXPECT_EQ(budget.used_bytes(), 12U);
    EXPECT_EQ(batch.get(first).data() + 4, batch.get(second).data());
    EXPECT_EQ(batch.get(second).front(), 4U);
    EXPECT_EQ(batch.get(second).back(), 11U);
    batch.clear();
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(IndexQueryIoBatch, LimitsPhysicalRangesAcrossReaders) {
    CountingIoReader first;
    CountingIoReader second;
    MemoryBudget budget(64);
    IoBatch batch(budget, {.bytes = 64, .ranges = 2});
    bool accepted = false;
    size_t handle = 0;
    ASSERT_TRUE(batch.try_add(first, 0, 4, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.try_add(second, 0, 4, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    const size_t previous = handle;
    ASSERT_TRUE(batch.try_add(first, 16, 4, &accepted, &handle).ok());
    EXPECT_FALSE(accepted);
    EXPECT_EQ(handle, previous);
    EXPECT_EQ(batch.pending(), 2U);
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(first.calls, 1U);
    EXPECT_EQ(second.calls, 1U);
    EXPECT_EQ(budget.used_bytes(), 8U);
}

TEST(IndexQueryIoBatch, LimitsPhysicalBytesAcrossReaders) {
    CountingIoReader first;
    CountingIoReader second;
    MemoryBudget budget(64);
    IoBatch batch(budget, {.bytes = 12, .ranges = 3});
    bool accepted = false;
    size_t handle = 0;
    ASSERT_TRUE(batch.try_add(first, 0, 8, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.try_add(second, 0, 4, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.try_add(second, 4, 1, &accepted, &handle).ok());
    EXPECT_FALSE(accepted);
    EXPECT_EQ(batch.pending(), 2U);
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(budget.used_bytes(), 12U);
    EXPECT_EQ(first.calls, 1U);
    EXPECT_EQ(second.calls, 1U);
}

TEST(IndexQueryIoBatch, CoalescingFreesARangeForAnotherReader) {
    CountingIoReader first;
    CountingIoReader second;
    MemoryBudget budget(16);
    IoBatch batch(budget, {.bytes = 16, .ranges = 2});
    bool accepted = false;
    size_t handle = 0;
    ASSERT_TRUE(batch.try_add(first, 0, 4, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.try_add(first, 8, 4, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.try_add(second, 0, 4, &accepted, &handle).ok());
    EXPECT_FALSE(accepted);
    ASSERT_TRUE(batch.try_add(first, 4, 4, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.try_add(second, 0, 4, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(first.calls, 1U);
    EXPECT_EQ(second.calls, 1U);
    EXPECT_EQ(budget.used_bytes(), 16U);
}

TEST(IndexQueryIoBatch, ReadFailureReleasesEarlierReadersAndAllowsRetry) {
    CountingIoReader first;
    CountingIoReader second;
    first.seed = 10;
    second.seed = 20;
    second.fail_call = 1;
    MemoryBudget budget(16);
    {
        IoBatch batch(budget, {.bytes = 16, .ranges = 2});
        bool accepted = false;
        size_t first_handle = 0;
        size_t second_handle = 0;
        ASSERT_TRUE(batch.try_add(first, 0, 8, &accepted, &first_handle).ok());
        ASSERT_TRUE(accepted);
        ASSERT_TRUE(batch.try_add(second, 0, 8, &accepted, &second_handle).ok());
        ASSERT_TRUE(accepted);
        EXPECT_FALSE(batch.fetch().ok());
        EXPECT_EQ(first.calls, 1U);
        EXPECT_EQ(second.calls, 1U);
        EXPECT_EQ(budget.used_bytes(), 0U);
        ASSERT_TRUE(batch.fetch().ok());
        EXPECT_EQ(budget.used_bytes(), 16U);
        EXPECT_EQ(batch.get(first_handle).front(), 10U);
        EXPECT_EQ(batch.get(second_handle).front(), 20U);
    }
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(IndexQueryIoBatch, ClearReleasesReadersAndMakesAnEmptyWaveHarmless) {
    CountingIoReader reader;
    MemoryBudget budget(8);
    IoBatch batch(budget, {.bytes = 8, .ranges = 1});
    bool accepted = false;
    size_t handle = 0;
    ASSERT_TRUE(batch.try_add(reader, 0, 8, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.fetch().ok());
    batch.clear();
    EXPECT_EQ(budget.used_bytes(), 0U);
    EXPECT_EQ(batch.pending(), 0U);
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(reader.calls, 1U);
    ASSERT_TRUE(batch.try_add(reader, 8, 8, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(batch.get(handle).front(), 8U);
    EXPECT_EQ(budget.used_bytes(), 8U);
}

TEST(IndexQueryIoBatch, RejectsOverflowWithoutChangingTheRegisteredWave) {
    CountingIoReader first;
    CountingIoReader second;
    MemoryBudget budget(8);
    IoBatch batch(budget, {.bytes = 8, .ranges = 2});
    bool accepted = false;
    size_t handle = 0;
    ASSERT_TRUE(batch.try_add(first, 0, 4, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    handle = 99;
    EXPECT_TRUE(batch.try_add(second, std::numeric_limits<uint64_t>::max(), 2, &accepted, &handle)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    EXPECT_EQ(handle, 99U);
    EXPECT_EQ(batch.pending(), 1U);
    ASSERT_TRUE(batch.try_add(second, 0, 4, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(first.calls, 1U);
    EXPECT_EQ(second.calls, 1U);
    EXPECT_EQ(budget.used_bytes(), 8U);
}

TEST(IndexQueryIoBatch, PreservesOtherOwnersWhenTheWholeWaveIsRejected) {
    CountingIoReader reader;
    MemoryBudget budget(12);
    MemoryBudget::Reservation other;
    ASSERT_TRUE(budget.reserve(8, &other).ok());
    IoBatch batch(budget, {.bytes = 8, .ranges = 1});
    bool accepted = false;
    size_t handle = 0;
    ASSERT_TRUE(batch.try_add(reader, 0, 8, &accepted, &handle).ok());
    ASSERT_TRUE(accepted);
    EXPECT_TRUE(batch.fetch().is<ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_EQ(reader.calls, 0U);
    EXPECT_EQ(budget.used_bytes(), 8U);
    other.reset();
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(budget.used_bytes(), 8U);
}

TEST(IndexQueryIoBatch, AdmitsAnOversizedFirstRangeOnlyWhenExplicitlyAllowed) {
    CountingIoReader first;
    CountingIoReader second;
    MemoryBudget budget(16);
    IoBatch batch(budget, {.bytes = 4, .ranges = 2});
    bool accepted = false;
    size_t handle = 99;
    ASSERT_TRUE(batch.try_add(first, 0, 8, &accepted, &handle).ok());
    EXPECT_FALSE(accepted);
    EXPECT_EQ(handle, 99U);
    ASSERT_TRUE(batch.try_add(first, 0, 8, &accepted, &handle, true).ok());
    ASSERT_TRUE(accepted);
    ASSERT_TRUE(batch.try_add(second, 0, 1, &accepted, &handle, true).ok());
    EXPECT_FALSE(accepted);
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(first.calls, 1U);
    EXPECT_EQ(second.calls, 0U);
    EXPECT_EQ(budget.used_bytes(), 8U);
}

TEST(IndexQueryIoBatch, OversizedWaveSharesExistingBytesWithoutGrowing) {
    CountingIoReader reader;
    MemoryBudget budget(16);
    IoBatch batch(budget, {.bytes = 4, .ranges = 1});
    bool accepted = false;
    size_t original = 0;
    ASSERT_TRUE(batch.try_add(reader, 0, 8, &accepted, &original, true).ok());
    ASSERT_TRUE(accepted);
    size_t overlapping = 0;
    ASSERT_TRUE(batch.try_add(reader, 2, 4, &accepted, &overlapping).ok());
    ASSERT_TRUE(accepted);
    size_t rejected = 99;
    ASSERT_TRUE(batch.try_add(reader, 7, 2, &accepted, &rejected, true).ok());
    EXPECT_FALSE(accepted);
    EXPECT_EQ(rejected, 99U);
    ASSERT_TRUE(batch.fetch().ok());
    EXPECT_EQ(batch.get(original).data() + 2, batch.get(overlapping).data());
    EXPECT_EQ(budget.used_bytes(), 8U);
    EXPECT_EQ(reader.calls, 1U);
}

TEST(IndexQueryIoBatch, OversizedFirstRangeStillRequiresARangeSlotAndMemory) {
    CountingIoReader reader;
    MemoryBudget budget(7);
    IoBatch no_ranges(budget, {.bytes = 4, .ranges = 0});
    bool accepted = false;
    size_t handle = 99;
    ASSERT_TRUE(no_ranges.try_add(reader, 0, 8, &accepted, &handle, true).ok());
    EXPECT_FALSE(accepted);
    EXPECT_EQ(handle, 99U);
    IoBatch no_memory(budget, {.bytes = 4, .ranges = 1});
    ASSERT_TRUE(no_memory.try_add(reader, 0, 8, &accepted, &handle, true).ok());
    ASSERT_TRUE(accepted);
    EXPECT_TRUE(no_memory.fetch().is<ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_EQ(reader.calls, 0U);
    EXPECT_EQ(budget.used_bytes(), 0U);
}

} // namespace
} // namespace doris::index_query
