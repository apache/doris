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

#include "storage/index/snii/writer/term_posting_source.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/writer/memory_reporter.h"

namespace doris::snii::writer {
namespace {

template <size_t N>
void expect_values(std::span<const uint32_t> actual, const std::array<uint32_t, N>& expected) {
    ASSERT_EQ(actual.size(), expected.size());
    EXPECT_TRUE(std::equal(actual.begin(), actual.end(), expected.begin()));
}

TEST(SniiTermPostingBufferTest, AppendsPositionedChunksAndReusesReservedCapacity) {
    MemoryReporter reporter;
    {
        TermPostingBuffer buffer(&reporter);
        const std::array<uint32_t, 2> first_docids {1, 4};
        const std::array<uint32_t, 2> first_freqs {2, 1};
        const std::array<uint32_t, 3> first_positions {0, 3, 9};
        ASSERT_TRUE(buffer.append(first_docids, first_freqs, first_positions).ok());
        EXPECT_EQ(buffer.document_count(), 2U);
        expect_values(buffer.docids(), first_docids);
        expect_values(buffer.freqs(), first_freqs);
        expect_values(buffer.positions_flat(), first_positions);

        const int64_t charged_bytes = reporter.current_bytes();
        EXPECT_GT(charged_bytes, 0);
        buffer.clear_reuse();
        EXPECT_TRUE(buffer.empty());
        EXPECT_EQ(reporter.current_bytes(), charged_bytes);

        const std::array<uint32_t, 1> second_docids {7};
        const std::array<uint32_t, 1> second_freqs {1};
        const std::array<uint32_t, 1> second_positions {2};
        ASSERT_TRUE(buffer.append(second_docids, second_freqs, second_positions).ok());
        expect_values(buffer.docids(), second_docids);
        EXPECT_EQ(reporter.current_bytes(), charged_bytes);
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiTermPostingBufferTest, AcceptsDocsOnlyAndRejectsInvalidShapesWithoutMutation) {
    TermPostingBuffer buffer(nullptr);
    const std::array<uint32_t, 2> docids {2, 9};
    ASSERT_TRUE(buffer.append(docids, {}, {}).ok());
    expect_values(buffer.docids(), docids);
    EXPECT_TRUE(buffer.freqs().empty());
    EXPECT_TRUE(buffer.positions_flat().empty());

    const std::array<uint32_t, 1> wrong_freqs {1};
    EXPECT_TRUE(buffer.append(docids, wrong_freqs, {}).is<ErrorCode::INVALID_ARGUMENT>());
    expect_values(buffer.docids(), docids);

    const std::array<uint32_t, 2> freqs {1, 2};
    const std::array<uint32_t, 2> too_few_positions {0, 1};
    EXPECT_TRUE(buffer.append(docids, freqs, too_few_positions).is<ErrorCode::INVALID_ARGUMENT>());
    expect_values(buffer.docids(), docids);

    buffer.clear_reuse();
    ASSERT_TRUE(buffer.append(docids, freqs, {}).ok());
    expect_values(buffer.docids(), docids);
    expect_values(buffer.freqs(), freqs);
    EXPECT_TRUE(buffer.positions_flat().empty());
}

TEST(SniiTermPostingBufferTest, RejectsReplacementBeforeGrowingPastHardLimit) {
    constexpr uint64_t kInitialBytes = 6 * sizeof(uint32_t);
    MemoryReporter reporter(nullptr, kInitialBytes, MemoryReporter::CapPolicy::kHardLimit);
    {
        TermPostingBuffer buffer(&reporter);
        const std::array<uint32_t, 2> docids {1, 2};
        const std::array<uint32_t, 2> freqs {1, 1};
        const std::array<uint32_t, 2> positions {0, 0};
        ASSERT_TRUE(buffer.append(docids, freqs, positions).ok());
        ASSERT_EQ(reporter.current_bytes(), static_cast<int64_t>(kInitialBytes));

        EXPECT_TRUE(buffer.append(docids, freqs, positions).is<ErrorCode::MEM_LIMIT_EXCEEDED>());
        expect_values(buffer.docids(), docids);
        expect_values(buffer.freqs(), freqs);
        expect_values(buffer.positions_flat(), positions);
        EXPECT_EQ(reporter.current_bytes(), static_cast<int64_t>(kInitialBytes));
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiTermPostingBufferTest, ProvidesDirectWritableFillStorage) {
    MemoryReporter reporter;
    TermPostingBuffer buffer(&reporter);
    MutableTermPostingSpan span;

    ASSERT_TRUE(buffer.grow_uninitialized(/*document_count=*/2, /*has_freqs=*/true,
                                          /*position_count=*/3, &span)
                        .ok());
    ASSERT_EQ(span.docids.size(), 2U);
    ASSERT_EQ(span.freqs.size(), 2U);
    ASSERT_EQ(span.positions_flat.size(), 3U);
    span.docids[0] = 7;
    span.docids[1] = 11;
    span.freqs[0] = 1;
    span.freqs[1] = 2;
    span.positions_flat[0] = 3;
    span.positions_flat[1] = 5;
    span.positions_flat[2] = 8;

    expect_values(buffer.docids(), std::array<uint32_t, 2> {7, 11});
    expect_values(buffer.freqs(), std::array<uint32_t, 2> {1, 2});
    expect_values(buffer.positions_flat(), std::array<uint32_t, 3> {3, 5, 8});
    EXPECT_GT(reporter.current_bytes(), 0);
}

TEST(SniiTermPostingBufferTest, ReleasesOversizedLaneBeforeLoweringCharge) {
    constexpr size_t kRetainedCapacity = 8192;
    MemoryReporter reporter;
    TermPostingBuffer buffer(&reporter);
    MutableTermPostingSpan span;
    ASSERT_TRUE(buffer.grow_uninitialized(/*document_count=*/2, /*has_freqs=*/true,
                                          /*position_count=*/kRetainedCapacity + 1, &span)
                        .ok());
    ASSERT_EQ(reporter.current_bytes(),
              static_cast<int64_t>((kRetainedCapacity + 5) * sizeof(uint32_t)));

    buffer.clear_reuse_and_release_excess(kRetainedCapacity);
    EXPECT_TRUE(buffer.empty());
    EXPECT_EQ(reporter.current_bytes(), 4 * static_cast<int64_t>(sizeof(uint32_t)));

    ASSERT_TRUE(buffer.grow_uninitialized(/*document_count=*/1, /*has_freqs=*/true,
                                          /*position_count=*/1, &span)
                        .ok());
    EXPECT_EQ(reporter.current_bytes(), 5 * static_cast<int64_t>(sizeof(uint32_t)));
}

TEST(SniiTermPostingBufferTest, AppendsPositionsIncrementallyToWritableFill) {
    TermPostingBuffer buffer(nullptr);
    MutableTermPostingSpan documents;
    ASSERT_TRUE(buffer.grow_uninitialized(/*document_count=*/2, /*has_freqs=*/true,
                                          /*position_count=*/0, &documents)
                        .ok());
    documents.docids[0] = 3;
    documents.docids[1] = 8;
    documents.freqs[0] = 1;
    documents.freqs[1] = 2;

    ASSERT_TRUE(buffer.append_position(4).ok());
    ASSERT_TRUE(buffer.append_position(2).ok());
    ASSERT_TRUE(buffer.append_position(9).ok());

    expect_values(buffer.docids(), std::array<uint32_t, 2> {3, 8});
    expect_values(buffer.freqs(), std::array<uint32_t, 2> {1, 2});
    expect_values(buffer.positions_flat(), std::array<uint32_t, 3> {4, 2, 9});
}

TEST(SniiTermPostingBufferTest, RejectsPositionGrowthBeforeExceedingHardLimit) {
    constexpr uint64_t kHardLimitBytes = 5 * sizeof(uint32_t);
    MemoryReporter reporter(nullptr, kHardLimitBytes, MemoryReporter::CapPolicy::kHardLimit);
    {
        TermPostingBuffer buffer(&reporter);
        MutableTermPostingSpan document;
        ASSERT_TRUE(buffer.grow_uninitialized(/*document_count=*/1, /*has_freqs=*/true,
                                              /*position_count=*/0, &document)
                            .ok());
        document.docids[0] = 7;
        document.freqs[0] = 2;
        ASSERT_TRUE(buffer.append_position(1).ok());
        ASSERT_TRUE(buffer.append_position(5).ok());
        ASSERT_EQ(reporter.current_bytes(), 4 * static_cast<int64_t>(sizeof(uint32_t)));

        EXPECT_TRUE(buffer.append_position(9).is<ErrorCode::MEM_LIMIT_EXCEEDED>());
        expect_values(buffer.docids(), std::array<uint32_t, 1> {7});
        expect_values(buffer.freqs(), std::array<uint32_t, 1> {2});
        expect_values(buffer.positions_flat(), std::array<uint32_t, 2> {1, 5});
        EXPECT_EQ(reporter.current_bytes(), 4 * static_cast<int64_t>(sizeof(uint32_t)));
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiTermPostingBufferTest, GrowsIncrementalRunsGeometrically) {
    size_t positive_reservations = 0;
    MemoryReporter reporter([&](int64_t delta) {
        if (delta > 0) {
            ++positive_reservations;
        }
    });
    TermPostingBuffer buffer(&reporter);
    for (uint32_t docid = 0; docid < 8; ++docid) {
        const std::array<uint32_t, 1> docids {docid};
        const std::array<uint32_t, 1> freqs {1};
        const std::array<uint32_t, 1> positions {docid};
        ASSERT_TRUE(buffer.append(docids, freqs, positions).ok());
    }

    EXPECT_EQ(buffer.document_count(), 8U);
    EXPECT_EQ(positive_reservations, 7U);
}

TEST(SniiTermPostingSourceTest, SpanSourceFillsExactWindowsWithoutOwningPostings) {
    const std::vector<uint32_t> docids {1, 4, 9};
    const std::vector<uint32_t> freqs {2, 1, 3};
    const std::vector<uint32_t> positions {0, 2, 1, 3, 7, 8};
    SpanTermPostingSource source(docids, freqs, positions);
    TermPostingBuffer buffer(nullptr);
    bool exhausted = false;

    ASSERT_TRUE(source.fill(2, &buffer, &exhausted).ok());
    EXPECT_FALSE(exhausted);
    expect_values(buffer.docids(), std::array<uint32_t, 2> {1, 4});
    expect_values(buffer.freqs(), std::array<uint32_t, 2> {2, 1});
    expect_values(buffer.positions_flat(), std::array<uint32_t, 3> {0, 2, 1});

    buffer.clear_reuse();
    ASSERT_TRUE(source.fill(2, &buffer, &exhausted).ok());
    EXPECT_TRUE(exhausted);
    expect_values(buffer.docids(), std::array<uint32_t, 1> {9});
    expect_values(buffer.freqs(), std::array<uint32_t, 1> {3});
    expect_values(buffer.positions_flat(), std::array<uint32_t, 3> {3, 7, 8});
}

} // namespace
} // namespace doris::snii::writer
