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

#include "io/fs/file_range_coalescer.h"

#include <gtest/gtest.h>

#include <vector>

namespace doris::io {
namespace {

FileRangeCoalesceOptions options(size_t max_gap_bytes, size_t max_range_bytes,
                                 double max_read_amplification_ratio) {
    return {.max_gap_bytes = max_gap_bytes,
            .max_range_bytes = max_range_bytes,
            .max_read_amplification_ratio = max_read_amplification_ratio};
}

FileRange file_range(size_t offset, size_t size) {
    return {.offset = offset, .size = size};
}

std::vector<FileRange> coalesce(const std::vector<FileRange>& ranges,
                                const FileRangeCoalesceOptions& coalesce_options) {
    EXPECT_TRUE(coalesce_options.validate().ok());
    return FileRangeCoalescer::coalesce(ranges, coalesce_options);
}

TEST(FileRangeCoalescerTest, EmptyInputProducesEmptyOutput) {
    EXPECT_TRUE(coalesce({}, options(0, 1, 1.0)).empty());
}

TEST(FileRangeCoalescerTest, AcceptsAllThreeLimitsAtTheirBoundary) {
    const std::vector<FileRange> ranges {file_range(0, 32), file_range(96, 32)};
    const auto result = coalesce(ranges, options(64, 128, 2.0));

    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ(result.front(), file_range(0, 128));
}

TEST(FileRangeCoalescerTest, SplitsWhenGapExceedsLimit) {
    const std::vector<FileRange> ranges {file_range(0, 32), file_range(97, 32)};
    const auto result = coalesce(ranges, options(64, 1024, 4.0));

    EXPECT_EQ(result, (std::vector<FileRange> {file_range(0, 32), file_range(97, 32)}));
}

TEST(FileRangeCoalescerTest, SplitsWhenRangeSpanExceedsLimit) {
    const std::vector<FileRange> ranges {file_range(0, 80), file_range(100, 80)};
    const auto result = coalesce(ranges, options(32, 160, 2.0));

    EXPECT_EQ(result, (std::vector<FileRange> {file_range(0, 80), file_range(100, 80)}));
}

TEST(FileRangeCoalescerTest, AppliesReadAmplificationToTheWholeCandidateRange) {
    const std::vector<FileRange> ranges {file_range(0, 100), file_range(150, 100),
                                         file_range(300, 100)};
    const auto result = coalesce(ranges, options(50, 1024, 1.3));

    EXPECT_EQ(result, (std::vector<FileRange> {file_range(0, 250), file_range(300, 100)}));
}

TEST(FileRangeCoalescerTest, KeepsContiguousInputSplittableAtAtomicBoundaries) {
    const std::vector<FileRange> ranges {file_range(0, 80), file_range(80, 80),
                                         file_range(160, 80)};
    const auto result = coalesce(ranges, options(0, 160, 1.0));

    EXPECT_EQ(result, (std::vector<FileRange> {file_range(0, 160), file_range(160, 80)}));
}

TEST(FileRangeCoalescerTest, KeepsInputLargerThanMaxRangeAsOneRead) {
    const std::vector<FileRange> ranges {file_range(0, 192), file_range(192, 32),
                                         file_range(256, 32)};
    const auto result = coalesce(ranges, options(64, 128, 2.0));

    EXPECT_EQ(result, (std::vector<FileRange> {file_range(0, 192), file_range(192, 96)}));
}

} // namespace
} // namespace doris::io
