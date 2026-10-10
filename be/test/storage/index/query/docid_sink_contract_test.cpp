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
#include <cstdint>
#include <limits>
#include <vector>

#include "storage/index/query/docid_sink.h"
#include "storage/index/query/roaring_docid_sink.h"

namespace doris::index_query {
namespace {

TEST(DocIdSinkContractTest, AppendsSortedSpansAndHalfOpenRanges) {
    std::vector<uint32_t> docs;
    VectorDocIdSink sink(docs);
    EXPECT_FALSE(sink.dedups());
    ASSERT_TRUE(sink.append_sorted({}).ok());
    const std::vector<uint32_t> first {0, 2, 7};
    ASSERT_TRUE(sink.append_sorted(first).ok());
    ASSERT_TRUE(sink.append_range(8, 11).ok());
    ASSERT_TRUE(sink.append_range(11, 11).ok());
    EXPECT_EQ(docs, (std::vector<uint32_t> {0, 2, 7, 8, 9, 10}));
}

TEST(DocIdSinkContractTest, SupportsTheLastDocumentAndRejectsOutOfRangeEnd) {
    std::vector<uint32_t> docs;
    VectorDocIdSink sink(docs);
    constexpr uint32_t kLast = std::numeric_limits<uint32_t>::max();
    ASSERT_TRUE(sink.append_range(kLast - 2, static_cast<uint64_t>(kLast) + 1).ok());
    const std::vector<uint32_t> expected {kLast - 2, kLast - 1, kLast};
    EXPECT_EQ(docs, expected);
    EXPECT_FALSE(sink.append_range(kLast, static_cast<uint64_t>(kLast) + 2).ok());
    EXPECT_EQ(docs, expected);
}

TEST(DocIdSinkContractTest, RepeatedSmallRangesKeepGeometricCapacityGrowth) {
    std::vector<uint32_t> docs;
    VectorDocIdSink sink(docs);
    size_t reallocations = 0;
    for (uint32_t doc = 0; doc < 4096; ++doc) {
        const size_t previous = docs.capacity();
        ASSERT_TRUE(sink.append_range(doc, doc + 1).ok());
        reallocations += previous != docs.capacity();
    }
    EXPECT_LE(reallocations, 14);
    ASSERT_EQ(docs.size(), 4096);
    for (uint32_t doc = 0; doc < docs.size(); ++doc) {
        EXPECT_EQ(docs[doc], doc);
    }
}

TEST(DocIdSinkContractTest, RoaringSinkDeduplicatesAndOrdersMixedBatches) {
    roaring::Roaring bitmap;
    RoaringDocIdSink sink(bitmap);
    EXPECT_TRUE(sink.dedups());
    const std::vector<uint32_t> first {2, 4, 9};
    ASSERT_TRUE(sink.append_sorted(first).ok());
    ASSERT_TRUE(sink.append_range(0, 5).ok());
    sink.append(7);
    sink.append(7);
    const std::vector<uint32_t> actual(bitmap.begin(), bitmap.end());
    EXPECT_EQ(actual, (std::vector<uint32_t> {0, 1, 2, 3, 4, 7, 9}));
}

TEST(DocIdSinkContractTest, RoaringSinkSupportsTheLastDocument) {
    roaring::Roaring bitmap;
    RoaringDocIdSink sink(bitmap);
    constexpr uint32_t kLast = std::numeric_limits<uint32_t>::max();
    ASSERT_TRUE(sink.append_range(kLast - 2, static_cast<uint64_t>(kLast) + 1).ok());
    EXPECT_EQ(bitmap.cardinality(), 3);
    EXPECT_TRUE(bitmap.contains(kLast - 2));
    EXPECT_TRUE(bitmap.contains(kLast - 1));
    EXPECT_TRUE(bitmap.contains(kLast));
}

TEST(DocIdSinkContractTest, RoaringSinkRejectsOutOfRangeEndWithoutChangingTheResult) {
    roaring::Roaring bitmap;
    bitmap.add(17);
    const roaring::Roaring expected = bitmap;
    RoaringDocIdSink sink(bitmap);
    constexpr uint64_t kTooLarge = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 2;
    EXPECT_FALSE(sink.append_range(0, kTooLarge).ok());
    EXPECT_EQ(bitmap, expected);
}

} // namespace
} // namespace doris::index_query
