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

#include "storage/index/inverted/query/exec/roaring_docid_sink.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <numeric>
#include <random>
#include <span>
#include <string>
#include <vector>

namespace doris::index_query {
namespace {

using Spans = std::vector<std::vector<uint32_t>>;

// The sink must leave exactly what adding the documents one by one leaves, in containers that hold
// to CRoaring's own invariants: sorted arrays, cardinalities that match their bits.
void ExpectSameAsAddingOneByOne(const roaring::Roaring& start, const Spans& spans) {
    roaring::Roaring expected = start;
    roaring::Roaring actual = start;
    RoaringDocIdSink sink(actual);
    for (const auto& span : spans) {
        for (const uint32_t docid : span) {
            expected.add(docid);
        }
        ASSERT_TRUE(sink.append_sorted(span).ok());
    }
    EXPECT_EQ(actual.cardinality(), expected.cardinality());
    EXPECT_TRUE(actual == expected);
    const char* reason = "";
    EXPECT_TRUE(roaring::api::roaring_bitmap_internal_validate(&actual.roaring, &reason)) << reason;
}

Spans Batches(const std::vector<uint32_t>& docids, size_t batch) {
    Spans spans;
    for (size_t off = 0; off < docids.size(); off += batch) {
        const size_t end = std::min(docids.size(), off + batch);
        spans.emplace_back(docids.begin() + off, docids.begin() + end);
    }
    return spans;
}

std::vector<uint32_t> Iota(uint32_t first, uint32_t count) {
    std::vector<uint32_t> docids(count);
    std::iota(docids.begin(), docids.end(), first);
    return docids;
}

roaring::api::roaring_statistics_t StatsOf(const roaring::Roaring& bitmap) {
    roaring::api::roaring_statistics_t stats;
    roaring::api::roaring_bitmap_statistics(&bitmap.roaring, &stats);
    return stats;
}

// Densities on both sides of the array limit, documents spread evenly or in bursts, batches from one
// document to the whole posting.
TEST(RoaringDocIdSinkTest, MatchesSingleAddsForRandomPostings) {
    std::mt19937 rng(20261030);
    for (const double density : {0.0005, 0.02, 0.0625, 0.07, 0.125, 0.4, 0.9}) {
        std::bernoulli_distribution take(density);
        for (const bool bursts : {false, true}) {
            const uint32_t step = bursts ? 2000 : 1;
            std::vector<uint32_t> docids;
            for (uint32_t doc = 0; doc < 400000; doc += step) {
                if (take(rng)) {
                    for (uint32_t i = 0; i < step; ++i) {
                        docids.push_back(doc + i);
                    }
                }
            }
            for (const size_t batch : {size_t {1}, size_t {7}, size_t {1024}, docids.size() + 1}) {
                SCOPED_TRACE("density=" + std::to_string(density) + " bursts=" +
                             std::to_string(bursts) + " batch=" + std::to_string(batch));
                ExpectSameAsAddingOneByOne({}, Batches(docids, batch));
            }
        }
    }
}

// An array holds at most 4096 documents. One batch that ends at the limit leaves an array; one that
// crosses it, at any point, leaves a bitset holding every document.
TEST(RoaringDocIdSinkTest, TurnsAnArrayIntoABitsetAtItsLimit) {
    roaring::Roaring at_limit;
    RoaringDocIdSink at_limit_sink(at_limit);
    ASSERT_TRUE(at_limit_sink.append_sorted(Iota(0, 4096)).ok());
    EXPECT_EQ(StatsOf(at_limit).n_array_containers, 1U);
    EXPECT_EQ(at_limit.cardinality(), 4096U);

    for (const uint32_t count : {4097U, 4098U, 5000U, 65536U}) {
        const std::vector<uint32_t> docids = Iota(0, count);
        ExpectSameAsAddingOneByOne({}, {docids});
        roaring::Roaring crossed;
        RoaringDocIdSink sink(crossed);
        ASSERT_TRUE(sink.append_sorted(docids).ok());
        EXPECT_EQ(StatsOf(crossed).n_bitset_containers, 1U);
    }
    // The limit is reached by the document that the bulk add puts in on its own.
    ExpectSameAsAddingOneByOne({}, {Iota(0, 4095), {5000}, {5001}, Iota(6000, 3000)});
    ExpectSameAsAddingOneByOne({}, {Iota(0, 4096), {5000, 5001}});
}

TEST(RoaringDocIdSinkTest, MergesIntoExistingContainers) {
    const std::vector<uint32_t> docids = Iota(1000, 3000);

    // Array with documents after the batch, and one with a document inside it.
    roaring::Roaring array_after;
    array_after.add(60000);
    ExpectSameAsAddingOneByOne(array_after, {docids});
    roaring::Roaring array_inside;
    array_inside.add(1500);
    ExpectSameAsAddingOneByOne(array_inside, {docids});
    roaring::Roaring array_before;
    array_before.add(999);
    ExpectSameAsAddingOneByOne(array_before, {docids});

    // Bitset that already holds some of the batch: those must not be counted twice.
    roaring::Roaring bitset;
    for (uint32_t doc = 0; doc < 20000; doc += 3) {
        bitset.add(doc);
    }
    ASSERT_EQ(StatsOf(bitset).n_bitset_containers, 1U);
    ExpectSameAsAddingOneByOne(bitset, {docids});

    // Run container.
    roaring::Roaring runs;
    runs.addRange(500, 40000);
    runs.runOptimize();
    ASSERT_EQ(StatsOf(runs).n_run_containers, 1U);
    ExpectSameAsAddingOneByOne(runs, {docids, Iota(39000, 3000)});
}

// A streamed union hands over independently sorted spans, so documents repeat across them.
TEST(RoaringDocIdSinkTest, UnionsIndependentlySortedSpans) {
    std::mt19937 rng(7);
    Spans spans;
    for (int term = 0; term < 20; ++term) {
        std::vector<uint32_t> docids;
        for (uint32_t doc = rng() % 50; doc < 200000; doc += 1 + rng() % 30) {
            docids.push_back(doc);
        }
        spans.push_back(std::move(docids));
    }
    ExpectSameAsAddingOneByOne({}, spans);
}

TEST(RoaringDocIdSinkTest, KeepsDocumentsOfNeighbouringChunksApart) {
    ExpectSameAsAddingOneByOne({}, {{65534, 65535, 65536, 65537, 131071, 131072}});
    constexpr uint32_t kLast = std::numeric_limits<uint32_t>::max();
    ExpectSameAsAddingOneByOne({}, {{0, 1, kLast - 65536, kLast - 65535, kLast - 1, kLast}});
    ExpectSameAsAddingOneByOne({}, {{}, {kLast}, {}});
}

// A sink only promises sorted spans, and a damaged posting may not even be that: every document
// must still land in its own chunk, and an array must stay free of repeats.
TEST(RoaringDocIdSinkTest, AcceptsSpansThatAreNotStrictlyAscending) {
    ExpectSameAsAddingOneByOne({}, {{5, 5, 6, 6, 6, 7}});
    ExpectSameAsAddingOneByOne({}, {{70000, 3, 140000, 2, 2, 70001}});
    std::vector<uint32_t> repeated = Iota(0, 5000);
    repeated.insert(repeated.begin() + 2500, 2499);
    ExpectSameAsAddingOneByOne({}, {repeated});
}

} // namespace
} // namespace doris::index_query
