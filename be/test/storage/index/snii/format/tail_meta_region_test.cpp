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

#include "snii/format/tail_meta_region.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/encoding/byte_sink.h"

using namespace snii;
using doris::Status;
using snii::format::TailMetaRegionBuilder;
using snii::format::TailMetaRegionReader;

namespace {

std::vector<uint8_t> Pattern(size_t n, uint8_t base) {
    std::vector<uint8_t> v(n);
    for (size_t i = 0; i < n; ++i) {
        v[i] = static_cast<uint8_t>(base + i);
    }
    return v;
}

} // namespace

// Round-trip: add per-index meta blocks (opaque payloads), then locate each by
// (index_id, suffix) and verify the exact bytes come back.
TEST(SniiTailMetaRegion, RoundTripLocate) {
    auto a = Pattern(10, 0);
    auto b = Pattern(20, 100);

    TailMetaRegionBuilder builder;
    builder.add_index(7, "", Slice(a));
    builder.add_index(7, "suffix", Slice(b));

    ByteSink sink;
    builder.finish(&sink);

    TailMetaRegionReader reader;
    ASSERT_TRUE(TailMetaRegionReader::open(sink.view(), &reader).ok());
    EXPECT_EQ(reader.n_logical_indexes(), 2U);

    bool found = false;
    Slice block;
    ASSERT_TRUE(reader.find(7, "", &found, &block).ok());
    ASSERT_TRUE(found);
    ASSERT_EQ(block.size(), a.size());
    EXPECT_EQ(block[0], 0U);
    EXPECT_EQ(block[9], 9U);

    ASSERT_TRUE(reader.find(7, "suffix", &found, &block).ok());
    ASSERT_TRUE(found);
    ASSERT_EQ(block.size(), b.size());
    EXPECT_EQ(block[0], 100U);

    // Missing key.
    ASSERT_TRUE(reader.find(99, "", &found, &block).ok());
    EXPECT_FALSE(found);
}

// A single-index region round-trips.
TEST(SniiTailMetaRegion, SingleIndex) {
    auto a = Pattern(5, 42);
    TailMetaRegionBuilder builder;
    builder.add_index(1, "x", Slice(a));
    ByteSink sink;
    builder.finish(&sink);

    TailMetaRegionReader reader;
    ASSERT_TRUE(TailMetaRegionReader::open(sink.view(), &reader).ok());
    EXPECT_EQ(reader.n_logical_indexes(), 1U);
    bool found = false;
    Slice block;
    ASSERT_TRUE(reader.find(1, "x", &found, &block).ok());
    ASSERT_TRUE(found);
    EXPECT_EQ(block[0], 42U);
}

// Corruption of the region (a byte inside the meta_region_checksum coverage) is
// detected at open().
TEST(SniiTailMetaRegion, DetectsCorruption) {
    auto a = Pattern(10, 0);
    TailMetaRegionBuilder builder;
    builder.add_index(7, "", Slice(a));
    ByteSink sink;
    builder.finish(&sink);

    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GT(bytes.size(), 8U);
    bytes[bytes.size() / 2] ^= 0xFF; // flip a covered byte

    TailMetaRegionReader reader;
    Status s = TailMetaRegionReader::open(Slice(bytes), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// Empty region (no logical indexes) round-trips.
TEST(SniiTailMetaRegion, EmptyRegion) {
    TailMetaRegionBuilder builder;
    ByteSink sink;
    builder.finish(&sink);
    TailMetaRegionReader reader;
    ASSERT_TRUE(TailMetaRegionReader::open(sink.view(), &reader).ok());
    EXPECT_EQ(reader.n_logical_indexes(), 0U);
    bool found = true;
    Slice block;
    ASSERT_TRUE(reader.find(1, "", &found, &block).ok());
    EXPECT_FALSE(found);
}
