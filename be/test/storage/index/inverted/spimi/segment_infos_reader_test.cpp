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

#include "storage/index/inverted/spimi/segment_infos_reader.h"

#include <gtest/gtest.h>

#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/segment_infos_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

TEST(SegmentInfosReaderTest, RoundTripsSingleSegment) {
    MemoryByteOutput out;
    SegmentInfoEntry seg;
    seg.name = "_0";
    seg.doc_count = 42;
    seg.del_gen = -1;
    seg.doc_store_offset = -1;
    seg.has_single_norm_file = true;
    seg.is_compound_file = -1;

    SegmentInfosWriter w;
    w.WriteSegmentsN(&out, /*version=*/1, /*counter=*/1, {seg});

    const auto m = SegmentInfosReader::Read(out.bytes());
    EXPECT_EQ(m.version, 1);
    EXPECT_EQ(m.counter, 1);
    ASSERT_EQ(m.segments.size(), 1U);
    EXPECT_EQ(m.segments[0].name, "_0");
    EXPECT_EQ(m.segments[0].doc_count, 42);
    EXPECT_EQ(m.segments[0].del_gen, -1);
    EXPECT_EQ(m.segments[0].doc_store_offset, -1);
    EXPECT_TRUE(m.segments[0].has_single_norm_file);
    EXPECT_EQ(m.segments[0].is_compound_file, -1);
}

TEST(SegmentInfosReaderTest, RoundTripsMultipleSegments) {
    MemoryByteOutput out;
    std::vector<SegmentInfoEntry> segs;
    for (int i = 0; i < 5; ++i) {
        SegmentInfoEntry s;
        s.name = "_" + std::to_string(i);
        s.doc_count = (i + 1) * 100;
        s.del_gen = -1;
        s.doc_store_offset = -1;
        s.has_single_norm_file = true;
        s.is_compound_file = -1;
        segs.push_back(std::move(s));
    }
    SegmentInfosWriter w;
    w.WriteSegmentsN(&out, /*version=*/7, /*counter=*/5, segs);

    const auto m = SegmentInfosReader::Read(out.bytes());
    EXPECT_EQ(m.version, 7);
    EXPECT_EQ(m.counter, 5);
    ASSERT_EQ(m.segments.size(), 5U);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(m.segments[i].name, "_" + std::to_string(i));
        EXPECT_EQ(m.segments[i].doc_count, (i + 1) * 100);
    }
}

TEST(SegmentInfosReaderTest, RoundTripsHandcraftedSpimiSegmentName) {
    // Matches the exact entry that `SpimiFulltextWriter::EmitSegment`
    // writes (segment_name="_spimi_0", doc_count from caller).
    MemoryByteOutput out;
    SegmentInfoEntry seg;
    seg.name = "_spimi_0";
    seg.doc_count = 123;
    seg.del_gen = -1;
    seg.doc_store_offset = -1;
    seg.has_single_norm_file = true;
    seg.is_compound_file = -1;
    SegmentInfosWriter w;
    w.WriteSegmentsN(&out, /*version=*/1, /*counter=*/1, {seg});

    const auto m = SegmentInfosReader::Read(out.bytes());
    ASSERT_EQ(m.segments.size(), 1U);
    EXPECT_EQ(m.segments[0].name, "_spimi_0");
    EXPECT_EQ(m.segments[0].doc_count, 123);
}

} // namespace doris::segment_v2::inverted_index::spimi
