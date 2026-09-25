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

// A VARIANT column copies every index definition to each materialized subcolumn,
// so one container holds one logical index per (definition, subcolumn), and the
// definitions on one subcolumn share its suffix and its NULL rows. The compound
// writer stores their identical null bitmap once and points every such index at
// that region. These tests pin down when that happens, that every index still
// reads its own NULL rows back through the load, compaction and rewrite paths,
// and how many bytes it saves.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace {

using namespace doris::snii;            // NOLINT
using namespace doris::snii::snii_test; // NOLINT
using doris::Status;
using format::RegionRef;
using format::SectionRefs;
using writer::SniiCompoundWriter;
using writer::SniiIndexInput;
using writer::SniiStreamedIndexSession;

// The escaped suffix shape a VARIANT subcolumn index carries (v.payload.title).
const std::string kPath = "v%2Epayload%2Etitle";
const std::string kOtherPath = "v%2Epayload%2Ebody";

// A text index on `suffix` whose NULL rows are `null_docids`; the first non-NULL
// document carries the term "x".
SniiIndexInput PathIndex(uint64_t index_id, const std::string& suffix, uint32_t doc_count,
                         std::vector<uint32_t> null_docids) {
    SniiIndexInput input;
    input.index_id = index_id;
    input.index_suffix = suffix;
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = doc_count;
    uint32_t first_present = 0;
    while (std::ranges::binary_search(null_docids, first_present)) {
        ++first_present;
    }
    input.terms = {make_term("x", {{.docid = first_present, .positions = {0}}})};
    input.null_docids = std::move(null_docids);
    return input;
}

MemoryFile WriteContainer(const std::vector<SniiIndexInput>& inputs) {
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    for (const SniiIndexInput& input : inputs) {
        EXPECT_TRUE(compound.add_logical_index(input).ok());
    }
    EXPECT_TRUE(compound.finish().ok());
    return file;
}

RegionRef NullRegion(MemoryFile* file, uint64_t index_id, const std::string& suffix) {
    reader::SniiSegmentReader segment;
    EXPECT_TRUE(reader::SniiSegmentReader::open(file, &segment).ok());
    SectionRefs refs;
    EXPECT_TRUE(segment.section_refs_for_index(index_id, suffix, &refs).ok());
    return refs.null_bitmap;
}

// Reads one index's NULL rows through the same path compaction uses, which also
// checks the bitmap's doc and null counts against that index's own stats.
std::vector<uint32_t> ReadNulls(MemoryFile* file, uint64_t index_id, const std::string& suffix) {
    reader::SniiSegmentReader segment;
    EXPECT_TRUE(reader::SniiSegmentReader::open(file, &segment).ok());
    reader::LogicalIndexReader index;
    EXPECT_TRUE(segment.open_index(index_id, suffix, &index).ok());
    std::vector<uint32_t> nulls;
    EXPECT_TRUE(index.read_null_docids(&nulls).ok());
    EXPECT_EQ(index.stats().null_count, nulls.size());
    return nulls;
}

size_t CountOccurrences(const std::vector<uint8_t>& haystack, const std::vector<uint8_t>& needle) {
    size_t count = 0;
    for (auto it = haystack.begin();; ++it) {
        it = std::search(it, haystack.end(), needle.begin(), needle.end());
        if (it == haystack.end()) {
            return count;
        }
        ++count;
    }
}

std::vector<uint8_t> Bytes(const MemoryFile& file, const RegionRef& region) {
    const auto begin = file.data().begin() + static_cast<std::ptrdiff_t>(region.offset);
    return {begin, begin + static_cast<std::ptrdiff_t>(region.length)};
}

} // namespace

TEST(SniiSharedNullBitmap, DefinitionsOnOneSubcolumnReferenceOneRegion) {
    const std::vector<uint32_t> nulls = {1, 3, 5, 6};
    MemoryFile file =
            WriteContainer({PathIndex(11, kPath, 8, nulls), PathIndex(12, kPath, 8, nulls),
                            PathIndex(13, kPath, 8, nulls)});

    const RegionRef first = NullRegion(&file, 11, kPath);
    ASSERT_GT(first.length, 0U);
    for (uint64_t index_id : {12, 13}) {
        const RegionRef region = NullRegion(&file, index_id, kPath);
        EXPECT_EQ(region.offset, first.offset) << "index " << index_id;
        EXPECT_EQ(region.length, first.length) << "index " << index_id;
    }
    EXPECT_EQ(CountOccurrences(file.data(), Bytes(file, first)), 1U)
            << "the bitmap bytes must be stored once";
    for (uint64_t index_id : {11, 12, 13}) {
        EXPECT_EQ(ReadNulls(&file, index_id, kPath), nulls) << "index " << index_id;
    }
}

TEST(SniiSharedNullBitmap, DifferentBitmapsOnOneSubcolumnAreNotShared) {
    // Different NULL rows.
    {
        MemoryFile file =
                WriteContainer({PathIndex(11, kPath, 8, {1, 3}), PathIndex(12, kPath, 8, {1, 4})});
        EXPECT_NE(NullRegion(&file, 11, kPath).offset, NullRegion(&file, 12, kPath).offset);
        EXPECT_EQ(ReadNulls(&file, 11, kPath), (std::vector<uint32_t> {1, 3}));
        EXPECT_EQ(ReadNulls(&file, 12, kPath), (std::vector<uint32_t> {1, 4}));
    }
    // Same NULL rows, different document counts: the framed bitmap records the doc
    // count, and the reader rejects a bitmap whose doc count is not the index's own.
    {
        MemoryFile file =
                WriteContainer({PathIndex(11, kPath, 8, {1, 3}), PathIndex(12, kPath, 9, {1, 3})});
        EXPECT_NE(NullRegion(&file, 11, kPath).offset, NullRegion(&file, 12, kPath).offset);
        EXPECT_EQ(ReadNulls(&file, 11, kPath), (std::vector<uint32_t> {1, 3}));
        EXPECT_EQ(ReadNulls(&file, 12, kPath), (std::vector<uint32_t> {1, 3}));
    }
}

// Sharing is scoped to one suffix: indexes on different subcolumns keep their own
// sections contiguous even when their NULL rows happen to coincide.
TEST(SniiSharedNullBitmap, IdenticalBitmapsOnDifferentSubcolumnsAreNotShared) {
    const std::vector<uint32_t> nulls = {1, 3};
    MemoryFile file =
            WriteContainer({PathIndex(11, kPath, 8, nulls), PathIndex(11, kOtherPath, 8, nulls)});
    const RegionRef first = NullRegion(&file, 11, kPath);
    const RegionRef second = NullRegion(&file, 11, kOtherPath);
    EXPECT_NE(first.offset, second.offset);
    EXPECT_EQ(first.length, second.length);
    EXPECT_LT(first.offset + first.length, second.offset);
    EXPECT_EQ(ReadNulls(&file, 11, kPath), nulls);
    EXPECT_EQ(ReadNulls(&file, 11, kOtherPath), nulls);
}

// Compaction rebuilds every destination index through a streamed session. The
// sessions share the bitmap the same way, and each one still drops its bitmap bytes
// as soon as its session finishes: sharing retains nothing in memory.
TEST(SniiSharedNullBitmap, StreamedSessionsShareAndStillReleaseTheirBitmapBytes) {
    const std::vector<uint32_t> nulls = {0, 2, 7};
    writer::MemoryReporter reporter(nullptr, 1U << 20);
    MemoryFile file;
    SniiCompoundWriter compound(&file);
    for (uint64_t index_id : {21, 22}) {
        SniiIndexInput input = PathIndex(index_id, kPath, 8, nulls);
        input.terms.clear();
        input.mem_reporter = &reporter;
        SniiStreamedIndexSession* session = nullptr;
        assert_ok(compound.begin_streamed_index(std::move(input), &session));
        ASSERT_NE(session, nullptr);
        assert_ok(session->finish());
        EXPECT_EQ(reporter.current_bytes(), 0) << "index " << index_id;
    }
    assert_ok(compound.finish());

    const RegionRef first = NullRegion(&file, 21, kPath);
    const RegionRef second = NullRegion(&file, 22, kPath);
    ASSERT_GT(first.length, 0U);
    EXPECT_EQ(second.offset, first.offset);
    EXPECT_EQ(second.length, first.length);
    EXPECT_EQ(ReadNulls(&file, 21, kPath), nulls);
    EXPECT_EQ(ReadNulls(&file, 22, kPath), nulls);
}

// BUILD INDEX rewrites a container by copying its physical prefix and re-emitting
// the kept metadata groups. Dropping the index that wrote the shared region must not
// orphan the index that only references it.
TEST(SniiSharedNullBitmap, RewriteThatDropsTheWritingIndexKeepsTheSharedRegion) {
    const std::vector<uint32_t> nulls = {1, 2, 5};
    MemoryFile source =
            WriteContainer({PathIndex(11, kPath, 8, nulls), PathIndex(12, kPath, 8, nulls)});
    const RegionRef shared = NullRegion(&source, 12, kPath);
    ASSERT_EQ(shared.offset, NullRegion(&source, 11, kPath).offset);

    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&source, &segment));
    reader::SniiRewriteSnapshot snapshot;
    assert_ok(segment.prepare_rewrite_snapshot(
            {reader::LogicalIndexKey {.index_id = 12, .index_suffix = kPath}}, 8, &snapshot));
    EXPECT_GE(snapshot.physical_prefix_end(), shared.offset + shared.length);

    MemoryFile output;
    SniiCompoundWriter compound(&output);
    assert_ok(compound.inherit(snapshot, &source));
    // A new definition on the same subcolumn, added by the same rewrite.
    assert_ok(compound.add_logical_index(PathIndex(13, kPath, 8, nulls)));
    assert_ok(compound.finish());

    reader::SniiSegmentReader rewritten;
    assert_ok(reader::SniiSegmentReader::open(&output, &rewritten));
    EXPECT_EQ(rewritten.n_logical_indexes(), 2U);
    const RegionRef kept = NullRegion(&output, 12, kPath);
    EXPECT_EQ(kept.offset, shared.offset);
    EXPECT_EQ(kept.length, shared.length);
    EXPECT_EQ(ReadNulls(&output, 12, kPath), nulls);
    EXPECT_EQ(ReadNulls(&output, 13, kPath), nulls);
}

// Container bytes for N definitions on one subcolumn of a 1M-row segment whose NULL
// rows are pseudo-random (62.5% NULL, so the bitmap is made of bitset containers).
// The baseline puts the same N indexes on N distinct suffixes of equal length, which
// is exactly the layout before sharing: every index writes its own bitmap.
TEST(SniiSharedNullBitmap, SavesAllButOneCopyOfTheBitmap) {
    constexpr uint32_t kDocCount = 1'000'000;
    std::vector<uint32_t> nulls;
    for (uint32_t docid = 0; docid < kDocCount; ++docid) {
        if (((docid * 2654435761U) >> 28) < 10) {
            nulls.push_back(docid);
        }
    }

    for (size_t definitions : {2, 7}) {
        std::vector<SniiIndexInput> shared_inputs;
        std::vector<SniiIndexInput> separate_inputs;
        for (size_t i = 0; i < definitions; ++i) {
            shared_inputs.push_back(PathIndex(100 + i, kPath + "0", kDocCount, nulls));
            separate_inputs.push_back(
                    PathIndex(100 + i, kPath + std::to_string(i), kDocCount, nulls));
        }
        MemoryFile shared = WriteContainer(shared_inputs);
        MemoryFile separate = WriteContainer(separate_inputs);

        std::set<uint64_t> shared_regions;
        std::set<uint64_t> separate_regions;
        uint64_t bitmap_bytes = 0;
        for (size_t i = 0; i < definitions; ++i) {
            const RegionRef region = NullRegion(&shared, 100 + i, kPath + "0");
            shared_regions.insert(region.offset);
            bitmap_bytes = region.length;
            separate_regions.insert(
                    NullRegion(&separate, 100 + i, kPath + std::to_string(i)).offset);
        }
        EXPECT_EQ(shared_regions.size(), 1U);
        EXPECT_EQ(separate_regions.size(), definitions);
        EXPECT_EQ(ReadNulls(&shared, 100 + definitions - 1, kPath + "0"), nulls);

        const uint64_t expected_saving = (definitions - 1) * bitmap_bytes;
        const uint64_t saving = separate.data().size() - shared.data().size();
        // Section offsets are varints in the metadata, and they shrink along with the
        // container, so the measured saving can exceed the bitmap bytes by a few bytes.
        EXPECT_GE(saving, expected_saving);
        EXPECT_LE(saving, expected_saving + 16 * definitions);
        std::cout << "[shared-null-bitmap] definitions=" << definitions
                  << " null_rows=" << nulls.size() << "/" << kDocCount
                  << " bitmap_bytes=" << bitmap_bytes
                  << " container_bytes_separate=" << separate.data().size()
                  << " container_bytes_shared=" << shared.data().size() << " saved=" << saving
                  << std::endl;
    }
}
