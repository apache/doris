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

// SniiSegmentReader dispatch over blob logical index entries: open() accepts
// them (validating file bounds), blob_entry() exposes them, open_index()
// refuses them EXPLICITLY (they are not text inverted indexes), and
// prepare_rewrite_snapshot() refuses whole containers holding them -- blob hot
// files live inside the metadata area, which the physical-prefix inherit model
// does not cover yet.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/bootstrap_header.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/format/tail_pointer.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"

using namespace doris::snii;
using namespace doris::snii::format;
using doris::Status;

namespace {

constexpr size_t kV1TailPointerSize = 31;

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_seg_blob_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

std::vector<uint8_t> EncodeV1Tail(uint64_t directory_offset, Slice directory) {
    ByteSink covered;
    covered.put_fixed32(kTailMagic);
    covered.put_fixed16(1);
    covered.put_fixed64(directory_offset);
    covered.put_fixed64(directory.size());
    covered.put_fixed32(doris::snii::crc32c(directory));
    covered.put_u8(kV1TailPointerSize);
    ByteSink tail;
    tail.put_bytes(covered.view());
    tail.put_fixed32(doris::snii::crc32c(covered.view()));
    return tail.buffer();
}

LogicalIndexMetadataRef InvertedEntry(uint64_t index_id, std::string suffix, uint64_t group_off) {
    LogicalIndexMetadataRef entry;
    entry.index_id = index_id;
    entry.index_suffix = std::move(suffix);
    entry.core_metadata = {.offset = group_off, .length = 5};
    entry.sampled_term_index = {.offset = group_off + 5, .length = 5};
    entry.dict_block_directory = {.offset = group_off + 10, .length = 5};
    return entry;
}

LogicalIndexMetadataRef BkdEntry(uint64_t index_id, std::string suffix,
                                 std::vector<NamedBlobFileRef> files) {
    LogicalIndexMetadataRef entry;
    entry.index_id = index_id;
    entry.index_suffix = std::move(suffix);
    entry.kind = LogicalIndexKind::kBkd;
    entry.files = std::move(files);
    return entry;
}

// Container = [bootstrap][zero padding up to directory][directory][tail].
// Entry refs are fabricated; only structural validation runs on them here.
std::string CraftContainer(const std::vector<LogicalIndexMetadataRef>& entries,
                           uint64_t directory_offset) {
    ByteSink bootstrap;
    BootstrapHeader bh;
    bh.tail_pointer_size = static_cast<uint8_t>(tail_pointer_size());
    EXPECT_TRUE(encode_bootstrap_header(bh, &bootstrap).ok());

    ByteSink directory;
    EXPECT_TRUE(encode_metadata_directory(entries, &directory).ok());

    std::vector<uint8_t> file = bootstrap.buffer();
    EXPECT_LE(file.size(), directory_offset);
    file.resize(directory_offset, 0);
    file.insert(file.end(), directory.buffer().begin(), directory.buffer().end());
    const auto tail = EncodeV1Tail(directory_offset, directory.view());
    file.insert(file.end(), tail.begin(), tail.end());

    const std::string path = TempPath();
    io::LocalFileWriter writer;
    EXPECT_TRUE(writer.open(path).ok());
    EXPECT_TRUE(writer.append(Slice(file)).ok());
    EXPECT_TRUE(writer.finalize().ok());
    return path;
}

constexpr uint64_t kDirectoryOffset = 400;

std::string CraftMixedContainer() {
    return CraftContainer(
            {InvertedEntry(7, "text", 300),
             BkdEntry(9, "bkd",
                      {{.name = "bkd", .offset = 64, .length = 20, .crc32c = 0},
                       {.name = "bkd_meta", .offset = 320, .length = 12, .crc32c = 0},
                       {.name = "bkd_index", .offset = 332, .length = 0, .crc32c = 0}})},
            kDirectoryOffset);
}

TEST(SniiSegmentReaderBlob, OpenAcceptsBlobEntriesAndExposesThem) {
    const std::string path = CraftMixedContainer();
    io::LocalFileReader reader;
    ASSERT_TRUE(reader.open(path).ok());
    reader::SniiSegmentReader segment;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&reader, &segment).ok());
    EXPECT_EQ(2U, segment.n_logical_indexes());

    bool exists = false;
    ASSERT_TRUE(segment.index_exists(9, "bkd", &exists).ok());
    EXPECT_TRUE(exists);

    const LogicalIndexMetadataRef* entry = nullptr;
    ASSERT_TRUE(segment.blob_entry(9, "bkd", &entry).ok());
    ASSERT_NE(nullptr, entry);
    EXPECT_EQ(LogicalIndexKind::kBkd, entry->kind);
    EXPECT_EQ(3U, entry->files.size());

    // blob_entry on a TEXT index is a kind mismatch, not a lookup miss.
    const Status text = segment.blob_entry(7, "text", &entry);
    EXPECT_TRUE(text.is<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << text;
    const Status missing = segment.blob_entry(42, "", &entry);
    EXPECT_TRUE(missing.is<doris::ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>()) << missing;
    std::remove(path.c_str());
}

TEST(SniiSegmentReaderBlob, OpenIndexOnBlobEntryFailsExplicitly) {
    const std::string path = CraftMixedContainer();
    io::LocalFileReader reader;
    ASSERT_TRUE(reader.open(path).ok());
    reader::SniiSegmentReader segment;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&reader, &segment).ok());

    reader::LogicalIndexReader index;
    const Status status = segment.open_index(9, "bkd", &index);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status;
    std::remove(path.c_str());
}

TEST(SniiSegmentReaderBlob, OpenRejectsBlobFileOutsideMetadataDirectoryBound) {
    // 395 + 10 crosses the directory at 400: registered blob bytes may never
    // overlap the directory.
    const std::string path = CraftContainer(
            {BkdEntry(9, "bkd", {{.name = "bkd", .offset = 395, .length = 10, .crc32c = 0}})},
            kDirectoryOffset);
    io::LocalFileReader reader;
    ASSERT_TRUE(reader.open(path).ok());
    reader::SniiSegmentReader segment;
    const Status status = reader::SniiSegmentReader::open(&reader, &segment);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    std::remove(path.c_str());
}

// A rewrite driven by the TEXT index list decides what to carry over before it
// ever takes a snapshot, so it needs to know a blob entry is present even when
// nothing is inheritable -- otherwise the blob entry is silently left out of the
// new container.
TEST(SniiSegmentReaderBlob, HasBlobIndexReportsNonInvertedEntries) {
    const std::string mixed = CraftMixedContainer();
    io::LocalFileReader mixed_reader;
    ASSERT_TRUE(mixed_reader.open(mixed).ok());
    reader::SniiSegmentReader mixed_segment;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&mixed_reader, &mixed_segment).ok());
    EXPECT_TRUE(mixed_segment.has_blob_index());
    std::remove(mixed.c_str());

    const std::string text_only = CraftContainer({InvertedEntry(7, "text", 300)}, kDirectoryOffset);
    io::LocalFileReader text_reader;
    ASSERT_TRUE(text_reader.open(text_only).ok());
    reader::SniiSegmentReader text_segment;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&text_reader, &text_segment).ok());
    EXPECT_FALSE(text_segment.has_blob_index());
    std::remove(text_only.c_str());
}

TEST(SniiSegmentReaderBlob, PrepareRewriteSnapshotRefusesContainersWithBlobEntries) {
    // Blob HOT files live inside the metadata area, which the physical-prefix
    // inherit model does not cover; a rewrite over such a container must fail
    // loudly BEFORE reading any metadata group, whichever keys are kept.
    const std::string path = CraftMixedContainer();
    io::LocalFileReader reader;
    ASSERT_TRUE(reader.open(path).ok());
    reader::SniiSegmentReader segment;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&reader, &segment).ok());

    reader::SniiRewriteSnapshot snapshot;
    const Status keep_text = segment.prepare_rewrite_snapshot(
            {{.index_id = 7, .index_suffix = "text"}}, 8, &snapshot);
    EXPECT_TRUE(keep_text.is<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << keep_text;

    const Status keep_blob = segment.prepare_rewrite_snapshot(
            {{.index_id = 9, .index_suffix = "bkd"}}, 8, &snapshot);
    EXPECT_TRUE(keep_blob.is<doris::ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << keep_blob;
    std::remove(path.c_str());
}

} // namespace
