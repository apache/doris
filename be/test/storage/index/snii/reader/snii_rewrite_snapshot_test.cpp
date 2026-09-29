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

// Tests for the SNII rewrite snapshot: the read-only, fully validated description
// of one container that BUILD INDEX inherits from. The snapshot must
//   1. describe every kept logical index (key, doc count, section refs, raw
//      metadata group bytes), and
//   2. bound the valid physical prefix so a byte copy of [0, prefix_end) carries
//      every kept section and no metadata, directory, padding or tail byte, and
//   3. refuse to describe a source it cannot fully validate -- a missing key, a
//      duplicate request, a doc count that disagrees with the segment, a corrupt
//      metadata blob, a section reference outside the physical area, or a corrupt
//      bootstrap header (whose bytes the prefix copy would otherwise propagate).

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <numeric>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/format/bootstrap_header.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/format/tail_pointer.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::reader;
using namespace doris::snii::writer;
namespace ErrorCode = doris::ErrorCode;
using doris::Status;

namespace {

constexpr uint32_t kDocCount = 3;
constexpr uint64_t kIndexIdA = 1;
constexpr uint64_t kIndexIdB = 2;
constexpr const char* kSuffixA = "title";
constexpr const char* kSuffixB = "body";

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_rewrite_snapshot_test_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".idx";
}

// An in-memory reader over an owned, mutable byte image.
class ImageFileReader final : public io::FileReader {
public:
    explicit ImageFileReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset > bytes_.size() || len > bytes_.size() - offset) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "image reader: read past EOF");
        }
        out->assign(bytes_.begin() + static_cast<std::ptrdiff_t>(offset),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(offset + len));
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }
    std::vector<uint8_t>& bytes() { return bytes_; }

private:
    std::vector<uint8_t> bytes_;
};

std::vector<TermPostings> TokenizeDocs(const std::vector<std::string>& docs, bool has_positions) {
    SpimiTermBuffer buffer(has_positions);
    for (uint32_t docid = 0; docid < docs.size(); ++docid) {
        const std::string& doc = docs[docid];
        uint32_t position = 0;
        size_t start = 0;
        while (start <= doc.size()) {
            const size_t space = doc.find(' ', start);
            const std::string token = doc.substr(
                    start, space == std::string::npos ? std::string::npos : space - start);
            if (!token.empty()) {
                buffer.add_token(token, docid, position++);
            }
            if (space == std::string::npos) {
                break;
            }
            start = space + 1;
        }
    }
    return buffer.finalize_sorted();
}

// Writes a two-logical-index container: (kIndexIdA, kSuffixA) then
// (kIndexIdB, kSuffixB), each over its own corpus so their sections differ in
// size and B's sections sit strictly after A's.
std::vector<uint8_t> BuildTwoIndexImage() {
    SniiIndexInput first;
    first.index_id = kIndexIdA;
    first.index_suffix = kSuffixA;
    first.config = IndexConfig::kDocsPositions;
    first.doc_count = kDocCount;
    first.terms = TokenizeDocs({"alpha bravo", "bravo charlie", "alpha charlie delta"},
                               /*has_positions=*/true);
    first.target_dict_block_bytes = 256;

    SniiIndexInput second;
    second.index_id = kIndexIdB;
    second.index_suffix = kSuffixB;
    second.config = IndexConfig::kDocsPositions;
    second.doc_count = kDocCount;
    second.terms = TokenizeDocs(
            {"echo foxtrot golf hotel", "foxtrot india juliett", "golf kilo lima mike november"},
            /*has_positions=*/true);
    second.target_dict_block_bytes = 256;

    const std::string path = TempPath();
    {
        io::LocalFileWriter writer;
        EXPECT_TRUE(writer.open(path).ok());
        SniiCompoundWriter compound(&writer);
        EXPECT_TRUE(compound.add_logical_index(first).ok());
        EXPECT_TRUE(compound.add_logical_index(second).ok());
        EXPECT_TRUE(compound.finish().ok());
    }
    io::LocalFileReader reader;
    EXPECT_TRUE(reader.open(path).ok());
    std::vector<uint8_t> bytes;
    EXPECT_TRUE(reader.read_at(0, reader.size(), &bytes).ok());
    std::remove(path.c_str());
    return bytes;
}

uint64_t TailDirectoryOffset(const std::vector<uint8_t>& image) {
    const size_t footer = tail_pointer_size();
    EXPECT_GE(image.size(), footer);
    TailPointer tail;
    EXPECT_TRUE(
            decode_tail_pointer(Slice(image.data() + image.size() - footer, footer), &tail).ok());
    return tail.directory_offset;
}

MetadataDirectory ReadDirectory(const std::vector<uint8_t>& image) {
    const size_t footer = tail_pointer_size();
    TailPointer tail;
    EXPECT_TRUE(
            decode_tail_pointer(Slice(image.data() + image.size() - footer, footer), &tail).ok());
    MetadataDirectory directory;
    EXPECT_TRUE(MetadataDirectory::decode(Slice(image.data() + tail.directory_offset,
                                                static_cast<size_t>(tail.directory_length)),
                                          &directory)
                        .ok());
    return directory;
}

// Offset where the metadata area begins, i.e. the end of the physical sections.
uint64_t MetadataAreaBegin(const std::vector<uint8_t>& image) {
    const MetadataDirectory directory = ReadDirectory(image);
    uint64_t begin = TailDirectoryOffset(image);
    for (const auto& entry : directory.entries()) {
        begin = std::min(begin, entry.core_metadata.offset);
    }
    return begin;
}

uint64_t SectionEnd(const SectionRefs& refs) {
    uint64_t end = 0;
    for (const RegionRef& region :
         {refs.dict_region, refs.posting_region, refs.norms, refs.null_bitmap, refs.bsbf}) {
        end = std::max(end, region.offset + region.length);
    }
    return end;
}

std::vector<uint8_t> Subrange(const std::vector<uint8_t>& image, uint64_t offset, uint64_t length) {
    EXPECT_LE(offset + length, image.size());
    return std::vector<uint8_t>(image.begin() + static_cast<std::ptrdiff_t>(offset),
                                image.begin() + static_cast<std::ptrdiff_t>(offset + length));
}

// Rebuilds the whole metadata area of `image`: it re-encodes each logical index's
// Core metadata (after `mutate_core` may change it), re-emits the STI and DBD
// blobs verbatim, then writes a fresh directory and tail. Physical sections are
// untouched, so the result is a real container differing only in metadata.
// Entries are visited in on-disk group order.
std::vector<uint8_t> RebuildMetadataArea(
        const std::vector<uint8_t>& image,
        const std::function<void(uint64_t, CoreMetadata*)>& mutate_core) {
    const MetadataDirectory directory = ReadDirectory(image);
    std::vector<size_t> order(directory.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&directory](size_t lhs, size_t rhs) {
        return directory.entries()[lhs].core_metadata.offset <
               directory.entries()[rhs].core_metadata.offset;
    });

    std::vector<uint8_t> out = Subrange(image, 0, MetadataAreaBegin(image));
    std::vector<LogicalIndexMetadataRef> entries;
    entries.reserve(directory.size());
    for (const size_t index : order) {
        const LogicalIndexMetadataRef& source = directory.entries()[index];
        CoreMetadata core;
        EXPECT_TRUE(decode_core_metadata(Slice(image.data() + source.core_metadata.offset,
                                               static_cast<size_t>(source.core_metadata.length)),
                                         &core)
                            .ok());
        mutate_core(source.index_id, &core);
        ByteSink core_sink;
        EXPECT_TRUE(encode_core_metadata(core, &core_sink).ok());
        const std::vector<uint8_t> core_bytes = core_sink.buffer();
        const std::vector<uint8_t> sti_bytes =
                Subrange(image, source.sampled_term_index.offset, source.sampled_term_index.length);
        const std::vector<uint8_t> dbd_bytes = Subrange(image, source.dict_block_directory.offset,
                                                        source.dict_block_directory.length);

        LogicalIndexMetadataRef entry;
        entry.index_id = source.index_id;
        entry.index_suffix = source.index_suffix;
        entry.core_metadata = {.offset = out.size(), .length = core_bytes.size()};
        out.insert(out.end(), core_bytes.begin(), core_bytes.end());
        entry.sampled_term_index = {.offset = out.size(), .length = sti_bytes.size()};
        out.insert(out.end(), sti_bytes.begin(), sti_bytes.end());
        entry.dict_block_directory = {.offset = out.size(), .length = dbd_bytes.size()};
        out.insert(out.end(), dbd_bytes.begin(), dbd_bytes.end());
        entries.push_back(std::move(entry));
    }

    ByteSink directory_sink;
    EXPECT_TRUE(encode_metadata_directory(entries, &directory_sink).ok());
    const std::vector<uint8_t> directory_bytes = directory_sink.buffer();
    TailPointer tail;
    tail.directory_offset = out.size();
    tail.directory_length = directory_bytes.size();
    tail.directory_crc32c = doris::snii::crc32c(Slice(directory_bytes));
    out.insert(out.end(), directory_bytes.begin(), directory_bytes.end());
    ByteSink tail_sink;
    EXPECT_TRUE(encode_tail_pointer(tail, &tail_sink).ok());
    const std::vector<uint8_t> tail_bytes = tail_sink.buffer();
    out.insert(out.end(), tail_bytes.begin(), tail_bytes.end());
    return out;
}

std::vector<LogicalIndexKey> KeysOf(std::vector<std::pair<uint64_t, std::string>> raw) {
    std::vector<LogicalIndexKey> keys;
    keys.reserve(raw.size());
    for (auto& [index_id, suffix] : raw) {
        keys.push_back(LogicalIndexKey {.index_id = index_id, .index_suffix = std::move(suffix)});
    }
    return keys;
}

} // namespace

TEST(SniiRewriteSnapshot, DescribesEveryKeptLogicalIndex) {
    const std::vector<uint8_t> image = BuildTwoIndexImage();
    ImageFileReader reader(image);
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    SniiRewriteSnapshot snapshot;
    ASSERT_TRUE(
            segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, kSuffixA}, {kIndexIdB, kSuffixB}}),
                                             kDocCount, &snapshot)
                    .ok());

    ASSERT_EQ(2U, snapshot.inherited().size());
    const MetadataDirectory directory = ReadDirectory(image);
    for (const auto& inherited : snapshot.inherited()) {
        const LogicalIndexMetadataRef* entry =
                directory.find(inherited.index_id, inherited.index_suffix);
        ASSERT_NE(nullptr, entry);
        EXPECT_EQ(kDocCount, inherited.doc_count);

        SectionRefs expected_refs;
        ASSERT_TRUE(segment.section_refs_for_index(inherited.index_id, inherited.index_suffix,
                                                   &expected_refs)
                            .ok());
        EXPECT_EQ(expected_refs.posting_region.offset,
                  inherited.section_refs.posting_region.offset);
        EXPECT_EQ(expected_refs.posting_region.length,
                  inherited.section_refs.posting_region.length);
        EXPECT_EQ(expected_refs.dict_region.offset, inherited.section_refs.dict_region.offset);
        EXPECT_EQ(expected_refs.dict_region.length, inherited.section_refs.dict_region.length);
        EXPECT_GT(inherited.section_refs.dict_region.length, 0U);

        // The raw metadata group is the on-disk [Core][STI][DBD] run, verbatim.
        EXPECT_EQ(entry->core_metadata.length, inherited.core_length);
        EXPECT_EQ(entry->sampled_term_index.length, inherited.sampled_term_index_length);
        EXPECT_EQ(entry->dict_block_directory.length, inherited.dict_block_directory_length);
        const uint64_t group_length = entry->core_metadata.length +
                                      entry->sampled_term_index.length +
                                      entry->dict_block_directory.length;
        EXPECT_EQ(Subrange(image, entry->core_metadata.offset, group_length),
                  inherited.metadata_group);

        // Every inherited section reference lies inside the copied prefix.
        EXPECT_LE(SectionEnd(inherited.section_refs), snapshot.physical_prefix_end());
    }
}

TEST(SniiRewriteSnapshot, PrefixCoversSectionsAndExcludesMetadata) {
    const std::vector<uint8_t> image = BuildTwoIndexImage();
    ImageFileReader reader(image);
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    SniiRewriteSnapshot snapshot;
    ASSERT_TRUE(
            segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, kSuffixA}, {kIndexIdB, kSuffixB}}),
                                             kDocCount, &snapshot)
                    .ok());

    // The prefix starts with the bootstrap header and ends at the last kept
    // section: no metadata group, directory, padding or tail byte is inside it.
    EXPECT_GE(snapshot.physical_prefix_end(), kBootstrapHeaderSize);
    EXPECT_LE(snapshot.physical_prefix_end(), MetadataAreaBegin(image));
    EXPECT_LT(snapshot.physical_prefix_end(), TailDirectoryOffset(image));

    uint64_t last_section_end = 0;
    for (const auto& inherited : snapshot.inherited()) {
        last_section_end = std::max(last_section_end, SectionEnd(inherited.section_refs));
    }
    EXPECT_EQ(last_section_end, snapshot.physical_prefix_end());
}

TEST(SniiRewriteSnapshot, DroppedIndexIsAbsentAndShortensThePrefix) {
    const std::vector<uint8_t> image = BuildTwoIndexImage();
    ImageFileReader reader(image);
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    SniiRewriteSnapshot both;
    ASSERT_TRUE(
            segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, kSuffixA}, {kIndexIdB, kSuffixB}}),
                                             kDocCount, &both)
                    .ok());
    SniiRewriteSnapshot first_only;
    ASSERT_TRUE(segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, kSuffixA}}), kDocCount,
                                                 &first_only)
                        .ok());

    ASSERT_EQ(1U, first_only.inherited().size());
    EXPECT_EQ(kIndexIdA, first_only.inherited().front().index_id);
    EXPECT_EQ(kSuffixA, first_only.inherited().front().index_suffix);
    // Dropping the trailing index shortens the prefix: its sections are not copied.
    EXPECT_LT(first_only.physical_prefix_end(), both.physical_prefix_end());
}

TEST(SniiRewriteSnapshot, RejectsKeyMissingFromTheDirectory) {
    ImageFileReader reader(BuildTwoIndexImage());
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    SniiRewriteSnapshot snapshot;
    const Status status =
            segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, "absent"}}), kDocCount, &snapshot);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>()) << status;
}

TEST(SniiRewriteSnapshot, RejectsDuplicateRequestedKey) {
    ImageFileReader reader(BuildTwoIndexImage());
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    SniiRewriteSnapshot snapshot;
    const Status status = segment.prepare_rewrite_snapshot(
            KeysOf({{kIndexIdA, kSuffixA}, {kIndexIdA, kSuffixA}}), kDocCount, &snapshot);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
}

TEST(SniiRewriteSnapshot, RejectsDocCountDisagreeingWithTheSegment) {
    ImageFileReader reader(BuildTwoIndexImage());
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    SniiRewriteSnapshot snapshot;
    const Status status = segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, kSuffixA}}),
                                                           kDocCount + 1, &snapshot);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiRewriteSnapshot, RejectsCorruptMetadataBlob) {
    const std::vector<uint8_t> image = BuildTwoIndexImage();
    const MetadataDirectory directory = ReadDirectory(image);
    const LogicalIndexMetadataRef* entry = directory.find(kIndexIdA, kSuffixA);
    ASSERT_NE(nullptr, entry);

    ImageFileReader reader(image);
    // Flip one payload byte of the kept index's Core metadata blob: its framed
    // crc no longer matches, so the snapshot must refuse the whole source.
    reader.bytes()[entry->core_metadata.offset + entry->core_metadata.length - 1] ^= 0xFFU;
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    SniiRewriteSnapshot snapshot;
    const Status status =
            segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, kSuffixA}}), kDocCount, &snapshot);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiRewriteSnapshot, RejectsSectionReferenceOutsideThePhysicalArea) {
    const std::vector<uint8_t> good = BuildTwoIndexImage();
    const uint64_t metadata_begin = MetadataAreaBegin(good);
    // Push the kept index's posting region one byte past the physical area, so
    // inheriting it would reference bytes the prefix copy never carries.
    const std::vector<uint8_t> bad =
            RebuildMetadataArea(good, [metadata_begin](uint64_t index_id, CoreMetadata* core) {
                if (index_id == kIndexIdA) {
                    core->section_refs.posting_region.offset = metadata_begin;
                    core->section_refs.posting_region.length = 1;
                }
            });

    ImageFileReader reader(bad);
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    SniiRewriteSnapshot snapshot;
    const Status status =
            segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, kSuffixA}}), kDocCount, &snapshot);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
}

TEST(SniiRewriteSnapshot, RejectsCorruptBootstrapHeader) {
    const std::vector<uint8_t> image = BuildTwoIndexImage();
    ImageFileReader reader(image);
    // open() deliberately never reads the bootstrap header, but the prefix copy
    // would carry these bytes into the rewritten container, so the snapshot must
    // validate them.
    for (uint32_t offset = 0; offset < kBootstrapHeaderSize; ++offset) {
        reader.bytes()[offset] = 0xFFU;
    }
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    SniiRewriteSnapshot snapshot;
    const Status status =
            segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, kSuffixA}}), kDocCount, &snapshot);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>())
            << "the inherited prefix must not carry a corrupt bootstrap header: " << status;
}

// A duplicate key can never reach a container in the first place: the directory
// encoder refuses to serialize one (see metadata_directory_test), and the decoder
// refuses to parse one. That is why the snapshot only has to reject duplicates in
// the CALLER's keep list, which RejectsDuplicateRequestedKey covers.
