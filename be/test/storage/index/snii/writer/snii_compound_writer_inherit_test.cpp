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

// Tests for SniiCompoundWriter::inherit -- the one-shot operation BUILD INDEX uses
// to carry a source container's unchanged logical indexes into a new container
// without re-analyzing, decoding or re-encoding their postings.
//
// The contract under test:
//   1. Inherited postings are carried byte-for-byte and stay queryable, while new
//      indexes are appended after them in the same container.
//   2. Only the valid physical prefix is copied: the source's metadata groups,
//      directory and tail are NOT in the output, which ends with exactly one
//      directory and one tail of its own.
//   3. An index left out of the snapshot becomes invisible.
//   4. inherit must be the writer's first data operation, keys must stay unique,
//      and any read or write failure leaves no sealed container behind.
//   5. The copy runs through a fixed-size buffer, so its memory does not grow
//      with the source file.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/bootstrap_header.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/tail_pointer.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
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

constexpr uint64_t kIndexIdA = 11;
constexpr uint64_t kIndexIdB = 22;
constexpr uint64_t kIndexIdC = 33;
constexpr const char* kSuffixA = "title";
constexpr const char* kSuffixB = "body";
constexpr const char* kSuffixC = "author";

// Corpora chosen so each index owns a distinct term, making "is this index really
// readable in the output?" a real question a wrong inherit would answer wrong.
const char* const kTermA = "alpha";
const char* const kTermB = "bravo";
const char* const kTermC = "charlie";
constexpr uint32_t kDocCount = 4;

// An in-memory reader over an owned image that records every read range.
class ImageFileReader : public io::FileReader {
public:
    explicit ImageFileReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        reads_.push_back(io::Range {offset, len});
        if (offset > bytes_.size() || len > bytes_.size() - offset) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "image reader: read past EOF");
        }
        out->assign(bytes_.begin() + static_cast<std::ptrdiff_t>(offset),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(offset + len));
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }
    const std::vector<io::Range>& reads() const { return reads_; }
    void clear_reads() { reads_.clear(); }

private:
    std::vector<uint8_t> bytes_;
    std::vector<io::Range> reads_;
};

// Fails every read that would touch a byte at or past `fail_from`.
class TruncatedImageFileReader final : public ImageFileReader {
public:
    TruncatedImageFileReader(std::vector<uint8_t> bytes, uint64_t fail_from)
            : ImageFileReader(std::move(bytes)), fail_from_(fail_from) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset + len > fail_from_) {
            return Status::Error<ErrorCode::IO_ERROR, false>("injected source read failure");
        }
        return ImageFileReader::read_at(offset, len, out);
    }

private:
    uint64_t fail_from_;
};

// An in-memory sink; fails once the appended byte count would pass `fail_after`.
class MemoryFileWriter final : public io::FileWriter {
public:
    explicit MemoryFileWriter(uint64_t fail_after = std::numeric_limits<uint64_t>::max())
            : fail_after_(fail_after) {}

    Status append(Slice data) override {
        if (bytes_.size() + data.size() > fail_after_) {
            return Status::Error<ErrorCode::IO_ERROR, false>("injected sink write failure");
        }
        bytes_.insert(bytes_.end(), data.data(), data.data() + data.size());
        return Status::OK();
    }
    Status finalize() override {
        finalized_ = true;
        return Status::OK();
    }
    uint64_t bytes_written() const override { return bytes_.size(); }

    const std::vector<uint8_t>& bytes() const { return bytes_; }
    bool finalized() const { return finalized_; }

private:
    uint64_t fail_after_;
    std::vector<uint8_t> bytes_;
    bool finalized_ = false;
};

std::vector<TermPostings> SingleTermCorpus(const std::string& term, uint32_t doc_count) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    for (uint32_t docid = 0; docid < doc_count; ++docid) {
        buffer.add_token(term, docid, 0);
    }
    return buffer.finalize_sorted();
}

// A corpus large enough that the inherited prefix spans several copy chunks.
std::vector<TermPostings> WideCorpus(uint32_t term_count, uint32_t doc_count) {
    SpimiTermBuffer buffer(/*has_positions=*/true);
    for (uint32_t term = 0; term < term_count; ++term) {
        const std::string token = "term_" + std::to_string(1000000 + term);
        for (uint32_t docid = 0; docid < doc_count; ++docid) {
            buffer.add_token(token, docid, term % 7);
        }
    }
    return buffer.finalize_sorted();
}

SniiIndexInput MakeInput(uint64_t index_id, const char* suffix, std::vector<TermPostings> terms,
                         uint32_t doc_count) {
    SniiIndexInput in;
    in.index_id = index_id;
    in.index_suffix = suffix;
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = doc_count;
    in.terms = std::move(terms);
    in.target_dict_block_bytes = 256;
    return in;
}

std::vector<uint8_t> WriteContainer(const std::vector<SniiIndexInput*>& inputs) {
    MemoryFileWriter writer;
    SniiCompoundWriter compound(&writer);
    for (SniiIndexInput* in : inputs) {
        EXPECT_TRUE(compound.add_logical_index(*in).ok());
    }
    EXPECT_TRUE(compound.finish().ok());
    return writer.bytes();
}

// Source container carrying (kIndexIdA, kSuffixA) and (kIndexIdB, kSuffixB).
std::vector<uint8_t> BuildSourceImage() {
    SniiIndexInput first =
            MakeInput(kIndexIdA, kSuffixA, SingleTermCorpus(kTermA, kDocCount), kDocCount);
    SniiIndexInput second =
            MakeInput(kIndexIdB, kSuffixB, SingleTermCorpus(kTermB, kDocCount), kDocCount);
    return WriteContainer({&first, &second});
}

std::vector<LogicalIndexKey> KeysOf(std::vector<std::pair<uint64_t, std::string>> raw) {
    std::vector<LogicalIndexKey> keys;
    keys.reserve(raw.size());
    for (auto& [index_id, suffix] : raw) {
        keys.push_back(LogicalIndexKey {.index_id = index_id, .index_suffix = std::move(suffix)});
    }
    return keys;
}

SniiRewriteSnapshot SnapshotOf(ImageFileReader* reader,
                               const std::vector<std::pair<uint64_t, std::string>>& keep) {
    SniiSegmentReader segment;
    EXPECT_TRUE(SniiSegmentReader::open(reader, &segment).ok());
    SniiRewriteSnapshot snapshot;
    EXPECT_TRUE(segment.prepare_rewrite_snapshot(KeysOf(keep), kDocCount, &snapshot).ok());
    reader->clear_reads();
    return snapshot;
}

// True iff `haystack` contains `needle` anywhere.
bool Contains(const std::vector<uint8_t>& haystack, const std::vector<uint8_t>& needle) {
    if (needle.empty() || needle.size() > haystack.size()) {
        return false;
    }
    return std::search(haystack.begin(), haystack.end(), needle.begin(), needle.end()) !=
           haystack.end();
}

std::vector<uint8_t> Subrange(const std::vector<uint8_t>& image, uint64_t offset, uint64_t length) {
    EXPECT_LE(offset + length, image.size());
    return std::vector<uint8_t>(image.begin() + static_cast<std::ptrdiff_t>(offset),
                                image.begin() + static_cast<std::ptrdiff_t>(offset + length));
}

TailPointer ReadTail(const std::vector<uint8_t>& image) {
    const size_t footer = tail_pointer_size();
    EXPECT_GE(image.size(), footer);
    TailPointer tail;
    EXPECT_TRUE(
            decode_tail_pointer(Slice(image.data() + image.size() - footer, footer), &tail).ok());
    return tail;
}

// Reads one term's docids out of a container image.
std::vector<uint32_t> QueryTerm(const std::vector<uint8_t>& image, uint64_t index_id,
                                const char* suffix, const char* term) {
    ImageFileReader reader(image);
    SniiSegmentReader segment;
    EXPECT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());
    LogicalIndexReader index;
    EXPECT_TRUE(segment.open_index(index_id, suffix, &index).ok());
    std::vector<uint32_t> docids;
    EXPECT_TRUE(query::term_query(index, term, &docids).ok());
    return docids;
}

} // namespace

TEST(SniiCompoundWriterInherit, CarriesInheritedIndexesAndAppendsANewOne) {
    const std::vector<uint8_t> source = BuildSourceImage();
    ImageFileReader source_reader(source);
    const SniiRewriteSnapshot snapshot =
            SnapshotOf(&source_reader, {{kIndexIdA, kSuffixA}, {kIndexIdB, kSuffixB}});

    MemoryFileWriter sink;
    SniiCompoundWriter compound(&sink);
    ASSERT_TRUE(compound.inherit(snapshot, &source_reader).ok());
    SniiIndexInput fresh =
            MakeInput(kIndexIdC, kSuffixC, SingleTermCorpus(kTermC, kDocCount), kDocCount);
    ASSERT_TRUE(compound.add_logical_index(fresh).ok());
    ASSERT_TRUE(compound.finish().ok());

    const std::vector<uint8_t>& output = sink.bytes();
    ImageFileReader output_reader(output);
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&output_reader, &segment).ok());
    EXPECT_EQ(3U, segment.n_logical_indexes());

    const std::vector<uint32_t> expected_docids = {0, 1, 2, 3};
    EXPECT_EQ(expected_docids, QueryTerm(output, kIndexIdA, kSuffixA, kTermA));
    EXPECT_EQ(expected_docids, QueryTerm(output, kIndexIdB, kSuffixB, kTermB));
    EXPECT_EQ(expected_docids, QueryTerm(output, kIndexIdC, kSuffixC, kTermC));
}

TEST(SniiCompoundWriterInherit, CopiesThePhysicalPrefixByteForByte) {
    const std::vector<uint8_t> source = BuildSourceImage();
    ImageFileReader source_reader(source);
    const SniiRewriteSnapshot snapshot =
            SnapshotOf(&source_reader, {{kIndexIdA, kSuffixA}, {kIndexIdB, kSuffixB}});

    MemoryFileWriter sink;
    SniiCompoundWriter compound(&sink);
    ASSERT_TRUE(compound.inherit(snapshot, &source_reader).ok());
    ASSERT_EQ(snapshot.physical_prefix_end(), sink.bytes_written());
    SniiIndexInput fresh =
            MakeInput(kIndexIdC, kSuffixC, SingleTermCorpus(kTermC, kDocCount), kDocCount);
    ASSERT_TRUE(compound.add_logical_index(fresh).ok());
    ASSERT_TRUE(compound.finish().ok());

    const std::vector<uint8_t>& output = sink.bytes();
    const uint64_t prefix_end = snapshot.physical_prefix_end();
    ASSERT_GT(output.size(), prefix_end);
    EXPECT_EQ(Subrange(source, 0, prefix_end), Subrange(output, 0, prefix_end));

    // The inherited section references still resolve, because the prefix landed at
    // the very same offsets.
    ImageFileReader output_reader(output);
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&output_reader, &segment).ok());
    for (const auto& inherited : snapshot.inherited()) {
        SectionRefs refs;
        ASSERT_TRUE(
                segment.section_refs_for_index(inherited.index_id, inherited.index_suffix, &refs)
                        .ok());
        EXPECT_EQ(inherited.section_refs.posting_region.offset, refs.posting_region.offset);
        EXPECT_EQ(inherited.section_refs.posting_region.length, refs.posting_region.length);
        EXPECT_EQ(inherited.section_refs.dict_region.offset, refs.dict_region.offset);
        EXPECT_EQ(inherited.section_refs.dict_region.length, refs.dict_region.length);
    }
}

TEST(SniiCompoundWriterInherit, LeavesTheSourceMetadataDirectoryAndTailBehind) {
    const std::vector<uint8_t> source = BuildSourceImage();
    const TailPointer source_tail = ReadTail(source);
    const std::vector<uint8_t> source_directory_bytes =
            Subrange(source, source_tail.directory_offset, source_tail.directory_length);
    const std::vector<uint8_t> source_tail_bytes =
            Subrange(source, source.size() - tail_pointer_size(), tail_pointer_size());

    ImageFileReader source_reader(source);
    const SniiRewriteSnapshot snapshot =
            SnapshotOf(&source_reader, {{kIndexIdA, kSuffixA}, {kIndexIdB, kSuffixB}});
    MemoryFileWriter sink;
    SniiCompoundWriter compound(&sink);
    ASSERT_TRUE(compound.inherit(snapshot, &source_reader).ok());
    SniiIndexInput fresh =
            MakeInput(kIndexIdC, kSuffixC, SingleTermCorpus(kTermC, kDocCount), kDocCount);
    ASSERT_TRUE(compound.add_logical_index(fresh).ok());
    ASSERT_TRUE(compound.finish().ok());

    const std::vector<uint8_t>& output = sink.bytes();
    EXPECT_FALSE(Contains(output, source_directory_bytes))
            << "the source metadata directory was copied into the output";
    EXPECT_FALSE(Contains(output, source_tail_bytes))
            << "the source tail was copied into the output";

    // Exactly one directory and one tail, both belonging to the new container.
    const TailPointer output_tail = ReadTail(output);
    EXPECT_GE(output_tail.directory_offset, snapshot.physical_prefix_end());
    EXPECT_EQ(output.size(),
              output_tail.directory_offset + output_tail.directory_length + tail_pointer_size());
    EXPECT_TRUE(sink.finalized());
}

TEST(SniiCompoundWriterInherit, IndexLeftOutOfTheSnapshotBecomesInvisible) {
    const std::vector<uint8_t> source = BuildSourceImage();
    ImageFileReader source_reader(source);
    const SniiRewriteSnapshot snapshot = SnapshotOf(&source_reader, {{kIndexIdA, kSuffixA}});

    MemoryFileWriter sink;
    SniiCompoundWriter compound(&sink);
    ASSERT_TRUE(compound.inherit(snapshot, &source_reader).ok());
    ASSERT_TRUE(compound.finish().ok());

    ImageFileReader output_reader(sink.bytes());
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&output_reader, &segment).ok());
    EXPECT_EQ(1U, segment.n_logical_indexes());
    bool exists = true;
    ASSERT_TRUE(segment.index_exists(kIndexIdB, kSuffixB, &exists).ok());
    EXPECT_FALSE(exists);
    LogicalIndexReader dropped;
    EXPECT_TRUE(segment.open_index(kIndexIdB, kSuffixB, &dropped)
                        .is<ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND>());
    EXPECT_EQ((std::vector<uint32_t> {0, 1, 2, 3}),
              QueryTerm(sink.bytes(), kIndexIdA, kSuffixA, kTermA));
}

TEST(SniiCompoundWriterInherit, RejectsInheritAfterAnotherDataOperation) {
    const std::vector<uint8_t> source = BuildSourceImage();
    ImageFileReader source_reader(source);
    const SniiRewriteSnapshot snapshot = SnapshotOf(&source_reader, {{kIndexIdA, kSuffixA}});

    MemoryFileWriter sink;
    SniiCompoundWriter compound(&sink);
    SniiIndexInput fresh =
            MakeInput(kIndexIdC, kSuffixC, SingleTermCorpus(kTermC, kDocCount), kDocCount);
    ASSERT_TRUE(compound.add_logical_index(fresh).ok());
    const Status status = compound.inherit(snapshot, &source_reader);
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>()) << status;
    EXPECT_FALSE(compound.finish().ok())
            << "a writer whose inherit was rejected must not seal a container";
}

TEST(SniiCompoundWriterInherit, RejectsANewIndexReusingAnInheritedKey) {
    const std::vector<uint8_t> source = BuildSourceImage();
    ImageFileReader source_reader(source);
    const SniiRewriteSnapshot snapshot = SnapshotOf(&source_reader, {{kIndexIdA, kSuffixA}});

    MemoryFileWriter sink;
    SniiCompoundWriter compound(&sink);
    ASSERT_TRUE(compound.inherit(snapshot, &source_reader).ok());
    SniiIndexInput clashing =
            MakeInput(kIndexIdA, kSuffixA, SingleTermCorpus(kTermC, kDocCount), kDocCount);
    EXPECT_FALSE(compound.add_logical_index(clashing).ok());
    EXPECT_FALSE(compound.finish().ok())
            << "a duplicate logical index key must never reach a sealed directory";
}

TEST(SniiCompoundWriterInherit, SourceReadFailureSealsNothing) {
    const std::vector<uint8_t> source = BuildSourceImage();
    ImageFileReader snapshot_reader(source);
    const SniiRewriteSnapshot snapshot =
            SnapshotOf(&snapshot_reader, {{kIndexIdA, kSuffixA}, {kIndexIdB, kSuffixB}});
    ASSERT_GT(snapshot.physical_prefix_end(), kBootstrapHeaderSize);

    // Fail partway through the prefix, after the bootstrap header.
    TruncatedImageFileReader failing_reader(source, kBootstrapHeaderSize);
    MemoryFileWriter sink;
    SniiCompoundWriter compound(&sink);
    EXPECT_TRUE(compound.inherit(snapshot, &failing_reader).is<ErrorCode::IO_ERROR>());
    EXPECT_FALSE(compound.finish().ok());
    EXPECT_FALSE(sink.finalized());
}

TEST(SniiCompoundWriterInherit, SinkWriteFailureSealsNothing) {
    const std::vector<uint8_t> source = BuildSourceImage();
    ImageFileReader source_reader(source);
    const SniiRewriteSnapshot snapshot =
            SnapshotOf(&source_reader, {{kIndexIdA, kSuffixA}, {kIndexIdB, kSuffixB}});
    ASSERT_GT(snapshot.physical_prefix_end(), kBootstrapHeaderSize);

    MemoryFileWriter sink(/*fail_after=*/kBootstrapHeaderSize);
    SniiCompoundWriter compound(&sink);
    EXPECT_TRUE(compound.inherit(snapshot, &source_reader).is<ErrorCode::IO_ERROR>());
    EXPECT_FALSE(compound.finish().ok());
    EXPECT_FALSE(sink.finalized());
}

TEST(SniiCompoundWriterInherit, CopiesALargePrefixThroughAFixedSizeBuffer) {
    // 4000 terms x 40 docs produces a container spanning several copy chunks, so
    // "one read per chunk" is a real claim rather than a vacuous one.
    SniiIndexInput wide = MakeInput(kIndexIdA, kSuffixA, WideCorpus(4000, 40), 40);
    const std::vector<uint8_t> source = WriteContainer({&wide});

    ImageFileReader source_reader(source);
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&source_reader, &segment).ok());
    SniiRewriteSnapshot snapshot;
    ASSERT_TRUE(
            segment.prepare_rewrite_snapshot(KeysOf({{kIndexIdA, kSuffixA}}), 40, &snapshot).ok());
    ASSERT_GT(snapshot.physical_prefix_end(), 2 * SniiCompoundWriter::kInheritCopyChunkBytes);
    source_reader.clear_reads();

    MemoryFileWriter sink;
    SniiCompoundWriter compound(&sink);
    ASSERT_TRUE(compound.inherit(snapshot, &source_reader).ok());
    ASSERT_TRUE(compound.finish().ok());

    uint64_t copied_bytes = 0;
    size_t largest_read = 0;
    for (const io::Range& read : source_reader.reads()) {
        copied_bytes += read.len;
        largest_read = std::max(largest_read, read.len);
    }
    EXPECT_EQ(snapshot.physical_prefix_end(), copied_bytes)
            << "the prefix must be read exactly once, in full";
    EXPECT_LE(largest_read, SniiCompoundWriter::kInheritCopyChunkBytes)
            << "the copy buffer must not grow with the source file";
    EXPECT_GT(source_reader.reads().size(), 2U);
    EXPECT_EQ(Subrange(source, 0, snapshot.physical_prefix_end()),
              Subrange(sink.bytes(), 0, snapshot.physical_prefix_end()));
}
