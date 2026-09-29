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

// Focused open()-path tests for SniiSegmentReader. These pin two guarantees of
// the offset-0 bootstrap-read removal:
//   1. open() issues NO read intersecting the bootstrap header region
//      [0, kBootstrapHeaderSize) -- the redundant offset-0 cache block / remote
//      round-trip is gone.
//   2. The container version gate is preserved by the tail pointer: a corrupt
//      offset-0 bootstrap header no longer fails open(), but a corrupt tail
//      pointer format_version still does.

#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <limits>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/bootstrap_header.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/metadata_directory.h"
#include "storage/index/snii/format/tail_pointer.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::reader;
using namespace doris::snii::writer;
namespace ErrorCode = doris::ErrorCode;
using doris::Status;

namespace {

constexpr size_t kV1TailPointerSize = 31;

struct TailFields {
    uint64_t directory_offset = 0;
    uint64_t directory_length = 0;
};

TailFields ReadTailFields(const std::vector<uint8_t>& bytes) {
    EXPECT_GE(bytes.size(), kV1TailPointerSize);
    ByteSource source(Slice(bytes.data() + bytes.size() - kV1TailPointerSize, kV1TailPointerSize));
    uint32_t magic = 0;
    uint16_t version = 0;
    TailFields tail;
    uint32_t directory_crc = 0;
    uint8_t encoded_size = 0;
    uint32_t tail_crc = 0;
    EXPECT_TRUE(source.get_fixed32(&magic).ok());
    EXPECT_TRUE(source.get_fixed16(&version).ok());
    EXPECT_TRUE(source.get_fixed64(&tail.directory_offset).ok());
    EXPECT_TRUE(source.get_fixed64(&tail.directory_length).ok());
    EXPECT_TRUE(source.get_fixed32(&directory_crc).ok());
    EXPECT_TRUE(source.get_u8(&encoded_size).ok());
    EXPECT_TRUE(source.get_fixed32(&tail_crc).ok());
    EXPECT_EQ(kTailMagic, magic);
    EXPECT_EQ(kFormatVersion, version);
    EXPECT_EQ(kV1TailPointerSize, encoded_size);
    return tail;
}

MetadataDirectory ReadDirectory(const std::vector<uint8_t>& bytes, const TailFields& tail) {
    MetadataDirectory directory;
    EXPECT_LE(tail.directory_offset, bytes.size());
    EXPECT_LE(tail.directory_length, bytes.size() - tail.directory_offset);
    EXPECT_TRUE(MetadataDirectory::decode(Slice(bytes.data() + tail.directory_offset,
                                                static_cast<size_t>(tail.directory_length)),
                                          &directory)
                        .ok());
    return directory;
}

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_seg_open_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

// An in-memory FileReader over an owned byte buffer that RECORDS every read
// range. The buffer is mutable so a test can corrupt specific on-disk bytes
// before re-opening. read_batch is overridden so batched reads are recorded too
// (open() currently uses only read_at, but recording both keeps the assertion
// honest if that ever changes).
class RecordingFileReader : public io::FileReader {
public:
    explicit RecordingFileReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        reads_.push_back(io::Range {offset, len});
        if (offset > bytes_.size() || len > bytes_.size() - offset) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "recording reader: read past EOF");
        }
        out->assign(bytes_.begin() + static_cast<std::ptrdiff_t>(offset),
                    bytes_.begin() + static_cast<std::ptrdiff_t>(offset + len));
        return Status::OK();
    }

    Status read_batch(const std::vector<io::Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override {
        outs->resize(ranges.size());
        for (size_t i = 0; i < ranges.size(); ++i) {
            RETURN_IF_ERROR(read_at(ranges[i].offset, ranges[i].len, &(*outs)[i]));
        }
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }

    const std::vector<io::Range>& reads() const { return reads_; }
    std::vector<uint8_t>& bytes() { return bytes_; }

    // True iff any recorded read overlaps [lo, hi).
    bool any_read_intersects(uint64_t lo, uint64_t hi) const {
        for (const auto& r : reads_) {
            const uint64_t r_lo = r.offset;
            const uint64_t r_hi = r.offset + r.len;
            if (r_lo < hi && lo < r_hi) {
                return true;
            }
        }
        return false;
    }

private:
    std::vector<uint8_t> bytes_;
    std::vector<io::Range> reads_;
};

class TailOnlyDeclaredSizeReader final : public io::FileReader {
public:
    explicit TailOnlyDeclaredSizeReader(uint64_t directory_length)
            : size_(directory_length + tail_pointer_size()) {
        TailPointer tail;
        tail.directory_offset = 0;
        tail.directory_length = directory_length;
        ByteSink sink;
        EXPECT_TRUE(encode_tail_pointer(tail, &sink).ok());
        tail_bytes_ = sink.buffer();
    }

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset == size_ - tail_bytes_.size() && len == tail_bytes_.size()) {
            *out = tail_bytes_;
            return Status::OK();
        }
        ++non_footer_reads_;
        return Status::Error<ErrorCode::IO_ERROR, false>("unexpected non-footer read");
    }

    uint64_t size() const override { return size_; }
    size_t non_footer_reads() const { return non_footer_reads_; }

private:
    uint64_t size_;
    size_t non_footer_reads_ = 0;
    std::vector<uint8_t> tail_bytes_;
};

// Writes a minimal single-index docs+positions container and returns its bytes.
std::vector<uint8_t> BuildContainerBytes() {
    SpimiTermBuffer buf(/*has_positions=*/true);
    // A tiny deterministic corpus: a couple of terms across a few docs.
    const char* docs[] = {"alpha bravo", "bravo charlie", "alpha charlie delta"};
    for (uint32_t d = 0; d < 3; ++d) {
        std::string s = docs[d];
        uint32_t pos = 0;
        size_t start = 0;
        while (start <= s.size()) {
            size_t sp = s.find(' ', start);
            std::string tok =
                    s.substr(start, sp == std::string::npos ? std::string::npos : sp - start);
            if (!tok.empty()) {
                buf.add_token(tok, d, pos++);
            }
            if (sp == std::string::npos) {
                break;
            }
            start = sp + 1;
        }
    }
    std::vector<TermPostings> terms = buf.finalize_sorted();

    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = 3;
    in.terms = std::move(terms);
    in.target_dict_block_bytes = 256;

    const std::string path = TempPath();
    {
        io::LocalFileWriter w;
        EXPECT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        EXPECT_TRUE(cw.add_logical_index(in).ok());
        EXPECT_TRUE(cw.finish().ok());
    }

    io::LocalFileReader r;
    EXPECT_TRUE(r.open(path).ok());
    std::vector<uint8_t> bytes;
    EXPECT_TRUE(r.read_at(0, r.size(), &bytes).ok());
    std::remove(path.c_str());
    return bytes;
}

} // namespace

// Catches a metadata-layout or eager-open regression before the PB migration:
// the compact custom baseline must not grow, take extra reads, or undercharge
// the searcher cache. The literals are independently measured from the
// pre-PB container built above, not derived from reader implementation details.
TEST(SniiSegmentReaderOpen, PreservesPrePbMetadataBounds) {
    constexpr size_t kCompleteImageBytesUpperBound = 454;
    constexpr uint64_t kTailMetadataBytesUpperBound = 165;
    constexpr size_t kSegmentOpenReadCount = 2;
    constexpr size_t kLogicalIndexOpenAdditionalReadCount = 3;
    constexpr size_t kCoreOnlyAdditionalReadCount = 1;
    constexpr size_t kLogicalReaderMemoryUsageUpperBound = 1380;

    const std::vector<uint8_t> bytes = BuildContainerBytes();
    EXPECT_LE(bytes.size(), kCompleteImageBytesUpperBound);
    ASSERT_EQ(kV1TailPointerSize, tail_pointer_size());
    const TailFields tail = ReadTailFields(bytes);
    const MetadataDirectory directory = ReadDirectory(bytes, tail);
    ASSERT_EQ(1U, directory.size());
    const auto& entry = directory.entries().front();
    ASSERT_LE(entry.core_metadata.offset, tail.directory_offset);
    const uint64_t tail_metadata_bytes =
            tail.directory_offset + tail.directory_length - entry.core_metadata.offset;
    EXPECT_LE(tail_metadata_bytes, kTailMetadataBytesUpperBound);

    RecordingFileReader query_reader(bytes);
    SniiSegmentReader query_seg;
    ASSERT_TRUE(SniiSegmentReader::open(&query_reader, &query_seg).ok());
    const size_t segment_open_read_count = query_reader.reads().size();
    EXPECT_EQ(segment_open_read_count, kSegmentOpenReadCount);
    LogicalIndexReader query_index;
    ASSERT_TRUE(query_seg.open_index(1, "body", &query_index).ok());
    const size_t logical_index_open_additional_read_count =
            query_reader.reads().size() - segment_open_read_count;
    EXPECT_EQ(logical_index_open_additional_read_count, kLogicalIndexOpenAdditionalReadCount);
    ASSERT_GE(query_reader.reads().size(), segment_open_read_count + 1);
    const auto& group_read = query_reader.reads()[segment_open_read_count];
    EXPECT_EQ(entry.core_metadata.offset, group_read.offset);
    EXPECT_EQ(entry.core_metadata.length + entry.sampled_term_index.length +
                      entry.dict_block_directory.length,
              group_read.len);
    EXPECT_LE(query_index.memory_usage(), kLogicalReaderMemoryUsageUpperBound);

    RecordingFileReader core_reader(bytes);
    SniiSegmentReader core_seg;
    ASSERT_TRUE(SniiSegmentReader::open(&core_reader, &core_seg).ok());
    const size_t core_segment_open_read_count = core_reader.reads().size();
    SectionRefs section_refs;
    ASSERT_TRUE(core_seg.section_refs_for_index(1, "body", &section_refs).ok());
    const size_t core_only_additional_read_count =
            core_reader.reads().size() - core_segment_open_read_count;
    EXPECT_EQ(core_only_additional_read_count, kCoreOnlyAdditionalReadCount);
    ASSERT_GE(core_reader.reads().size(), core_segment_open_read_count + 1);
    const auto& core_read = core_reader.reads()[core_segment_open_read_count];
    EXPECT_EQ(entry.core_metadata.offset, core_read.offset);
    EXPECT_EQ(entry.core_metadata.length, core_read.len);
    EXPECT_GT(section_refs.dict_region.length, 0U);
}

TEST(SniiSegmentReaderOpen, ReadsExactlyFooterThenRawDirectory) {
    const std::vector<uint8_t> bytes = BuildContainerBytes();
    const TailFields tail = ReadTailFields(bytes);
    RecordingFileReader reader(bytes);
    SniiSegmentReader segment;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &segment).ok());

    ASSERT_EQ(2U, reader.reads().size());
    EXPECT_EQ(bytes.size() - kV1TailPointerSize, reader.reads()[0].offset);
    EXPECT_EQ(kV1TailPointerSize, reader.reads()[0].len);
    EXPECT_EQ(tail.directory_offset, reader.reads()[1].offset);
    EXPECT_EQ(tail.directory_length, reader.reads()[1].len);
}

TEST(SniiSegmentReaderOpen, RejectsDirectoryLargerThanProtobufIntLimitBeforeReadingIt) {
    const uint64_t oversized_directory = static_cast<uint64_t>(std::numeric_limits<int>::max()) + 1;
    TailOnlyDeclaredSizeReader reader(oversized_directory);
    SniiSegmentReader segment;
    const Status status = SniiSegmentReader::open(&reader, &segment);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_EQ(0U, reader.non_footer_reads());
}

// open() must succeed and must NOT read any byte in [0, kBootstrapHeaderSize).
TEST(SniiSegmentReaderOpen, IssuesNoReadAtBootstrapRegion) {
    std::vector<uint8_t> bytes = BuildContainerBytes();
    ASSERT_GT(bytes.size(), kBootstrapHeaderSize + tail_pointer_size());

    RecordingFileReader reader(std::move(bytes));
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&reader, &seg).ok());
    EXPECT_EQ(seg.n_logical_indexes(), 1U);

    // The container must still be usable (real, not vacuous): the logical index
    // opens and reports the corpus doc count.
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());
    EXPECT_EQ(idx.stats().doc_count, 3U);

    // Core assertion: segment and logical-index open touched the tail, never the
    // bootstrap header at the front of the file.
    EXPECT_FALSE(reader.any_read_intersects(0, kBootstrapHeaderSize))
            << "open path read the offset-0 bootstrap region";
    // And it issued at least one read (otherwise the assertion above is vacuous).
    EXPECT_GE(reader.reads().size(), 1U);
}

// A corrupt offset-0 bootstrap header no longer fails open(): nothing reads it.
TEST(SniiSegmentReaderOpen, IgnoresCorruptBootstrapHeader) {
    std::vector<uint8_t> bytes = BuildContainerBytes();
    ASSERT_GE(bytes.size(), kBootstrapHeaderSize);

    RecordingFileReader reader(std::move(bytes));
    // Smash the entire bootstrap header region.
    for (uint32_t i = 0; i < kBootstrapHeaderSize; ++i) {
        reader.bytes()[i] = 0xFFU;
    }

    SniiSegmentReader seg;
    EXPECT_TRUE(SniiSegmentReader::open(&reader, &seg).ok())
            << "open() must not depend on the offset-0 bootstrap header";
}

// The container version gate is preserved by the tail pointer: corrupting the
// tail pointer's format_version makes open() fail.
TEST(SniiSegmentReaderOpen, RejectsCorruptTailFormatVersion) {
    std::vector<uint8_t> good = BuildContainerBytes();
    ASSERT_GE(good.size(), tail_pointer_size());

    // Sanity: the unmodified container opens.
    {
        RecordingFileReader ok_reader(good);
        SniiSegmentReader seg;
        ASSERT_TRUE(SniiSegmentReader::open(&ok_reader, &seg).ok());
    }

    // The tail pointer is the last tail_pointer_size() bytes. Its layout is
    // u32 magic, u16 format_version, ... so format_version sits at
    // (size - tail_pointer_size) + 4.
    RecordingFileReader bad_reader(good);
    const uint64_t tp_start = bad_reader.size() - tail_pointer_size();
    const uint64_t fv_off = tp_start + 4; // skip the u32 magic
    // Write a wrong, never-valid format_version (kFormatVersion is small).
    bad_reader.bytes()[fv_off] = 0xFFU;
    bad_reader.bytes()[fv_off + 1] = 0xFFU;

    SniiSegmentReader seg;
    EXPECT_FALSE(SniiSegmentReader::open(&bad_reader, &seg).ok())
            << "open() must reject a container whose tail format_version is wrong";
}
