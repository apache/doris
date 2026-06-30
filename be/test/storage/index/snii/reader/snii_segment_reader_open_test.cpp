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
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/bootstrap_header.h"
#include "storage/index/snii/format/format_constants.h"
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

    // Core assertion: open() + the read_index_meta above touched the tail, never
    // the bootstrap header at the front of the file.
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
        reader.bytes()[i] = 0xFFu;
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
    bad_reader.bytes()[fv_off] = 0xFFu;
    bad_reader.bytes()[fv_off + 1] = 0xFFu;

    SniiSegmentReader seg;
    EXPECT_FALSE(SniiSegmentReader::open(&bad_reader, &seg).ok())
            << "open() must reject a container whose tail format_version is wrong";
}
