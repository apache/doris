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
#include <unistd.h>

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::writer;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_stream_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

std::vector<uint8_t> ReadAll(const std::string& path) {
    io::LocalFileReader r;
    EXPECT_TRUE(r.open(path).ok());
    std::vector<uint8_t> out;
    EXPECT_TRUE(r.read_at(0, r.size(), &out).ok());
    return out;
}

// Feeds a deterministic (term, doc, pos) stream into a SPIMI buffer. Docids
// arrive in ascending order per term (the normal tokenizer contract); some
// terms span many docs so both slim and (with enough docs) windowed paths and
// the DICT block splitter are exercised.
void Feed(SpimiTermBuffer* buf, uint32_t doc_count) {
    for (uint32_t d = 0; d < doc_count; ++d) {
        buf->add_token("alpha", d, 0); // every doc: high df
        if (d % 2 == 0) {
            buf->add_token("beta", d, 1); // half the docs
        }
        if (d % 7 == 0) {
            buf->add_token("gamma", d, 2);
            buf->add_token("gamma", d, 5); // freq 2 in this doc
        }
        if (d == 3 || d == 4) {
            buf->add_token("delta", d, d); // tiny df
        }
    }
}

// Writes a single-index container from a SniiIndexInput and returns the bytes.
std::vector<uint8_t> WriteContainer(const SniiIndexInput& in) {
    const std::string path = TempPath();
    io::LocalFileWriter writer;
    EXPECT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    EXPECT_TRUE(compound.add_logical_index(in).ok());
    EXPECT_TRUE(compound.finish().ok());
    std::vector<uint8_t> bytes = ReadAll(path);
    std::remove(path.c_str());
    return bytes;
}

SniiIndexInput BaseInput(uint32_t doc_count) {
    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = doc_count;
    in.target_dict_block_bytes = 512; // force several DICT blocks
    return in;
}

} // namespace

// The streaming term_source path must produce a BYTE-IDENTICAL container to the
// materialized terms vector path: the flat-array accumulator + stream-finalize
// must not change a single output byte.
TEST(SniiSpimiStreamingWriter, StreamingMatchesMaterializedBytes) {
    constexpr uint32_t kDocs = 300;

    SpimiTermBuffer mat_buf(/*has_positions=*/true);
    Feed(&mat_buf, kDocs);
    SniiIndexInput mat_in = BaseInput(kDocs);
    mat_in.terms = mat_buf.finalize_sorted();
    const std::vector<uint8_t> mat_bytes = WriteContainer(mat_in);

    SpimiTermBuffer stream_buf(/*has_positions=*/true);
    Feed(&stream_buf, kDocs);
    SniiIndexInput stream_in = BaseInput(kDocs);
    stream_in.term_source = &stream_buf;
    const std::vector<uint8_t> stream_bytes = WriteContainer(stream_in);

    ASSERT_EQ(mat_bytes.size(), stream_bytes.size());
    EXPECT_EQ(mat_bytes, stream_bytes);
}

// Position STREAMING (pos_pump) path: a term whose token count exceeds the
// streaming threshold (65536) has its positions pulled per-window from the arena
// chain instead of a materialized positions_flat. The streamed (term_source) build
// must still be BYTE-IDENTICAL to the materialized (finalize_sorted) build -- the
// pump must yield the exact same positions in the same order, so every .prx byte
// (and thus the whole container) matches. This is the byte-identity guard for the
// peak-RSS optimization.
TEST(SniiSpimiStreamingWriter, StreamedPositionsMatchMaterializedBytesHighDf) {
    // ~80k docs of a single high-df term gives that term > 65536 tokens, crossing the
    // streaming threshold. A few extra terms exercise the slim (non-streamed) path
    // alongside it so both encodings appear in one container.
    constexpr uint32_t kDocs = 80000;
    auto feed = [](SpimiTermBuffer* buf) {
        for (uint32_t d = 0; d < kDocs; ++d) {
            buf->add_token("hot", d, 0); // every doc: > 65536 tokens
            if (d % 1000 == 0) {
                buf->add_token("cold", d, 1); // tiny df (slim path)
            }
        }
    };

    SpimiTermBuffer mat_buf(/*has_positions=*/true);
    feed(&mat_buf);
    SniiIndexInput mat_in = BaseInput(kDocs);
    mat_in.terms = mat_buf.finalize_sorted(); // materialized: positions_flat
    const std::vector<uint8_t> mat_bytes = WriteContainer(mat_in);

    SpimiTermBuffer stream_buf(/*has_positions=*/true);
    feed(&stream_buf);
    SniiIndexInput stream_in = BaseInput(kDocs);
    stream_in.term_source = &stream_buf; // streamed: pos_pump for "hot"
    const std::vector<uint8_t> stream_bytes = WriteContainer(stream_in);

    ASSERT_EQ(mat_bytes.size(), stream_bytes.size());
    EXPECT_EQ(mat_bytes, stream_bytes);
}

// A term repeated MANY times within FEW docs crosses the streaming token threshold
// (ntok >= 65536) yet has a LOW df (< kSlimDfThreshold == 512), so it takes the SLIM
// writer path -- which reads positions_flat directly and does NOT honor pos_pump.
// The stream candidate must therefore fall back to materializing positions for this
// term; otherwise build_slim_entry sees an empty positions_flat and reads out of
// bounds (deterministic segfault). Byte-identity vs the materialized build proves
// both that it does not crash and that the fallback produces the exact same bytes.
TEST(SniiSpimiStreamingWriter, StreamedLowDfHighNtokMatchesMaterialized) {
    constexpr uint32_t kDocs = 200; // df = 200 (< 512 -> slim path)
    constexpr uint32_t kReps = 400; // 200 * 400 = 80000 tokens (> 65536 -> stream)
    auto feed = [](SpimiTermBuffer* buf) {
        for (uint32_t d = 0; d < kDocs; ++d) {
            for (uint32_t p = 0; p < kReps; ++p) {
                buf->add_token("rep", d, p);
            }
        }
    };

    SpimiTermBuffer mat_buf(/*has_positions=*/true);
    feed(&mat_buf);
    SniiIndexInput mat_in = BaseInput(kDocs);
    mat_in.terms = mat_buf.finalize_sorted();
    const std::vector<uint8_t> mat_bytes = WriteContainer(mat_in);

    SpimiTermBuffer stream_buf(/*has_positions=*/true);
    feed(&stream_buf);
    SniiIndexInput stream_in = BaseInput(kDocs);
    stream_in.term_source = &stream_buf;
    const std::vector<uint8_t> stream_bytes = WriteContainer(stream_in);

    ASSERT_EQ(mat_bytes.size(), stream_bytes.size());
    EXPECT_EQ(mat_bytes, stream_bytes);
}

// The streaming path drains its source: after build the buffer is empty.
TEST(SniiSpimiStreamingWriter, StreamingConsumesSource) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    Feed(&buf, 50);
    EXPECT_GT(buf.unique_terms(), 0U);

    SniiIndexInput in = BaseInput(50);
    in.term_source = &buf;
    LogicalIndexWriter writer(in);
    // build() streams the posting region straight into a FileWriter sink; this test
    // only asserts the source is drained, so a throwaway temp sink suffices.
    const std::string post_path = TempPath();
    io::LocalFileWriter post;
    ASSERT_TRUE(post.open(post_path).ok());
    ASSERT_TRUE(writer.build(&post).ok());
    EXPECT_EQ(buf.unique_terms(), 0U);
    std::remove(post_path.c_str());
}
