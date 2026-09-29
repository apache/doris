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
#include <numeric>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/posting_window_emitter.h"
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

std::vector<uint8_t> WriteStreamedContainer(SniiIndexInput in, TermPostings postings) {
    const std::string path = TempPath();
    io::LocalFileWriter writer;
    EXPECT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    SniiStreamedIndexSession* session = nullptr;
    EXPECT_TRUE(compound.begin_streamed_index(std::move(in), &session).ok());
    SpanTermPostingSource source(postings.docids, postings.freqs, postings.positions_flat);
    EXPECT_TRUE(session->push_term(StreamedTermPostings {
                                           .term = std::move(postings.term),
                                           .retain_positions = postings.retain_positions,
                                           .source = &source,
                                   })
                        .ok());
    EXPECT_TRUE(session->finish().ok());
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

// A high-df term is consumed through bounded posting windows instead of first
// materializing a complete positions_flat vector. The streamed build must remain
// byte-identical to the explicit materialized test path, including every PRX byte.
TEST(SniiSpimiStreamingWriter, StreamedPositionsMatchMaterializedBytesHighDf) {
    // The wide term spans many posting windows. The small term exercises the slim
    // encoding shape in the same container.
    constexpr uint32_t kDocs = 80000;
    auto feed = [](SpimiTermBuffer* buf) {
        for (uint32_t d = 0; d < kDocs; ++d) {
            buf->add_token("hot", d, 0);
            if (d % 1000 == 0) {
                buf->add_token("cold", d, 1);
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
    stream_in.term_source = &stream_buf;
    const std::vector<uint8_t> stream_bytes = WriteContainer(stream_in);

    ASSERT_EQ(mat_bytes.size(), stream_bytes.size());
    EXPECT_EQ(mat_bytes, stream_bytes);
}

// A low-df term with many positions stays on the same bounded source contract while
// selecting the slim encoding shape. Byte identity covers that cross-product.
TEST(SniiSpimiStreamingWriter, StreamedLowDfHighNtokMatchesMaterialized) {
    constexpr uint32_t kDocs = 200;
    constexpr uint32_t kReps = 400;
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

// The import/SPIMI source path and the compaction-style pushed session must
// converge on the one window emitter after their producer-specific plumbing.
TEST(SniiSpimiStreamingWriter, SpimiAndStreamedSessionUseSameWindowEmitter) {
    constexpr uint32_t kDocs = 1024;

    doris::snii::writer::testing::reset_window_emitter_counters();
    SpimiTermBuffer spimi(/*has_positions=*/true);
    for (uint32_t doc = 0; doc < kDocs; ++doc) {
        spimi.add_token("hot", doc, 0);
    }
    SniiIndexInput spimi_input = BaseInput(kDocs);
    spimi_input.term_source = &spimi;
    const std::vector<uint8_t> spimi_bytes = WriteContainer(spimi_input);
    EXPECT_EQ(doris::snii::writer::testing::window_emitter_finished_terms(), 1U);

    doris::snii::writer::testing::reset_window_emitter_counters();
    TermPostings postings;
    postings.term = "hot";
    postings.docids.resize(kDocs);
    std::iota(postings.docids.begin(), postings.docids.end(), 0);
    postings.freqs.assign(kDocs, 1);
    postings.positions_flat.assign(kDocs, 0);
    const std::vector<uint8_t> streamed_bytes =
            WriteStreamedContainer(BaseInput(kDocs), std::move(postings));
    EXPECT_EQ(doris::snii::writer::testing::window_emitter_finished_terms(), 1U);
    EXPECT_EQ(streamed_bytes, spimi_bytes);
}
