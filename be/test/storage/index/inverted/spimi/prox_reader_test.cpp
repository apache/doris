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

#include "storage/index/inverted/spimi/prox_reader.h"

#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Drives `FreqProxEncoder` to populate a `.prx` buffer for one term
// and returns the bytes alongside the per-doc freq list the reader
// must consume. Mirrors the in-memory fixture used elsewhere — the
// reader is validated against bytes the real writer produced, so
// any drift between writer/reader would surface here.
struct OneTermProxFixture {
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    std::vector<int32_t> freqs;

    void WriteTerm(const std::vector<std::vector<int32_t>>& docs_positions) {
        FreqProxEncoder enc(&frq, &prx);
        int32_t total_df = static_cast<int32_t>(docs_positions.size());
        enc.StartTerm(total_df);
        int32_t doc_id = 0;
        for (const auto& positions : docs_positions) {
            const auto freq = static_cast<int32_t>(positions.size());
            freqs.push_back(freq);
            enc.StartDoc(doc_id++, freq);
            for (int32_t p : positions) {
                enc.AddPosition(p);
            }
            enc.FinishDoc();
        }
        (void)enc.FinishTerm();
    }
};

} // namespace

TEST(SpimiProxReaderTest, ReadsSinglePositionForSingleDoc) {
    OneTermProxFixture fx;
    fx.WriteTerm({{0}});
    const auto out = SpimiProxReader::ReadPositions(fx.prx.bytes(), fx.freqs);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0], (std::vector<int32_t> {0}));
}

TEST(SpimiProxReaderTest, ReadsMultiplePositionsInOneDoc) {
    // Positions are delta-encoded within a doc, so {3, 8, 17} on the
    // wire is VInt(3), VInt(5), VInt(9). The reader must accumulate
    // deltas back to absolute values.
    OneTermProxFixture fx;
    fx.WriteTerm({{3, 8, 17}});
    const auto out = SpimiProxReader::ReadPositions(fx.prx.bytes(), fx.freqs);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0], (std::vector<int32_t> {3, 8, 17}));
}

TEST(SpimiProxReaderTest, ResetsPositionDeltaPerDoc) {
    // Doc 0 ends at position 17; doc 1 starts at position 2.
    // The delta on the wire for doc 1's first position is 2, NOT
    // -15 (which would underflow VInt). Reader must reset its
    // `last` accumulator to 0 between docs.
    OneTermProxFixture fx;
    fx.WriteTerm({{3, 8, 17}, {2, 4}, {0, 11, 12, 13}});
    const auto out = SpimiProxReader::ReadPositions(fx.prx.bytes(), fx.freqs);
    ASSERT_EQ(out.size(), 3U);
    EXPECT_EQ(out[0], (std::vector<int32_t> {3, 8, 17}));
    EXPECT_EQ(out[1], (std::vector<int32_t> {2, 4}));
    EXPECT_EQ(out[2], (std::vector<int32_t> {0, 11, 12, 13}));
}

TEST(SpimiProxReaderTest, DuplicatePositionsZeroDelta) {
    // The encoder asserts non-decreasing positions, so duplicates
    // are allowed (delta=0). Reader must accept zero VInts.
    OneTermProxFixture fx;
    fx.WriteTerm({{5, 5, 5}});
    const auto out = SpimiProxReader::ReadPositions(fx.prx.bytes(), fx.freqs);
    ASSERT_EQ(out.size(), 1U);
    EXPECT_EQ(out[0], (std::vector<int32_t> {5, 5, 5}));
}

TEST(SpimiProxReaderTest, RandomisedDocPositionsRoundTrip) {
    OneTermProxFixture fx;
    std::mt19937 rng(0xC0DEBABEU);
    std::vector<std::vector<int32_t>> docs;
    for (int d = 0; d < 30; ++d) {
        const int n = 1 + static_cast<int>(rng() % 6);
        std::vector<int32_t> positions;
        int32_t last = 0;
        for (int i = 0; i < n; ++i) {
            last += static_cast<int32_t>(rng() % 8);
            positions.push_back(last);
        }
        docs.push_back(std::move(positions));
    }
    fx.WriteTerm(docs);
    const auto out = SpimiProxReader::ReadPositions(fx.prx.bytes(), fx.freqs);
    ASSERT_EQ(out.size(), docs.size());
    for (size_t i = 0; i < docs.size(); ++i) {
        EXPECT_EQ(out[i], docs[i]) << "doc=" << i;
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
