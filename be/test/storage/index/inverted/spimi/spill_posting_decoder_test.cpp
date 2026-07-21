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

#include <cstdint>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/posting_decoder.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Helper: encode one term's posting data using FreqProxEncoder, then
// decode with PostingDecoder and compare.
struct EncodedTerm {
    std::vector<uint8_t> frq_bytes;
    std::vector<uint8_t> prx_bytes;
    TermInfo info;
};

// Encode a single term with positions using the default (VInt) code mode.
EncodedTerm EncodeOneTermWithProx(
        const std::vector<std::pair<int32_t, std::vector<int32_t>>>& docs) {
    MemoryByteOutput frq_out, prx_out;
    FreqProxEncoder enc(&frq_out, &prx_out);
    const auto df = static_cast<int32_t>(docs.size());
    enc.StartTerm(df);
    for (const auto& [doc_id, positions] : docs) {
        enc.StartDoc(doc_id, static_cast<int32_t>(positions.size()));
        for (int32_t pos : positions) {
            enc.AddPosition(pos);
        }
        enc.FinishDoc();
    }
    const TermInfo info = enc.FinishTerm();
    return {frq_out.bytes(), prx_out.bytes(), info};
}

// Encode a single term without positions (omit_term_freq_and_positions).
EncodedTerm EncodeOneTermNoProx(const std::vector<int32_t>& doc_ids) {
    MemoryByteOutput frq_out, prx_out;
    FreqProxEncoder enc(&frq_out, &prx_out, TermDictWriter::kDefaultSkipInterval,
                        TermDictWriter::kMaxSkipLevels,
                        /*omit_term_freq_and_positions=*/true);
    const auto df = static_cast<int32_t>(doc_ids.size());
    enc.StartTerm(df);
    for (int32_t doc_id : doc_ids) {
        enc.StartDoc(doc_id, 1);
        enc.FinishDoc();
    }
    const TermInfo info = enc.FinishTerm();
    return {frq_out.bytes(), prx_out.bytes(), info};
}

} // namespace

TEST(PostingDecoderTest, DefaultModeSingleDocOnePosition) {
    auto encoded = EncodeOneTermWithProx({{0, {5}}});
    auto docs = PostingDecoder::Decode(encoded.frq_bytes.data(), encoded.frq_bytes.size(),
                                       encoded.prx_bytes.data(), encoded.prx_bytes.size(),
                                       /*doc_freq=*/1, /*has_prox=*/true, encoded.info.is_slim);

    ASSERT_EQ(docs.size(), 1U);
    EXPECT_EQ(docs[0].doc_id, 0);
    EXPECT_EQ(docs[0].freq, 1);
    ASSERT_EQ(docs[0].positions.size(), 1U);
    EXPECT_EQ(docs[0].positions[0], 5);
}

TEST(PostingDecoderTest, DefaultModeMultiDoc) {
    auto encoded = EncodeOneTermWithProx({{2, {0}}, {7, {3}}, {15, {1, 4}}});
    auto docs = PostingDecoder::Decode(encoded.frq_bytes.data(), encoded.frq_bytes.size(),
                                       encoded.prx_bytes.data(), encoded.prx_bytes.size(),
                                       /*doc_freq=*/3, /*has_prox=*/true, encoded.info.is_slim);

    ASSERT_EQ(docs.size(), 3U);
    EXPECT_EQ(docs[0].doc_id, 2);
    EXPECT_EQ(docs[0].freq, 1);
    ASSERT_EQ(docs[0].positions.size(), 1U);
    EXPECT_EQ(docs[0].positions[0], 0);

    EXPECT_EQ(docs[1].doc_id, 7);
    EXPECT_EQ(docs[1].freq, 1);
    ASSERT_EQ(docs[1].positions.size(), 1U);
    EXPECT_EQ(docs[1].positions[0], 3);

    EXPECT_EQ(docs[2].doc_id, 15);
    EXPECT_EQ(docs[2].freq, 2);
    ASSERT_EQ(docs[2].positions.size(), 2U);
    EXPECT_EQ(docs[2].positions[0], 1);
    EXPECT_EQ(docs[2].positions[1], 4);
}

TEST(PostingDecoderTest, DefaultModeHighFreqDoc) {
    // One doc with freq=3 at positions 0, 4, 9.
    auto encoded = EncodeOneTermWithProx({{5, {0, 4, 9}}});
    auto docs = PostingDecoder::Decode(encoded.frq_bytes.data(), encoded.frq_bytes.size(),
                                       encoded.prx_bytes.data(), encoded.prx_bytes.size(),
                                       /*doc_freq=*/1, /*has_prox=*/true, encoded.info.is_slim);

    ASSERT_EQ(docs.size(), 1U);
    EXPECT_EQ(docs[0].doc_id, 5);
    EXPECT_EQ(docs[0].freq, 3);
    ASSERT_EQ(docs[0].positions.size(), 3U);
    EXPECT_EQ(docs[0].positions[0], 0);
    EXPECT_EQ(docs[0].positions[1], 4);
    EXPECT_EQ(docs[0].positions[2], 9);
}

TEST(PostingDecoderTest, NoProxModeDecodesDocIdsOnly) {
    auto encoded = EncodeOneTermNoProx({0, 3, 10});
    auto docs =
            PostingDecoder::Decode(encoded.frq_bytes.data(), encoded.frq_bytes.size(), nullptr, 0,
                                   /*doc_freq=*/3, /*has_prox=*/false, encoded.info.is_slim);

    ASSERT_EQ(docs.size(), 3U);
    EXPECT_EQ(docs[0].doc_id, 0);
    EXPECT_EQ(docs[0].freq, 1);
    EXPECT_TRUE(docs[0].positions.empty());

    EXPECT_EQ(docs[1].doc_id, 3);
    EXPECT_EQ(docs[2].doc_id, 10);
}

TEST(PostingDecoderTest, MultiPositionDocPositionDeltaResetsPerDoc) {
    // Two docs with positions; position deltas reset to 0 at each new doc.
    auto encoded = EncodeOneTermWithProx({{1, {0, 2, 5}}, {4, {1, 3}}});
    auto docs = PostingDecoder::Decode(encoded.frq_bytes.data(), encoded.frq_bytes.size(),
                                       encoded.prx_bytes.data(), encoded.prx_bytes.size(),
                                       /*doc_freq=*/2, /*has_prox=*/true, encoded.info.is_slim);

    ASSERT_EQ(docs.size(), 2U);

    // Doc 1: positions 0, 2, 5 (absolute).
    EXPECT_EQ(docs[0].doc_id, 1);
    EXPECT_EQ(docs[0].freq, 3);
    ASSERT_EQ(docs[0].positions.size(), 3U);
    EXPECT_EQ(docs[0].positions[0], 0);
    EXPECT_EQ(docs[0].positions[1], 2);
    EXPECT_EQ(docs[0].positions[2], 5);

    // Doc 4: positions 1, 3 (absolute, delta resets).
    EXPECT_EQ(docs[1].doc_id, 4);
    EXPECT_EQ(docs[1].freq, 2);
    ASSERT_EQ(docs[1].positions.size(), 2U);
    EXPECT_EQ(docs[1].positions[0], 1);
    EXPECT_EQ(docs[1].positions[1], 3);
}

TEST(PostingDecoderTest, ZeroDocFreqThrows) {
    std::vector<uint8_t> dummy = {0x00};
    EXPECT_ANY_THROW({
        PostingDecoder::Decode(dummy.data(), dummy.size(), nullptr, 0,
                               /*doc_freq=*/0, /*has_prox=*/false);
    });
}

TEST(PostingDecoderTest, EmptyFrqBufferThrows) {
    EXPECT_ANY_THROW({
        PostingDecoder::Decode(nullptr, 0, nullptr, 0,
                               /*doc_freq=*/1, /*has_prox=*/false);
    });
}

TEST(PostingDecoderTest, UnknownCodeModeThrows) {
    // Byte 0xFF is not a valid code mode.
    std::vector<uint8_t> bad = {0xFF, 0x01, 0x02};
    EXPECT_ANY_THROW({
        PostingDecoder::Decode(bad.data(), bad.size(), nullptr, 0,
                               /*doc_freq=*/1, /*has_prox=*/false);
    });
}

TEST(PostingDecoderTest, LargeDocIdDelta) {
    // Two docs with a large gap to test VInt delta encoding.
    auto encoded = EncodeOneTermWithProx({{0, {0}}, {10000, {5}}});
    auto docs = PostingDecoder::Decode(encoded.frq_bytes.data(), encoded.frq_bytes.size(),
                                       encoded.prx_bytes.data(), encoded.prx_bytes.size(),
                                       /*doc_freq=*/2, /*has_prox=*/true, encoded.info.is_slim);

    ASSERT_EQ(docs.size(), 2U);
    EXPECT_EQ(docs[0].doc_id, 0);
    EXPECT_EQ(docs[1].doc_id, 10000);
    ASSERT_EQ(docs[1].positions.size(), 1U);
    EXPECT_EQ(docs[1].positions[0], 5);
}

TEST(PostingDecoderTest, SingleDocNoPositions) {
    // Single doc, freq=1, no positions.
    auto encoded = EncodeOneTermNoProx({42});
    auto docs =
            PostingDecoder::Decode(encoded.frq_bytes.data(), encoded.frq_bytes.size(), nullptr, 0,
                                   /*doc_freq=*/1, /*has_prox=*/false, encoded.info.is_slim);

    ASSERT_EQ(docs.size(), 1U);
    EXPECT_EQ(docs[0].doc_id, 42);
    EXPECT_EQ(docs[0].freq, 1);
    EXPECT_TRUE(docs[0].positions.empty());
}

} // namespace doris::segment_v2::inverted_index::spimi
