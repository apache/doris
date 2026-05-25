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

#include "storage/index/inverted/spimi/clucene_term_positions.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "storage/index/inverted/spimi/lucene_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_writer.h"
#include "storage/index/inverted/spimi/term_dict_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// End-to-end fixture: writes a SPIMI segment, holds the byte
// buffers, exposes TermDictReader + field info needed by
// SpimiCLuceneTermPositions. Tests assert that the (doc, freq,
// position*) tuples the writer accepted are recoverable.
struct PositionsFixture {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    MemoryLuceneOutput frq;
    MemoryLuceneOutput prx;
    int32_t skip_interval;
    std::vector<FieldInfoEntry> field_infos;
    std::vector<std::wstring> field_names_wide;
    std::unique_ptr<TermDictReader> dict;

    PositionsFixture(int32_t skip_iv = TermDictWriter::kDefaultSkipInterval, bool has_prox = true)
            : skip_interval(skip_iv) {
        field_infos.push_back(
                {.name = "body", .is_indexed = true, .omit_norms = true, .has_prox = has_prox});
        field_names_wide.emplace_back(L"body");
    }

    void Write(const std::vector<std::tuple<std::string, uint32_t, uint32_t>>& posts) {
        SpimiPostingBuffer buffer;
        for (const auto& [term, doc, pos] : posts) {
            buffer.Append(term, doc, pos);
        }
        buffer.Sort();
        SegmentWriter w(&tis, &tii, &frq, &prx, TermDictWriter::kDefaultIndexInterval,
                        skip_interval, TermDictWriter::kMaxSkipLevels,
                        /*omit_term_freq_and_positions=*/!field_infos[0].has_prox);
        w.Emit(buffer, /*field_number=*/0);
        w.Close();
        dict = std::make_unique<TermDictReader>(tis.bytes(), tii.bytes());
    }

    std::unique_ptr<SpimiCLuceneTermPositions> Make() {
        return std::make_unique<SpimiCLuceneTermPositions>(
                dict.get(), frq.bytes().data(), frq.bytes().size(), prx.bytes().data(),
                prx.bytes().size(), &field_infos, &field_names_wide);
    }
};

} // namespace

TEST(SpimiCLuceneTermPositionsTest, SingleDocSinglePosition) {
    PositionsFixture fx;
    fx.Write({{"alpha", 0, 0}});
    auto tp = fx.Make();
    auto* term = _CLNEW lucene::index::Term(L"body", L"alpha");
    tp->seek(term);
    ASSERT_TRUE(tp->next());
    EXPECT_EQ(tp->doc(), 0);
    EXPECT_EQ(tp->freq(), 1);
    EXPECT_EQ(tp->nextPosition(), 0);
    EXPECT_FALSE(tp->next());
    _CLDECDELETE(term);
}

TEST(SpimiCLuceneTermPositionsTest, MultiplePositionsInSingleDoc) {
    // Delta-encoded on the wire. Reader must accumulate.
    PositionsFixture fx;
    fx.Write({{"x", 0, 3}, {"x", 0, 8}, {"x", 0, 17}});
    auto tp = fx.Make();
    auto* term = _CLNEW lucene::index::Term(L"body", L"x");
    tp->seek(term);
    ASSERT_TRUE(tp->next());
    EXPECT_EQ(tp->freq(), 3);
    EXPECT_EQ(tp->nextPosition(), 3);
    EXPECT_EQ(tp->nextPosition(), 8);
    EXPECT_EQ(tp->nextPosition(), 17);
    EXPECT_FALSE(tp->next());
    _CLDECDELETE(term);
}

TEST(SpimiCLuceneTermPositionsTest, ResetsPositionsAcrossDocs) {
    PositionsFixture fx;
    // Doc 0: [3, 8]; doc 1: [2]; doc 2: [11, 13]. Reader must reset
    // the per-doc position cursor when advancing docs.
    fx.Write({{"x", 0, 3}, {"x", 0, 8}, {"x", 1, 2}, {"x", 2, 11}, {"x", 2, 13}});
    auto tp = fx.Make();
    auto* term = _CLNEW lucene::index::Term(L"body", L"x");
    tp->seek(term);

    ASSERT_TRUE(tp->next());
    EXPECT_EQ(tp->doc(), 0);
    EXPECT_EQ(tp->nextPosition(), 3);
    EXPECT_EQ(tp->nextPosition(), 8);

    ASSERT_TRUE(tp->next());
    EXPECT_EQ(tp->doc(), 1);
    EXPECT_EQ(tp->nextPosition(), 2);

    ASSERT_TRUE(tp->next());
    EXPECT_EQ(tp->doc(), 2);
    EXPECT_EQ(tp->nextPosition(), 11);
    EXPECT_EQ(tp->nextPosition(), 13);

    EXPECT_FALSE(tp->next());
    _CLDECDELETE(term);
}

TEST(SpimiCLuceneTermPositionsTest, OmitTfapFieldRejectsNextPosition) {
    PositionsFixture fx(/*skip_iv=*/16, /*has_prox=*/false);
    fx.Write({{"x", 0, 0}, {"x", 1, 0}});
    auto tp = fx.Make();
    auto* term = _CLNEW lucene::index::Term(L"body", L"x");
    tp->seek(term);
    ASSERT_TRUE(tp->next());
    // omit_tfap → no positions exist on disk. The earlier impl
    // returned 0 here, which silently produced wrong matches for
    // phrase queries running against an omit_tfap field. P47
    // tightened this to throw `CL_ERR_UnsupportedOperation` so
    // misuse surfaces loudly.
    EXPECT_THROW(tp->nextPosition(), CLuceneError);
    _CLDECDELETE(term);
}

TEST(SpimiCLuceneTermPositionsTest, PayloadVirtualsReturnDefaults) {
    PositionsFixture fx;
    fx.Write({{"x", 0, 0}});
    auto tp = fx.Make();
    auto* term = _CLNEW lucene::index::Term(L"body", L"x");
    tp->seek(term);
    ASSERT_TRUE(tp->next());
    (void)tp->nextPosition();
    EXPECT_EQ(tp->getPayloadLength(), 0);
    EXPECT_EQ(tp->getPayload(nullptr), nullptr);
    EXPECT_FALSE(tp->isPayloadAvailable());
    _CLDECDELETE(term);
}

TEST(SpimiCLuceneTermPositionsTest, DiamondDowncastHelpers) {
    // The __asTermDocs / __asTermPositions hooks let the CLucene
    // query engine recover the right base pointer without dynamic
    // RTTI. Verify both return the same `this` pointer.
    PositionsFixture fx;
    fx.Write({{"x", 0, 0}});
    auto tp = fx.Make();
    auto* term = _CLNEW lucene::index::Term(L"body", L"x");
    tp->seek(term);
    lucene::index::TermDocs* td = tp->__asTermDocs();
    lucene::index::TermPositions* tp_recovered = tp->__asTermPositions();
    EXPECT_NE(td, nullptr);
    EXPECT_NE(tp_recovered, nullptr);
    // Each pointer is to the same logical object — calling next()
    // on td and reading doc() via tp must reflect the same state.
    ASSERT_TRUE(td->next());
    EXPECT_EQ(tp_recovered->doc(), td->doc());
    _CLDECDELETE(term);
}

TEST(SpimiCLuceneTermPositionsTest, HighDfTermRoutesThroughPforPath) {
    // skip_interval=8 ⇒ encoder uses kCodeModeSpimiPfor at df ≥ 8.
    // Both .frq (via SpimiTermDocsReader inside parent) and .prx
    // (via SpimiProxReader) must recover correctly.
    PositionsFixture fx(/*skip_iv=*/8);
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> posts;
    for (uint32_t d = 0; d < 30; ++d) {
        // Each doc has 2 positions: d and d+1.
        posts.emplace_back("the", d, d);
        posts.emplace_back("the", d, d + 1);
    }
    fx.Write(posts);
    auto tp = fx.Make();
    auto* term = _CLNEW lucene::index::Term(L"body", L"the");
    tp->seek(term);
    EXPECT_EQ(tp->docFreq(), 30);

    for (uint32_t expected_doc = 0; expected_doc < 30; ++expected_doc) {
        ASSERT_TRUE(tp->next()) << "doc=" << expected_doc;
        EXPECT_EQ(tp->doc(), static_cast<int32_t>(expected_doc));
        EXPECT_EQ(tp->freq(), 2);
        EXPECT_EQ(tp->nextPosition(), static_cast<int32_t>(expected_doc));
        EXPECT_EQ(tp->nextPosition(), static_cast<int32_t>(expected_doc + 1));
    }
    EXPECT_FALSE(tp->next());
    _CLDECDELETE(term);
}

} // namespace doris::segment_v2::inverted_index::spimi
