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

#include "storage/index/inverted/spimi/query_term_docs.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/query_term_enum.h"
#include "storage/index/inverted/spimi/segment_writer.h"
#include "storage/index/inverted/spimi/term_dict_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// End-to-end fixture: writes a SPIMI segment, owns the byte
// buffers, exposes constructed TermDictReader + FieldInfos, and
// makes it easy to construct SpimiQueryTermDocs over the same
// data the writer produced. This catches both writer/reader
// drift and CLucene contract regressions in one place.
struct TermDocsFixture {
    MemoryByteOutput tis;
    MemoryByteOutput tii;
    MemoryByteOutput frq;
    MemoryByteOutput prx;
    int32_t skip_interval;
    std::vector<FieldInfoEntry> field_infos;
    std::vector<std::wstring> field_names_wide;
    std::unique_ptr<TermDictReader> dict;

    explicit TermDocsFixture(int32_t skip_iv = TermDictWriter::kDefaultSkipInterval,
                             std::string_view field_name = "body", bool has_prox = true)
            : skip_interval(skip_iv) {
        field_infos.push_back({.name = std::string(field_name),
                               .is_indexed = true,
                               .omit_norms = true,
                               .has_prox = has_prox});
        field_names_wide.push_back(Utf8ToWide(std::string(field_name)));
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

    std::unique_ptr<SpimiQueryTermDocs> MakeTermDocs() {
        return std::make_unique<SpimiQueryTermDocs>(dict.get(), frq.bytes().data(),
                                                    frq.bytes().size(), &field_infos,
                                                    &field_names_wide);
    }

    // Same as Write(), but emits the V4 WINDOWED `.frq`/`.prx` format so the
    // SpimiQueryTermDocs path exercises the LAZY window-addressed reader.
    void WriteWindowed(const std::vector<std::tuple<std::string, uint32_t, uint32_t>>& posts) {
        SpimiPostingBuffer buffer;
        for (const auto& [term, doc, pos] : posts) {
            buffer.Append(term, doc, pos);
        }
        buffer.Sort();
        SegmentWriter w(&tis, &tii, &frq, &prx, TermDictWriter::kDefaultIndexInterval,
                        skip_interval, TermDictWriter::kMaxSkipLevels,
                        /*omit_term_freq_and_positions=*/!field_infos[0].has_prox,
                        /*use_windowed=*/true);
        w.Emit(buffer, /*field_number=*/0);
        w.Close();
        dict = std::make_unique<TermDictReader>(tis.bytes(), tii.bytes());
    }

    std::unique_ptr<SpimiQueryTermEnum> MakeEnum() {
        return std::make_unique<SpimiQueryTermEnum>(tis.bytes().data(), tis.bytes().size(),
                                                    skip_interval, field_names_wide);
    }
};

} // namespace

TEST(SpimiQueryTermDocsTest, SeekByTermAndIterate) {
    TermDocsFixture fx;
    fx.Write({{"alpha", 0, 0}, {"alpha", 5, 0}, {"beta", 1, 0}});
    auto td = fx.MakeTermDocs();

    auto* term = _CLNEW lucene::index::Term(L"body", L"alpha");
    td->seek(term);
    EXPECT_EQ(td->docFreq(), 2);

    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 0);
    EXPECT_EQ(td->freq(), 1);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 5);
    EXPECT_EQ(td->freq(), 1);
    EXPECT_FALSE(td->next());
    _CLDECDELETE(term);
}

TEST(SpimiQueryTermDocsTest, SeekMissingTermYieldsZeroDocFreq) {
    TermDocsFixture fx;
    fx.Write({{"alpha", 0, 0}});
    auto td = fx.MakeTermDocs();

    auto* term = _CLNEW lucene::index::Term(L"body", L"missing");
    td->seek(term);
    EXPECT_EQ(td->docFreq(), 0);
    EXPECT_FALSE(td->next());
    _CLDECDELETE(term);
}

TEST(SpimiQueryTermDocsTest, SeekUnknownFieldYieldsZeroDocFreq) {
    TermDocsFixture fx;
    fx.Write({{"alpha", 0, 0}});
    auto td = fx.MakeTermDocs();

    auto* term = _CLNEW lucene::index::Term(L"unknown_field", L"alpha");
    td->seek(term);
    EXPECT_EQ(td->docFreq(), 0);
    EXPECT_FALSE(td->next());
    _CLDECDELETE(term);
}

TEST(SpimiQueryTermDocsTest, SeekByTermEnumFastPath) {
    // When the TermEnum is our own SpimiQueryTermEnum, seek skips
    // the .tis lookup and uses the cached TermInfo.
    TermDocsFixture fx;
    fx.Write({{"alpha", 0, 0}, {"alpha", 7, 0}, {"alpha", 7, 1}, {"beta", 2, 0}});
    auto td = fx.MakeTermDocs();
    auto en = fx.MakeEnum();

    ASSERT_TRUE(en->next()); // alpha
    td->seek(en.get());
    EXPECT_EQ(td->docFreq(), 2);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 0);
    EXPECT_EQ(td->freq(), 1);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 7);
    EXPECT_EQ(td->freq(), 2);
    EXPECT_FALSE(td->next());

    ASSERT_TRUE(en->next()); // beta
    td->seek(en.get());
    EXPECT_EQ(td->docFreq(), 1);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 2);
}

TEST(SpimiQueryTermDocsTest, BatchReadAndSkipTo) {
    TermDocsFixture fx;
    // Multiple docs containing "x" — exercise both batch read and
    // skipTo (binary-search-like fast forward).
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> posts;
    for (uint32_t d : {1U, 3U, 5U, 7U, 9U, 11U, 13U}) {
        posts.emplace_back("x", d, 0);
    }
    fx.Write(posts);
    auto td = fx.MakeTermDocs();
    auto* term = _CLNEW lucene::index::Term(L"body", L"x");
    td->seek(term);
    EXPECT_EQ(td->docFreq(), 7);

    int32_t docs[3];
    int32_t freqs[3];
    int32_t n = td->read(docs, freqs, 3);
    EXPECT_EQ(n, 3);
    EXPECT_EQ(docs[0], 1);
    EXPECT_EQ(docs[1], 3);
    EXPECT_EQ(docs[2], 5);
    EXPECT_EQ(freqs[0], 1);

    // skipTo past several docs.
    ASSERT_TRUE(td->skipTo(10));
    EXPECT_EQ(td->doc(), 11);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 13);
    EXPECT_FALSE(td->next());

    // skipTo past end returns false.
    td->seek(term);
    EXPECT_FALSE(td->skipTo(1000));
    _CLDECDELETE(term);
}

TEST(SpimiQueryTermDocsTest, HighDfTermRoutesThroughPforPath) {
    // skip_interval=8 ⇒ encoder switches to kCodeModeSpimiPfor at
    // df >= 8. End-to-end recovery via TermDocs validates the full
    // SpimiTermDocsReader → SpimiQueryTermDocs chain across the
    // SPIMI PFOR format.
    TermDocsFixture fx(/*skip_iv=*/8);
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> posts;
    for (uint32_t d = 0; d < 100; ++d) {
        posts.emplace_back("the", d, 0);
    }
    fx.Write(posts);
    auto td = fx.MakeTermDocs();
    auto* term = _CLNEW lucene::index::Term(L"body", L"the");
    td->seek(term);
    EXPECT_EQ(td->docFreq(), 100);
    for (int32_t i = 0; i < 100; ++i) {
        ASSERT_TRUE(td->next()) << "i=" << i;
        EXPECT_EQ(td->doc(), i);
        EXPECT_EQ(td->freq(), 1);
    }
    EXPECT_FALSE(td->next());
    _CLDECDELETE(term);
}

TEST(SpimiQueryTermDocsTest, ResetsStateBetweenSeeks) {
    TermDocsFixture fx;
    fx.Write({{"alpha", 0, 0}, {"alpha", 3, 0}, {"beta", 5, 0}, {"beta", 7, 0}, {"beta", 9, 0}});
    auto td = fx.MakeTermDocs();

    auto* alpha = _CLNEW lucene::index::Term(L"body", L"alpha");
    td->seek(alpha);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 0);

    // Seek to a different term — internal index must reset.
    auto* beta = _CLNEW lucene::index::Term(L"body", L"beta");
    td->seek(beta);
    EXPECT_EQ(td->docFreq(), 3);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 5); // not 3 or 0 from previous seek
    _CLDECDELETE(alpha);
    _CLDECDELETE(beta);
}

// End-to-end: a V4 WINDOWED segment queried through SpimiQueryTermDocs must
// route through the LAZY window reader and return byte-identical results to the
// legacy eager path. This is the wiring regression for the lazy path; the
// per-window decode correctness itself is covered exhaustively by
// window_term_reader_test.cpp.
TEST(SpimiQueryTermDocsTest, WindowedSegmentSeekIterateAndSkip) {
    TermDocsFixture fx;
    // A large term ("x", on many docs) so it encodes as a multi-window V4 block;
    // plus a small term ("y") to exercise a single-window windowed block.
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> posts;
    std::vector<uint32_t> x_docs;
    uint32_t doc = 1;
    for (int i = 0; i < 3000; ++i) {
        posts.emplace_back("x", doc, 0);
        // a second position on every 3rd doc → varied freq.
        if (i % 3 == 0) {
            posts.emplace_back("x", doc, 1);
        }
        x_docs.push_back(doc);
        doc += 1 + (i % 7); // irregular gaps → fine windowing
    }
    posts.emplace_back("y", 2, 0);
    posts.emplace_back("y", 100, 0);
    fx.WriteWindowed(posts);

    // The windowed block must really have been emitted (else this test is vacuous).
    ASSERT_EQ(fx.frq.bytes()[0], FreqProxEncoder::kCodeModeSpimiWindowed);

    auto td = fx.MakeTermDocs();
    auto* term = _CLNEW lucene::index::Term(L"body", L"x");

    // Full scan matches the known doc ids.
    td->seek(term);
    EXPECT_EQ(td->docFreq(), static_cast<int32_t>(x_docs.size()));
    EXPECT_EQ(td->doc(), -1); // pre-start
    size_t i = 0;
    while (td->next()) {
        ASSERT_LT(i, x_docs.size());
        EXPECT_EQ(td->doc(), static_cast<int32_t>(x_docs[i]));
        EXPECT_GE(td->freq(), 1);
        ++i;
    }
    EXPECT_EQ(i, x_docs.size());
    EXPECT_FALSE(td->next());

    // skipTo lands on the first doc >= target.
    td->seek(term);
    const int32_t target = static_cast<int32_t>(x_docs[x_docs.size() - 10]);
    ASSERT_TRUE(td->skipTo(target));
    EXPECT_EQ(td->doc(), target);
    // continue to end.
    size_t j = x_docs.size() - 10;
    while (td->next()) {
        ++j;
        ASSERT_LT(j, x_docs.size());
        EXPECT_EQ(td->doc(), static_cast<int32_t>(x_docs[j]));
    }
    EXPECT_EQ(j, x_docs.size() - 1);

    // skipTo past end exhausts.
    td->seek(term);
    EXPECT_FALSE(td->skipTo(static_cast<int32_t>(x_docs.back()) + 1));

    // readRange chunk path over the windowed term.
    td->seek(term);
    DocRange dr;
    size_t collected = 0;
    while (td->readRange(&dr)) {
        if (dr.type_ == DocRangeType::kMany) {
            collected += dr.doc_many_size_;
        } else {
            collected += dr.doc_range.second - dr.doc_range.first;
        }
    }
    EXPECT_EQ(collected, x_docs.size());

    // Small windowed term.
    auto* y = _CLNEW lucene::index::Term(L"body", L"y");
    td->seek(y);
    EXPECT_EQ(td->docFreq(), 2);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 2);
    ASSERT_TRUE(td->next());
    EXPECT_EQ(td->doc(), 100);
    EXPECT_FALSE(td->next());

    _CLDECDELETE(term);
    _CLDECDELETE(y);
}

} // namespace doris::segment_v2::inverted_index::spimi
