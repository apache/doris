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

#include "storage/index/inverted/spimi/segment_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/lucene_output.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/segment_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Drives the SPIMI writer pipeline end-to-end against in-memory
// buffers and constructs a `SpimiSegmentReader` over the output. A
// passing test means a query through the production-shaped read
// path (FieldInfos → TermDict → SpimiTermDocsReader) recovers the
// exact (doc_id, freq) postings the writer accepted.
struct SegmentFixture {
    MemoryLuceneOutput tis;
    MemoryLuceneOutput tii;
    MemoryLuceneOutput frq;
    MemoryLuceneOutput prx;
    MemoryLuceneOutput fnm;
    int32_t skip_interval;

    explicit SegmentFixture(int32_t skip_iv = TermDictWriter::kDefaultSkipInterval,
                            std::string_view field_name = "body")
            : skip_interval(skip_iv) {
        // Field 0 = the only field. has_prox=true → freq/positions kept.
        FieldInfoEntry fi {.name = std::string(field_name),
                           .is_indexed = true,
                           .omit_norms = true,
                           .has_prox = true};
        FieldInfosWriter fw(&fnm);
        fw.Write({fi});
    }

    SpimiSegmentReader Build(
            const std::vector<std::tuple<std::string, uint32_t, uint32_t>>& posts) {
        SpimiPostingBuffer buffer;
        for (const auto& [term, doc, pos] : posts) {
            buffer.Append(term, doc, pos);
        }
        buffer.Sort();
        SegmentWriter w(&tis, &tii, &frq, &prx, TermDictWriter::kDefaultIndexInterval,
                        skip_interval, TermDictWriter::kMaxSkipLevels);
        w.Emit(buffer, /*field_number=*/0);
        w.Close();
        return SpimiSegmentReader(tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes(), fnm.bytes());
    }
};

} // namespace

TEST(FieldInfosReaderTest, ReadsBackRoundTrippedFields) {
    // Two fields: body (full prox) + tags (omit_tfap → has_prox=false).
    std::vector<FieldInfoEntry> input {
            {.name = "body", .is_indexed = true, .omit_norms = true, .has_prox = true},
            {.name = "tags", .is_indexed = true, .omit_norms = true, .has_prox = false}};
    MemoryLuceneOutput out;
    FieldInfosWriter w(&out);
    w.Write(input);

    const auto round_tripped = FieldInfosReader::Read(out.bytes());
    ASSERT_EQ(round_tripped.size(), 2U);
    EXPECT_EQ(round_tripped[0].name, "body");
    EXPECT_TRUE(round_tripped[0].has_prox);
    EXPECT_EQ(round_tripped[1].name, "tags");
    EXPECT_FALSE(round_tripped[1].has_prox);
}

TEST(SpimiSegmentReaderTest, FindsSingleTermInSingleDoc) {
    SegmentFixture fx;
    auto reader = fx.Build({{"alpha", 0, 0}});

    const auto docs = reader.Search("body", "alpha");
    ASSERT_EQ(docs.size(), 1U);
    EXPECT_EQ(docs[0].first, 0);  // doc_id
    EXPECT_EQ(docs[0].second, 1); // freq (one position recorded)
}

TEST(SpimiSegmentReaderTest, ReturnsEmptyForMissingTerm) {
    SegmentFixture fx;
    auto reader = fx.Build({{"alpha", 0, 0}, {"beta", 1, 0}});
    EXPECT_TRUE(reader.Search("body", "missing").empty());
}

TEST(SpimiSegmentReaderTest, ReturnsEmptyForMissingField) {
    SegmentFixture fx;
    auto reader = fx.Build({{"alpha", 0, 0}});
    EXPECT_TRUE(reader.Search("not_a_field", "alpha").empty());
}

TEST(SpimiSegmentReaderTest, RecoversMultiDocPostingsWithFreqs) {
    SegmentFixture fx;
    // "the" appears 3 times in doc 0, 1 in doc 2, 2 in doc 5.
    auto reader = fx.Build({{"the", 0, 0},
                            {"the", 0, 1},
                            {"the", 0, 2},
                            {"the", 2, 0},
                            {"the", 5, 0},
                            {"the", 5, 7},
                            {"and", 1, 0},
                            {"and", 3, 0}});

    const auto the_docs = reader.Search("body", "the");
    ASSERT_EQ(the_docs.size(), 3U);
    EXPECT_EQ(the_docs[0], (std::pair<int32_t, int32_t> {0, 3}));
    EXPECT_EQ(the_docs[1], (std::pair<int32_t, int32_t> {2, 1}));
    EXPECT_EQ(the_docs[2], (std::pair<int32_t, int32_t> {5, 2}));

    const auto and_docs = reader.Search("body", "and");
    ASSERT_EQ(and_docs.size(), 2U);
    EXPECT_EQ(and_docs[0], (std::pair<int32_t, int32_t> {1, 1}));
    EXPECT_EQ(and_docs[1], (std::pair<int32_t, int32_t> {3, 1}));
}

TEST(SpimiSegmentReaderTest, HighDfTermRoutesThroughPforPath) {
    // skip_interval=8 ⇒ any term with df ≥ 8 triggers SPIMI PFOR
    // (kCodeModeSpimiPfor=0x05) at the .frq level. End-to-end
    // recovery via SegmentReader must still match exactly.
    SegmentFixture fx(/*skip_iv=*/8);
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> posts;
    for (uint32_t d = 0; d < 50; ++d) {
        posts.emplace_back("common", d, 0);
    }
    posts.emplace_back("rare", 100, 0);
    auto reader = fx.Build(posts);

    const auto common_docs = reader.Search("body", "common");
    ASSERT_EQ(common_docs.size(), 50U);
    for (size_t i = 0; i < 50U; ++i) {
        EXPECT_EQ(common_docs[i].first, static_cast<int32_t>(i)) << "i=" << i;
        EXPECT_EQ(common_docs[i].second, 1);
    }
    const auto rare_docs = reader.Search("body", "rare");
    ASSERT_EQ(rare_docs.size(), 1U);
    EXPECT_EQ(rare_docs[0].first, 100);
}

TEST(SpimiSegmentReaderTest, RandomisedSegmentRoundTrip) {
    // 30 terms, mixed cardinality. Some will exceed skip_interval=16
    // and route through the PFOR path; some will not. Reader must
    // recover ALL of them.
    SegmentFixture fx(/*skip_iv=*/16);
    std::mt19937 rng(0xBEEF12U);
    // freq counts ALL occurrences of (term, doc) — the SPIMI buffer
    // keeps duplicate positions, so the writer records their count
    // verbatim into the .frq freq stream. Don't dedupe positions.
    std::map<std::string, std::map<uint32_t, int32_t>> truth;
    std::vector<std::tuple<std::string, uint32_t, uint32_t>> posts;
    for (int i = 0; i < 2000; ++i) {
        char tbuf[8];
        std::snprintf(tbuf, sizeof(tbuf), "t%03d", static_cast<int>(rng() % 30));
        const std::string term = tbuf;
        const uint32_t doc = rng() % 300;
        const uint32_t pos = rng() % 100;
        posts.emplace_back(term, doc, pos);
        truth[term][doc] += 1;
    }

    auto reader = fx.Build(posts);
    for (const auto& [term, doc_map] : truth) {
        const auto found = reader.Search("body", term);
        ASSERT_EQ(found.size(), doc_map.size()) << "term=" << term;
        size_t i = 0;
        for (const auto& [doc, count] : doc_map) {
            EXPECT_EQ(found[i].first, static_cast<int32_t>(doc)) << "term=" << term << " i=" << i;
            EXPECT_EQ(found[i].second, count) << "term=" << term << " doc=" << doc;
            ++i;
        }
    }
    // 20 negative probes.
    for (int t = 0; t < 20; ++t) {
        char tbuf[16];
        std::snprintf(tbuf, sizeof(tbuf), "absent%03d", t);
        EXPECT_TRUE(reader.Search("body", tbuf).empty()) << tbuf;
    }
}

TEST(SpimiSegmentReaderTest, MultipleFieldsLookupByName) {
    // Build .fnm with two fields. Only field 0 has postings written
    // (SegmentWriter is per-field); we only verify name resolution
    // here.
    MemoryLuceneOutput fnm;
    {
        FieldInfosWriter w(&fnm);
        w.Write({{.name = "body", .is_indexed = true, .omit_norms = true, .has_prox = true},
                 {.name = "title", .is_indexed = true, .omit_norms = true, .has_prox = true}});
    }
    MemoryLuceneOutput tis, tii, frq, prx;
    SpimiPostingBuffer buf;
    buf.Append("hello", 0, 0);
    buf.Sort();
    SegmentWriter w(&tis, &tii, &frq, &prx);
    w.Emit(buf, /*field=*/0);
    w.Close();

    SpimiSegmentReader reader(tis.bytes(), tii.bytes(), frq.bytes(), prx.bytes(), fnm.bytes());
    ASSERT_EQ(reader.FieldInfos().size(), 2U);
    EXPECT_EQ(reader.FieldInfos()[0].name, "body");
    EXPECT_EQ(reader.FieldInfos()[1].name, "title");

    // Lookup on field 0 hits, field 1 returns empty (no postings).
    EXPECT_EQ(reader.Search("body", "hello").size(), 1U);
    EXPECT_TRUE(reader.Search("title", "hello").empty());
}

} // namespace doris::segment_v2::inverted_index::spimi
