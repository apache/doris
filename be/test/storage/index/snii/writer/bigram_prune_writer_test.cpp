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

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

// G01 part B writer gate: threshold-boundary DETERMINISM of the flush-time bigram
// df-prune, observed through the always-on writer seams
// (bigram_terms_materialized / bigram_terms_pruned), the dict (lookup hit/miss),
// the docs-only survivor layout (prx_len == 0), and the per-index meta threshold
// declaration. df == threshold-1 must prune; df == threshold must materialize --
// exactly, every build.
using namespace doris::snii;
using namespace doris::snii::snii_test;
namespace wtesting = doris::snii::writer::testing;

namespace {

constexpr uint32_t kDocCount = 100;
constexpr uint32_t kThreshold = 4;

std::vector<PostingDoc> one_position_docs(std::vector<uint32_t> docids) {
    std::vector<PostingDoc> docs;
    docs.reserve(docids.size());
    for (uint32_t docid : docids) {
        docs.push_back({docid, {0}});
    }
    return docs;
}

// A tiny kDocsPositions index: two unigrams, the sentinel, and two bigram terms
// straddling the threshold -- (aa,bb) at df == kThreshold - 1 (prune side) and
// (bb,cc) at df == kThreshold (materialize side).
Status build_boundary_index(MemoryFile* file, uint32_t bigram_prune_min_df,
                            reader::SniiSegmentReader* segment_reader,
                            reader::LogicalIndexReader* index_reader,
                            uint64_t bigram_prune_max_df = 0) {
    writer::SniiIndexInput input;
    input.index_id = 3;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = kDocCount;
    input.bigram_prune_min_df = bigram_prune_min_df;
    input.bigram_prune_max_df = bigram_prune_max_df;
    input.terms = {
            make_term("aa", one_position_docs({1, 2, 3})),
            make_term("bb", one_position_docs({1, 2, 3, 4, 5, 6})),
            make_term(format::make_phrase_bigram_sentinel_term(), one_position_docs({0})),
            make_term(format::make_phrase_bigram_term("aa", "bb"),
                      one_position_docs({1, 2, 3})), // df == kThreshold - 1
            make_term(format::make_phrase_bigram_term("bb", "cc"),
                      one_position_docs({4, 5, 6, 7})), // df == kThreshold
    };
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    writer::SniiCompoundWriter writer(file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    RETURN_IF_ERROR(writer.finish());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(file, segment_reader));
    return segment_reader->open_index(input.index_id, input.index_suffix, index_reader);
}

bool lookup_term(const reader::LogicalIndexReader& idx, const std::string& term,
                 format::DictEntry* entry) {
    bool found = false;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(idx.lookup(term, &found, entry, &frq_base, &prx_base));
    return found;
}

} // namespace

TEST(SniiBigramPruneWriter, ThresholdBoundaryIsDeterministic) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;

    wtesting::reset_bigram_prune_counters();
    assert_ok(build_boundary_index(&file, kThreshold, &segment_reader, &index_reader));

    // Exactly one prune decision on each side of the boundary; the sentinel is
    // counted by NEITHER seam (it is never prunable).
    EXPECT_EQ(wtesting::bigram_terms_pruned(), 1U);       // (aa,bb) df 3 < 4
    EXPECT_EQ(wtesting::bigram_terms_materialized(), 1U); // (bb,cc) df 4 >= 4

    // Dict state matches the seams: the pruned bigram has NO entry, the survivor
    // and the sentinel do.
    format::DictEntry entry;
    EXPECT_FALSE(lookup_term(index_reader, format::make_phrase_bigram_term("aa", "bb"), &entry));
    ASSERT_TRUE(lookup_term(index_reader, format::make_phrase_bigram_term("bb", "cc"), &entry));
    EXPECT_EQ(entry.df, kThreshold);
    // Docs-only survivor: no positions anywhere (inline prx bytes empty, no span).
    EXPECT_EQ(entry.prx_len, 0U);
    EXPECT_TRUE(entry.prx_bytes.empty());
    ASSERT_TRUE(lookup_term(index_reader, format::make_phrase_bigram_sentinel_term(), &entry));

    // Unigrams are untouched by the diet: positions still written.
    ASSERT_TRUE(lookup_term(index_reader, "aa", &entry));
    EXPECT_TRUE(entry.prx_len > 0 || !entry.prx_bytes.empty());

    // The applied threshold is declared in the per-index meta.
    EXPECT_EQ(index_reader.bigram_prune_min_df(), kThreshold);
}

TEST(SniiBigramPruneWriter, MaxDfUpperBoundaryIsDeterministic) {
    // G15 upper gate, min gate off (0) to isolate it: df == max must
    // materialize (the gate drops strictly-above), df == max + 1 must be
    // pruned -- exactly, every build -- and the meta must declare the applied
    // max so the reader still falls back on the resulting dict miss.
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;

    wtesting::reset_bigram_prune_counters();
    assert_ok(build_boundary_index(&file, /*bigram_prune_min_df=*/0, &segment_reader, &index_reader,
                                   /*bigram_prune_max_df=*/3));

    // (aa,bb) df 3 == max -> materialize; (bb,cc) df 4 > max -> pruned. The
    // upper drop is counted by its OWN seam (never by the min-gate one); the
    // sentinel (df 1) counts nowhere, as always.
    EXPECT_EQ(wtesting::bigram_terms_max_pruned(), 1U);
    EXPECT_EQ(wtesting::bigram_terms_pruned(), 0U);
    EXPECT_EQ(wtesting::bigram_terms_materialized(), 1U);

    format::DictEntry entry;
    ASSERT_TRUE(lookup_term(index_reader, format::make_phrase_bigram_term("aa", "bb"), &entry));
    EXPECT_EQ(entry.df, 3U);
    EXPECT_FALSE(lookup_term(index_reader, format::make_phrase_bigram_term("bb", "cc"), &entry));
    ASSERT_TRUE(lookup_term(index_reader, format::make_phrase_bigram_sentinel_term(), &entry));

    // Max-only declaration: min stays 0, max lands in the meta (this is what
    // keeps the reader's dict-miss fallback armed without the min gate).
    EXPECT_EQ(index_reader.bigram_prune_min_df(), 0U);
    EXPECT_EQ(index_reader.bigram_prune_max_df(), 3U);
}

TEST(SniiBigramPruneWriter, MinAndMaxGatesTogetherKeepOnlyMiddleBand) {
    // Both gates armed at once (min == 4, max == 6): the df axis must split into
    // exactly three bands -- df < 4 min-pruned, 4 <= df <= 6 materialized
    // (BOTH boundaries inclusive-keep), df > 6 max-pruned -- each drop counted
    // by its OWN seam, and the meta declaring BOTH applied thresholds.
    constexpr uint32_t kMin = 4;
    constexpr uint64_t kMax = 6;
    writer::SniiIndexInput input;
    input.index_id = 3;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = kDocCount;
    input.bigram_prune_min_df = kMin;
    input.bigram_prune_max_df = kMax;
    input.terms = {
            make_term("aa", one_position_docs({1, 2, 3})),
            make_term(format::make_phrase_bigram_sentinel_term(), one_position_docs({0})),
            make_term(format::make_phrase_bigram_term("aa", "bb"),
                      one_position_docs({1, 2, 3})), // df 3 <  min: LOW tail, pruned
            make_term(format::make_phrase_bigram_term("bb", "cc"),
                      one_position_docs({1, 2, 3, 4})), // df 4 == min: kept
            make_term(format::make_phrase_bigram_term("cc", "dd"),
                      one_position_docs({1, 2, 3, 4, 5, 6})), // df 6 == max: kept
            make_term(format::make_phrase_bigram_term("dd", "ee"),
                      one_position_docs({1, 2, 3, 4, 5, 6, 7})), // df 7 > max: HIGH tail, pruned
    };
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    MemoryFile file;
    wtesting::reset_bigram_prune_counters();
    writer::SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());

    EXPECT_EQ(wtesting::bigram_terms_pruned(), 1U);       // (aa,bb) via the MIN gate
    EXPECT_EQ(wtesting::bigram_terms_max_pruned(), 1U);   // (dd,ee) via the MAX gate
    EXPECT_EQ(wtesting::bigram_terms_materialized(), 2U); // the middle band

    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &index_reader));

    format::DictEntry entry;
    EXPECT_FALSE(lookup_term(index_reader, format::make_phrase_bigram_term("aa", "bb"), &entry));
    ASSERT_TRUE(lookup_term(index_reader, format::make_phrase_bigram_term("bb", "cc"), &entry));
    EXPECT_EQ(entry.df, kMin);
    ASSERT_TRUE(lookup_term(index_reader, format::make_phrase_bigram_term("cc", "dd"), &entry));
    EXPECT_EQ(entry.df, kMax);
    EXPECT_FALSE(lookup_term(index_reader, format::make_phrase_bigram_term("dd", "ee"), &entry));
    ASSERT_TRUE(lookup_term(index_reader, format::make_phrase_bigram_sentinel_term(), &entry));

    EXPECT_EQ(index_reader.bigram_prune_min_df(), kMin);
    EXPECT_EQ(index_reader.bigram_prune_max_df(), kMax);
}

TEST(SniiBigramPruneWriter, ThresholdZeroKeepsLegacyLayout) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;

    wtesting::reset_bigram_prune_counters();
    assert_ok(
            build_boundary_index(&file, /*bigram_prune_min_df=*/0, &segment_reader, &index_reader));

    // No pruning; both bigrams materialize (the seam counts them either way).
    EXPECT_EQ(wtesting::bigram_terms_pruned(), 0U);
    EXPECT_EQ(wtesting::bigram_terms_materialized(), 2U);

    // Legacy layout: every bigram present WITH positions; meta declares nothing.
    format::DictEntry entry;
    ASSERT_TRUE(lookup_term(index_reader, format::make_phrase_bigram_term("aa", "bb"), &entry));
    EXPECT_TRUE(entry.prx_len > 0 || !entry.prx_bytes.empty());
    ASSERT_TRUE(lookup_term(index_reader, format::make_phrase_bigram_term("bb", "cc"), &entry));
    EXPECT_TRUE(entry.prx_len > 0 || !entry.prx_bytes.empty());
    EXPECT_EQ(index_reader.bigram_prune_min_df(), 0U);
    EXPECT_EQ(index_reader.bigram_prune_max_df(), 0U);
}

TEST(SniiBigramPruneWriter, DocsOnlyConfigForcesThresholdOff) {
    // A non-positional index never emits bigrams; a (mis)configured threshold must
    // not leak into its meta (the reader would otherwise take a pointless
    // fallback branch for every 2-term phrase-shaped lookup).
    MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 4;
    input.index_suffix = "Tag";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = kDocCount;
    input.bigram_prune_min_df = kThreshold;
    input.bigram_prune_max_df = kThreshold;
    input.terms = {make_term("aa", one_position_docs({1, 2, 3}))};

    writer::SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());

    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &index_reader));
    EXPECT_EQ(index_reader.bigram_prune_min_df(), 0U);
    EXPECT_EQ(index_reader.bigram_prune_max_df(), 0U);
}

TEST(SniiBigramPruneWriter, DefaultThresholdFormula) {
    // max(64, doc_count / 10000): the floor holds through 640k docs, then the
    // 0.01% ratio takes over.
    EXPECT_EQ(format::default_phrase_bigram_prune_min_df(0), 64U);
    EXPECT_EQ(format::default_phrase_bigram_prune_min_df(9000), 64U);
    EXPECT_EQ(format::default_phrase_bigram_prune_min_df(640000), 64U);
    EXPECT_EQ(format::default_phrase_bigram_prune_min_df(649999), 64U);
    EXPECT_EQ(format::default_phrase_bigram_prune_min_df(650000), 65U);
    EXPECT_EQ(format::default_phrase_bigram_prune_min_df(10'000'000), 1000U);
}

// G16: a prune-mode bigram whose df crosses the windowed threshold writes NO
// freq-block -- its frq span is exactly the docs-only prefix [prelude][dd-block]
// (frq_docs_len == frq_len), the prelude flags declare has_freq=false, and the
// per-window freq locators/crcs vanish with it. A same-df unigram keeps its
// freq suffix untouched. The docid-only read path (the ONLY consumer of pair
// postings) must decode the freq-less posting identically. Slim survivors are
// covered by the boundary tests above and KEEP freq (their DictEntry region
// metadata is tier-conditioned, not per-entry) -- the elision seam counts
// windowed entries only.
TEST(SniiBigramPruneWriter, WindowedSurvivorDropsFreqBlock) {
    wtesting::reset_bigram_prune_counters();
    constexpr uint32_t kDf = format::kSlimDfThreshold + 300; // windowed on both terms
    std::vector<uint32_t> ids(kDf);
    std::iota(ids.begin(), ids.end(), 1U);

    writer::SniiIndexInput input;
    input.index_id = 3;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 2 * kDf;
    input.bigram_prune_min_df = kThreshold; // prune mode ON
    input.terms = {
            make_term("aa", one_position_docs(ids)),
            make_term(format::make_phrase_bigram_sentinel_term(), one_position_docs({0})),
            make_term(format::make_phrase_bigram_term("aa", "bb"), one_position_docs(ids)),
    };
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    MemoryFile file;
    writer::SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader idx;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &idx));

    bool found = false;
    format::DictEntry bigram_entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(idx.lookup(format::make_phrase_bigram_term("aa", "bb"), &found, &bigram_entry,
                         &frq_base, &prx_base));
    ASSERT_TRUE(found);
    EXPECT_TRUE(bigram_entry.has_sb);                           // windowed, not slim
    EXPECT_EQ(bigram_entry.prx_len, 0U);                        // G01 diet
    EXPECT_EQ(bigram_entry.frq_docs_len, bigram_entry.frq_len); // G16: no freq-block
    EXPECT_EQ(wtesting::bigram_freqs_elided(), 1U);             // sentinel is slim: no count

    format::DictEntry unigram_entry;
    uint64_t uni_frq_base = 0;
    uint64_t uni_prx_base = 0;
    assert_ok(idx.lookup("aa", &found, &unigram_entry, &uni_frq_base, &uni_prx_base));
    ASSERT_TRUE(found);
    EXPECT_TRUE(unigram_entry.has_sb);
    EXPECT_LT(unigram_entry.frq_docs_len, unigram_entry.frq_len); // unigram keeps freq

    // G16-e: the docs-only bigram uses kBigramWindowDocs-sized windows -- at
    // this df it collapses to ONE window, so its prelude is strictly smaller
    // than the unigram's (4 x 256-doc adaptive windows at the same df).
    EXPECT_LT(bigram_entry.prelude_len, unigram_entry.prelude_len);

    std::vector<uint32_t> docids;
    assert_ok(query::internal::read_docid_posting(idx, bigram_entry, frq_base, prx_base, &docids));
    ASSERT_EQ(docids.size(), kDf);
    EXPECT_EQ(docids.front(), 1U);
    EXPECT_EQ(docids.back(), kDf);

    // A want_freq read of the elided entry must fail with the SEMANTIC guard
    // error, not a deep region-decode corruption. This also pins the prelude
    // flags: had the writer declared has_freq=true over an empty freq-block,
    // the guard would pass and the failure (if any) would read differently.
    reader::DecodedPosting decoded;
    Status st =
            reader::read_windowed_posting(idx, bigram_entry, frq_base, prx_base,
                                          /*want_positions=*/false, /*want_freq=*/true, &decoded);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("freqs requested but prelude has none"), std::string::npos)
            << st.to_string();
}

// Legacy mode (threshold 0) must keep the pre-G01 layout for WINDOWED bigrams
// too: positions AND freq both present. Pins the write_freq predicate to the
// same min-df escape hatch as write_prx (the G16 review found no test asserted
// legacy freq presence).
TEST(SniiBigramPruneWriter, LegacyWindowedBigramKeepsFreqAndPrx) {
    constexpr uint32_t kDf = format::kSlimDfThreshold + 300;
    std::vector<uint32_t> ids(kDf);
    std::iota(ids.begin(), ids.end(), 1U);

    writer::SniiIndexInput input;
    input.index_id = 3;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 2 * kDf;
    input.bigram_prune_min_df = 0; // legacy: no pruning, full layout
    input.terms = {
            make_term("aa", one_position_docs(ids)),
            make_term(format::make_phrase_bigram_sentinel_term(), one_position_docs({0})),
            make_term(format::make_phrase_bigram_term("aa", "bb"), one_position_docs(ids)),
    };
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    MemoryFile file;
    writer::SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader idx;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &idx));

    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(idx.lookup(format::make_phrase_bigram_term("aa", "bb"), &found, &entry, &frq_base,
                         &prx_base));
    ASSERT_TRUE(found);
    EXPECT_TRUE(entry.has_sb);
    EXPECT_GT(entry.prx_len, 0U);                 // legacy keeps positions
    EXPECT_LT(entry.frq_docs_len, entry.frq_len); // legacy keeps freq

    reader::DecodedPosting decoded;
    assert_ok(reader::read_windowed_posting(idx, entry, frq_base, prx_base,
                                            /*want_positions=*/true, /*want_freq=*/true, &decoded));
    ASSERT_EQ(decoded.docids.size(), kDf);
    ASSERT_EQ(decoded.freqs.size(), kDf);
    EXPECT_EQ(decoded.freqs.front(), 1U);
}

// G16-c: write_freq == false drops the freq layout for UNIGRAMS too (freq
// serves only BM25 scoring, which the Doris adapter resolves as unreachable
// for plain positions indexes). Windowed unigrams self-describe via the
// prelude flags; slim pod / inline unigrams carry a zero-length freq region
// (value-driven: frq_docs_len == frq_len / frq_bytes == [dd]). Positions stay
// intact -- the phrase fallback (want_positions=true, want_freq=false) must
// decode identically.
TEST(SniiBigramPruneWriter, WriteFreqOffDropsUnigramFreqLayout) {
    constexpr uint32_t kDf = format::kSlimDfThreshold + 300; // windowed
    std::vector<uint32_t> ids(kDf);
    std::iota(ids.begin(), ids.end(), 1U);

    writer::SniiIndexInput input;
    input.index_id = 3;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 2 * kDf;
    input.bigram_prune_min_df = kThreshold;
    input.write_freq = false; // G16-c drop
    input.terms = {
            make_term("aa", one_position_docs(ids)),          // windowed unigram
            make_term("zz", one_position_docs({1, 2, 3, 4})), // slim unigram
            make_term(format::make_phrase_bigram_sentinel_term(), one_position_docs({0})),
            make_term(format::make_phrase_bigram_term("aa", "bb"), one_position_docs(ids)),
    };
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    MemoryFile file;
    writer::SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader idx;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &idx));

    bool found = false;
    format::DictEntry uni;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(idx.lookup("aa", &found, &uni, &frq_base, &prx_base));
    ASSERT_TRUE(found);
    EXPECT_TRUE(uni.has_sb);
    EXPECT_EQ(uni.frq_docs_len, uni.frq_len); // no freq-block
    EXPECT_GT(uni.prx_len, 0U);               // positions kept
    // G16-f: the freq-dropped index also elides the per-entry ttf/max_freq
    // stats (block header kNoTermStats); df stays for the count fastpath.
    EXPECT_EQ(uni.ttf_delta, 0U);
    EXPECT_EQ(uni.max_freq, 0U);
    EXPECT_EQ(uni.df, kDf);

    // Phrase-fallback-shaped read (positions without freq) decodes fully.
    reader::DecodedPosting decoded;
    assert_ok(reader::read_windowed_posting(idx, uni, frq_base, prx_base,
                                            /*want_positions=*/true, /*want_freq=*/false,
                                            &decoded));
    ASSERT_EQ(decoded.docids.size(), kDf);
    ASSERT_EQ(decoded.positions.size(), kDf);
    EXPECT_TRUE(decoded.freqs.empty());

    // A scoring-shaped read fails with the semantic guard error.
    Status st =
            reader::read_windowed_posting(idx, uni, frq_base, prx_base,
                                          /*want_positions=*/false, /*want_freq=*/true, &decoded);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("freqs requested but prelude has none"), std::string::npos);

    // Slim unigram: zero-length freq region, docs-only prefix == whole span.
    format::DictEntry slim;
    uint64_t sfrq = 0;
    uint64_t sprx = 0;
    assert_ok(idx.lookup("zz", &found, &slim, &sfrq, &sprx));
    ASSERT_TRUE(found);
    EXPECT_FALSE(slim.has_sb);
    if (slim.kind == format::DictEntryKind::kInline) {
        EXPECT_EQ(slim.frq_bytes.size(), slim.inline_dd_disk_len);
    } else {
        EXPECT_EQ(slim.frq_docs_len, slim.frq_len);
    }
    std::vector<uint32_t> slim_docids;
    assert_ok(query::internal::read_docid_posting(idx, slim, sfrq, sprx, &slim_docids));
    EXPECT_EQ(slim_docids.size(), 4U);
}

namespace doris::segment_v2 {
// Production freq policy resolver (index_file_writer.cpp) -- declared here so
// the UT drives the actual decision line the Doris adapter uses.
bool snii_effective_write_freq(doris::snii::format::IndexConfig index_config);
} // namespace doris::segment_v2

// G16-c: the adapter-level policy line. A scoring config ALWAYS keeps freq; a
// plain positions config follows the escape-hatch BE config (default false ==
// drop). This is the only live control of the production freq layout, so pin
// all three states.
TEST(SniiBigramPruneWriter, EffectiveWriteFreqResolver) {
    const bool saved = doris::config::snii_positions_index_write_freq;
    doris::config::snii_positions_index_write_freq = false;
    EXPECT_FALSE(doris::segment_v2::snii_effective_write_freq(format::IndexConfig::kDocsPositions));
    EXPECT_TRUE(doris::segment_v2::snii_effective_write_freq(
            format::IndexConfig::kDocsPositionsScoring));
    doris::config::snii_positions_index_write_freq = true;
    EXPECT_TRUE(doris::segment_v2::snii_effective_write_freq(format::IndexConfig::kDocsPositions));
    doris::config::snii_positions_index_write_freq = saved;
}
