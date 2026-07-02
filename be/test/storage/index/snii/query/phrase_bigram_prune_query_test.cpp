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

#include "common/status.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

// G01 "bigram diet" reader/writer behavior over the shared 9000-doc fixture
// (snii_query_test_util.h), built in three layouts:
//   control : include_phrase_bigrams=true,  threshold 0  (legacy: every hand-fed
//             bigram materialized WITH positions; miss == empty)
//   pruned  : include_phrase_bigrams=true,  threshold 64 (low-df bigrams dropped,
//             the surviving df-9000 bigram(repeat,repeat) written docs-only, meta
//             records the threshold)
//   nobigram: include_phrase_bigrams=false, threshold 0  (no bigram terms and no
//             sentinel -> the generic positions-verification path == ground truth)
// Fixture bigram dfs: (failed,order)=3, (failed,ordinal)=1, (order,ordinal)=2,
// (repeat,repeat)=9000, all consistent with the unigram position layout, so every
// routing (bigram direct / pruned fallback / generic) must produce EQUAL results.
using namespace doris::snii;
using namespace doris::snii::snii_test;
using doris::snii::query::phrase_prefix_query;
using doris::snii::query::phrase_query;
namespace qinternal = doris::snii::query::internal;

namespace {

constexpr uint32_t kFixtureDocs = 9000;
constexpr uint32_t kPruneThreshold = 64;

struct Fixture {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
};

void build_fixture(Fixture* f, bool include_bigrams, uint32_t bigram_prune_min_df) {
    assert_ok(build_reader(&f->file, &f->segment_reader, &f->index_reader, include_bigrams,
                           bigram_prune_min_df));
}

std::vector<uint32_t> run_phrase(const reader::LogicalIndexReader& idx,
                                 const std::vector<std::string>& terms) {
    std::vector<uint32_t> docids;
    assert_ok(phrase_query(idx, terms, &docids));
    return docids;
}

std::vector<uint32_t> all_docids(uint32_t end_exclusive) {
    std::vector<uint32_t> docids(end_exclusive);
    std::iota(docids.begin(), docids.end(), 0U);
    return docids;
}

void reset_query_counters() {
    qinternal::query_test_counters() = qinternal::QueryTestCounters {};
}

} // namespace

// The pruned segment's meta declares the applied threshold; legacy layouts stay 0.
TEST(SniiPhraseBigramPrune, MetaDeclaresThresholdOnlyOnPrunedSegments) {
    Fixture control, pruned;
    build_fixture(&control, /*include_bigrams=*/true, /*threshold=*/0);
    build_fixture(&pruned, /*include_bigrams=*/true, kPruneThreshold);

    EXPECT_EQ(control.index_reader.bigram_prune_min_df(), 0U);
    EXPECT_EQ(pruned.index_reader.bigram_prune_min_df(), kPruneThreshold);
}

// Verification-gate case 1a: a HOT pair (df >= threshold) survives pruning and is
// answered DIRECTLY from the bigram posting (seam: one hit, zero fallbacks), with
// results equal to the unpruned control AND the pure-positions ground truth.
TEST(SniiPhraseBigramPrune, HotPairAnsweredViaBigramNoFallback) {
    Fixture control, pruned, nobigram;
    build_fixture(&control, true, 0);
    build_fixture(&pruned, true, kPruneThreshold);
    build_fixture(&nobigram, false, 0);

    const std::vector<std::string> hot {"repeat", "repeat"};
    const std::vector<uint32_t> truth = run_phrase(nobigram.index_reader, hot);
    EXPECT_EQ(truth, all_docids(kFixtureDocs)); // repeat@{0,1,2} everywhere

    EXPECT_EQ(run_phrase(control.index_reader, hot), truth);

    reset_query_counters();
    EXPECT_EQ(run_phrase(pruned.index_reader, hot), truth);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);
}

// Verification-gate case 1b: COLD pairs (df < threshold) are pruned from the dict;
// their 2-term phrases take the fallback (seam) and still EQUAL the unpruned
// control and the ground truth.
TEST(SniiPhraseBigramPrune, ColdPairFallsBackAndEqualsControl) {
    Fixture control, pruned, nobigram;
    build_fixture(&control, true, 0);
    build_fixture(&pruned, true, kPruneThreshold);
    build_fixture(&nobigram, false, 0);

    const std::vector<std::vector<std::string>> cold_phrases {
            {"failed", "order"},   // pruned df=3 -> {5000, 7000, 8000}
            {"failed", "ordinal"}, // pruned df=1 -> {6000}
            {"order", "ordinal"},  // pruned df=2 -> {5000, 7000}
    };
    const std::vector<std::vector<uint32_t>> expected {
            {5000, 7000, 8000},
            {6000},
            {5000, 7000},
    };

    for (size_t i = 0; i < cold_phrases.size(); ++i) {
        const std::vector<uint32_t> truth = run_phrase(nobigram.index_reader, cold_phrases[i]);
        EXPECT_EQ(truth, expected[i]);
        EXPECT_EQ(run_phrase(control.index_reader, cold_phrases[i]), truth);

        // The pruned bigram term must be absent from the dict...
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(pruned.index_reader.lookup(
                format::make_phrase_bigram_term(cold_phrases[i][0], cold_phrases[i][1]), &found,
                &entry, &frq_base, &prx_base));
        EXPECT_FALSE(found);

        // ... and the query must reroute through the fallback exactly once.
        reset_query_counters();
        EXPECT_EQ(run_phrase(pruned.index_reader, cold_phrases[i]), truth);
        EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);
        EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 0U);
    }
}

// Verification-gate case 2: the surviving bigram is written DOCS-ONLY on the
// pruned segment (prx_len == 0, no inline prx bytes) yet its 2-term phrase answer
// is identical -- the bigram hit path never reads bigram positions.
TEST(SniiPhraseBigramPrune, SurvivingBigramIsDocsOnlyAndAnswersEqually) {
    Fixture control, pruned;
    build_fixture(&control, true, 0);
    build_fixture(&pruned, true, kPruneThreshold);

    const std::string bigram = format::make_phrase_bigram_term("repeat", "repeat");
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;

    assert_ok(pruned.index_reader.lookup(bigram, &found, &entry, &frq_base, &prx_base));
    ASSERT_TRUE(found);
    EXPECT_EQ(entry.df, kFixtureDocs);
    EXPECT_EQ(entry.prx_len, 0U); // no .prx span streamed for the bigram
    EXPECT_TRUE(entry.prx_bytes.empty());

    // The legacy control keeps bigram positions (windowed df-9000 term -> a real
    // prx span), pinning that the diet actually changed the pruned layout.
    format::DictEntry control_entry;
    assert_ok(control.index_reader.lookup(bigram, &found, &control_entry, &frq_base, &prx_base));
    ASSERT_TRUE(found);
    EXPECT_GT(control_entry.prx_len, 0U);

    EXPECT_EQ(run_phrase(pruned.index_reader, {"repeat", "repeat"}),
              run_phrase(control.index_reader, {"repeat", "repeat"}));
}

// Verification-gate case 4: LEGACY segment (no meta threshold): a bigram dict miss
// keeps meaning EMPTY (the legacy writer materialized every adjacent pair, so a
// miss proves no adjacency) -- no fallback fires even though the unigram positions
// would match. The same query on a pruned-flag segment DOES fall back and returns
// the positions truth, which is exactly the semantic the meta flag gates.
TEST(SniiPhraseBigramPrune, LegacySegmentMissStaysEmptyPrunedSegmentFallsBack) {
    Fixture control, pruned, nobigram;
    build_fixture(&control, true, 0);
    build_fixture(&pruned, true, kPruneThreshold);
    build_fixture(&nobigram, false, 0);

    // "almost"@1 / "order"@2 are adjacent in most docs, but the fixture feeds NO
    // bigram(almost, order) term -- simulating a pair the pruned writer dropped.
    const std::vector<std::string> phrase {"almost", "order"};
    const std::vector<uint32_t> truth = run_phrase(nobigram.index_reader, phrase);
    ASSERT_FALSE(truth.empty()); // docs minus {4000 (no almost), 5000/7000/8000 (order not @2)}
    EXPECT_EQ(truth.size(), kFixtureDocs - 4);

    reset_query_counters();
    EXPECT_TRUE(run_phrase(control.index_reader, phrase).empty()); // legacy: miss == empty
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 0U);

    reset_query_counters();
    EXPECT_EQ(run_phrase(pruned.index_reader, phrase), truth); // pruned flag: fallback
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);
}

// Verification-gate case 5: phrase_prefix regression on the pruned segment. The
// multi-tail path filters bigram tails and verifies with UNIGRAM positions only
// (untouched by the diet), so pruned results equal the legacy control's.
TEST(SniiPhraseBigramPrune, PhrasePrefixUnaffectedByPruning) {
    Fixture control, pruned;
    build_fixture(&control, true, 0);
    build_fixture(&pruned, true, kPruneThreshold);

    const std::vector<uint32_t> expected {5000, 6000, 7000, 8000};

    std::vector<uint32_t> control_docs;
    assert_ok(phrase_prefix_query(control.index_reader, {"failed", "ord"}, &control_docs, 10));
    EXPECT_EQ(control_docs, expected);

    std::vector<uint32_t> pruned_docs;
    assert_ok(phrase_prefix_query(pruned.index_reader, {"failed", "ord"}, &pruned_docs, 10));
    EXPECT_EQ(pruned_docs, expected);

    // A single-tail expansion exercises the ExecuteResolvedPhraseTerms path too.
    std::vector<uint32_t> single_tail;
    assert_ok(phrase_prefix_query(pruned.index_reader, {"failed", "ordi"}, &single_tail, 10));
    EXPECT_EQ(single_tail, std::vector<uint32_t> {6000});
}

namespace {

// A pprefix-focused fixture (its own index; independent of the shared 9000-doc
// one): a HOT anchor whose tail-prefix family STRADDLES the prune threshold,
// plus one REAL term living inside the hidden-marker byte region.
//   vulcan             @0 in docs [0, 9000)    hot anchor (df 9000)
//   warpcore           @1 in docs [0, 8000)    bigram(vulcan,warpcore)  df=8000 SURVIVES
//   warpdrive          @1 in docs [8000, 8003) bigram(vulcan,warpdrive) df=3    PRUNED
//   warpgate           @1 in docs [8003, 8010) bigram(vulcan,warpgate)  df=7    PRUNED
//   "\x1F" "zz_raw"    @1 in docs {42, 43}     a raw (untokenized-value style)
//                                              term that sorts AFTER the marker
//                                              cluster; the "\x1F" tail prefix
//                                              range covers every hidden bigram
//                                              dict term on bigram-bearing
//                                              layouts.
// include_bigrams=false builds the pure-positions ground-truth layout.
Status build_warp_reader(Fixture* f, bool include_bigrams, uint32_t bigram_prune_min_df) {
    constexpr uint32_t kDocCount = 9000;
    writer::SniiIndexInput input;
    input.index_id = 11;
    input.index_suffix = "Warp";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = kDocCount;
    input.bigram_prune_min_df = bigram_prune_min_df;
    input.terms = {make_term("vulcan", docs_with_one_position(0, kDocCount, 0)),
                   make_term("warpcore", docs_with_one_position(0, 8000, 1)),
                   make_term("warpdrive", docs_with_one_position(8000, 8003, 1)),
                   make_term("warpgate", docs_with_one_position(8003, 8010, 1)),
                   make_term("\x1F"
                             "zz_raw",
                             {{.docid = 42, .positions = {1}}, {.docid = 43, .positions = {1}}})};
    if (include_bigrams) {
        input.terms.push_back(make_term(format::make_phrase_bigram_sentinel_term(),
                                        {{.docid = 0, .positions = {0}}}));
        input.terms.push_back(make_term(format::make_phrase_bigram_term("vulcan", "warpcore"),
                                        docs_with_one_position(0, 8000, 0)));
        input.terms.push_back(make_term(format::make_phrase_bigram_term("vulcan", "warpdrive"),
                                        docs_with_one_position(8000, 8003, 0)));
        input.terms.push_back(make_term(format::make_phrase_bigram_term("vulcan", "warpgate"),
                                        docs_with_one_position(8003, 8010, 0)));
    }
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    writer::SniiCompoundWriter writer(&f->file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    RETURN_IF_ERROR(writer.finish());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(&f->file, &f->segment_reader));
    return f->segment_reader.open_index(input.index_id, input.index_suffix, &f->index_reader);
}

void build_warp_fixture(Fixture* f, bool include_bigrams, uint32_t bigram_prune_min_df) {
    assert_ok(build_warp_reader(f, include_bigrams, bigram_prune_min_df));
}

std::vector<uint32_t> run_phrase_prefix(const reader::LogicalIndexReader& idx,
                                        const std::vector<std::string>& terms,
                                        int32_t max_expansions) {
    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(idx, terms, &docids, max_expansions));
    return docids;
}

} // namespace

// G01 pprefix regression, the shape production hit: the anchor is HOT and its
// tail-prefix family MIXES a surviving (anchor,tail) bigram with pruned ones.
// The pprefix answer must come from UNIGRAM positions for EVERY expanded tail:
// a path that consulted the hidden (anchor,tail) bigram postings as
// authoritative would silently drop the pruned tails' docs (their pair left no
// dict trace) and could not position-verify the surviving pair (its posting is
// docs-only). Assert exact equality between the pruned segment, the unpruned
// control, and the no-bigram ground truth -- after pinning the dict-level
// premise (SOME pairs survive, SOME are pruned) so the fixture cannot rot.
TEST(SniiPhraseBigramPrune, PhrasePrefixMixedHotAndPrunedTailPairsEqualControl) {
    Fixture control, pruned, nobigram;
    build_warp_fixture(&control, /*include_bigrams=*/true, /*threshold=*/0);
    build_warp_fixture(&pruned, /*include_bigrams=*/true, kPruneThreshold);
    build_warp_fixture(&nobigram, /*include_bigrams=*/false, /*threshold=*/0);

    // Premise: the hot pair kept a docs-only dict entry on the pruned segment,
    // both cold pairs left none; the control kept all three (with positions).
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(pruned.index_reader.lookup(format::make_phrase_bigram_term("vulcan", "warpcore"),
                                         &found, &entry, &frq_base, &prx_base));
    ASSERT_TRUE(found);
    EXPECT_EQ(entry.df, 8000U);
    EXPECT_EQ(entry.prx_len, 0U); // the survivor is docs-only (G01 part B)
    assert_ok(pruned.index_reader.lookup(format::make_phrase_bigram_term("vulcan", "warpdrive"),
                                         &found, &entry, &frq_base, &prx_base));
    EXPECT_FALSE(found);
    assert_ok(pruned.index_reader.lookup(format::make_phrase_bigram_term("vulcan", "warpgate"),
                                         &found, &entry, &frq_base, &prx_base));
    EXPECT_FALSE(found);
    assert_ok(control.index_reader.lookup(format::make_phrase_bigram_term("vulcan", "warpdrive"),
                                          &found, &entry, &frq_base, &prx_base));
    ASSERT_TRUE(found); // the legacy control materialized every pair

    // Multi-tail pprefix over the mixed family: every tail verified by unigram
    // positions, so all three layouts agree with the ground truth.
    const std::vector<std::string> query {"vulcan", "warp"};
    const std::vector<uint32_t> truth = run_phrase_prefix(nobigram.index_reader, query, 10);
    EXPECT_EQ(truth, all_docids(8010));
    EXPECT_EQ(run_phrase_prefix(control.index_reader, query, 10), truth);
    EXPECT_EQ(run_phrase_prefix(pruned.index_reader, query, 10), truth);

    // A capped expansion must budget REAL tails only, identically on every
    // layout: the first two dict-order tails are warpcore + warpdrive.
    const std::vector<uint32_t> capped = run_phrase_prefix(nobigram.index_reader, query, 2);
    EXPECT_EQ(capped, all_docids(8003));
    EXPECT_EQ(run_phrase_prefix(control.index_reader, query, 2), capped);
    EXPECT_EQ(run_phrase_prefix(pruned.index_reader, query, 2), capped);

    // 2-term MATCH_PHRASE cross-checks over the same pairs: the hot pair is
    // answered bigram-DIRECT from the docs-only posting, the pruned pair takes
    // the positions fallback -- both equal to the pprefix per-tail evidence.
    reset_query_counters();
    EXPECT_EQ(run_phrase(pruned.index_reader, {"vulcan", "warpcore"}), all_docids(8000));
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 0U);
    reset_query_counters();
    EXPECT_EQ(run_phrase(pruned.index_reader, {"vulcan", "warpdrive"}),
              (std::vector<uint32_t> {8000, 8001, 8002}));
    EXPECT_EQ(qinternal::query_test_counters().bigram_fallbacks, 1U);
    EXPECT_EQ(qinternal::query_test_counters().bigram_hits, 0U);
}

// G01 pprefix regression: hidden bigram dict terms that fall inside the tail
// prefix range must NOT consume max_expansions slots. Pre-fix,
// prefix_terms(max_expansions) filled the whole budget with hidden terms and
// only then erased them, silently dropping the REAL tail -- and because the
// pruned layout materializes FEWER hidden terms (sentinel + 1 survivor) than
// the legacy control (sentinel + 3 pairs), the same query returned DIFFERENT
// results per segment layout. The raw "\x1F"-lead term stands in for an
// untokenized value whose prefix range overlaps the hidden bigram cluster.
TEST(SniiPhraseBigramPrune, PhrasePrefixExpansionBudgetIgnoresHiddenBigramTerms) {
    Fixture control, pruned, nobigram;
    build_warp_fixture(&control, /*include_bigrams=*/true, /*threshold=*/0);
    build_warp_fixture(&pruned, /*include_bigrams=*/true, kPruneThreshold);
    build_warp_fixture(&nobigram, /*include_bigrams=*/false, /*threshold=*/0);

    const std::vector<std::string> query {"vulcan", "\x1F"};
    const std::vector<uint32_t> truth {42, 43}; // vulcan@0 followed by the raw term@1

    EXPECT_EQ(run_phrase_prefix(nobigram.index_reader, query, 3), truth);
    EXPECT_EQ(run_phrase_prefix(control.index_reader, query, 3), truth);
    EXPECT_EQ(run_phrase_prefix(pruned.index_reader, query, 3), truth);
}
