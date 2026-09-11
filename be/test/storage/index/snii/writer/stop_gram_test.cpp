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

// stop-gram on the write side: terms above the df threshold keep their dictionary entry
// and lose their posting list.
//
// The measured motivation, on 200k rows of real log text at the default gram scheme: the
// 855 terms above the query-side cost gate's threshold are 0.7% of the vocabulary and hold
// 88.6% of the posting entries, and the gate refuses to read every one of them. This suite
// pins the three things that has to mean in the file:
//
//   * the entry survives, carrying its term key and its real df, so a reader can tell
//     "matches everything" from "absent", which mean opposite things;
//   * the posting bytes are actually gone -- the saving is the entire point, and an
//     implementation that wrote them and merely flagged the entry would pass every
//     behavioural assertion while saving nothing;
//   * the df is exact, not the count of whatever prefix the writer happened to look at
//     before deciding, because the reader's cost gate reads it.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/snii/format/bsbf.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::writer {
namespace {

using snii_test::assert_ok;
using snii_test::make_term;
using snii_test::MemoryFile;
using snii_test::PostingDoc;

constexpr uint32_t kDocCount = 4000;

// A term appearing in every `stride`-th document, so its df is exactly known and can be
// placed either side of a threshold on purpose.
std::vector<PostingDoc> EveryNth(uint32_t stride) {
    std::vector<PostingDoc> docs;
    for (uint32_t docid = 0; docid < kDocCount; docid += stride) {
        docs.push_back(PostingDoc {.docid = docid, .positions = {}});
    }
    return docs;
}

struct Segment {
    MemoryFile file;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
};

// Builds a docs-only GRAM-FAMILY segment holding the given terms, with stop-gram at
// `threshold`. The gram scheme is not decoration: both stop-gram and the high-df digest are
// gated on it, because a match-all term is only sound where the candidates get re-checked
// and the digest is only ever read by the gram query's cost gate.
void Build(Segment* out, const std::vector<std::pair<std::string, uint32_t>>& term_strides,
           uint32_t threshold, bool with_gram_scheme = true) {
    SniiIndexInput input;
    input.index_id = 11;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = kDocCount;
    input.stop_gram_df_threshold = threshold;
    if (with_gram_scheme) {
        segment_v2::gram::GramScheme scheme;
        scheme.mode = segment_v2::gram::GramMode::SPARSE;
        scheme.min_len = 3;
        scheme.max_len = 4;
        scheme.density_permille = 250;
        input.gram_scheme = scheme;
    }
    for (const auto& [term, stride] : term_strides) {
        input.terms.push_back(make_term(term, EveryNth(stride)));
    }
    SniiCompoundWriter writer(&out->file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());
    assert_ok(reader::SniiSegmentReader::open(&out->file, &out->segment));
    assert_ok(out->segment.open_index(11, "body", &out->index));
}

struct Looked {
    bool found = false;
    format::DictEntry entry;
};

Looked Lookup(reader::LogicalIndexReader* index, const std::string& term) {
    Looked l;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index->lookup(term, &l.found, &l.entry, &frq_base, &prx_base));
    return l;
}

} // namespace

// The threshold splits the vocabulary: below it a term keeps its posting, above it the
// entry stays and the posting goes.
TEST(SniiStopGram, DropsPostingsOnlyAboveTheThreshold) {
    // Strides 2 and 4 give df 2000 and 1000; strides 100 and 400 give df 40 and 10.
    Segment seg;
    Build(&seg, {{"common_a", 2}, {"common_b", 4}, {"rare_a", 100}, {"rare_b", 400}},
          /*threshold=*/500);

    const Looked common_a = Lookup(&seg.index, "common_a");
    ASSERT_TRUE(common_a.found) << "a dropped posting must leave the term in the dictionary";
    EXPECT_TRUE(common_a.entry.posting_dropped);
    EXPECT_EQ(common_a.entry.df, 2000U) << "the df must be the real one, not a scan prefix";

    const Looked common_b = Lookup(&seg.index, "common_b");
    ASSERT_TRUE(common_b.found);
    EXPECT_TRUE(common_b.entry.posting_dropped);
    EXPECT_EQ(common_b.entry.df, 1000U);

    const Looked rare_a = Lookup(&seg.index, "rare_a");
    ASSERT_TRUE(rare_a.found);
    EXPECT_FALSE(rare_a.entry.posting_dropped) << "a term inside the budget keeps its posting";
    EXPECT_EQ(rare_a.entry.df, 40U);

    const Looked rare_b = Lookup(&seg.index, "rare_b");
    ASSERT_TRUE(rare_b.found);
    EXPECT_FALSE(rare_b.entry.posting_dropped);
    EXPECT_EQ(rare_b.entry.df, 10U);

    // A term that was never indexed is still absent -- the opposite of dropped.
    const Looked missing = Lookup(&seg.index, "never_indexed");
    EXPECT_FALSE(missing.found);
}

// The saving has to be real. Same corpus, same everything, threshold off versus on.
TEST(SniiStopGram, ActuallyRemovesThePostingBytes) {
    const std::vector<std::pair<std::string, uint32_t>> corpus = {
            {"common_a", 2}, {"common_b", 4}, {"rare_a", 100}, {"rare_b", 400}};

    Segment off;
    Build(&off, corpus, /*threshold=*/0);
    Segment on;
    Build(&on, corpus, /*threshold=*/500);

    EXPECT_LT(on.file.data().size(), off.file.data().size())
            << "dropping the two dense postings must shrink the segment";
    // The two dense terms hold the great majority of the postings here (3000 of 3050
    // entries), so the drop has to be large rather than incidental.
    EXPECT_LT(on.file.data().size(), off.file.data().size() * 2 / 3);
}

// 0 keeps every posting: an index built with the feature off must be byte-identical to one
// built by a version that never had it.
TEST(SniiStopGram, ZeroThresholdDropsNothing) {
    Segment seg;
    Build(&seg, {{"common_a", 2}, {"rare_a", 100}}, /*threshold=*/0);
    EXPECT_FALSE(Lookup(&seg.index, "common_a").entry.posting_dropped);
    EXPECT_FALSE(Lookup(&seg.index, "rare_a").entry.posting_dropped);
}

// A term exactly at the threshold is kept: the rule is "above", so the boundary term is
// still readable and the reader's gate -- which uses the same comparison -- agrees.
TEST(SniiStopGram, TheTermExactlyAtTheThresholdIsKept) {
    Segment seg;
    // stride 4 over 4000 docs is df 1000 exactly.
    Build(&seg, {{"boundary", 4}}, /*threshold=*/1000);
    const Looked boundary = Lookup(&seg.index, "boundary");
    ASSERT_TRUE(boundary.found);
    EXPECT_FALSE(boundary.entry.posting_dropped) << "df == threshold is not above it";
    EXPECT_EQ(boundary.entry.df, 1000U);
}

// ---- the high-df digest, which the query side uses to skip reading df at all ----

TEST(SniiStopGram, HighDfDigestRecordsTheCommonTermsAndBoundsTheRest) {
    // 4000 docs puts the digest floor at 4000 / kHighDfDigestDivisor = 2, so terms at
    // stride 2 and 4 (df 2000 and 1000) are well above it and the two rare ones are not.
    Segment seg;
    Build(&seg, {{"common_a", 2}, {"common_b", 4}, {"rare_a", 100}, {"rare_b", 400}},
          /*threshold=*/0);

    const auto& digest = seg.index.high_df_terms();
    ASSERT_FALSE(digest.empty()) << "terms this common must reach the digest";

    bool exact = false;
    EXPECT_EQ(digest.df_upper_bound(format::bsbf_hash("common_a"), &exact), 2000U);
    EXPECT_TRUE(exact) << "a term in the digest yields its real df, not a bound";
    EXPECT_EQ(digest.df_upper_bound(format::bsbf_hash("common_b"), &exact), 1000U);
    EXPECT_TRUE(exact);

    // A term the digest omits still gets a usable answer -- an upper bound rather than an
    // exact df. That is what lets the query side decide a node is worth reading without
    // touching the dictionary.
    const uint32_t bound = digest.df_upper_bound(format::bsbf_hash("never_indexed"), &exact);
    EXPECT_FALSE(exact);
    EXPECT_EQ(bound, digest.df_ceiling);
    EXPECT_LT(bound, 1000U) << "the ceiling must sit below the terms the digest kept";
}

// The digest has to stay proportional to the index it describes. On a segment this small
// the df floor is 2 documents, so most of the vocabulary clears it and only the cap keeps
// the digest from dwarfing the index -- the failure this pins is a 45 KB digest attached to
// a 96 KB index, which is what an absolute cap alone produced.
TEST(SniiStopGram, HighDfDigestStaysProportionalToTheVocabulary) {
    std::vector<std::pair<std::string, uint32_t>> corpus;
    // 300 distinct terms, every one of them appearing in at least two documents and so
    // above this segment's floor.
    for (uint32_t i = 0; i < 300; i++) {
        corpus.emplace_back("term_" + std::to_string(i), 2 + (i % 50));
    }
    Segment seg;
    Build(&seg, corpus, /*threshold=*/0);

    const auto& digest = seg.index.high_df_terms();
    const uint64_t terms = seg.index.stats().term_count;
    ASSERT_GT(terms, 0U);
    const size_t allowed = std::min(
            format::kMaxHighDfDigestTerms,
            std::max<size_t>(format::kMinHighDfDigestTerms,
                             static_cast<size_t>(terms) / format::kHighDfDigestVocabularyShare));
    EXPECT_LE(digest.term_hash.size(), allowed)
            << "digest of " << digest.term_hash.size() << " entries over " << terms << " terms";
    // Truncating must not cost the reader its bound: everything dropped is still covered.
    EXPECT_GT(digest.df_ceiling, 0U) << "a truncated digest must still bound what it omits";
}

// The digest is decoded with the core metadata and stays resident for as long as the reader
// does, so the searcher cache has to be charged for it. Left out, the cache believes it holds
// less than it does -- by up to 48 KiB per cached segment index at the writer's cap, across
// however many segments the cache holds.
//
// Two readers over the same corpus isolate the charge: the only resident metadata the gram
// scheme adds is the digest, so the difference between them is what the digest costs.
TEST(SniiStopGram, TheSearcherCacheChargeIncludesTheResidentDigest) {
    std::vector<std::pair<std::string, uint32_t>> corpus;
    for (uint32_t i = 0; i < 300; i++) {
        corpus.emplace_back("term_" + std::to_string(i), 2 + (i % 50));
    }
    Segment with_digest;
    Build(&with_digest, corpus, /*threshold=*/0);
    Segment without_digest;
    Build(&without_digest, corpus, /*threshold=*/0, /*with_gram_scheme=*/false);

    const auto& digest = with_digest.index.high_df_terms();
    ASSERT_FALSE(digest.empty())
            << "this corpus must reach the digest for the test to mean anything";
    ASSERT_TRUE(without_digest.index.high_df_terms().empty());

    const size_t digest_bytes = digest.term_hash.capacity() * sizeof(uint64_t) +
                                digest.df.capacity() * sizeof(uint32_t);
    ASSERT_GT(digest_bytes, 0U);
    const size_t charged = with_digest.index.memory_usage();
    const size_t baseline = without_digest.index.memory_usage();
    ASSERT_GE(charged, baseline);
    EXPECT_GE(charged - baseline, digest_bytes)
            << "charge " << charged << " over baseline " << baseline << " omits " << digest_bytes
            << " bytes of resident digest";
}

// An index no gram query can reach must not carry the digest at all. It would be pure loss:
// bytes in the metadata and cycles on every term, for a structure only the gram cost gate
// consults. Segments of such indexes must stay byte-identical to what an earlier build
// produced.
TEST(SniiStopGram, NonGramIndexesCarryNoDigest) {
    SniiIndexInput input;
    input.index_id = 11;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = kDocCount;
    // No gram_scheme: an ordinary full-text index.
    input.terms.push_back(make_term("common_a", EveryNth(2)));
    input.terms.push_back(make_term("rare_a", EveryNth(100)));

    MemoryFile file;
    SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(11, "body", &index));

    const auto& digest = index.high_df_terms();
    EXPECT_TRUE(digest.empty()) << "a non-gram index must build no digest";
    EXPECT_EQ(digest.df_ceiling, 0U)
            << "and no ceiling either, so the reader knows there is no bound rather than "
               "mistaking zero for one";
}

// Same standing-down at the small end, and for the same reason the reader and stop-gram
// stand down there: a floor clamped to 1 makes "common" mean "occurs twice".
TEST(SniiStopGram, SegmentsBelowTheDivisorCarryNoDigest) {
    SniiIndexInput input;
    input.index_id = 11;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = format::kHighDfDigestDivisor - 1;
    segment_v2::gram::GramScheme scheme;
    scheme.mode = segment_v2::gram::GramMode::SPARSE;
    scheme.min_len = 3;
    scheme.max_len = 4;
    scheme.density_permille = 250;
    input.gram_scheme = scheme;
    std::vector<PostingDoc> docs;
    for (uint32_t d = 0; d < input.doc_count; d += 2) {
        docs.push_back(PostingDoc {.docid = d, .positions = {}});
    }
    input.terms.push_back(make_term("everywhere", docs));

    MemoryFile file;
    SniiCompoundWriter writer(&file);
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&file, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(11, "body", &index));

    EXPECT_TRUE(index.high_df_terms().empty());
    EXPECT_EQ(index.high_df_terms().df_ceiling, 0U);
}

// The digest is a binary search over ascending hashes; unsorted content would return wrong
// bounds silently rather than fail, so the writer has to emit it sorted.
TEST(SniiStopGram, HighDfDigestIsAscendingByHash) {
    Segment seg;
    Build(&seg, {{"a_common", 2}, {"b_common", 3}, {"c_common", 4}, {"d_common", 5}},
          /*threshold=*/0);
    const auto& digest = seg.index.high_df_terms();
    ASSERT_GE(digest.term_hash.size(), 2U);
    EXPECT_TRUE(std::ranges::is_sorted(digest.term_hash));
    EXPECT_EQ(digest.term_hash.size(), digest.df.size());
}

// A df far above the first fill still comes out exact: the writer drains the source rather
// than reporting the prefix it examined before deciding.
TEST(SniiStopGram, ReportsAnExactDfForAVeryCommonTerm) {
    Segment seg;
    Build(&seg, {{"everywhere", 1}}, /*threshold=*/10);
    const Looked everywhere = Lookup(&seg.index, "everywhere");
    ASSERT_TRUE(everywhere.found);
    EXPECT_TRUE(everywhere.entry.posting_dropped);
    EXPECT_EQ(everywhere.entry.df, kDocCount);
}

} // namespace doris::snii::writer
