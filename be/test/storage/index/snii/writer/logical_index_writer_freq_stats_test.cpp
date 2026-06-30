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
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

// T12 -- writer fused freqs statistics (single pass total + max), reused for the
// has_prx position-count check, stats_.sum_total_term_freq, and the DictEntry
// ttf_delta/max_freq. This suite guards:
//   * the deterministic op-count: exactly ONE term-level freqs scan per term
//     (was 3N docs-only / 4N with positions) via the term_freq_scans() seam;
//   * value bit-identity: ttf_delta / max_freq / sum_total_term_freq read back
//     equal to an independent reference, across windowed + slim(pod_ref/inline);
//   * the fused pure helper on boundary inputs (empty/single/zeros/equal/u32max/
//     large random) vs a naive reference;
//   * the validate_term error paths preserved after dropping its internal
//     freqs-sum loop (length / position-count / strict-ascending), and that the
//     has_prx position-count check now consumes the FUSED total.
namespace doris::snii::writer {
namespace {

using doris::Status; // RETURN_IF_ERROR / Status::OK() expand to a bare Status.
using snii_test::assert_ok;
using snii_test::make_term;
using snii_test::MemoryFile;
using snii_test::PostingDoc;

// One term whose postings are known up front, so the reference total/max can be
// recomputed in the test independently of the writer.
struct KnownTerm {
    std::string term;
    std::vector<PostingDoc> docs;

    uint64_t ref_sum() const {
        uint64_t sum = 0;
        for (const PostingDoc& doc : docs) {
            sum += doc.positions.size();
        }
        return sum;
    }
    uint32_t ref_max() const {
        uint32_t max = 0;
        for (const PostingDoc& doc : docs) {
            max = std::max<uint32_t>(max, static_cast<uint32_t>(doc.positions.size()));
        }
        return max;
    }
    uint32_t df() const { return static_cast<uint32_t>(docs.size()); }
};

// 480 docids with irregular gaps (deterministic LCG): the slim docs region PFOR
// exceeds the 256B inline threshold so the term becomes a pod_ref, while df < 512
// keeps it slim (not windowed). Mirrors the proven generator in snii_query_test.
std::vector<uint32_t> slim_pod_ref_docids() {
    std::vector<uint32_t> ids;
    ids.reserve(480);
    uint32_t cur = 0;
    uint32_t state = 0x9e3779b9U;
    for (int i = 0; i < 480; ++i) {
        ids.push_back(cur);
        state = state * 1664525U + 1013904223U;
        cur += 1U + (state >> 23) % 250U; // gap in [1, 250]
    }
    return ids;
}

// A corpus covering every term-level encoding branch with non-trivial freqs:
//   "wide"    df=600 >= kSlimDfThreshold(512) -> windowed (3 base-unit windows),
//             freqs cycle 1,2,3 (max=3) so the per-window MaxOf is also exercised.
//   "slimref" df=480 < 512, irregular gaps -> slim pod_ref, freq 1 (max=1).
//   "tiny"    df=4 -> slim inline, one doc freq 2 (max=2).
//   "mid"     df=300 consecutive -> slim, one doc freq 2 (max=2).
std::vector<KnownTerm> make_known_corpus() {
    std::vector<KnownTerm> corpus;

    KnownTerm wide;
    wide.term = "wide";
    for (uint32_t i = 0; i < 600; ++i) {
        const uint32_t freq = (i % 3) + 1;
        std::vector<uint32_t> positions;
        positions.reserve(freq);
        for (uint32_t p = 0; p < freq; ++p) {
            positions.push_back(p);
        }
        wide.docs.push_back({i, std::move(positions)});
    }
    corpus.push_back(std::move(wide));

    KnownTerm slimref;
    slimref.term = "slimref";
    for (uint32_t docid : slim_pod_ref_docids()) {
        slimref.docs.push_back({docid, {0}});
    }
    corpus.push_back(std::move(slimref));

    KnownTerm tiny;
    tiny.term = "tiny";
    tiny.docs = {{.docid = 10, .positions = {0, 1}},
                 {.docid = 20, .positions = {0}},
                 {.docid = 30, .positions = {0}},
                 {.docid = 40, .positions = {0}}};
    corpus.push_back(std::move(tiny));

    KnownTerm mid;
    mid.term = "mid";
    for (uint32_t i = 0; i < 300; ++i) {
        mid.docs.push_back({i, i == 0 ? std::vector<uint32_t> {0, 1} : std::vector<uint32_t> {0}});
    }
    corpus.push_back(std::move(mid));

    return corpus;
}

const KnownTerm& find_term(const std::vector<KnownTerm>& corpus, std::string_view term) {
    for (const KnownTerm& kt : corpus) {
        if (std::string_view(kt.term) == term) {
            return kt;
        }
    }
    ADD_FAILURE() << "term not in corpus: " << term;
    return corpus.front();
}

// Builds the corpus into `file` and opens a reader over it. The corpus is left
// untouched so the caller can recompute references.
Status build_corpus_reader(MemoryFile* file, reader::SniiSegmentReader* segment_reader,
                           reader::LogicalIndexReader* index_reader,
                           const std::vector<KnownTerm>& corpus, format::IndexConfig config) {
    SniiIndexInput input;
    input.index_id = 7;
    input.index_suffix = "Body";
    input.config = config;
    uint32_t max_docid = 0;
    input.terms.reserve(corpus.size());
    for (const KnownTerm& kt : corpus) {
        for (const PostingDoc& doc : kt.docs) {
            max_docid = std::max(max_docid, doc.docid);
        }
        input.terms.push_back(make_term(kt.term, kt.docs));
    }
    input.doc_count = max_docid + 1;
    std::ranges::sort(input.terms, [](const TermPostings& lhs, const TermPostings& rhs) {
        return lhs.term < rhs.term;
    });

    SniiCompoundWriter writer(file);
    RETURN_IF_ERROR(writer.add_logical_index(input));
    RETURN_IF_ERROR(writer.finish());
    EXPECT_TRUE(file->finalized());

    RETURN_IF_ERROR(reader::SniiSegmentReader::open(file, segment_reader));
    return segment_reader->open_index(input.index_id, input.index_suffix, index_reader);
}

format::DictEntry lookup_entry(const reader::LogicalIndexReader& index_reader,
                               std::string_view term) {
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index_reader.lookup(term, &found, &entry, &frq_base, &prx_base));
    EXPECT_TRUE(found) << term;
    return entry;
}

// -------------------------------------------------------------------------
// Deterministic perf: exactly one term-level freqs scan per term.
// -------------------------------------------------------------------------

// The op-count seam (testing::term_freq_scans) increments once per fuse_freq_stats
// call, and process_term calls it exactly once per term -> N for an N-term build.
// Before the fusion the instrumented build scanned freqs 4x per term with
// positions (validate-sum + stats-sum + ttf-sum + max), i.e. 4 * N, so this
// assertion fails on the un-fused code and passes after the single-pass fuse.
TEST(SniiWriterTest, ProcessTermScansFreqsOncePerTerm) {
    const std::vector<KnownTerm> corpus = make_known_corpus();
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;

    testing::reset_term_freq_scans();
    assert_ok(build_corpus_reader(&file, &segment_reader, &index_reader, corpus,
                                  format::IndexConfig::kDocsPositions));

    EXPECT_EQ(testing::term_freq_scans(), corpus.size());
}

// -------------------------------------------------------------------------
// Value bit-identity: ttf_delta / max_freq / sum_total_term_freq.
// -------------------------------------------------------------------------

// FW-EQ-1 / FW-EQ-2: every term's read-back ttf_delta == sum(freqs) and
// max_freq == max(freqs), and the index-level sum_total_term_freq == the sum of
// every term's sum(freqs). The reference is computed independently from the known
// corpus. Holds before AND after the fusion (guards the CSE did not change values).
TEST(SniiWriterTest, FusedFreqStatsPreserveTtfMaxAndSum) {
    const std::vector<KnownTerm> corpus = make_known_corpus();
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_corpus_reader(&file, &segment_reader, &index_reader, corpus,
                                  format::IndexConfig::kDocsPositions));

    uint64_t expected_total = 0;
    for (const KnownTerm& kt : corpus) {
        const format::DictEntry entry = lookup_entry(index_reader, kt.term);
        EXPECT_EQ(entry.df, kt.df()) << kt.term;
        EXPECT_EQ(entry.ttf_delta, kt.ref_sum()) << kt.term;
        EXPECT_EQ(entry.max_freq, kt.ref_max()) << kt.term;
        expected_total += kt.ref_sum();
    }
    EXPECT_EQ(index_reader.stats().sum_total_term_freq, expected_total);
}

// FW-DF-windowed / FW-DF-slim-inline (+ slim pod_ref): the fused ttf/max are
// correct on each encoding branch. Also guards that the windowed per-window MaxOf
// / sum (NOT collapsed by this task) still produce a term whose term-level fused
// values match the reference.
TEST(SniiWriterTest, FusedFreqStatsCorrectAcrossWindowedAndSlimEncodings) {
    const std::vector<KnownTerm> corpus = make_known_corpus();
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_corpus_reader(&file, &segment_reader, &index_reader, corpus,
                                  format::IndexConfig::kDocsPositions));

    const format::DictEntry wide = lookup_entry(index_reader, "wide");
    EXPECT_EQ(wide.enc, format::DictEntryEnc::kWindowed);
    EXPECT_EQ(wide.ttf_delta, find_term(corpus, "wide").ref_sum());
    EXPECT_EQ(wide.max_freq, find_term(corpus, "wide").ref_max());
    EXPECT_EQ(wide.max_freq, 3U); // sanity: cycling 1,2,3

    const format::DictEntry slimref = lookup_entry(index_reader, "slimref");
    EXPECT_EQ(slimref.enc, format::DictEntryEnc::kSlim);
    EXPECT_EQ(slimref.kind, format::DictEntryKind::kPodRef);
    EXPECT_EQ(slimref.ttf_delta, find_term(corpus, "slimref").ref_sum());
    EXPECT_EQ(slimref.max_freq, find_term(corpus, "slimref").ref_max());

    const format::DictEntry tiny = lookup_entry(index_reader, "tiny");
    EXPECT_EQ(tiny.enc, format::DictEntryEnc::kSlim);
    EXPECT_EQ(tiny.kind, format::DictEntryKind::kInline);
    EXPECT_EQ(tiny.ttf_delta, find_term(corpus, "tiny").ref_sum());
    EXPECT_EQ(tiny.max_freq, find_term(corpus, "tiny").ref_max());
}

// -------------------------------------------------------------------------
// Pure helper boundaries (real production helper via the testing seam).
// -------------------------------------------------------------------------

// FW-PURE-*: fuse_freq_stats on degenerate/boundary inputs equals the hand
// computed reference. max stays 0 for an all-zero input (a freq of 0 never lowers
// the running max), and the u32 sum promotes into the u64 total.
TEST(SniiWriterTest, FuseFreqStatsMatchesReferenceOnEdgeInputs) {
    {
        const FreqStats fs = testing::fuse_freq_stats_for_test({});
        EXPECT_EQ(fs.total_freq, 0U);
        EXPECT_EQ(fs.max_freq, 0U);
    }
    {
        const FreqStats fs = testing::fuse_freq_stats_for_test({7});
        EXPECT_EQ(fs.total_freq, 7U);
        EXPECT_EQ(fs.max_freq, 7U);
    }
    {
        const FreqStats fs = testing::fuse_freq_stats_for_test({0, 0, 0});
        EXPECT_EQ(fs.total_freq, 0U);
        EXPECT_EQ(fs.max_freq, 0U);
    }
    {
        const FreqStats fs = testing::fuse_freq_stats_for_test({5, 5, 5, 5});
        EXPECT_EQ(fs.total_freq, 20U);
        EXPECT_EQ(fs.max_freq, 5U);
    }
    {
        const FreqStats fs = testing::fuse_freq_stats_for_test({1, UINT32_MAX, 2});
        EXPECT_EQ(fs.total_freq, static_cast<uint64_t>(UINT32_MAX) + 3U);
        EXPECT_EQ(fs.max_freq, UINT32_MAX);
    }
}

// FW-PURE-rand: a large fixed-seed random input agrees with an independent
// std::accumulate (u64) / std::max_element reference.
TEST(SniiWriterTest, FuseFreqStatsMatchesNaiveOnLargeRandomInput) {
    std::mt19937 rng(0xC0FFEEU);
    std::vector<uint32_t> freqs(4096);
    for (uint32_t& f : freqs) {
        f = static_cast<uint32_t>(rng());
    }

    const FreqStats fs = testing::fuse_freq_stats_for_test(freqs);
    EXPECT_EQ(fs.total_freq, std::accumulate(freqs.begin(), freqs.end(), uint64_t {0}));
    EXPECT_EQ(fs.max_freq, *std::ranges::max_element(freqs));
}

// -------------------------------------------------------------------------
// Error paths preserved after dropping validate_term's internal sum loop.
// -------------------------------------------------------------------------

// FW-VAL-len-mismatch: freqs.size() != docids.size() is still rejected (the
// length check is independent of the freqs sum and must remain).
TEST(SniiWriterTest, ValidateTermRejectsFreqDocidLengthMismatch) {
    TermPostings tp;
    tp.term = "x";
    tp.docids = {0, 1};
    tp.freqs = {1}; // length 1 != 2

    SniiIndexInput input;
    input.index_id = 1;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = 2;
    input.terms = {std::move(tp)};

    MemoryFile file;
    SniiCompoundWriter writer(&file);
    const Status status = writer.add_logical_index(input);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
}

// FW-VAL-nonasc: non-strictly-ascending docids still rejected.
TEST(SniiWriterTest, ValidateTermRejectsNonAscendingDocids) {
    TermPostings tp;
    tp.term = "x";
    tp.docids = {5, 5}; // equal -> not strictly ascending
    tp.freqs = {1, 1};

    SniiIndexInput input;
    input.index_id = 1;
    input.index_suffix = "Body";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = 6;
    input.terms = {std::move(tp)};

    MemoryFile file;
    SniiCompoundWriter writer(&file);
    const Status status = writer.add_logical_index(input);
    EXPECT_TRUE(status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
}

// FW-VAL-prx: the has_prx position-count check now consumes the FUSED total.
// A mismatch (positions != sum(freqs)) is rejected; a match is accepted AND the
// ttf/max read back from the fused stats. The accepted sub-case is the guard that
// validate_term receives the correct total (a mis-wired/zero total would wrongly
// reject this valid term).
TEST(SniiWriterTest, ValidateTermUsesFusedTotalForPositionCount) {
    SniiIndexInput base;
    base.index_id = 1;
    base.index_suffix = "Body";
    base.config = format::IndexConfig::kDocsPositions;
    base.doc_count = 1;

    {
        TermPostings tp;
        tp.term = "x";
        tp.docids = {0};
        tp.freqs = {2};          // sum(freqs) = 2
        tp.positions_flat = {7}; // but only 1 position
        SniiIndexInput input = base;
        input.terms = {std::move(tp)};

        MemoryFile file;
        SniiCompoundWriter writer(&file);
        const Status status = writer.add_logical_index(input);
        EXPECT_TRUE(status.is<doris::ErrorCode::INVALID_ARGUMENT>()) << status.to_string();
    }
    {
        TermPostings tp;
        tp.term = "x";
        tp.docids = {0};
        tp.freqs = {2};             // sum(freqs) = 2
        tp.positions_flat = {7, 8}; // matches
        SniiIndexInput input = base;
        input.terms = {std::move(tp)};

        MemoryFile file;
        reader::SniiSegmentReader segment_reader;
        reader::LogicalIndexReader index_reader;
        SniiCompoundWriter writer(&file);
        assert_ok(writer.add_logical_index(input));
        assert_ok(writer.finish());
        assert_ok(reader::SniiSegmentReader::open(&file, &segment_reader));
        assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &index_reader));

        const format::DictEntry entry = lookup_entry(index_reader, "x");
        EXPECT_EQ(entry.ttf_delta, 2U);
        EXPECT_EQ(entry.max_freq, 2U);
    }
}

} // namespace
} // namespace doris::snii::writer
