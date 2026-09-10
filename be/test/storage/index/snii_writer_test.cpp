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

// SNII writer and reader regression tests:
//   - heap_bytes() accessors on the resident format readers
//     (SampledTermIndexReader / DictBlockDirectoryReader / DictBlockReader) that
//     LogicalIndexReader::memory_usage() sums so the searcher-cache charge stops
//     under-counting. Exact hand-computed equality for SSO terms; the string-heap
//     accumulation is exercised with an over-15-byte term.
//   - geometric null-docid accumulation growth.

#include <gtest/gtest-spi.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/gram_family.h"
#include "storage/index/inverted/tokenizer/ngram/gram_tokenizer.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {
uint32_t snii_effective_stop_gram_df_threshold(const snii::writer::SniiIndexInput& input);
} // namespace doris::segment_v2

namespace {
// The write path holds a segment's first rows back to solve its density from them (see
// gram_density.h). These tests inspect the term buffer directly, without a flush, so they pin
// the density instead: what they are about is how a row is cut at a given rate.
class ScopedConfiguredDensity {
public:
    ScopedConfiguredDensity() : _saved(doris::config::enable_gram_index_adaptive_density) {
        doris::config::enable_gram_index_adaptive_density = false;
    }
    ~ScopedConfiguredDensity() { doris::config::enable_gram_index_adaptive_density = _saved; }

private:
    bool _saved;
};

using doris::snii::ByteSink;
using doris::snii::Slice;
using namespace doris::snii::format; // NOLINT(google-build-using-namespace)
namespace gram = doris::segment_v2::gram;

// Inject a gram-family (ngram tokenizer + mode=sparse) tokenizer/analyzer policy pair, swapping
// ExecEnv::_index_policy_mgr for a private manager and restoring it afterwards, for the tests
// of gram-family detection and the SNII writer forcing docs-only.
class ScopedGramPolicies {
public:
    ScopedGramPolicies() {
        auto* exec_env = doris::ExecEnv::GetInstance();
        previous_ = exec_env->index_policy_mgr();
        exec_env->_index_policy_mgr = &manager_;

        // IndexPolicyMgr's _name_to_id is a single namespace across policy types
        // (apply_policy_changes deduplicates by name and rejects a later policy outright when its
        // name clashes with an existing one, see index_policy_mgr.cpp:86-92). A tokenizer and an
        // analyzer therefore cannot share a name, so the tokenizer gets its own
        // "gram_sparse_tokenizer" while the analyzer keeps "gram_sparse" (the name the index's
        // analyzer property refers to).
        doris::TIndexPolicy tokenizer;
        tokenizer.id = 9001;
        tokenizer.name = "gram_sparse_tokenizer";
        tokenizer.type = doris::TIndexPolicyType::TOKENIZER;
        tokenizer.properties["type"] = "ngram";
        tokenizer.properties["mode"] = "sparse";

        doris::TIndexPolicy analyzer;
        analyzer.id = 9002;
        analyzer.name = "gram_sparse";
        analyzer.type = doris::TIndexPolicyType::ANALYZER;
        analyzer.properties["tokenizer"] = "gram_sparse_tokenizer";

        // The same gram-family tokenizer, but with a token filter attached: R22 requires such an
        // analyzer never to count as gram family (the filter rewrites tokens, so the stored term
        // is no longer what the extractor produced).
        doris::TIndexPolicy filtered_analyzer;
        filtered_analyzer.id = 9003;
        filtered_analyzer.name = filtered_analyzer_name();
        filtered_analyzer.type = doris::TIndexPolicyType::ANALYZER;
        filtered_analyzer.properties["tokenizer"] = "gram_sparse_tokenizer";
        filtered_analyzer.properties["token_filter"] = "lowercase";

        manager_.apply_policy_changes({tokenizer, analyzer, filtered_analyzer}, {});
    }

    ~ScopedGramPolicies() { doris::ExecEnv::GetInstance()->_index_policy_mgr = previous_; }

    static std::string filtered_analyzer_name() { return "gram_sparse_filtered"; }

    doris::IndexPolicyMgr& manager() { return manager_; }

private:
    doris::IndexPolicyMgr manager_;
    doris::IndexPolicyMgr* previous_ = nullptr;
};

// A fatal assertion inside a helper FUNCTION only aborts the helper; the calling
// test keeps running and may dereference state that failed to initialize (this
// bit us as a null-analyzer SEGV). A macro expands in the test body, so the
// fatal assertion aborts the test itself.
bool status_is_ok(const doris::Status& status) {
    return status.ok();
}

#define ASSERT_OK(status) ASSERT_PRED1(status_is_ok, status)

int assert_ok_evaluation_count = 0;

doris::Status counted_failure_status() {
    ++assert_ok_evaluation_count;
    return doris::Status::InternalError("counted failure");
}

TEST(AssertOkMacroTest, FailingExpressionIsEvaluatedOnce) {
    assert_ok_evaluation_count = 0;
    EXPECT_FATAL_FAILURE(ASSERT_OK(counted_failure_status()), "counted failure");
    EXPECT_EQ(assert_ok_evaluation_count, 1);
}

void init_failure_index_meta(doris::TabletIndex* index_meta, int64_t index_id) {
    doris::TabletIndexPB index_pb;
    index_pb.set_index_type(doris::IndexType::INVERTED);
    index_pb.set_index_id(index_id);
    index_pb.set_index_name("analyzer_failure_latch");
    index_pb.add_col_unique_id(0);
    index_pb.mutable_properties()->insert({"parser", "english"});
    index_pb.mutable_properties()->insert({"support_phrase", "true"});
    index_meta->init_from_pb(index_pb);
}

// An analyzer that throws INVERTED_INDEX_ANALYZER_ERROR to simulate a token filter's runtime
// failure. Previously, a token filter's UTF-8 validation provided this failure path.
class ThrowingTokenStream final : public lucene::analysis::TokenStream {
public:
    lucene::analysis::Token* next(lucene::analysis::Token*) override {
        throw doris::Exception(doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                               "analyzer failure injected by test");
    }
    void close() override {}
    void reset() override {}
};

class ThrowingAnalyzer final : public lucene::analysis::Analyzer {
public:
    bool isSDocOpt() override { return true; }

    lucene::analysis::TokenStream* tokenStream(const TCHAR*, lucene::util::Reader*) override {
        return new ThrowingTokenStream();
    }

    lucene::analysis::TokenStream* reusableTokenStream(const TCHAR*,
                                                       lucene::util::Reader*) override {
        _reusable = std::make_unique<ThrowingTokenStream>();
        return _reusable.get();
    }

private:
    std::unique_ptr<ThrowingTokenStream> _reusable;
};
// Generic index_meta construction: push an arbitrary property table into a fresh TabletIndex, so
// the gram-family detection tests can pass custom properties directly instead of hardcoding them
// in the function body like the two dedicated helpers above.
void init_gram_index_meta(doris::TabletIndex* index_meta, int64_t index_id,
                          const std::map<std::string, std::string>& properties) {
    doris::TabletIndexPB index_pb;
    index_pb.set_index_type(doris::IndexType::INVERTED);
    index_pb.set_index_id(index_id);
    index_pb.set_index_name("gram_family_writer");
    index_pb.add_col_unique_id(0);
    for (const auto& [key, value] : properties) {
        index_pb.mutable_properties()->insert({key, value});
    }
    index_meta->init_from_pb(index_pb);
}

std::shared_ptr<lucene::analysis::Analyzer> create_failure_analyzer() {
    return std::make_shared<ThrowingAnalyzer>();
}

doris::Slice malformed_value_after_valid_token() {
    static const std::string input =
            std::string("VALID B") + static_cast<char>(0xFF) + std::string("AD");
    return doris::Slice(input);
}

// The stop-gram resolver decides whether an index may drop posting lists at all. Each of
// its three conditions is a correctness guard rather than a tuning choice, so each gets
// pinned here: dropping a posting makes a term match every document, which only a
// re-checked (gram) predicate survives, only a docs-only index can afford (there are no
// positions to lose), and only a segment worth the saving should pay for. There is no
// switch: these three conditions are the whole policy.
TEST(SniiWriterTest, EffectiveStopGramThresholdResolver) {
    doris::snii::writer::SniiIndexInput input;
    input.config = IndexConfig::kDocsOnly;
    input.doc_count = 1000000;

    // No gram scheme: an ordinary full-text index has no re-check to make a match-all
    // term safe.
    EXPECT_EQ(doris::segment_v2::snii_effective_stop_gram_df_threshold(input), 0U);

    doris::segment_v2::gram::GramScheme scheme;
    scheme.mode = doris::segment_v2::gram::GramMode::SPARSE;
    scheme.min_len = 3;
    scheme.max_len = 16;
    scheme.density_permille = 250;
    input.gram_scheme = scheme;
    // Derived, not configured: doc_count / kHighDfDigestDivisor, times the query side's
    // budget multiple -- the same line the reader's gate draws.
    EXPECT_EQ(doris::segment_v2::snii_effective_stop_gram_df_threshold(input), 1500U)
            << "1,000,000 / 2000 * 3";

    // Positions would be dropped along with the posting, taking phrase capability with
    // them, so a positional index is refused even with a gram scheme.
    input.config = IndexConfig::kDocsPositions;
    EXPECT_EQ(doris::segment_v2::snii_effective_stop_gram_df_threshold(input), 0U);
    input.config = IndexConfig::kDocsOnly;

    // The threshold scales with the segment rather than with a constant: a tenth of the
    // rows gives a tenth of the line.
    input.doc_count = 100000;
    EXPECT_EQ(doris::segment_v2::snii_effective_stop_gram_df_threshold(input), 150U);
    input.doc_count = 1000000;

    // A segment with fewer rows than the digest's divisor has no meaningful notion of a
    // common term, and both sides stand down there: the reader's digest ceiling lands at 0
    // and its gate falls back rather than deriving a budget, so the writer must not drop
    // postings either.
    input.doc_count = 100;
    EXPECT_EQ(doris::segment_v2::snii_effective_stop_gram_df_threshold(input), 0U);
    input.doc_count = 1999;
    EXPECT_EQ(doris::segment_v2::snii_effective_stop_gram_df_threshold(input), 0U);
    input.doc_count = 2000;
    EXPECT_EQ(doris::segment_v2::snii_effective_stop_gram_df_threshold(input), 3U)
            << "the first segment size at which the line is derived rather than clamped";
}

// What the threshold costs, pinned so it cannot be forgotten.
//
// The line is drawn for size: it drops 88.6% of all posting entries on log text and takes
// index/data from 1.097 to 0.218, which is what the index's size target requires. It is far
// below the df of the terms a CJK query is made of -- those columns index one term per code
// point, and an ordinary character sits in a few percent of the rows. Measured on weibo with
// stop-gram enabled, a four-character literal proposes 500,000 candidates for its 18 matching
// rows and eliminates none, against 148 candidates and 499,852 rows eliminated without it.
// ASCII is not exempt either: `/history/images/` matches 0.73% of httplogs and loses its
// filtering the same way. What survives is the needle -- 13 rows in 3,000,000 keeps a 121x
// cold speedup.
//
// The assertion below is the arithmetic that produces that outcome. If someone raises this
// line to protect CJK, this test is where they will see what it was bought with.
TEST(SniiWriterTest, StopGramLineSitsFarBelowOrdinaryCjkCharacterFrequency) {
    doris::snii::writer::SniiIndexInput input;
    input.config = IndexConfig::kDocsOnly;
    input.doc_count = 500000;
    doris::segment_v2::gram::GramScheme scheme;
    scheme.mode = doris::segment_v2::gram::GramMode::SPARSE;
    scheme.min_len = 3;
    scheme.max_len = 16;
    scheme.density_permille = 250;
    input.gram_scheme = scheme;

    const uint32_t threshold = doris::segment_v2::snii_effective_stop_gram_df_threshold(input);
    EXPECT_EQ(threshold, 750U) << "500,000 / 2000 * 3, i.e. 0.15% of the segment";
    // A character in 1% of the posts is dropped, and it would still have removed 99% of the
    // rows. That is the trade, stated as a number rather than left to be rediscovered.
    EXPECT_LT(threshold, 5000U)
            << "terms that still filter are dropped; this is deliberate, for size";
}

// SampledTermIndexReader::heap_bytes(): all-SSO sample terms have no per-string
// heap, so the charge is exactly n_blocks * sizeof(std::string) (reserve-exact
// backing buffer).
TEST(SniiSegmentReaderTest, SampledTermIndexHeapBytesMatchesFormula) {
    const std::vector<std::string> terms = {"s000", "s001", "s002", "s003", "s004", "s005"};
    SampledTermIndexBuilder builder;
    for (const auto& term : terms) {
        builder.add_block_first_term(term); // strictly ascending, SSO
    }
    ByteSink sink;
    builder.finish(&sink);

    SampledTermIndexReader reader;
    ASSERT_OK(SampledTermIndexReader::open(sink.view(), &reader));
    ASSERT_EQ(reader.n_blocks(), terms.size());
    EXPECT_EQ(reader.heap_bytes(), terms.size() * sizeof(std::string));
}

// The std_string_heap_bytes accumulation: an over-15-byte sample term adds its
// heap buffer on top of the vector buffer.
TEST(SniiSegmentReaderTest, SampledTermIndexHeapBytesCountsLongTerms) {
    const std::string long_term = "b_this_is_a_long_sample_term_well_over_15_bytes";
    ASSERT_GT(long_term.size(), 15U);
    const std::vector<std::string> terms = {"a_short", long_term, "c_short"};
    SampledTermIndexBuilder builder;
    for (const auto& term : terms) {
        builder.add_block_first_term(term);
    }
    ByteSink sink;
    builder.finish(&sink);

    SampledTermIndexReader reader;
    ASSERT_OK(SampledTermIndexReader::open(sink.view(), &reader));
    const size_t vector_only = terms.size() * sizeof(std::string);
    EXPECT_GT(reader.heap_bytes(), vector_only);
    // capacity() >= size(), so the long term contributes >= size()+1 heap bytes.
    EXPECT_GE(reader.heap_bytes(), vector_only + long_term.size() + 1);
    // Cross-check the shared helper on an SSO vs non-SSO string.
    EXPECT_EQ(std_string_heap_bytes(std::string("short")), 0U);
    EXPECT_GT(std_string_heap_bytes(long_term), 0U);
}

// DictBlockDirectoryReader::heap_bytes(): BlockRef is trivially copyable, so the
// charge is exactly n_blocks * sizeof(BlockRef).
TEST(SniiSegmentReaderTest, DictBlockDirectoryHeapBytesMatchesFormula) {
    DictBlockDirectoryBuilder builder;
    constexpr uint32_t kBlocks = 5;
    for (uint32_t i = 0; i < kBlocks; ++i) {
        BlockRef ref;
        ref.offset = 100000ULL * (i + 1); // multi-byte varints -> each ref > 8 bytes
        ref.length = 640;
        ref.n_entries = 3;
        ref.flags = 0;
        ref.checksum = 0xDEAD0000U + i;
        builder.add(ref);
    }
    ByteSink sink;
    builder.finish(&sink);

    DictBlockDirectoryReader reader;
    ASSERT_OK(DictBlockDirectoryReader::open(sink.view(), &reader));
    ASSERT_EQ(reader.n_blocks(), kBlocks);
    EXPECT_EQ(reader.heap_bytes(), static_cast<size_t>(kBlocks) * sizeof(BlockRef));
}

// A minimal slim pod_ref entry that round-trips at tier T1 (extra tier>=T2 fields
// are ignored on encode). Terms are supplied by the caller in ascending order.
DictEntry make_pod_ref(std::string term) {
    DictEntry e;
    e.term = std::move(term);
    e.kind = DictEntryKind::kPodRef;
    e.enc = DictEntryEnc::kSlim;
    e.df = 3;
    e.frq_off_delta = 0;
    e.frq_len = 128;
    e.dd_meta.uncomp_len = 70;
    e.dd_meta.crc = 0xABCD1234U;
    e.prx_off_delta = 0;
    e.prx_len = 64;
    return e;
}

std::vector<uint8_t> build_dict_block(const std::vector<std::string>& terms,
                                      uint32_t anchor_interval) {
    DictBlockBuilder builder(IndexTier::kT1, /*has_positions=*/false, /*frq_base=*/0,
                             /*prx_base=*/0, anchor_interval);
    for (const auto& term : terms) {
        builder.add_entry(make_pod_ref(term));
    }
    ByteSink sink;
    builder.finish(&sink);
    return sink.buffer();
}

// DictBlockReader::heap_bytes(): with anchor_interval 16 and 20 SSO entries there
// are two anchors (indices 0 and 16), each with an SSO anchor term, so the charge
// is exactly n_anchors * (sizeof(uint32_t) + sizeof(std::string)).
TEST(SniiSegmentReaderTest, DictBlockAnchorHeapBytesMatchesFormula) {
    constexpr uint32_t kEntries = 20;
    constexpr uint32_t kAnchorInterval = 16;
    std::vector<std::string> terms;
    terms.reserve(kEntries);
    for (uint32_t i = 0; i < kEntries; ++i) {
        // "dt_00".."dt_19": strictly ascending, 5 bytes (SSO).
        terms.push_back("dt_" + std::string(1, static_cast<char>('0' + i / 10)) +
                        std::string(1, static_cast<char>('0' + i % 10)));
    }
    const std::vector<uint8_t> bytes = build_dict_block(terms, kAnchorInterval);

    DictBlockReader reader;
    ASSERT_OK(
            DictBlockReader::open(Slice(bytes), IndexTier::kT1, /*has_positions=*/false, &reader));
    ASSERT_EQ(reader.n_entries(), kEntries);

    const size_t n_anchors = (kEntries + kAnchorInterval - 1) / kAnchorInterval; // == 2
    EXPECT_EQ(reader.heap_bytes(), n_anchors * (sizeof(uint32_t) + sizeof(std::string)));
}

// A long (> 15 byte) anchor term adds its heap buffer beyond the anchor vectors.
TEST(SniiSegmentReaderTest, DictBlockAnchorHeapBytesCountsLongAnchor) {
    // One entry -> one anchor (entry 0), whose term is > 15 bytes.
    const std::string long_term = "a_dict_anchor_term_well_over_15_bytes";
    ASSERT_GT(long_term.size(), 15U);
    const std::vector<uint8_t> bytes = build_dict_block({long_term}, /*anchor_interval=*/16);

    DictBlockReader reader;
    ASSERT_OK(
            DictBlockReader::open(Slice(bytes), IndexTier::kT1, /*has_positions=*/false, &reader));
    ASSERT_EQ(reader.n_entries(), 1U);
    const size_t vector_only = sizeof(uint32_t) + sizeof(std::string); // one anchor
    EXPECT_GT(reader.heap_bytes(), vector_only);
    EXPECT_GE(reader.heap_bytes(), vector_only + long_term.size() + 1);
}

// ==================== null-docids growth-policy regression pins ====================
//
// append_nullable feeds add_nulls once per NULL RUN -- millions of calls on a
// large interleaved-null compaction segment. An exact reserve(size()+count)
// inside add_nulls capped capacity at "just enough", so EVERY subsequent call
// reallocated + memcpy'd the whole array: O(runs x N) total memcpy (the
// agentlogs full-compaction pathology: ~TBs of memcpy per tablet, 8x+ slower
// than V3). These pins count capacity changes across many small appends: with
// geometric growth that is O(log n); with the exact-reserve bug it was one per
// call. add_nulls touches only _null_docids/_rid, so a scaffold-free writer
// (null collaborators, no init()) exercises the real production code path.

TEST(SniiWriterNullDocids, AddNullsGrowsGeometricallyNotQuadratically) {
    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, nullptr,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
    constexpr uint32_t kRuns = 4096;
    size_t capacity_changes = 0;
    size_t last_cap = writer.null_docids_for_test().capacity();
    for (uint32_t i = 0; i < kRuns; ++i) {
        ASSERT_OK(writer.add_nulls(1));
        const size_t cap = writer.null_docids_for_test().capacity();
        if (cap != last_cap) {
            ++capacity_changes;
            last_cap = cap;
        }
    }
    // Geometric growth reallocates O(log n) times (libstdc++ doubling: ~13 for
    // 4096); the exact-reserve bug reallocated on every call (4096). The bound
    // leaves generous headroom for any sane growth policy while still failing
    // a per-call realloc by two orders of magnitude.
    EXPECT_LE(capacity_changes, 64U) << "add_nulls reallocates per call again";
    // Content unchanged by the policy fix: docids 0..kRuns-1 in order.
    const auto& nulls = writer.null_docids_for_test();
    ASSERT_EQ(nulls.size(), kRuns);
    EXPECT_EQ(nulls.front(), 0U);
    EXPECT_EQ(nulls.back(), kRuns - 1);
    EXPECT_TRUE(std::ranges::is_sorted(nulls));
}

TEST(SniiWriterFailureLatch, AnalyzerFailureDiscardsStateAndBlocksFinish) {
    doris::TabletIndex index_meta;
    init_failure_index_meta(&index_meta, 91);

    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_OK(writer.init());

    writer.set_analysis_for_test(
            doris::segment_v2::inverted_index::InvertedIndexAnalyzer::create_reader({}),
            create_failure_analyzer());

    const doris::Slice value = malformed_value_after_valid_token();
    auto add_status = writer.add_values("", &value, 1);
    ASSERT_EQ(add_status.code(), doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    EXPECT_EQ(writer.term_buffer_for_test(), nullptr);
    EXPECT_EQ(writer.memory_reporter_for_test(), nullptr);

    EXPECT_EQ(writer.add_nulls(1).code(), doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    EXPECT_EQ(writer.finish().code(), doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    EXPECT_EQ(writer.term_buffer_for_test(), nullptr);
    EXPECT_EQ(writer.memory_reporter_for_test(), nullptr);
}

TEST(SniiWriterTest, GramTokenizerForcesDocsOnlyAndIsRecognised) {
    ScopedConfiguredDensity fixed_density;
    ScopedGramPolicies policies;

    const std::map<std::string, std::string> props {{"analyzer", "gram_sparse"},
                                                    {"support_phrase", "true"}}; // ask for pos
    doris::TabletIndex index_meta;
    init_gram_index_meta(&index_meta, 9010, props);

    auto scheme = gram::resolve_gram_scheme(index_meta.properties(), &policies.manager());
    ASSERT_TRUE(scheme.has_value());
    EXPECT_EQ(scheme->mode, gram::GramMode::SPARSE);

    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_OK(writer.init());
    // The gram family forces docs-only and ignores support_phrase.
    EXPECT_EQ(writer.config_for_test(), IndexConfig::kDocsOnly);
    ASSERT_TRUE(writer.gram_scheme_for_test().has_value());
    EXPECT_TRUE(writer.gram_scheme_for_test().value() == *scheme);

    const std::vector<doris::Slice> values {doris::Slice("rpc error: code = Unavailable"),
                                            doris::Slice("手机微博")};
    ASSERT_OK(writer.add_values("c", values.data(), values.size()));
    auto postings = writer.term_buffer_for_test()->finalize_sorted();
    std::vector<std::string> terms;
    terms.reserve(postings.size());
    for (auto& posting : postings) {
        terms.push_back(posting.term);
    }
    std::ranges::sort(terms);
    // Non-ASCII text contributes no terms: only the ASCII segment of the row is indexed.
    std::vector<std::string> expected {" Unavai", "ailable", "cod", "ode = U", "or: co"};
    std::ranges::sort(expected);
    EXPECT_EQ(terms, expected);
}

// Regression guard for the reusable-token-stream lane. The gram family pulls its stream from
// Analyzer::reusableTokenStream, which differs from tokenStream in two ways that both bite here:
// it hands back the SAME cached stream every row, and it does NOT reset it. The writer has to
// reset it itself -- DorisTokenizer::reset() is what promotes the new reader from _in_pending to
// _in, and GramTokenizer::reset() is what re-extracts and rewinds the cursor. Skip that and every
// row after the first is indexed as a repeat of the first, silently. Checking the union of terms
// would not catch it; checking which docid each term landed under does. The three rows below have
// pairwise disjoint gram sets and deliberately differ in length.
TEST(SniiWriterTest, GramRowsStayIndependentAcrossTheReusedTokenStream) {
    ScopedConfiguredDensity fixed_density;
    ScopedGramPolicies policies;

    const std::map<std::string, std::string> props {{"analyzer", "gram_sparse"}};
    doris::TabletIndex index_meta;
    init_gram_index_meta(&index_meta, 9014, props);

    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_OK(writer.init());
    ASSERT_TRUE(writer.gram_scheme_for_test().has_value());

    const std::vector<doris::Slice> values {
            doris::Slice("rpc error: code = Unavailable desc = transport is closing"),
            doris::Slice("手机微博"), doris::Slice("SELECT COUNT(*) FROM tbl WHERE k = 42")};
    ASSERT_OK(writer.add_values("c", values.data(), values.size()));

    // Ground truth: the same extractor the writer used, driven one row at a time.
    gram::GramExtractor extractor(*writer.gram_scheme_for_test());
    std::map<std::string, std::vector<uint32_t>> expected;
    for (uint32_t docid = 0; docid < values.size(); docid++) {
        std::vector<std::string> grams;
        extractor.grams_of_literal(std::string_view(values[docid].data, values[docid].size),
                                   &grams);
        for (auto& gram : grams) {
            expected[gram].push_back(docid);
        }
    }
    ASSERT_GE(expected.size(), 3U);

    std::map<std::string, std::vector<uint32_t>> actual;
    for (auto& posting : writer.term_buffer_for_test()->finalize_sorted()) {
        actual[posting.term] = posting.docids;
    }
    EXPECT_EQ(actual, expected);
}

// The tokenizer's scratch is ordinary heap, so the process-wide MemTrackerLimiter always saw it
// through the allocation hook -- but this writer's own MemoryReporter did not, so the number
// driving its flush/spill decision understated real RSS. A long, highly repetitive row makes the
// wiring checkable: SPARSE collapses it to a handful of distinct grams, so the term buffer (the
// other contributor to the reporter) stays small while the tokenizer's scratch is sized by the
// row. Without _report_gram_buffers_capacity the total sits far below the scratch alone.
TEST(SniiWriterTest, GramTokenizerBuffersReachTheMemoryReporter) {
    ScopedConfiguredDensity fixed_density;
    ScopedGramPolicies policies;

    const std::map<std::string, std::string> props {{"analyzer", "gram_sparse"}};
    doris::TabletIndex index_meta;
    init_gram_index_meta(&index_meta, 9015, props);

    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_OK(writer.init());
    ASSERT_TRUE(writer.gram_scheme_for_test().has_value());

    std::string row;
    while (row.size() < 256 * 1024) {
        row += "2026-09-06 INFO rpc request completed service=Frontend latency_ms=17 ";
    }
    const doris::Slice value(row.data(), row.size());
    ASSERT_OK(writer.add_values("c", &value, 1));

    // Ground truth: the same tokenizer on the same row, driven standalone. reset() is what fills
    // the buffers, so their capacity is settled by the time it returns.
    doris::segment_v2::inverted_index::GramTokenizer tokenizer(*writer.gram_scheme_for_test());
    auto reader = std::make_shared<lucene::util::SStringReader<char>>();
    reader->init(row.data(), static_cast<int32_t>(row.size()), false);
    tokenizer.set_reader(reader);
    tokenizer.reset();
    const size_t scratch_bytes = tokenizer.reserved_bytes();
    ASSERT_GT(scratch_bytes, 512U * 1024U) << "the row is meant to dwarf the distinct-term set";

    EXPECT_GE(writer.memory_reporter_for_test()->current_bytes(),
              static_cast<int64_t>(scratch_bytes));
}

// A non-gram-family index (the built-in parser="english", with no analyzer/normalizer property)
// must resolve to no scheme, whether or not the process's current IndexPolicyMgr is empty
// (resolve_gram_scheme short-circuits to nullopt for both mgr==nullptr and "empty analyzer name",
// without depending on whether the policy manager has been initialized).
TEST(SniiWriterTest, NonGramAnalyzerHasNoScheme) {
    const std::map<std::string, std::string> props {{"parser", "english"}};
    doris::TabletIndex index_meta;
    init_gram_index_meta(&index_meta, 9011, props);
    EXPECT_FALSE(gram::resolve_gram_scheme(index_meta.properties(),
                                           doris::ExecEnv::GetInstance()->index_policy_mgr())
                         .has_value());
}

// R21 regression guard: built-in analyzer names (standard/english/...) are never registered as
// index policies, so asking the policy manager for one always throws "Policy not found". If
// gram-family detection went through the policy manager, every existing index with
// PROPERTIES("analyzer"="standard") would fail to build -- so this deliberately installs a
// manager holding gram policies only, making sure a built-in name neither enters the manager nor
// changes the previous config conclusion.
TEST(SniiWriterTest, BuiltinAnalyzerNameStillInitialises) {
    ScopedGramPolicies policies; // only gram policies in the manager, no "standard"/"english"
    const std::vector<std::map<std::string, std::string>> cases {
            {{"analyzer", "standard"}, {"support_phrase", "true"}},
            {{"parser", "english"}}}; // the legacy parser spelling
    int64_t index_id = 9012;
    for (const auto& props : cases) {
        doris::TabletIndex index_meta;
        init_gram_index_meta(&index_meta, index_id++, props);
        doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                        doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
        ASSERT_OK(writer.init());
        EXPECT_FALSE(writer.gram_scheme_for_test().has_value());
        // config is decided purely by support_phrase, independently of gram-family detection.
        EXPECT_EQ(writer.config_for_test(), props.contains("support_phrase")
                                                    ? IndexConfig::kDocsPositions
                                                    : IndexConfig::kDocsOnly);
    }
}

// The negative case: an analyzer name that is neither built-in nor present in the policy manager
// -- a genuine configuration error, which must keep its existing behaviour (an
// INVERTED_INDEX_ANALYZER_ERROR while creating the analyzer) and must not be swallowed by R21's
// short circuit.
TEST(SniiWriterTest, MissingAnalyzerPolicyFailsInit) {
    ScopedGramPolicies policies;
    const std::map<std::string, std::string> props {{"analyzer", "no_such_policy"}};
    doris::TabletIndex index_meta;
    init_gram_index_meta(&index_meta, 9014, props);
    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
    const auto status = writer.init();
    EXPECT_EQ(status.code(), doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR) << status;
}

// R22: once a token filter is attached to a gram-family tokenizer, the stored term is no longer
// GramExtractor.extract(raw column value), so it must be treated as "not gram family" -- neither
// reporting a scheme nor quietly dropping the phrase positions the user asked for.
TEST(SniiWriterTest, TokenFilteredGramAnalyzerIsNotGramFamily) {
    ScopedGramPolicies policies;
    const std::string analyzer_name = ScopedGramPolicies::filtered_analyzer_name();
    auto provider = policies.manager().get_analyzer_provider_by_name(analyzer_name);
    ASSERT_NE(provider, nullptr);
    EXPECT_FALSE(provider->gram_scheme().has_value());

    const std::map<std::string, std::string> props {{"analyzer", analyzer_name},
                                                    {"support_phrase", "true"}};
    doris::TabletIndex index_meta;
    init_gram_index_meta(&index_meta, 9015, props);
    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_OK(writer.init());
    EXPECT_FALSE(writer.gram_scheme_for_test().has_value());
    EXPECT_EQ(writer.config_for_test(), IndexConfig::kDocsPositions); // docs-only not forced
}

// The other half of R22: an index-level char_filter is wrapped around the reader by the writer
// itself and is invisible to the policy provider, so the provider still reports a scheme -- the
// writer has to suppress it.
TEST(SniiWriterTest, IndexLevelCharFilterIsNotGramFamily) {
    ScopedGramPolicies policies;
    EXPECT_TRUE(policies.manager()
                        .get_analyzer_provider_by_name("gram_sparse")
                        ->gram_scheme()
                        .has_value());

    const std::map<std::string, std::string> props {{"analyzer", "gram_sparse"},
                                                    {"char_filter_type", "char_replace"},
                                                    {"char_filter_pattern", "._"}};
    doris::TabletIndex index_meta;
    init_gram_index_meta(&index_meta, 9016, props);
    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_OK(writer.init());
    EXPECT_FALSE(writer.gram_scheme_for_test().has_value());
}

TEST(SniiDocIdSinkGrowth, AppendRangeGrowsGeometrically) {
    std::vector<uint32_t> docids;
    doris::snii::query::VectorDocIdSink sink(docids);
    constexpr uint32_t kRuns = 4096;
    size_t capacity_changes = 0;
    size_t last_cap = docids.capacity();
    for (uint32_t i = 0; i < kRuns; ++i) {
        ASSERT_OK(sink.append_range(i, static_cast<uint64_t>(i) + 1)); // one docid per run
        const size_t cap = docids.capacity();
        if (cap != last_cap) {
            ++capacity_changes;
            last_cap = cap;
        }
    }
    EXPECT_LE(capacity_changes, 64U) << "append_range reallocates per call again";
    ASSERT_EQ(docids.size(), kRuns);
    EXPECT_EQ(docids.front(), 0U);
    EXPECT_EQ(docids.back(), kRuns - 1);
    EXPECT_TRUE(std::ranges::is_sorted(docids));
}

} // namespace
