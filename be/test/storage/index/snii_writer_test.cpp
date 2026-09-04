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
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/inverted/common_grams/common_word_set.h"
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
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {
bool snii_effective_write_freq(snii::format::IndexConfig index_config);
}

namespace {

using doris::snii::ByteSink;
using doris::snii::Slice;
using namespace doris::snii::format; // NOLINT(google-build-using-namespace)

class ScopedCommonGramsPolicies {
public:
    ScopedCommonGramsPolicies() {
        auto* exec_env = doris::ExecEnv::GetInstance();
        previous_ = exec_env->index_policy_mgr();
        exec_env->_index_policy_mgr = &manager_;

        doris::TIndexPolicy tokenizer;
        tokenizer.id = 920010;
        tokenizer.name = "snii_writer_cg_tokenizer";
        tokenizer.type = doris::TIndexPolicyType::TOKENIZER;
        tokenizer.properties["type"] = "char_group";
        tokenizer.properties["tokenize_on_chars"] = "[\\u0020]";
        tokenizer.properties["max_token_length"] = "16383";

        doris::TIndexPolicy common_grams;
        common_grams.id = 920011;
        common_grams.name = "snii_writer_cg_filter";
        common_grams.type = doris::TIndexPolicyType::TOKEN_FILTER;
        common_grams.properties["type"] = "common_grams";

        doris::TIndexPolicy analyzer;
        analyzer.id = 920012;
        analyzer.name = analyzer_name();
        analyzer.type = doris::TIndexPolicyType::ANALYZER;
        analyzer.properties["tokenizer"] = tokenizer.name;
        analyzer.properties["token_filter"] = "lowercase," + common_grams.name;
        manager_.apply_policy_changes({tokenizer, common_grams, analyzer}, {});
    }

    ~ScopedCommonGramsPolicies() { doris::ExecEnv::GetInstance()->_index_policy_mgr = previous_; }

    static std::string analyzer_name() { return "snii_writer_cg_analyzer"; }

    // Derive a metadata seed from the analyzer's real identity, the same
    // way production derives it for a fresh segment; hand-rolled fingerprints can
    // never match the provider identity and fail init's consistency check.
    doris::segment_v2::inverted_index::CommonGramsSegmentMetadata metadata_seed() {
        auto provider = manager_.get_analyzer_provider_by_name(analyzer_name());
        DORIS_CHECK(provider != nullptr);
        const auto* identity = provider->common_grams_identity();
        DORIS_CHECK(identity != nullptr);
        return doris::segment_v2::inverted_index::make_common_grams_segment_metadata(*identity);
    }

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
    index_pb.set_index_name("common_grams_failure_latch");
    index_pb.add_col_unique_id(0);
    index_pb.mutable_properties()->insert({"parser", "english"});
    index_pb.mutable_properties()->insert({"support_phrase", "true"});
    index_meta->init_from_pb(index_pb);
}

void init_common_grams_index_meta(doris::TabletIndex* index_meta, int64_t index_id) {
    doris::TabletIndexPB index_pb;
    index_pb.set_index_type(doris::IndexType::INVERTED);
    index_pb.set_index_id(index_id);
    index_pb.set_index_name("common_grams_typed_writer");
    index_pb.add_col_unique_id(0);
    index_pb.mutable_properties()->insert({"analyzer", ScopedCommonGramsPolicies::analyzer_name()});
    index_pb.mutable_properties()->insert({"support_phrase", "true"});
    index_meta->init_from_pb(index_pb);
}

std::string encoded_gram(std::string_view left, std::string_view right) {
    auto encoded = doris::segment_v2::inverted_index::encode_common_gram(left, right);
    EXPECT_TRUE(encoded.has_value()) << encoded.error();
    return encoded.value();
}

std::shared_ptr<lucene::analysis::Analyzer> create_failure_analyzer() {
    using namespace doris::segment_v2::inverted_index; // NOLINT
    Settings tokenizer_settings;
    tokenizer_settings.set("tokenize_on_chars", "[whitespace]");
    CustomAnalyzerConfig::Builder builder;
    builder.with_tokenizer_config("char_group", tokenizer_settings);
    builder.add_token_filter_config("lowercase", {});
    builder.add_token_filter_config("common_grams", {});
    return CustomAnalyzer::build_custom_analyzer(builder.build(), AnalysisPurpose::kIndex);
}

doris::Slice malformed_value_after_valid_token() {
    static const std::string input =
            std::string("VALID B") + static_cast<char>(0xFF) + std::string("AD");
    return doris::Slice(input);
}

TEST(SniiWriterTest, EffectiveWriteFreqResolver) {
    const bool saved = doris::config::snii_positions_index_write_freq;
    doris::config::snii_positions_index_write_freq = false;
    EXPECT_FALSE(doris::segment_v2::snii_effective_write_freq(IndexConfig::kDocsPositions));
    EXPECT_TRUE(doris::segment_v2::snii_effective_write_freq(IndexConfig::kDocsPositionsScoring));
    doris::config::snii_positions_index_write_freq = true;
    EXPECT_TRUE(doris::segment_v2::snii_effective_write_freq(IndexConfig::kDocsPositions));
    doris::config::snii_positions_index_write_freq = saved;
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
    e.ttf_delta = 6;
    e.max_freq = 9;
    e.frq_off_delta = 0;
    e.frq_len = 128;
    e.frq_docs_len = 64; // dd region on-disk length (<= frq_len)
    e.dd_meta.uncomp_len = 70;
    e.dd_meta.crc = 0xABCD1234U;
    e.freq_meta.uncomp_len = 40;
    e.freq_meta.crc = 0x55AA00FFU;
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

TEST(SniiCommonGramsWriter, EncodesTypedTermsAndBuildsSemanticScoringInput) {
    using doris::segment_v2::inverted_index::PLAIN_ESCAPE_PREFIX;
    ScopedCommonGramsPolicies policies;
    doris::TabletIndex index_meta;
    init_common_grams_index_meta(&index_meta, 93);

    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR,
                                                    policies.metadata_seed());
    ASSERT_OK(writer.init());

    const std::string marker_plain = std::string("\x1f") + "raw";
    const std::string first_text = marker_plain + " of the";
    const std::string second_text = "plain terms";
    const std::vector<doris::Slice> values = {doris::Slice(first_text), doris::Slice(second_text)};
    ASSERT_OK(writer.add_values("", values.data(), values.size()));
    ASSERT_OK(writer.add_nulls(1));

    auto postings = writer.term_buffer_for_test()->finalize_sorted();
    std::map<std::string, doris::snii::writer::TermPostings> by_term;
    for (auto& posting : postings) {
        EXPECT_FALSE(posting.term.starts_with(doris::snii::format::kPhraseBigramTermMarker));
        const std::string term = posting.term;
        by_term.emplace(term, std::move(posting));
    }

    const std::string escaped_plain = std::string(1, PLAIN_ESCAPE_PREFIX) + "Graw";
    ASSERT_TRUE(by_term.contains(escaped_plain));
    EXPECT_EQ(by_term.at(escaped_plain).docids, (std::vector<uint32_t> {0}));
    EXPECT_EQ(by_term.at(escaped_plain).positions_flat, (std::vector<uint32_t> {1}));
    ASSERT_TRUE(by_term.contains(encoded_gram(marker_plain, "of")));
    EXPECT_EQ(by_term.at(encoded_gram(marker_plain, "of")).docids, (std::vector<uint32_t> {0}));
    // A gram keeps positions only when BOTH sides are common words
    // (add_common_gram_and_plain's retain flag); a mixed pair such as
    // escaped-raw + common keeps co-occurrence docids but never participates
    // in phrase-position semantics.
    EXPECT_TRUE(by_term.at(encoded_gram(marker_plain, "of")).positions_flat.empty());
    ASSERT_TRUE(by_term.contains(encoded_gram("of", "the")));
    EXPECT_EQ(by_term.at(encoded_gram("of", "the")).positions_flat, (std::vector<uint32_t> {2}));
    EXPECT_EQ(by_term.at("of").positions_flat, (std::vector<uint32_t> {2}));
    EXPECT_EQ(by_term.at("the").positions_flat, (std::vector<uint32_t> {3}));
    EXPECT_EQ(by_term.at("plain").positions_flat, (std::vector<uint32_t> {1}));
    EXPECT_EQ(by_term.at("terms").positions_flat, (std::vector<uint32_t> {2}));

    EXPECT_EQ(writer.encoded_norms_for_test(),
              (std::vector<uint8_t> {doris::snii::query::encode_norm(3),
                                     doris::snii::query::encode_norm(2),
                                     doris::snii::query::encode_norm(0)}));
    EXPECT_EQ(writer.scoring_token_count_for_test(), 5U);
    const auto metadata = writer.common_grams_metadata_for_test();
    EXPECT_EQ(metadata.scoring_doc_count, 3U);
    EXPECT_EQ(metadata.scoring_token_count, 5U);
}

TEST(SniiCommonGramsWriter, EscapedPlainOverflowPoisonsAllAccumulatedState) {
    ScopedCommonGramsPolicies policies;
    doris::TabletIndex index_meta;
    init_common_grams_index_meta(&index_meta, 94);

    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR,
                                                    policies.metadata_seed());
    ASSERT_OK(writer.init());

    const std::string valid_text = "man of the year";
    const doris::Slice valid_value(valid_text);
    ASSERT_OK(writer.add_values("", &valid_value, 1));
    ASSERT_OK(writer.add_nulls(1));
    ASSERT_NE(writer.term_buffer_for_test(), nullptr);
    EXPECT_FALSE(writer.encoded_norms_for_test().empty());
    EXPECT_FALSE(writer.null_docids_for_test().empty());
    EXPECT_GT(writer.scoring_token_count_for_test(), 0U);

    std::string oversized_after_escape(
            doris::segment_v2::inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES, 'x');
    oversized_after_escape.front() = '\x1f';
    const doris::Slice value(oversized_after_escape);
    auto status = writer.add_values("", &value, 1);
    ASSERT_EQ(status.code(), doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    ASSERT_EQ(writer.term_buffer_for_test(), nullptr);
    ASSERT_EQ(writer.memory_reporter_for_test(), nullptr);
    EXPECT_TRUE(writer.encoded_norms_for_test().empty());
    EXPECT_TRUE(writer.null_docids_for_test().empty());
    EXPECT_EQ(writer.scoring_token_count_for_test(), 0U);
    EXPECT_EQ(writer.finish().code(), doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
}

TEST(SniiCommonGramsWriter, PlainControlKeepsRawTermsAndLegacyConfig) {
    doris::TabletIndex index_meta;
    init_failure_index_meta(&index_meta, 95);
    doris::segment_v2::SniiIndexColumnWriter writer(nullptr, &index_meta,
                                                    doris::FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_OK(writer.init());

    const std::string raw = std::string("\x1f") + "literal";
    const doris::Slice value(raw);
    ASSERT_OK(writer.add_values("", &value, 1));
    auto postings = writer.term_buffer_for_test()->finalize_sorted();
    ASSERT_FALSE(postings.empty());
    EXPECT_TRUE(std::ranges::none_of(postings, [](const auto& posting) {
        return posting.term.starts_with(doris::segment_v2::inverted_index::PLAIN_ESCAPE_PREFIX);
    }));
    EXPECT_EQ(writer.config_for_test(), IndexConfig::kDocsPositions);
    EXPECT_TRUE(writer.encoded_norms_for_test().empty());
    EXPECT_FALSE(writer.has_common_grams_metadata_seed_for_test());
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
