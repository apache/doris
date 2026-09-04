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
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::query {
namespace {

using segment_v2::inverted_index::CommonGramsCoverage;
using segment_v2::inverted_index::CommonGramsPlanCostModel;
using segment_v2::inverted_index::CommonGramsQueryIdentity;
using segment_v2::inverted_index::CommonGramsSegmentMetadata;
using segment_v2::inverted_index::PlainTermKeyVersion;
using segment_v2::inverted_index::ScoringCoverage;
using segment_v2::inverted_index::COMMON_GRAMS_KEY_VERSION_V1;
using segment_v2::inverted_index::COMMON_GRAMS_NORM_SEMANTICS_VERSION_V1;
using segment_v2::inverted_index::COMMON_GRAMS_SCORING_STATS_VERSION_V1;
using segment_v2::inverted_index::COMMON_GRAMS_SEMANTICS_VERSION_V1;
using segment_v2::inverted_index::encode_common_gram;
using segment_v2::inverted_index::encode_plain_term;
using snii_test::MemoryFile;
using snii_test::ScopedEnv;
using snii_test::assert_ok;
using snii_test::make_term;

constexpr uint64_t kIndexId = 51;
constexpr std::string_view kIndexSuffix = "body";

class RecordingReader final : public io::FileReader {
public:
    explicit RecordingReader(io::FileReader* inner) : inner_(inner) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        ++default_read_calls_;
        return inner_->read_at(offset, len, out);
    }

    Status read_batch(const std::vector<io::Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override {
        ++default_read_calls_;
        return inner_->read_batch(ranges, outs);
    }

    uint64_t size() const override { return inner_->size(); }

    void reset_counts() { default_read_calls_ = 0; }

    size_t default_read_calls() const { return default_read_calls_; }

private:
    io::FileReader* inner_;
    size_t default_read_calls_ = 0;
};

struct Fixture {
    Fixture() : recording_reader(&file) {}

    MemoryFile file;
    RecordingReader recording_reader;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
};

CommonGramsSegmentMetadata complete_metadata() {
    CommonGramsSegmentMetadata metadata;
    metadata.plain_term_key_version = PlainTermKeyVersion::kEscapedV1;
    metadata.common_grams_coverage = CommonGramsCoverage::kComplete;
    metadata.common_grams_semantics_version = COMMON_GRAMS_SEMANTICS_VERSION_V1;
    metadata.common_grams_key_version = COMMON_GRAMS_KEY_VERSION_V1;
    metadata.common_grams_dictionary_identity = "builtin:lucene_english_stop:v1";
    metadata.base_analyzer_fingerprint = "common-grams-query-base-v1";
    metadata.common_grams_fingerprint = "common-grams-query-v1";
    return metadata;
}

CommonGramsQueryIdentity complete_query_identity() {
    const CommonGramsSegmentMetadata metadata = complete_metadata();
    return {.common_grams_dictionary_identity = metadata.common_grams_dictionary_identity,
            .base_analyzer_fingerprint = metadata.base_analyzer_fingerprint,
            .common_grams_fingerprint = metadata.common_grams_fingerprint};
}

CommonGramsSegmentMetadata complete_scoring_metadata(uint32_t doc_count,
                                                     uint64_t scoring_token_count) {
    CommonGramsSegmentMetadata metadata = complete_metadata();
    metadata.scoring_coverage = ScoringCoverage::kComplete;
    metadata.scoring_stats_version = COMMON_GRAMS_SCORING_STATS_VERSION_V1;
    metadata.norm_semantics_version = COMMON_GRAMS_NORM_SEMANTICS_VERSION_V1;
    metadata.scoring_doc_count = doc_count;
    metadata.scoring_token_count = scoring_token_count;
    return metadata;
}

CommonGramsPlanCostModel no_hysteresis_cost_model() {
    return {.position_verify_factor = 0, .common_grams_cost_ratio_percent = 100};
}

CommonGramsPlanCostModel verify_cost_model() {
    return {.position_verify_factor = 7, .common_grams_cost_ratio_percent = 100};
}

std::string plain_key(std::string_view term) {
    auto encoded = encode_plain_term(term, PlainTermKeyVersion::kEscapedV1);
    EXPECT_TRUE(encoded.has_value());
    return encoded.has_value() ? std::move(encoded.value()) : std::string();
}

std::string gram_key(std::string_view left, std::string_view right) {
    auto encoded = encode_common_gram(left, right);
    EXPECT_TRUE(encoded.has_value());
    return encoded.has_value() ? std::move(encoded.value()) : std::string();
}

std::vector<writer::TermPostings> plan_terms() {
    return {
            make_term(plain_key("the"), {{.docid = 0, .positions = {0}},
                                         {.docid = 1, .positions = {0}},
                                         {.docid = 2, .positions = {0}},
                                         {.docid = 3, .positions = {0}},
                                         {.docid = 4, .positions = {0}},
                                         {.docid = 5, .positions = {0}},
                                         {.docid = 6, .positions = {0}},
                                         {.docid = 7, .positions = {0}}}),
            make_term(plain_key("gap"), {{.docid = 2, .positions = {1}},
                                         {.docid = 3, .positions = {1}},
                                         {.docid = 4, .positions = {1}},
                                         {.docid = 5, .positions = {1}},
                                         {.docid = 6, .positions = {1}},
                                         {.docid = 7, .positions = {1}}}),
            make_term(plain_key("wolf"), {{.docid = 0, .positions = {1}},
                                          {.docid = 2, .positions = {2}},
                                          {.docid = 3, .positions = {2}},
                                          {.docid = 4, .positions = {2}},
                                          {.docid = 5, .positions = {2}},
                                          {.docid = 6, .positions = {2}},
                                          {.docid = 7, .positions = {2}}}),
            make_term(plain_key("woman"), {{.docid = 1, .positions = {1}}}),
            make_term(gram_key("the", "wolf"), {{.docid = 0, .positions = {0}}}),
            make_term(gram_key("the", "woman"), {{.docid = 1, .positions = {0}}}),
            make_term(gram_key("the", "gap"), {{.docid = 2, .positions = {0}},
                                               {.docid = 3, .positions = {0}},
                                               {.docid = 4, .positions = {0}},
                                               {.docid = 5, .positions = {0}},
                                               {.docid = 6, .positions = {0}},
                                               {.docid = 7, .positions = {0}}}),
    };
}

Status build_fixture(Fixture* fixture) {
    writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = std::string(kIndexSuffix);
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 8;
    input.terms = plan_terms();
    input.target_dict_block_bytes = 1;
    input.common_grams_metadata = complete_metadata();
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    writer::SniiCompoundWriter compound_writer(&fixture->file);
    RETURN_IF_ERROR(compound_writer.add_logical_index(input));
    RETURN_IF_ERROR(compound_writer.finish());
    RETURN_IF_ERROR(
            reader::SniiSegmentReader::open(&fixture->recording_reader, &fixture->segment_reader));
    RETURN_IF_ERROR(
            fixture->segment_reader.open_index(kIndexId, kIndexSuffix, &fixture->index_reader));
    fixture->recording_reader.reset_counts();
    return Status::OK();
}

constexpr uint32_t kScoringDocCount = 520;

std::vector<writer::TermPostings> scoring_plan_terms() {
    auto all_the = snii_test::docs_with_one_position(0, kScoringDocCount, 0);
    auto all_wolf = snii_test::docs_with_one_position(0, kScoringDocCount, 1);
    auto all_the_wolf = snii_test::docs_with_one_position(0, kScoringDocCount, 0);
    return {
            make_term(plain_key(std::string("\x1f"
                                            "literal")),
                      {{.docid = 0, .positions = {2}}}),
            make_term(gram_key("the", "wolf"), std::move(all_the_wolf)),
            make_term(gram_key("the", "woman"), {{.docid = 0, .positions = {0}}}),
            make_term(plain_key("the"), std::move(all_the)),
            make_term(plain_key("wolf"), std::move(all_wolf)),
            make_term(plain_key("woman"), {{.docid = 0, .positions = {1}}}),
            make_term(plain_key("zeta"), {{.docid = 0, .positions = {3}}}),
    };
}

Status build_scoring_fixture(Fixture* fixture,
                             CommonGramsCoverage coverage = CommonGramsCoverage::kComplete) {
    writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = std::string(kIndexSuffix);
    input.config = format::IndexConfig::kDocsPositionsScoring;
    input.doc_count = kScoringDocCount;
    input.encoded_norms.assign(kScoringDocCount, 1);
    input.terms = scoring_plan_terms();
    input.target_dict_block_bytes = 1U << 20;
    input.common_grams_metadata = complete_scoring_metadata(kScoringDocCount, 1043);
    input.common_grams_metadata->common_grams_coverage = coverage;
    std::ranges::sort(input.terms,
                      [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
                          return lhs.term < rhs.term;
                      });

    writer::SniiCompoundWriter compound_writer(&fixture->file);
    RETURN_IF_ERROR(compound_writer.add_logical_index(input));
    RETURN_IF_ERROR(compound_writer.finish());
    RETURN_IF_ERROR(
            reader::SniiSegmentReader::open(&fixture->recording_reader, &fixture->segment_reader));
    return fixture->segment_reader.open_index(kIndexId, kIndexSuffix, &fixture->index_reader);
}

format::DictEntry find_entry(const reader::LogicalIndexReader& index_reader,
                             std::string_view term) {
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    EXPECT_TRUE(index_reader.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    EXPECT_TRUE(found) << term;
    return entry;
}

uint64_t frequency_bytes(const format::DictEntry& entry) {
    if (entry.kind == format::DictEntryKind::kInline) {
        EXPECT_GE(entry.frq_bytes.size(), entry.inline_dd_disk_len);
        return entry.frq_bytes.size() - entry.inline_dd_disk_len;
    }
    EXPECT_GE(entry.frq_len, entry.frq_docs_len);
    return entry.frq_len - entry.frq_docs_len;
}

uint64_t phrase_visible_bytes(const format::DictEntry& entry) {
    if (entry.kind == format::DictEntryKind::kInline) {
        return entry.inline_dd_disk_len + entry.prx_bytes.size();
    }
    return entry.frq_docs_len + entry.prx_len;
}

uint64_t docid_visible_bytes(const format::DictEntry& entry) {
    return entry.kind == format::DictEntryKind::kInline ? entry.inline_dd_disk_len
                                                        : entry.frq_docs_len;
}

segment_v2::InvertedIndexQueryInfo query_info(segment_v2::TermKeyKind kind, std::string term) {
    segment_v2::InvertedIndexQueryInfo info;
    segment_v2::TermInfo term_info;
    term_info.term = std::move(term);
    term_info.position = 1;
    term_info.key_kind = kind;
    info.term_infos.push_back(std::move(term_info));
    return info;
}

segment_v2::InvertedIndexQueryInfo plain_query_info(std::string first, std::string second) {
    segment_v2::InvertedIndexQueryInfo info;
    info.term_infos.emplace_back(std::move(first), 1);
    info.term_infos.emplace_back(std::move(second), 2);
    return info;
}

// Repeating the query re-plans from scratch and must land on the same plan and the same docs:
// nothing memoizes the choice, so the second run is only as stable as the inputs make it.
TEST(SniiCommonGramsDictPlan, RepeatedExactPlanningUsesDefaultReads) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    Fixture fixture;
    assert_ok(build_fixture(&fixture));
    const CommonGramsQueryIdentity identity = complete_query_identity();
    const auto plain = plain_query_info("the", "wolf");
    const auto gram = query_info(segment_v2::TermKeyKind::kCommonGram, gram_key("the", "wolf"));

    std::vector<uint32_t> expected;
    assert_ok(phrase_query(fixture.index_reader, {"the", "wolf"}, &expected));

    for (int run = 0; run < 2; ++run) {
        SCOPED_TRACE(run);
        fixture.recording_reader.reset_counts();
        ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kPlain;
        std::vector<uint32_t> docs;
        assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &docs,
                                             nullptr, &selected_plan, no_hysteresis_cost_model()));

        EXPECT_EQ(docs, expected);
        EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
        EXPECT_GT(fixture.recording_reader.default_read_calls(), 0U);
    }
}

TEST(SniiCommonGramsDictPlan, RepeatedPrefixPlanningUsesDefaultReads) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    Fixture fixture;
    assert_ok(build_fixture(&fixture));
    const CommonGramsQueryIdentity identity = complete_query_identity();
    const auto plain = plain_query_info("the", "wo");
    const auto gram = query_info(segment_v2::TermKeyKind::kCommonGram, gram_key("the", "wo"));

    std::vector<uint32_t> expected;
    assert_ok(phrase_prefix_query(fixture.index_reader, {"the", "wo"}, &expected,
                                  /*max_expansions=*/50));

    for (int run = 0; run < 2; ++run) {
        SCOPED_TRACE(run);
        fixture.recording_reader.reset_counts();
        PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
        std::vector<uint32_t> docs;
        assert_ok(planned_phrase_prefix_query(
                fixture.index_reader, plain, gram, &identity, &docs, nullptr,
                /*max_expansions=*/50, &selected_plan, no_hysteresis_cost_model()));

        EXPECT_EQ(docs, expected);
        EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
        EXPECT_GT(fixture.recording_reader.default_read_calls(), 0U);
    }
}
TEST(SniiCommonGramsDictPlan, IncompatibleExactAndPrefixFallbackUseDefaultReads) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    Fixture fixture;
    assert_ok(build_fixture(&fixture));
    CommonGramsQueryIdentity incompatible_identity = complete_query_identity();
    incompatible_identity.common_grams_fingerprint = "incompatible-common-grams-query";

    const auto exact_plain = plain_query_info("the", "wolf");
    const auto exact_gram =
            query_info(segment_v2::TermKeyKind::kCommonGram, gram_key("the", "wolf"));
    ExactPhrasePlanKind exact_plan = ExactPhrasePlanKind::kCommonGrams;
    std::vector<uint32_t> exact_docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, exact_plain, exact_gram,
                                         &incompatible_identity, &exact_docs, nullptr, &exact_plan,
                                         no_hysteresis_cost_model()));

    EXPECT_EQ(exact_plan, ExactPhrasePlanKind::kPlain);
    EXPECT_EQ(exact_docs, (std::vector<uint32_t> {0}));
    EXPECT_GT(fixture.recording_reader.default_read_calls(), 0U);

    fixture.recording_reader.reset_counts();
    const auto prefix_plain = plain_query_info("the", "wo");
    const auto prefix_gram =
            query_info(segment_v2::TermKeyKind::kCommonGram, gram_key("the", "wo"));
    PhrasePrefixPlanKind prefix_plan = PhrasePrefixPlanKind::kCommonGrams;
    std::vector<uint32_t> prefix_docs;
    assert_ok(planned_phrase_prefix_query(
            fixture.index_reader, prefix_plain, prefix_gram, &incompatible_identity, &prefix_docs,
            nullptr, /*max_expansions=*/50, &prefix_plan, no_hysteresis_cost_model()));

    EXPECT_EQ(prefix_plan, PhrasePrefixPlanKind::kPlain);
    EXPECT_EQ(prefix_docs, (std::vector<uint32_t> {0, 1}));
    EXPECT_GT(fixture.recording_reader.default_read_calls(), 0U);
}

TEST(SniiCommonGramsDictPlan, ScoringIndexOmitsOnlyGramFrequencyAndTermStats) {
    Fixture fixture;
    assert_ok(build_scoring_fixture(&fixture));

    const std::string escaped_plain =
            plain_key(std::string("\x1f"
                                  "literal"));
    const std::string dense_gram = gram_key("the", "wolf");
    const std::string small_gram = gram_key("the", "woman");
    ASSERT_LT(escaped_plain, dense_gram);
    ASSERT_LT(dense_gram, plain_key("the"));

    const format::DictEntry escaped_entry = find_entry(fixture.index_reader, escaped_plain);
    const format::DictEntry dense_gram_entry = find_entry(fixture.index_reader, dense_gram);
    const format::DictEntry small_gram_entry = find_entry(fixture.index_reader, small_gram);
    const format::DictEntry the_entry = find_entry(fixture.index_reader, plain_key("the"));

    EXPECT_TRUE(escaped_entry.term_stats_present);
    EXPECT_GT(frequency_bytes(escaped_entry), 0U);
    EXPECT_FALSE(dense_gram_entry.term_stats_present);
    EXPECT_EQ(frequency_bytes(dense_gram_entry), 0U);
    EXPECT_FALSE(small_gram_entry.term_stats_present);
    EXPECT_EQ(frequency_bytes(small_gram_entry), 0U);
    EXPECT_TRUE(the_entry.term_stats_present);
    EXPECT_GT(frequency_bytes(the_entry), 0U);

    stats::SniiStatsProvider stats_provider;
    assert_ok(stats::SniiStatsProvider::open(&fixture.index_reader, &stats_provider));
    uint64_t total_term_freq = 0;
    assert_ok(stats_provider.total_term_freq(plain_key("the"), &total_term_freq));
    EXPECT_EQ(total_term_freq, kScoringDocCount);
    EXPECT_FALSE(stats_provider.total_term_freq(dense_gram, &total_term_freq).ok());

    std::vector<uint32_t> plain_docs;
    assert_ok(
            phrase_query(fixture.index_reader, {plain_key("the"), plain_key("wolf")}, &plain_docs));
    EXPECT_EQ(plain_docs.size(), kScoringDocCount);
}

TEST(SniiCommonGramsDictPlan, IncompleteCoverageKeepsGramFrequencyAndTermStats) {
    for (const CommonGramsCoverage coverage :
         {CommonGramsCoverage::kMixed, CommonGramsCoverage::kNone}) {
        SCOPED_TRACE(static_cast<uint8_t>(coverage));
        Fixture fixture;
        assert_ok(build_scoring_fixture(&fixture, coverage));

        const format::DictEntry gram_entry =
                find_entry(fixture.index_reader, gram_key("the", "wolf"));
        EXPECT_TRUE(gram_entry.term_stats_present);
        EXPECT_GT(frequency_bytes(gram_entry), 0U);
    }
}

TEST(SniiCommonGramsDictPlan, ExactPlanCostMatchesExecutorReads) {
    Fixture fixture;
    assert_ok(build_scoring_fixture(&fixture));
    const std::string the = plain_key("the");
    const std::string wolf = plain_key("wolf");
    const std::string gram = gram_key("the", "wolf");
    const CommonGramsQueryIdentity identity = complete_query_identity();

    QueryProfile profile;
    std::vector<uint32_t> planned_docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain_query_info("the", "wolf"),
                                         query_info(segment_v2::TermKeyKind::kCommonGram, gram),
                                         &identity, &planned_docs, &profile, nullptr,
                                         verify_cost_model()));

    EXPECT_EQ(planned_docs.size(), kScoringDocCount);
    const auto& stats = profile.phrase_query_stats;
    EXPECT_EQ(stats.common_grams_plain_posting_bytes,
              phrase_visible_bytes(find_entry(fixture.index_reader, the)) +
                      phrase_visible_bytes(find_entry(fixture.index_reader, wolf)));
    EXPECT_EQ(stats.common_grams_gram_posting_bytes,
              docid_visible_bytes(find_entry(fixture.index_reader, gram)));
    EXPECT_EQ(stats.common_grams_plain_estimated_cost,
              stats.common_grams_plain_posting_bytes +
                      static_cast<uint64_t>(kScoringDocCount) * 7 * 2);
    EXPECT_EQ(stats.common_grams_gram_estimated_cost, stats.common_grams_gram_posting_bytes);
}

TEST(SniiCommonGramsDictPlan, TwoTermPrefixGramPlanCostsOnlyDocidUnion) {
    Fixture fixture;
    assert_ok(build_fixture(&fixture));
    const CommonGramsQueryIdentity identity = complete_query_identity();

    QueryProfile profile;
    std::vector<uint32_t> planned_docs;
    assert_ok(planned_phrase_prefix_query(
            fixture.index_reader, plain_query_info("the", "wo"),
            query_info(segment_v2::TermKeyKind::kCommonGram, gram_key("the", "wo")), &identity,
            &planned_docs, &profile, /*max_expansions=*/50, nullptr, verify_cost_model()));

    EXPECT_EQ(planned_docs, (std::vector<uint32_t> {0, 1}));
    const auto& stats = profile.phrase_query_stats;
    EXPECT_EQ(stats.common_grams_gram_posting_bytes,
              docid_visible_bytes(find_entry(fixture.index_reader, gram_key("the", "wolf"))) +
                      docid_visible_bytes(
                              find_entry(fixture.index_reader, gram_key("the", "woman"))));
    EXPECT_EQ(stats.common_grams_gram_estimated_cost, stats.common_grams_gram_posting_bytes);
}

} // namespace
} // namespace doris::snii::query
