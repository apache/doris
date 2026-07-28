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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/inverted/common_grams/common_word_set.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/count_query.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/internal/term_expansion.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/regexp_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/query/wildcard_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris::snii::query {
namespace {

using segment_v2::inverted_index::CommonGramsCoverage;
using segment_v2::inverted_index::CommonGramsPlanCostModel;
using segment_v2::inverted_index::CommonGramsQueryIdentity;
using segment_v2::inverted_index::CommonGramsSegmentMetadata;
using segment_v2::inverted_index::PlainTermKeyVersion;
using segment_v2::inverted_index::COMMON_GRAMS_KEY_VERSION_V1;
using segment_v2::inverted_index::COMMON_GRAMS_SEMANTICS_VERSION_V1;
using segment_v2::inverted_index::encode_common_gram;
using segment_v2::inverted_index::encode_plain_term;
using snii_test::MemoryFile;
using snii_test::ScopedEnv;
using snii_test::assert_ok;
using snii_test::make_term;

constexpr uint64_t kIndexId = 41;
constexpr std::string_view kIndexSuffix = "body";

struct Fixture {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
};

CommonGramsSegmentMetadata metadata_for(PlainTermKeyVersion version) {
    CommonGramsSegmentMetadata metadata;
    metadata.plain_term_key_version = version;
    metadata.common_grams_coverage = version == PlainTermKeyVersion::kRawNoInternal
                                             ? CommonGramsCoverage::kNone
                                             : CommonGramsCoverage::kMixed;
    return metadata;
}

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

CommonGramsSegmentMetadata hybrid_metadata() {
    CommonGramsSegmentMetadata metadata = complete_metadata();
    metadata.common_grams_coverage = CommonGramsCoverage::kMixed;
    return metadata;
}

CommonGramsQueryIdentity complete_query_identity() {
    const auto metadata = complete_metadata();
    return {.common_grams_dictionary_identity = metadata.common_grams_dictionary_identity,
            .base_analyzer_fingerprint = metadata.base_analyzer_fingerprint,
            .common_grams_fingerprint = metadata.common_grams_fingerprint};
}

CommonGramsPlanCostModel no_hysteresis_cost_model() {
    return {.position_verify_factor = 0, .common_grams_cost_ratio_percent = 100, .generation = 0};
}

CommonGramsPlanCostModel verification_dominated_cost_model() {
    return {.position_verify_factor = 100000,
            .common_grams_cost_ratio_percent = 100,
            .generation = 0};
}

Status build_fixture(
        Fixture* fixture, std::vector<writer::TermPostings> terms,
        std::optional<CommonGramsSegmentMetadata> metadata = std::nullopt,
        format::CommonGramsPostingPolicy posting_policy = format::CommonGramsPostingPolicy::kNone) {
    std::ranges::sort(terms, [](const writer::TermPostings& lhs, const writer::TermPostings& rhs) {
        return lhs.term < rhs.term;
    });
    writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = std::string(kIndexSuffix);
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 8;
    input.terms = std::move(terms);
    input.target_dict_block_bytes = 64;
    input.common_grams_metadata = std::move(metadata);
    input.common_grams_posting_policy = posting_policy;

    writer::SniiCompoundWriter compound(&fixture->file);
    RETURN_IF_ERROR(compound.add_logical_index(input));
    RETURN_IF_ERROR(compound.finish());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(&fixture->file, &fixture->segment_reader));
    return fixture->segment_reader.open_index(kIndexId, kIndexSuffix, &fixture->index_reader);
}

Status build_streamed_fixture(Fixture* fixture, writer::SpimiTermBuffer* terms, uint32_t doc_count,
                              CommonGramsSegmentMetadata metadata,
                              format::CommonGramsPostingPolicy posting_policy) {
    writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = std::string(kIndexSuffix);
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = doc_count;
    input.term_source = terms;
    input.target_dict_block_bytes = 64;
    input.common_grams_metadata = std::move(metadata);
    input.common_grams_posting_policy = posting_policy;

    writer::SniiCompoundWriter compound(&fixture->file);
    RETURN_IF_ERROR(compound.add_logical_index(input));
    RETURN_IF_ERROR(compound.finish());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(&fixture->file, &fixture->segment_reader));
    return fixture->segment_reader.open_index(kIndexId, kIndexSuffix, &fixture->index_reader);
}

std::string encoded_plain(std::string_view logical, PlainTermKeyVersion version) {
    auto encoded = encode_plain_term(logical, version);
    EXPECT_TRUE(encoded.has_value());
    return encoded.has_value() ? std::move(encoded.value()) : std::string();
}

std::vector<writer::TermPostings> escaped_terms() {
    const std::string literal_marker =
            std::string(segment_v2::inverted_index::CG_V1_MARKER) + "literal";
    auto gram = encode_common_gram("the", "cat");
    EXPECT_TRUE(gram.has_value());
    return {
            make_term(encoded_plain(literal_marker, PlainTermKeyVersion::kEscapedV1),
                      {{.docid = 0, .positions = {0}}}),
            make_term(gram.has_value() ? std::move(gram.value()) : std::string(),
                      {{.docid = 1, .positions = {0}}}),
            make_term(std::string(format::kPhraseBigramTermMarker) + "legacy",
                      {{.docid = 2, .positions = {0}}}),
            make_term("alpha", {{.docid = 3, .positions = {0}}}),
            make_term("beta", {{.docid = 3, .positions = {1}}, {.docid = 4, .positions = {0}}}),
    };
}

enum class TestGramPostingShape : uint8_t {
    kPositioned,
    kHybrid,
    kDocsOnly,
};

std::vector<writer::TermPostings> build_common_grams_terms(
        const std::vector<std::vector<std::string>>& docs, bool include_grams = true,
        TestGramPostingShape posting_shape = TestGramPostingShape::kPositioned) {
    writer::SpimiTermBuffer postings(/*has_positions=*/true);
    const auto& common_words =
            segment_v2::inverted_index::CommonWordSet::builtin_english_stop_words_v1();
    for (uint32_t docid = 0; docid < docs.size(); ++docid) {
        const auto& terms = docs[docid];
        for (uint32_t position = 0; position < terms.size(); ++position) {
            postings.add_token(encoded_plain(terms[position], PlainTermKeyVersion::kEscapedV1),
                               docid, position);
            if (!include_grams || position + 1 == terms.size()) {
                continue;
            }
            const bool left_common = common_words.contains(terms[position]);
            const bool right_common = common_words.contains(terms[position + 1]);
            if (!left_common && !right_common) {
                continue;
            }
            auto gram = encode_common_gram(terms[position], terms[position + 1]);
            EXPECT_TRUE(gram.has_value());
            if (gram.has_value()) {
                const bool retain_positions = posting_shape == TestGramPostingShape::kPositioned ||
                                              (posting_shape == TestGramPostingShape::kHybrid &&
                                               left_common && right_common);
                postings.add_token(std::move(gram.value()), docid, position, retain_positions);
            }
        }
    }
    return postings.finalize_sorted();
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

bool entry_has_prx(const format::DictEntry& entry) {
    return entry.kind == format::DictEntryKind::kInline ? !entry.prx_bytes.empty()
                                                        : entry.prx_len != 0;
}

void expect_all_synthetic_grams_docs_only(const reader::LogicalIndexReader& index_reader,
                                          const std::vector<std::vector<std::string>>& docs) {
    const auto& common_words =
            segment_v2::inverted_index::CommonWordSet::builtin_english_stop_words_v1();
    std::set<std::string> grams;
    for (const auto& terms : docs) {
        for (size_t position = 0; position + 1 < terms.size(); ++position) {
            if (!common_words.contains(terms[position]) &&
                !common_words.contains(terms[position + 1])) {
                continue;
            }
            auto gram = encode_common_gram(terms[position], terms[position + 1]);
            ASSERT_TRUE(gram.has_value());
            grams.emplace(std::move(gram.value()));
        }
    }
    ASSERT_FALSE(grams.empty());
    for (const std::string& gram : grams) {
        EXPECT_FALSE(entry_has_prx(find_entry(index_reader, gram))) << gram;
    }
}

segment_v2::InvertedIndexQueryInfo query_info(
        const std::vector<std::pair<segment_v2::TermKeyKind, std::string>>& terms) {
    segment_v2::InvertedIndexQueryInfo info;
    info.term_infos.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        segment_v2::TermInfo term_info;
        term_info.term = terms[i].second;
        term_info.position = static_cast<int32_t>(i + 1);
        term_info.key_kind = terms[i].first;
        info.term_infos.push_back(std::move(term_info));
    }
    return info;
}

segment_v2::InvertedIndexQueryInfo plain_query_info(const std::vector<std::string>& terms) {
    std::vector<std::pair<segment_v2::TermKeyKind, std::string>> typed_terms;
    typed_terms.reserve(terms.size());
    for (const std::string& term : terms) {
        typed_terms.emplace_back(segment_v2::TermKeyKind::kPlain, term);
    }
    return query_info(typed_terms);
}

std::string gram_key(std::string_view left, std::string_view right) {
    auto gram = encode_common_gram(left, right);
    EXPECT_TRUE(gram.has_value());
    return gram.has_value() ? std::move(gram.value()) : std::string();
}

TEST(SniiCommonGramsNamespaceQuery, WideSpilledDocsOnlyGramDeltaStreamRoundTrips) {
    constexpr uint32_t kDocumentCount = 768;
    writer::SpimiTermBuffer source(/*has_positions=*/true);
    source.enable_common_gram_pair_keys();
    source.set_forced_spill_min_arena_bytes(0);
    source.set_max_run_files(2);
    const writer::PlainTermId left = source.intern_plain_term("of");
    const writer::PlainTermId right = source.intern_plain_term("world");
    for (uint32_t docid = 0; docid < kDocumentCount; ++docid) {
        source.add_common_gram(left, right, docid, /*pos=*/0, /*retain_positions=*/false);
        if (docid == 255 || docid == 383 || docid == 511) {
            source.request_global_spill_for_test();
        }
    }
    ASSERT_TRUE(source.status().ok()) << source.status();
    ASSERT_GT(source.run_count_for_test(), 0U);
    ASSERT_LE(source.run_count_for_test(), source.max_run_files());

    Fixture fixture;
    assert_ok(build_streamed_fixture(&fixture, &source, kDocumentCount, hybrid_metadata(),
                                     format::CommonGramsPostingPolicy::kDocsOnlyV1));

    const std::string gram = gram_key("of", "world");
    const format::DictEntry entry = find_entry(fixture.index_reader, gram);
    EXPECT_EQ(entry.enc, format::DictEntryEnc::kWindowed);
    EXPECT_FALSE(entry_has_prx(entry));
    EXPECT_EQ(entry.df, kDocumentCount);

    std::vector<uint32_t> actual;
    assert_ok(term_query(fixture.index_reader, gram, &actual));
    ASSERT_EQ(actual.size(), kDocumentCount);
    for (uint32_t docid = 0; docid < kDocumentCount; ++docid) {
        EXPECT_EQ(actual[docid], docid);
    }
}

void expect_planned_prefix_matches_plain(const reader::LogicalIndexReader& idx,
                                         const segment_v2::InvertedIndexQueryInfo& plain_query,
                                         const segment_v2::InvertedIndexQueryInfo& gram_query,
                                         const CommonGramsQueryIdentity* identity,
                                         int32_t max_expansions,
                                         PhrasePrefixPlanKind expected_plan) {
    std::vector<std::string> plain_terms;
    plain_terms.reserve(plain_query.term_infos.size());
    for (const auto& term_info : plain_query.term_infos) {
        plain_terms.push_back(term_info.get_single_term());
    }
    SCOPED_TRACE(::testing::Message() << "first term: " << plain_terms.front()
                                      << ", term count: " << plain_terms.size());

    std::vector<uint32_t> expected;
    assert_ok(phrase_prefix_query(idx, plain_terms, &expected, max_expansions));
    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> actual;
    assert_ok(planned_phrase_prefix_query(idx, plain_query, gram_query, identity, &actual, nullptr,
                                          max_expansions, &selected_plan,
                                          no_hysteresis_cost_model()));
    EXPECT_EQ(selected_plan, expected_plan);
    EXPECT_EQ(actual, expected);
}

TEST(SniiCommonGramsNamespaceQuery, EscapedExactCountAnyAndAllUsePhysicalPlainKeys) {
    Fixture fixture;
    assert_ok(build_fixture(&fixture, escaped_terms(),
                            metadata_for(PlainTermKeyVersion::kEscapedV1)));
    const std::string literal_marker =
            std::string(segment_v2::inverted_index::CG_V1_MARKER) + "literal";

    std::string physical_marker;
    bool representable = false;
    assert_ok(internal::route_plain_query_term(fixture.index_reader, literal_marker,
                                               &physical_marker, &representable));
    ASSERT_TRUE(representable);
    EXPECT_NE(physical_marker, literal_marker);
    std::vector<uint32_t> docs;
    assert_ok(term_query(fixture.index_reader, physical_marker, &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));

    uint64_t count = 0;
    assert_ok(count_only_term_df(fixture.index_reader, physical_marker, &count));
    EXPECT_EQ(count, 1U);

    std::vector<std::string> physical_terms {literal_marker, "alpha"};
    bool all_representable = false;
    assert_ok(internal::route_plain_query_terms(fixture.index_reader, {literal_marker, "alpha"},
                                                &physical_terms, &all_representable));
    ASSERT_TRUE(all_representable);
    assert_ok(boolean_or(fixture.index_reader, physical_terms, &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {0, 3}));
    assert_ok(boolean_and(fixture.index_reader, physical_terms, &docs));
    EXPECT_TRUE(docs.empty());
}

TEST(SniiCommonGramsNamespaceQuery, GeneratedGramRequiresPlannerBeforePhysicalLookup) {
    Fixture fixture;
    assert_ok(build_fixture(&fixture, escaped_terms(),
                            metadata_for(PlainTermKeyVersion::kEscapedV1)));
    auto gram = encode_common_gram("the", "cat");
    ASSERT_TRUE(gram.has_value());

    segment_v2::TermInfo term_info;
    term_info.term = std::move(gram.value());
    term_info.key_kind = segment_v2::TermKeyKind::kCommonGram;
    std::string physical_term;
    bool representable = false;
    EXPECT_TRUE(internal::route_query_term(fixture.index_reader, term_info, &physical_term,
                                           &representable)
                        .is<ErrorCode::INVERTED_INDEX_BYPASS>());

    segment_v2::InvertedIndexQueryInfo query_info;
    query_info.term_infos.emplace_back(term_info);
    query_info.term_infos.emplace_back("plain", 1);
    std::vector<std::string> physical_terms {term_info.get_single_term(), "plain"};
    bool all_representable = false;
    EXPECT_TRUE(internal::route_query_terms(fixture.index_reader, query_info, &physical_terms,
                                            &all_representable)
                        .is<ErrorCode::INVERTED_INDEX_BYPASS>());
}

TEST(SniiCommonGramsNamespaceQuery, EscapedExpansionDecodesPlainAndSkipsInternalBudget) {
    Fixture fixture;
    assert_ok(build_fixture(&fixture, escaped_terms(),
                            metadata_for(PlainTermKeyVersion::kEscapedV1)));
    const std::string literal_marker =
            std::string(segment_v2::inverted_index::CG_V1_MARKER) + "literal";

    std::vector<uint32_t> docs;
    assert_ok(prefix_query(fixture.index_reader, literal_marker, &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));

    assert_ok(prefix_query(fixture.index_reader, "", &docs, /*max_expansions=*/2));
    EXPECT_EQ(docs, (std::vector<uint32_t> {0, 3}));
    assert_ok(wildcard_query(fixture.index_reader, "*", &docs, /*max_expansions=*/2));
    EXPECT_EQ(docs, (std::vector<uint32_t> {0, 3}));
    assert_ok(regexp_query(fixture.index_reader, ".*", &docs, /*max_expansions=*/2));
    EXPECT_EQ(docs, (std::vector<uint32_t> {0, 3}));

    assert_ok(phrase_prefix_query(fixture.index_reader, {"alpha", "b"}, &docs,
                                  /*max_expansions=*/2));
    EXPECT_EQ(docs, (std::vector<uint32_t> {3}));
}

TEST(SniiCommonGramsNamespaceQuery, EmptyLeadingRangeSkipsColdDictWhenNamespaceIsFirst) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    Fixture fixture;
    auto gram = encode_common_gram("the", "cat");
    ASSERT_TRUE(gram.has_value());
    assert_ok(build_fixture(&fixture,
                            {make_term(std::move(gram.value()), {{.docid = 1, .positions = {0}}})},
                            metadata_for(PlainTermKeyVersion::kEscapedV1)));

    fixture.file.clear_reads();
    size_t visited = 0;
    assert_ok(fixture.index_reader.visit_term_range(
            {}, segment_v2::inverted_index::INTERNAL_TERM_NAMESPACE_BEGIN,
            [&](reader::LogicalIndexReader::PrefixHit&&, bool*) {
                ++visited;
                return Status::OK();
            }));

    EXPECT_EQ(visited, 0U);
    EXPECT_TRUE(fixture.file.reads().empty());
}

TEST(SniiCommonGramsNamespaceQuery, LegacyRawWithoutReservedKeysAllowsLeadingExpansion) {
    Fixture fixture;
    assert_ok(build_fixture(&fixture, {make_term("alpha", {{.docid = 1, .positions = {0}}}),
                                       make_term("screenshot", {{.docid = 2, .positions = {0}}})}));

    std::vector<uint32_t> docs;
    assert_ok(wildcard_query(fixture.index_reader, "*shot", &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {2}));
    assert_ok(regexp_query(fixture.index_reader, ".*shot", &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {2}));
    assert_ok(prefix_query(fixture.index_reader, "", &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {1, 2}));
}

TEST(SniiCommonGramsNamespaceQuery, LegacyLeadingExpansionReadsEachColdDictBlockOnce) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    Fixture fixture;
    assert_ok(build_fixture(&fixture, {make_term("alpha", {{.docid = 1, .positions = {0}}}),
                                       make_term("screenshot", {{.docid = 2, .positions = {0}}})}));

    fixture.file.clear_reads();
    size_t visited = 0;
    assert_ok(internal::visit_expanded_plain_terms(
            fixture.index_reader, "", [](std::string_view) { return true; },
            [&](reader::LogicalIndexReader::PrefixHit&&, bool*) {
                ++visited;
                return Status::OK();
            }));

    EXPECT_EQ(visited, 2U);
    std::set<std::pair<uint64_t, size_t>> unique_reads;
    for (const auto& read : fixture.file.reads()) {
        unique_reads.emplace(read.offset, read.len);
    }
    EXPECT_EQ(unique_reads.size(), fixture.file.reads().size());
}

TEST(SniiCommonGramsNamespaceQuery, LegacyReservedQueriesBypassTheIndex) {
    for (const std::string& hidden :
         {std::string(format::kPhraseBigramTermMarker) + "legacy",
          std::string(segment_v2::inverted_index::CG_V1_MARKER) + "legacy"}) {
        Fixture fixture;
        assert_ok(build_fixture(&fixture, {make_term(hidden, {{.docid = 1, .positions = {0}}}),
                                           make_term("alpha", {{.docid = 2, .positions = {0}}})}));

        std::string physical;
        bool representable = false;
        EXPECT_TRUE(internal::route_plain_query_term(fixture.index_reader, hidden, &physical,
                                                     &representable)
                            .is<ErrorCode::INVERTED_INDEX_BYPASS>());
        std::vector<uint32_t> docs;
        EXPECT_TRUE(prefix_query(fixture.index_reader, "", &docs)
                            .is<ErrorCode::INVERTED_INDEX_BYPASS>());
        EXPECT_TRUE(wildcard_query(fixture.index_reader, "*", &docs)
                            .is<ErrorCode::INVERTED_INDEX_BYPASS>());
        EXPECT_TRUE(regexp_query(fixture.index_reader, ".*", &docs)
                            .is<ErrorCode::INVERTED_INDEX_BYPASS>());
    }
}

TEST(SniiCommonGramsNamespaceQuery, RawNoInternalTreatsReservedBytesAsPlain) {
    Fixture fixture;
    const std::string marker_literal = std::string(format::kPhraseBigramTermMarker) + "literal";
    assert_ok(build_fixture(&fixture,
                            {make_term(marker_literal, {{.docid = 5, .positions = {0}}}),
                             make_term("alpha", {{.docid = 6, .positions = {0}}})},
                            metadata_for(PlainTermKeyVersion::kRawNoInternal)));

    std::string physical;
    bool representable = false;
    assert_ok(internal::route_plain_query_term(fixture.index_reader, marker_literal, &physical,
                                               &representable));
    ASSERT_TRUE(representable);
    EXPECT_EQ(physical, marker_literal);
    std::vector<uint32_t> docs;
    assert_ok(term_query(fixture.index_reader, physical, &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {5}));
    assert_ok(prefix_query(fixture.index_reader, "\x1f", &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {5}));
    assert_ok(wildcard_query(fixture.index_reader, "\x1f*", &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {5}));
    assert_ok(regexp_query(fixture.index_reader, "\x1f.*", &docs));
    EXPECT_EQ(docs, (std::vector<uint32_t> {5}));
}

TEST(SniiCommonGramsNamespaceQuery, EscapedUnrepresentableBoundaryIsAuthoritativeAbsent) {
    Fixture fixture;
    assert_ok(build_fixture(&fixture, {make_term("alpha", {{.docid = 0, .positions = {0}}})},
                            metadata_for(PlainTermKeyVersion::kEscapedV1)));
    std::string logical(segment_v2::inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES, 'x');
    logical.front() = segment_v2::inverted_index::PLAIN_ESCAPE_PREFIX;

    std::string physical;
    bool representable = true;
    assert_ok(internal::route_plain_query_term(fixture.index_reader, logical, &physical,
                                               &representable));
    EXPECT_FALSE(representable);
    EXPECT_TRUE(physical.empty());

    std::vector<uint32_t> docs;
    assert_ok(prefix_query(fixture.index_reader, logical, &docs));
    EXPECT_TRUE(docs.empty());
}

TEST(SniiCommonGramsNamespaceQuery, EscapedAllUnrepresentableAnyTermsResolveEmpty) {
    Fixture fixture;
    assert_ok(build_fixture(&fixture, {make_term("alpha", {{.docid = 0, .positions = {0}}})},
                            metadata_for(PlainTermKeyVersion::kEscapedV1)));
    std::string escape_term(segment_v2::inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES, 'x');
    escape_term.front() = segment_v2::inverted_index::PLAIN_ESCAPE_PREFIX;
    std::string marker_term(segment_v2::inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES, 'x');
    marker_term.front() = '\x1f';

    std::vector<std::string> physical_terms {escape_term, marker_term};
    bool all_representable = true;
    assert_ok(internal::route_plain_query_terms(fixture.index_reader, {escape_term, marker_term},
                                                &physical_terms, &all_representable));
    EXPECT_FALSE(all_representable);
    EXPECT_TRUE(physical_terms.empty());

    std::vector<uint32_t> docs;
    assert_ok(boolean_or(fixture.index_reader, physical_terms, &docs));
    EXPECT_TRUE(docs.empty());
}

TEST(SniiCommonGramsNamespaceQuery, ExactPlannerReusesMatcherForEveryCommonTermShape) {
    const std::vector<std::vector<std::string>> corpus = {
            {"cat", "dog", "fox"}, {"cat", "dog", "the"}, {"cat", "the", "dog"},
            {"cat", "the", "the"}, {"the", "cat", "dog"}, {"the", "cat", "the"},
            {"the", "the", "cat"}, {"the", "the", "the"}};
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus), complete_metadata()));
    const auto identity = complete_query_identity();

    using Kind = segment_v2::TermKeyKind;
    const std::vector<segment_v2::InvertedIndexQueryInfo> gram_plans = {
            query_info({{Kind::kPlain, "cat"}, {Kind::kPlain, "dog"}, {Kind::kPlain, "fox"}}),
            query_info({{Kind::kPlain, "cat"}, {Kind::kCommonGram, gram_key("dog", "the")}}),
            query_info({{Kind::kCommonGram, gram_key("cat", "the")},
                        {Kind::kCommonGram, gram_key("the", "dog")}}),
            query_info({{Kind::kCommonGram, gram_key("cat", "the")},
                        {Kind::kCommonGram, gram_key("the", "the")}}),
            query_info({{Kind::kCommonGram, gram_key("the", "cat")},
                        {Kind::kPlain, "cat"},
                        {Kind::kPlain, "dog"}}),
            query_info({{Kind::kCommonGram, gram_key("the", "cat")},
                        {Kind::kCommonGram, gram_key("cat", "the")}}),
            query_info({{Kind::kCommonGram, gram_key("the", "the")},
                        {Kind::kCommonGram, gram_key("the", "cat")}}),
            query_info({{Kind::kCommonGram, gram_key("the", "the")},
                        {Kind::kCommonGram, gram_key("the", "the")}})};

    for (size_t i = 0; i < corpus.size(); ++i) {
        std::vector<uint32_t> plain_docs;
        assert_ok(phrase_query(fixture.index_reader, corpus[i], &plain_docs));
        ASSERT_EQ(plain_docs, (std::vector<uint32_t> {static_cast<uint32_t>(i)}));

        std::vector<uint32_t> planned_docs;
        assert_ok(planned_exact_phrase_query(fixture.index_reader, plain_query_info(corpus[i]),
                                             gram_plans[i], &identity, &planned_docs));
        EXPECT_EQ(planned_docs, plain_docs) << "shape " << i;
    }
}

TEST(SniiCommonGramsNamespaceQuery, TwoTermsUseSingleGramTermQueryWithoutPrxDecode) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "cat"}, {"the", "gap", "cat"}, {"cat", "the"}};
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus), complete_metadata()));
    const auto identity = complete_query_identity();

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "cat"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "cat")}});
    QueryProfile profile;
    ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &docs,
                                         &profile, &selected_plan));

    EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
    EXPECT_EQ(profile.prx_decode_stats.frame_count(), 0U);
}

TEST(SniiCommonGramsNamespaceQuery,
     DocsOnlyAllCommonExactUsesTermDocsetOrPlainPrxVerificationByLength) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "of"},
            {"the", "gap", "of"},
            {"the", "of", "a"},
            {"the", "of", "gap", "of", "a"},
            {"the", "of", "a", "in", "the"},
            {"the", "of", "gap", "of", "a", "gap", "a", "in", "gap", "in", "the"},
    };
    Fixture fixture;
    assert_ok(build_fixture(&fixture,
                            build_common_grams_terms(corpus, /*include_grams=*/true,
                                                     TestGramPostingShape::kDocsOnly),
                            hybrid_metadata(), format::CommonGramsPostingPolicy::kDocsOnlyV1));
    expect_all_synthetic_grams_docs_only(fixture.index_reader, corpus);
    const auto identity = complete_query_identity();

    using Kind = segment_v2::TermKeyKind;
    for (const std::vector<std::string>& terms :
         {std::vector<std::string> {"the", "of"}, std::vector<std::string> {"the", "of", "a"},
          std::vector<std::string> {"the", "of", "a", "in", "the"}}) {
        std::vector<std::string> gram_keys;
        std::vector<std::pair<Kind, std::string>> gram_terms;
        for (size_t i = 0; i + 1 < terms.size(); ++i) {
            gram_keys.push_back(gram_key(terms[i], terms[i + 1]));
            gram_terms.emplace_back(Kind::kCommonGram, gram_keys.back());
        }

        std::vector<uint32_t> expected;
        assert_ok(phrase_query(fixture.index_reader, terms, &expected));
        std::vector<uint32_t> gram_candidates;
        assert_ok(boolean_and(fixture.index_reader, gram_keys, &gram_candidates));
        if (terms.size() == 2) {
            ASSERT_EQ(gram_candidates, expected);
        } else {
            ASSERT_GT(gram_candidates.size(), expected.size());
        }

        QueryProfile profile;
        ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kPlain;
        std::vector<uint32_t> actual;
        assert_ok(planned_exact_phrase_query(fixture.index_reader, plain_query_info(terms),
                                             query_info(gram_terms), &identity, &actual, &profile,
                                             &selected_plan, no_hysteresis_cost_model(),
                                             CommonGramsPlanDebugOverride::kForceCommonGrams));
        EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
        EXPECT_EQ(actual, expected) << "term count " << terms.size();
        if (terms.size() == 2) {
            EXPECT_EQ(profile.prx_decode_stats.frame_count(), 0U);
        }
    }
}

TEST(SniiCommonGramsNamespaceQuery,
     DocsOnlyAllCommonPhrasePrefixUsesGramCandidatesThenPlainPrxVerification) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "of", "a", "in"},        {"the", "of", "a", "into"},
            {"the", "of", "a", "it"},        {"the", "of", "gap", "of", "a", "gap", "a", "in"},
            {"the", "of", "a", "gap", "in"},
    };
    Fixture fixture;
    assert_ok(build_fixture(&fixture,
                            build_common_grams_terms(corpus, /*include_grams=*/true,
                                                     TestGramPostingShape::kDocsOnly),
                            hybrid_metadata(), format::CommonGramsPostingPolicy::kDocsOnlyV1));
    expect_all_synthetic_grams_docs_only(fixture.index_reader, corpus);
    const auto identity = complete_query_identity();

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "of", "a", "i"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "of")},
                                  {Kind::kCommonGram, gram_key("of", "a")},
                                  {Kind::kCommonGram, gram_key("a", "i")}});
    std::vector<uint32_t> expected;
    assert_ok(phrase_prefix_query(fixture.index_reader, {"the", "of", "a", "i"}, &expected,
                                  /*max_expansions=*/50));
    ASSERT_EQ(expected, (std::vector<uint32_t> {0, 1, 2}));

    std::vector<uint32_t> in_gram_candidates;
    assert_ok(boolean_and(fixture.index_reader,
                          {gram_key("the", "of"), gram_key("of", "a"), gram_key("a", "in")},
                          &in_gram_candidates));
    ASSERT_TRUE(std::ranges::find(in_gram_candidates, 3) != in_gram_candidates.end());

    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> actual;
    assert_ok(planned_phrase_prefix_query(
            fixture.index_reader, plain, gram, &identity, &actual, nullptr,
            /*max_expansions=*/50, &selected_plan, no_hysteresis_cost_model(),
            CommonGramsPlanDebugOverride::kForceCommonGrams));
    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_EQ(actual, expected);
}

TEST(SniiCommonGramsNamespaceQuery, RepeatedGramUsesExistingRepeatedTermPhrasePath) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "the", "the"}, {"the", "gap", "the"}, {"the", "the", "cat"}};
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus), complete_metadata()));
    const auto identity = complete_query_identity();

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "the", "the"});
    const std::string the_the = gram_key("the", "the");
    const auto gram = query_info({{Kind::kCommonGram, the_the}, {Kind::kCommonGram, the_the}});
    ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &docs,
                                         nullptr, &selected_plan, no_hysteresis_cost_model()));

    EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
}

TEST(SniiCommonGramsNamespaceQuery,
     HybridExactUsesDocsOnlyPrefilterAndMinimalPositionedCoverFreshAndCached) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "of", "wolf"},
            {"the", "of", "gap", "of", "wolf"},
            {"the", "gap", "of", "wolf"},
    };
    Fixture fixture;
    assert_ok(build_fixture(
            &fixture,
            build_common_grams_terms(corpus, /*include_grams=*/true, TestGramPostingShape::kHybrid),
            hybrid_metadata(), format::CommonGramsPostingPolicy::kHybridV1));
    const auto identity = complete_query_identity();

    const std::string the_of = gram_key("the", "of");
    const std::string of_wolf = gram_key("of", "wolf");
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, the_of)));
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, of_wolf)));

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "of", "wolf"});
    const auto gram = query_info({{Kind::kCommonGram, the_of}, {Kind::kCommonGram, of_wolf}});
    std::vector<uint32_t> expected;
    assert_ok(phrase_query(fixture.index_reader, {"the", "of", "wolf"}, &expected));
    ASSERT_EQ(expected, (std::vector<uint32_t> {0}));

    ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kCommonGrams;
    std::vector<uint32_t> cost_selected_docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity,
                                         &cost_selected_docs, nullptr, &selected_plan,
                                         no_hysteresis_cost_model()));
    EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
    EXPECT_EQ(cost_selected_docs, expected);

    selected_plan = ExactPhrasePlanKind::kPlain;
    std::vector<uint32_t> fresh_docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &fresh_docs,
                                         nullptr, &selected_plan, no_hysteresis_cost_model(),
                                         CommonGramsPlanDebugOverride::kForceCommonGrams));
    EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
    EXPECT_EQ(fresh_docs, expected);
}

TEST(SniiCommonGramsNamespaceQuery, HybridExactPrunesDominatedPositionedMiddleEdgeFreshAndCached) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "of", "a", "in", "wolf"},
            {"the", "of", "a", "gap", "a", "in", "wolf"},
            {"the", "gap", "of", "a", "in", "wolf"},
    };
    Fixture fixture;
    assert_ok(build_fixture(
            &fixture,
            build_common_grams_terms(corpus, /*include_grams=*/true, TestGramPostingShape::kHybrid),
            hybrid_metadata(), format::CommonGramsPostingPolicy::kHybridV1));
    const auto identity = complete_query_identity();

    const std::string the_of = gram_key("the", "of");
    const std::string of_a = gram_key("of", "a");
    const std::string a_in = gram_key("a", "in");
    const std::string in_wolf = gram_key("in", "wolf");
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, the_of)));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, of_a)));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, a_in)));
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, in_wolf)));

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "of", "a", "in", "wolf"});
    const auto gram = query_info({{Kind::kCommonGram, the_of},
                                  {Kind::kCommonGram, of_a},
                                  {Kind::kCommonGram, a_in},
                                  {Kind::kCommonGram, in_wolf}});
    std::vector<uint32_t> expected;
    assert_ok(phrase_query(fixture.index_reader, {"the", "of", "a", "in", "wolf"}, &expected));
    ASSERT_EQ(expected, (std::vector<uint32_t> {0}));

    ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kPlain;
    std::vector<uint32_t> fresh_docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &fresh_docs,
                                         nullptr, &selected_plan,
                                         verification_dominated_cost_model()));
    EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
    EXPECT_EQ(fresh_docs, expected);
}

TEST(SniiCommonGramsNamespaceQuery,
     HybridExactEmptyCandidateIntersectionIsAuthoritativeFreshAndCached) {
    const std::vector<std::vector<std::string>> corpus = {
            {"wolf", "the", "gap", "fox", "gap", "cat"},
            {"gap", "the", "fox", "gap", "cat", "gap", "wolf"},
            {"wolf", "gap", "the", "gap", "fox", "cat"},
            {"wolf", "gap", "the", "gap", "fox", "cat"},
            {"wolf", "gap", "the", "gap", "fox", "cat"},
            {"wolf", "gap", "the", "gap", "fox", "cat"},
            {"wolf", "gap", "the", "gap", "fox", "cat"},
            {"wolf", "gap", "the", "gap", "fox", "cat"},
    };
    Fixture fixture;
    assert_ok(build_fixture(
            &fixture,
            build_common_grams_terms(corpus, /*include_grams=*/true, TestGramPostingShape::kHybrid),
            hybrid_metadata(), format::CommonGramsPostingPolicy::kHybridV1));
    const auto identity = complete_query_identity();

    const std::string wolf_the = gram_key("wolf", "the");
    const std::string the_fox = gram_key("the", "fox");
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, wolf_the)));
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, the_fox)));

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"wolf", "the", "fox", "cat"});
    const auto gram = query_info({{Kind::kCommonGram, wolf_the},
                                  {Kind::kCommonGram, the_fox},
                                  {Kind::kPlain, "fox"},
                                  {Kind::kPlain, "cat"}});
    QueryProfile fresh_profile;
    ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &docs,
                                         &fresh_profile, &selected_plan,
                                         verification_dominated_cost_model()));
    EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
    EXPECT_TRUE(docs.empty());
    EXPECT_EQ(fresh_profile.phrase_query_stats.common_grams_authoritative_empty, 1U);
}

TEST(SniiCommonGramsNamespaceQuery, ExactPlannerMatchesPlainAtRequiredPhraseLengths) {
    const std::vector<size_t> lengths = {1, 2, 3, 6, 10};
    std::vector<std::vector<std::string>> corpus;
    corpus.reserve(lengths.size());
    for (size_t length : lengths) {
        corpus.emplace_back(length, "the");
    }
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus), complete_metadata()));
    const auto identity = complete_query_identity();

    using Kind = segment_v2::TermKeyKind;
    const std::string the_the = gram_key("the", "the");
    for (size_t length : lengths) {
        const std::vector<std::string> plain_terms(length, "the");
        std::vector<std::pair<Kind, std::string>> gram_terms;
        if (length == 1) {
            gram_terms.emplace_back(Kind::kPlain, "the");
        } else {
            gram_terms.assign(length - 1, {Kind::kCommonGram, the_the});
        }

        std::vector<uint32_t> plain_docs;
        assert_ok(phrase_query(fixture.index_reader, plain_terms, &plain_docs));
        std::vector<uint32_t> planned_docs;
        assert_ok(planned_exact_phrase_query(fixture.index_reader, plain_query_info(plain_terms),
                                             query_info(gram_terms), &identity, &planned_docs));
        EXPECT_EQ(planned_docs, plain_docs) << "length " << length;
    }
}

TEST(SniiCommonGramsNamespaceQuery, CompleteCoverageGramMissIsAuthoritativeEmpty) {
    const std::vector<std::vector<std::string>> corpus = {{"the", "wolf"}};
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus, /*include_grams=*/false),
                            complete_metadata()));
    const auto identity = complete_query_identity();

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "wolf"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "wolf")}});
    ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &docs,
                                         nullptr, &selected_plan, /*cost_model=*/ {}));

    EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
    EXPECT_TRUE(docs.empty());
}

TEST(SniiCommonGramsNamespaceQuery, CompleteCoverageChecksGramMissBeforeCostGate) {
    using Kind = segment_v2::TermKeyKind;
    const std::string the_of = gram_key("the", "of");
    Fixture fixture;
    assert_ok(build_fixture(&fixture,
                            {make_term("the", {{.docid = 0, .positions = {0}}}),
                             make_term("of", {{.docid = 0, .positions = {1}}}),
                             make_term("wolf", {{.docid = 0, .positions = {2}}}),
                             make_term(the_of, {{.docid = 0, .positions = {0}},
                                                {.docid = 1, .positions = {0}},
                                                {.docid = 2, .positions = {0}},
                                                {.docid = 3, .positions = {0}},
                                                {.docid = 4, .positions = {0}},
                                                {.docid = 5, .positions = {0}},
                                                {.docid = 6, .positions = {0}},
                                                {.docid = 7, .positions = {0}}})},
                            complete_metadata()));
    const auto plain = plain_query_info({"the", "of", "wolf"});
    const auto gram =
            query_info({{Kind::kCommonGram, the_of}, {Kind::kCommonGram, gram_key("of", "wolf")}});
    const auto identity = complete_query_identity();

    ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &docs,
                                         nullptr, &selected_plan));

    EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kCommonGrams);
    EXPECT_TRUE(docs.empty());
}

TEST(SniiCommonGramsNamespaceQuery, CompleteCoveragePlainMissIsAuthoritativeEmpty) {
    using Kind = segment_v2::TermKeyKind;
    const std::string the_wolf = gram_key("the", "wolf");
    Fixture fixture;
    assert_ok(build_fixture(&fixture,
                            {make_term("the", {{.docid = 0, .positions = {0}},
                                               {.docid = 1, .positions = {0}},
                                               {.docid = 2, .positions = {0}},
                                               {.docid = 3, .positions = {0}},
                                               {.docid = 4, .positions = {0}},
                                               {.docid = 5, .positions = {0}},
                                               {.docid = 6, .positions = {0}},
                                               {.docid = 7, .positions = {0}}}),
                             make_term(the_wolf, {{.docid = 0, .positions = {0}}})},
                            complete_metadata()));
    const auto plain = plain_query_info({"the", "wolf"});
    const auto gram = query_info({{Kind::kCommonGram, the_wolf}});
    const auto identity = complete_query_identity();

    QueryProfile fresh_profile;
    ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kCommonGrams;
    std::vector<uint32_t> docs;
    assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &docs,
                                         &fresh_profile, &selected_plan, /*cost_model=*/ {}));

    EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kPlain);
    EXPECT_TRUE(docs.empty());
    EXPECT_EQ(fresh_profile.phrase_query_stats.common_grams_plain_plans, 1U);
    EXPECT_EQ(fresh_profile.phrase_query_stats.common_grams_authoritative_empty, 1U);
    EXPECT_GT(fresh_profile.phrase_query_stats.common_grams_planning_ns, 0U);
}

TEST(SniiCommonGramsNamespaceQuery, OldAndIncompleteCoverageForcePlainPlan) {
    const std::vector<std::vector<std::string>> corpus = {{"the", "wolf"}};
    const auto terms = build_common_grams_terms(corpus, /*include_grams=*/false);
    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "wolf"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "wolf")}});
    const auto identity = complete_query_identity();

    for (const auto& metadata : {std::optional<CommonGramsSegmentMetadata> {},
                                 std::optional<CommonGramsSegmentMetadata> {
                                         metadata_for(PlainTermKeyVersion::kEscapedV1)}}) {
        Fixture fixture;
        assert_ok(build_fixture(&fixture, terms, metadata));
        ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kCommonGrams;
        std::vector<uint32_t> docs;
        assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, &identity, &docs,
                                             nullptr, &selected_plan));

        EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kPlain);
        EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
    }
}

TEST(SniiCommonGramsNamespaceQuery, MismatchedOrMissingQueryIdentityForcesPlainPlan) {
    const std::vector<std::vector<std::string>> corpus = {{"the", "wolf"}};
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus, /*include_grams=*/false),
                            complete_metadata()));

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "wolf"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "wolf")}});
    auto mismatched = complete_query_identity();
    mismatched.common_grams_fingerprint = "different-common-grams-policy";
    const CommonGramsQueryIdentity* identities[] = {nullptr, &mismatched};
    for (const CommonGramsQueryIdentity* identity : identities) {
        ExactPhrasePlanKind selected_plan = ExactPhrasePlanKind::kCommonGrams;
        std::vector<uint32_t> docs;
        assert_ok(planned_exact_phrase_query(fixture.index_reader, plain, gram, identity, &docs,
                                             nullptr, &selected_plan));

        EXPECT_EQ(selected_plan, ExactPhrasePlanKind::kPlain);
        EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
    }
}

TEST(SniiCommonGramsNamespaceQuery, PhrasePrefixPlannerMapsFrozenTailTerms) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "wolf"}, {"the", "woman"}, {"the", "gap", "wolf"}, {"alpha", "wolf"}};
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus), complete_metadata()));
    const auto identity = complete_query_identity();

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "wo"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "wo")}});
    QueryProfile profile;
    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain, gram, &identity, &docs,
                                          &profile, /*max_expansions=*/50, &selected_plan));

    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_EQ(docs, (std::vector<uint32_t> {0, 1}));
    EXPECT_GT(profile.phrase_query_stats.common_grams_planning_ns, 0U);
}

TEST(SniiCommonGramsNamespaceQuery,
     HybridPrefixUnionsDocsOnlyMappedTailsBeforePlainVerificationFreshAndCached) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "of", "wolf"},
            {"the", "of", "woman"},
            {"the", "of", "gap", "of", "wolf"},
            {"the", "of", "gap", "of", "woman"},
            {"the", "gap", "of", "wolf"},
    };
    Fixture fixture;
    assert_ok(build_fixture(
            &fixture,
            build_common_grams_terms(corpus, /*include_grams=*/true, TestGramPostingShape::kHybrid),
            hybrid_metadata(), format::CommonGramsPostingPolicy::kHybridV1));
    const auto identity = complete_query_identity();

    const std::string the_of = gram_key("the", "of");
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, the_of)));
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, gram_key("of", "wolf"))));
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, gram_key("of", "woman"))));

    struct PrefixCase {
        std::string prefix;
        std::vector<uint32_t> present_gram_tail_ordinals;
        std::vector<uint32_t> expected_docs;
    };
    const std::vector<PrefixCase> cases = {
            {.prefix = "wo", .present_gram_tail_ordinals = {0, 1}, .expected_docs = {0, 1}},
            {.prefix = "wol", .present_gram_tail_ordinals = {0}, .expected_docs = {0}},
    };

    using Kind = segment_v2::TermKeyKind;
    for (const PrefixCase& test_case : cases) {
        SCOPED_TRACE(test_case.prefix);
        const auto plain = plain_query_info({"the", "of", test_case.prefix});
        const auto gram = query_info({{Kind::kCommonGram, the_of},
                                      {Kind::kCommonGram, gram_key("of", test_case.prefix)}});
        std::vector<uint32_t> expected;
        assert_ok(phrase_prefix_query(fixture.index_reader, {"the", "of", test_case.prefix},
                                      &expected, /*max_expansions=*/50));
        ASSERT_EQ(expected, test_case.expected_docs);

        PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kCommonGrams;
        std::vector<uint32_t> cost_selected_docs;
        assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain, gram, &identity,
                                              &cost_selected_docs, nullptr, /*max_expansions=*/50,
                                              &selected_plan, no_hysteresis_cost_model()));
        EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
        EXPECT_EQ(cost_selected_docs, expected);

        selected_plan = PhrasePrefixPlanKind::kPlain;
        std::vector<uint32_t> fresh_docs;
        assert_ok(planned_phrase_prefix_query(
                fixture.index_reader, plain, gram, &identity, &fresh_docs, nullptr,
                /*max_expansions=*/50, &selected_plan, no_hysteresis_cost_model(),
                CommonGramsPlanDebugOverride::kForceCommonGrams));
        EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
        EXPECT_EQ(fresh_docs, expected);
    }
}

TEST(SniiCommonGramsNamespaceQuery,
     HybridPrefixPositionedTailUsesSparseLogicalOffsetFreshAndCached) {
    const std::vector<std::vector<std::string>> corpus = {{"one", "of", "the", "in", "the"}};
    Fixture fixture;
    assert_ok(build_fixture(
            &fixture,
            build_common_grams_terms(corpus, /*include_grams=*/true, TestGramPostingShape::kHybrid),
            hybrid_metadata(), format::CommonGramsPostingPolicy::kHybridV1));
    const auto identity = complete_query_identity();

    const std::string one_of = gram_key("one", "of");
    const std::string of_the = gram_key("of", "the");
    const std::string the_in = gram_key("the", "in");
    const std::string in_the = gram_key("in", "the");
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, one_of)));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, of_the)));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, the_in)));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, in_the)));

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"one", "of", "the", "in", "th"});
    const auto gram = query_info({{Kind::kCommonGram, one_of},
                                  {Kind::kCommonGram, of_the},
                                  {Kind::kCommonGram, the_in},
                                  {Kind::kCommonGram, gram_key("in", "th")}});
    std::vector<uint32_t> expected;
    assert_ok(phrase_prefix_query(fixture.index_reader, {"one", "of", "the", "in", "th"}, &expected,
                                  /*max_expansions=*/50));
    ASSERT_EQ(expected, (std::vector<uint32_t> {0}));

    QueryProfile fresh_profile;
    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> fresh_docs;
    assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain, gram, &identity, &fresh_docs,
                                          &fresh_profile, /*max_expansions=*/50, &selected_plan,
                                          verification_dominated_cost_model()));
    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_EQ(fresh_docs, expected);
    EXPECT_EQ(fresh_profile.prx_decode_stats.selected_positions, 3U);
}

TEST(SniiCommonGramsNamespaceQuery,
     HybridPrefixMultiplePositionedTailsUseSparseCoverFreshAndCached) {
    const std::vector<std::vector<std::string>> corpus = {
            {"one", "of", "the", "in", "the"},
            {"one", "of", "the", "in", "this"},
    };
    Fixture fixture;
    assert_ok(build_fixture(
            &fixture,
            build_common_grams_terms(corpus, /*include_grams=*/true, TestGramPostingShape::kHybrid),
            hybrid_metadata(), format::CommonGramsPostingPolicy::kHybridV1));
    const auto identity = complete_query_identity();

    const std::string one_of = gram_key("one", "of");
    const std::string of_the = gram_key("of", "the");
    const std::string the_in = gram_key("the", "in");
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, one_of)));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, of_the)));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, the_in)));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, gram_key("in", "the"))));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, gram_key("in", "this"))));

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"one", "of", "the", "in", "th"});
    const auto gram = query_info({{Kind::kCommonGram, one_of},
                                  {Kind::kCommonGram, of_the},
                                  {Kind::kCommonGram, the_in},
                                  {Kind::kCommonGram, gram_key("in", "th")}});
    std::vector<uint32_t> expected;
    assert_ok(phrase_prefix_query(fixture.index_reader, {"one", "of", "the", "in", "th"}, &expected,
                                  /*max_expansions=*/50));
    ASSERT_EQ(expected, (std::vector<uint32_t> {0, 1}));

    QueryProfile fresh_profile;
    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> fresh_docs;
    assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain, gram, &identity, &fresh_docs,
                                          &fresh_profile, /*max_expansions=*/50, &selected_plan,
                                          verification_dominated_cost_model()));
    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_EQ(fresh_docs, expected);
    EXPECT_EQ(fresh_profile.prx_decode_stats.selected_positions, 6U);
}

TEST(SniiCommonGramsNamespaceQuery,
     HybridPrefixSplitsPositionedAndDocsOnlyMappedTailsFreshAndCached) {
    const std::vector<std::vector<std::string>> corpus = {
            {"one", "of", "the", "in", "the"},
            {"one", "of", "the", "in", "thing"},
    };
    Fixture fixture;
    assert_ok(build_fixture(
            &fixture,
            build_common_grams_terms(corpus, /*include_grams=*/true, TestGramPostingShape::kHybrid),
            hybrid_metadata(), format::CommonGramsPostingPolicy::kHybridV1));
    const auto identity = complete_query_identity();

    const std::string one_of = gram_key("one", "of");
    const std::string of_the = gram_key("of", "the");
    const std::string the_in = gram_key("the", "in");
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, one_of)));
    EXPECT_TRUE(entry_has_prx(find_entry(fixture.index_reader, gram_key("in", "the"))));
    EXPECT_FALSE(entry_has_prx(find_entry(fixture.index_reader, gram_key("in", "thing"))));

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"one", "of", "the", "in", "t"});
    const auto gram = query_info({{Kind::kCommonGram, one_of},
                                  {Kind::kCommonGram, of_the},
                                  {Kind::kCommonGram, the_in},
                                  {Kind::kCommonGram, gram_key("in", "t")}});
    std::vector<uint32_t> expected;
    assert_ok(phrase_prefix_query(fixture.index_reader, {"one", "of", "the", "in", "t"}, &expected,
                                  /*max_expansions=*/50));
    ASSERT_EQ(expected, (std::vector<uint32_t> {0, 1}));

    QueryProfile fresh_profile;
    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> fresh_docs;
    assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain, gram, &identity, &fresh_docs,
                                          &fresh_profile, /*max_expansions=*/50, &selected_plan,
                                          verification_dominated_cost_model()));
    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_EQ(fresh_docs, expected);
    EXPECT_EQ(fresh_profile.prx_decode_stats.selected_positions, 7U);
}

TEST(SniiCommonGramsNamespaceQuery,
     HybridPrefixEmptyCandidatesAreAuthoritativeForFreshAndCachedPlans) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "of", "gap"},
            {"x", "of", "wolf"},
            {"x", "of", "woman"},
            {"the", "x", "of", "x", "wolf", "woman"},
            {"the", "x", "of", "x", "wolf", "woman"},
            {"the", "x", "of", "x", "wolf", "woman"},
            {"the", "x", "of", "x", "wolf", "woman"},
            {"the", "x", "of", "x", "wolf", "woman"},
    };
    Fixture fixture;
    assert_ok(build_fixture(
            &fixture,
            build_common_grams_terms(corpus, /*include_grams=*/true, TestGramPostingShape::kHybrid),
            hybrid_metadata(), format::CommonGramsPostingPolicy::kHybridV1));
    const auto identity = complete_query_identity();

    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "of", "wo"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "of")},
                                  {Kind::kCommonGram, gram_key("of", "wo")}});

    QueryProfile fresh_profile;
    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain, gram, &identity, &docs,
                                          &fresh_profile, /*max_expansions=*/50, &selected_plan,
                                          verification_dominated_cost_model()));
    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_TRUE(docs.empty());
    EXPECT_EQ(fresh_profile.phrase_query_stats.common_grams_authoritative_empty, 1U);
}

TEST(SniiCommonGramsNamespaceQuery, PhrasePrefixPlannerDropsOnlyMissingMappedTails) {
    const std::vector<std::vector<std::string>> corpus = {{"the", "wolf"}, {"the", "woman"}};
    auto terms = build_common_grams_terms(corpus);
    const std::string missing_gram = gram_key("the", "woman");
    std::erase_if(terms,
                  [&](const writer::TermPostings& term) { return term.term == missing_gram; });

    Fixture fixture;
    assert_ok(build_fixture(&fixture, std::move(terms), complete_metadata()));
    const auto identity = complete_query_identity();
    using Kind = segment_v2::TermKeyKind;

    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain_query_info({"the", "wo"}),
                                          query_info({{Kind::kCommonGram, gram_key("the", "wo")}}),
                                          &identity, &docs, nullptr,
                                          /*max_expansions=*/50, &selected_plan));

    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
}

TEST(SniiCommonGramsNamespaceQuery, PhrasePrefixPlannerTreatsAllMappedTailMissesAsEmpty) {
    const std::vector<std::vector<std::string>> corpus = {{"the", "wolf"}, {"the", "woman"}};
    auto terms = build_common_grams_terms(corpus);
    const std::string wolf_gram = gram_key("the", "wolf");
    const std::string woman_gram = gram_key("the", "woman");
    std::erase_if(terms, [&](const writer::TermPostings& term) {
        return term.term == wolf_gram || term.term == woman_gram;
    });

    Fixture fixture;
    assert_ok(build_fixture(&fixture, std::move(terms), complete_metadata()));
    const auto identity = complete_query_identity();
    using Kind = segment_v2::TermKeyKind;

    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain_query_info({"the", "wo"}),
                                          query_info({{Kind::kCommonGram, gram_key("the", "wo")}}),
                                          &identity, &docs, nullptr,
                                          /*max_expansions=*/50, &selected_plan));

    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_TRUE(docs.empty());
}

TEST(SniiCommonGramsNamespaceQuery, PhrasePrefixRepeatedGramBuildsOneTermPlan) {
    const std::vector<std::vector<std::string>> corpus = {
            {"the", "the", "the"}, {"the", "the", "thing"}, {"the", "gap", "the"}};
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus), complete_metadata()));
    const auto identity = complete_query_identity();
    using Kind = segment_v2::TermKeyKind;
    const std::string the_the = gram_key("the", "the");

    internal::query_test_counters() = {};
    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_phrase_prefix_query(
            fixture.index_reader, plain_query_info({"the", "the", "th"}),
            query_info({{Kind::kCommonGram, the_the}, {Kind::kCommonGram, gram_key("the", "th")}}),
            &identity, &docs, nullptr, /*max_expansions=*/1, &selected_plan,
            no_hysteresis_cost_model()));

    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
    EXPECT_EQ(internal::query_test_counters().resolved_term_entry_moves, 1U);
}

TEST(SniiCommonGramsNamespaceQuery, PhrasePrefixPlannerReusesExistingMatcherShapes) {
    const std::vector<std::vector<std::string>> corpus = {
            {"foo", "of", "theta"},        {"foo", "of", "thing"},
            {"foo", "gap", "of", "theta"}, {"foo", "of", "gap", "theta"},
            {"the", "bar", "baz"},         {"the", "bar", "batch"},
            {"the", "gap", "bar", "baz"},  {"foo", "theater", "the", "x", "bar", "bask"},
    };
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus), complete_metadata()));
    const auto identity = complete_query_identity();
    using Kind = segment_v2::TermKeyKind;

    expect_planned_prefix_matches_plain(fixture.index_reader, plain_query_info({"foo", "of", "th"}),
                                        query_info({{Kind::kCommonGram, gram_key("foo", "of")},
                                                    {Kind::kCommonGram, gram_key("of", "th")}}),
                                        &identity, /*max_expansions=*/50,
                                        PhrasePrefixPlanKind::kCommonGrams);
    expect_planned_prefix_matches_plain(
            fixture.index_reader, plain_query_info({"the", "bar", "baz"}),
            query_info({{Kind::kCommonGram, gram_key("the", "bar")},
                        {Kind::kPlain, "bar"},
                        {Kind::kPlain, "baz"}}),
            &identity, /*max_expansions=*/50, PhrasePrefixPlanKind::kCommonGrams);

    // The prefix crosses the common/non-common boundary: "the" is common,
    // "theater" is not. The phrase-prefix analyzer therefore emits no gram.
    expect_planned_prefix_matches_plain(fixture.index_reader, plain_query_info({"foo", "the"}),
                                        plain_query_info({"foo", "the"}), &identity,
                                        /*max_expansions=*/50, PhrasePrefixPlanKind::kPlain);
}

TEST(SniiCommonGramsNamespaceQuery, PhrasePrefixPlannerMatchesRequiredPhraseLengthsAndCaps) {
    const std::vector<size_t> lengths = {1, 2, 3, 6, 10};
    std::vector<std::vector<std::string>> corpus;
    corpus.reserve(lengths.size());
    for (size_t length : lengths) {
        corpus.emplace_back(length, "the");
    }
    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus), complete_metadata()));
    const auto identity = complete_query_identity();
    using Kind = segment_v2::TermKeyKind;
    const std::string the_the = gram_key("the", "the");

    for (size_t length : lengths) {
        std::vector<std::string> plain_terms(length, "the");
        plain_terms.back() = "th";
        std::vector<std::pair<Kind, std::string>> gram_terms;
        if (length == 1) {
            gram_terms.emplace_back(Kind::kPlain, "th");
        } else {
            gram_terms.assign(length - 1, {Kind::kCommonGram, the_the});
            gram_terms.back().second = gram_key("the", "th");
        }
        expect_planned_prefix_matches_plain(
                fixture.index_reader, plain_query_info(plain_terms), query_info(gram_terms),
                &identity, /*max_expansions=*/50,
                length == 1 ? PhrasePrefixPlanKind::kPlain : PhrasePrefixPlanKind::kCommonGrams);
    }

    const auto plain = plain_query_info({"the", "th"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "th")}});
    for (int32_t cap : {1, 2, 32, 50, 64}) {
        expect_planned_prefix_matches_plain(fixture.index_reader, plain, gram, &identity, cap,
                                            PhrasePrefixPlanKind::kCommonGrams);
    }
}

TEST(SniiCommonGramsNamespaceQuery, PhrasePrefixGramPlanEmitsMatchesBeforeEmptyFinalTailGroup) {
    std::vector<std::vector<std::string>> corpus(8);
    corpus[0] = {"foo", "of", "aa_000"};
    for (size_t i = 1; i < 64; ++i) {
        corpus[1].push_back("of");
        corpus[1].push_back(fmt::format("aa_{:03}", i));
    }
    for (size_t docid = 2; docid < corpus.size(); ++docid) {
        corpus[docid] = {"foo", "gap", "of", fmt::format("filler_{}", docid)};
    }

    Fixture fixture;
    assert_ok(build_fixture(&fixture, build_common_grams_terms(corpus), complete_metadata()));
    const auto identity = complete_query_identity();
    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"foo", "of", "aa_"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("foo", "of")},
                                  {Kind::kCommonGram, gram_key("of", "aa_")}});
    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kPlain;
    std::vector<uint32_t> docs;
    assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain, gram, &identity, &docs,
                                          nullptr, /*max_expansions=*/64, &selected_plan,
                                          no_hysteresis_cost_model()));

    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));
}

TEST(SniiCommonGramsNamespaceQuery, PhrasePrefixPlannerFallsBackForUnencodableMappedTail) {
    const std::string long_tail(segment_v2::inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES - 1, 'x');
    Fixture fixture;
    assert_ok(build_fixture(&fixture,
                            {make_term(encoded_plain("the", PlainTermKeyVersion::kEscapedV1),
                                       {{.docid = 0, .positions = {0}}}),
                             make_term(encoded_plain(long_tail, PlainTermKeyVersion::kEscapedV1),
                                       {{.docid = 0, .positions = {1}}})},
                            complete_metadata()));
    const auto identity = complete_query_identity();
    using Kind = segment_v2::TermKeyKind;
    const auto plain = plain_query_info({"the", "x"});
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "x")}});
    std::vector<uint32_t> expected;
    assert_ok(phrase_prefix_query(fixture.index_reader, {"the", "x"}, &expected,
                                  /*max_expansions=*/50));
    PhrasePrefixPlanKind selected_plan = PhrasePrefixPlanKind::kCommonGrams;
    std::vector<uint32_t> actual;
    assert_ok(planned_phrase_prefix_query(fixture.index_reader, plain, gram, &identity, &actual,
                                          nullptr, /*max_expansions=*/50, &selected_plan,
                                          /*cost_model=*/ {}));

    EXPECT_EQ(selected_plan, PhrasePrefixPlanKind::kPlain);
    EXPECT_EQ(actual, expected);
}

TEST(SniiCommonGramsNamespaceQuery, PhrasePrefixPlannerRequiresCompatibleCompleteCoverage) {
    const std::vector<std::vector<std::string>> corpus = {{"the", "wolf"}};
    const auto terms = build_common_grams_terms(corpus, /*include_grams=*/false);
    const auto plain = plain_query_info({"the", "wo"});
    using Kind = segment_v2::TermKeyKind;
    const auto gram = query_info({{Kind::kCommonGram, gram_key("the", "wo")}});
    const auto identity = complete_query_identity();

    for (const auto& metadata : {std::optional<CommonGramsSegmentMetadata> {},
                                 std::optional<CommonGramsSegmentMetadata> {
                                         metadata_for(PlainTermKeyVersion::kEscapedV1)}}) {
        Fixture fixture;
        assert_ok(build_fixture(&fixture, terms, metadata));
        expect_planned_prefix_matches_plain(fixture.index_reader, plain, gram, &identity,
                                            /*max_expansions=*/50, PhrasePrefixPlanKind::kPlain);
    }

    Fixture fixture;
    assert_ok(build_fixture(&fixture, terms, complete_metadata()));
    auto mismatched = identity;
    mismatched.common_grams_fingerprint = "different-common-grams-policy";
    expect_planned_prefix_matches_plain(fixture.index_reader, plain, gram, nullptr,
                                        /*max_expansions=*/50, PhrasePrefixPlanKind::kPlain);
    expect_planned_prefix_matches_plain(fixture.index_reader, plain, gram, &mismatched,
                                        /*max_expansions=*/50, PhrasePrefixPlanKind::kPlain);
}

TEST(SniiCommonGramsNamespaceQuery, DebugForceGramDoesNotBypassPlanEligibility) {
    const bool original_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add("snii.common_grams.force_gram_plan");
    Defer restore_debug_points([original_enable_debug_points] {
        DebugPoints::instance()->remove("snii.common_grams.force_gram_plan");
        config::enable_debug_points = original_enable_debug_points;
    });

    const std::vector<std::vector<std::string>> corpus = {{"the", "wolf"}};
    const auto identity = complete_query_identity();
    using Kind = segment_v2::TermKeyKind;
    const auto exact_plain = plain_query_info({"the", "wolf"});
    const auto exact_gram = query_info({{Kind::kCommonGram, gram_key("the", "wolf")}});

    Fixture incomplete_fixture;
    assert_ok(build_fixture(&incomplete_fixture, build_common_grams_terms(corpus),
                            metadata_for(PlainTermKeyVersion::kEscapedV1)));
    ExactPhrasePlanKind exact_plan = ExactPhrasePlanKind::kCommonGrams;
    std::vector<uint32_t> docs;
    assert_ok(planned_exact_phrase_query(incomplete_fixture.index_reader, exact_plain, exact_gram,
                                         &identity, &docs, nullptr, &exact_plan));
    EXPECT_EQ(exact_plan, ExactPhrasePlanKind::kPlain);
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));

    Fixture missing_gram_fixture;
    assert_ok(build_fixture(&missing_gram_fixture,
                            build_common_grams_terms(corpus, /*include_grams=*/false),
                            complete_metadata()));
    exact_plan = ExactPhrasePlanKind::kPlain;
    docs.clear();
    assert_ok(planned_exact_phrase_query(missing_gram_fixture.index_reader, exact_plain, exact_gram,
                                         &identity, &docs, nullptr, &exact_plan,
                                         /*cost_model=*/ {}));
    EXPECT_TRUE(docs.empty());

    PhrasePrefixPlanKind prefix_plan = PhrasePrefixPlanKind::kCommonGrams;
    docs.clear();
    assert_ok(planned_phrase_prefix_query(
            incomplete_fixture.index_reader, plain_query_info({"the", "wo"}),
            query_info({{Kind::kCommonGram, gram_key("the", "wo")}}), &identity, &docs, nullptr,
            /*max_expansions=*/50, &prefix_plan));
    EXPECT_EQ(prefix_plan, PhrasePrefixPlanKind::kPlain);
    EXPECT_EQ(docs, (std::vector<uint32_t> {0}));

    prefix_plan = PhrasePrefixPlanKind::kPlain;
    docs.clear();
    assert_ok(planned_phrase_prefix_query(
            missing_gram_fixture.index_reader, plain_query_info({"the", "wo"}),
            query_info({{Kind::kCommonGram, gram_key("the", "wo")}}), &identity, &docs, nullptr,
            /*max_expansions=*/50, &prefix_plan, /*cost_model=*/ {}));
    EXPECT_EQ(prefix_plan, PhrasePrefixPlanKind::kCommonGrams);
    EXPECT_TRUE(docs.empty());
}

} // namespace
} // namespace doris::snii::query
