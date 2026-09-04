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

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "storage/index/inverted/analyzer/analyzer_provider.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/compaction/eligibility.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"
#include "storage/tablet/tablet_schema.h"

namespace {

namespace ErrorCode = doris::ErrorCode;
using doris::Status;
using doris::TabletIndex;
using doris::TabletIndexPB;
using namespace doris::snii; // NOLINT
namespace inverted_index = doris::segment_v2::inverted_index;

class BufferFileReader final : public io::FileReader {
public:
    explicit BufferFileReader(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        if (offset > bytes_.size() || len > bytes_.size() - offset) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "eligibility fixture: read past EOF");
        }
        out->resize(len);
        if (len != 0) {
            std::memcpy(out->data(), bytes_.data() + offset, len);
        }
        return Status::OK();
    }

    uint64_t size() const override { return bytes_.size(); }

private:
    std::vector<uint8_t> bytes_;
};

struct IndexShape {
    format::IndexTier tier = format::IndexTier::kT2;
    bool has_norms = false;
    bool has_common_grams_metadata = false;
    std::optional<inverted_index::CommonGramsSegmentMetadata> common_grams_metadata = std::nullopt;
    format::CommonGramsPostingPolicy common_grams_posting_policy =
            format::CommonGramsPostingPolicy::kNone;
    format::StatsBlock stats {
            .doc_count = 8,
            .indexed_doc_count = 6,
            .term_count = 0,
            .sum_total_term_freq = 0,
            .null_count = 2,
    };
};

struct OpenedIndex {
    std::unique_ptr<BufferFileReader> file;
    reader::LogicalIndexReader reader;
};

std::unique_ptr<OpenedIndex> open_index(const IndexShape& shape) {
    std::vector<uint8_t> file_bytes;
    format::SectionRefs refs;
    if (shape.has_norms || shape.tier == format::IndexTier::kT3) {
        format::NormsPodWriter norms;
        for (uint64_t doc = 0; doc < shape.stats.doc_count; ++doc) {
            norms.add(1);
        }
        ByteSink norms_frame;
        norms.finish(&norms_frame);
        file_bytes = norms_frame.buffer();
        refs.norms = {.offset = 0, .length = file_bytes.size()};
    }

    ByteSink sampled_frame;
    format::SampledTermIndexBuilder sampled;
    sampled.finish(&sampled_frame);
    ByteSink directory_frame;
    format::DictBlockDirectoryBuilder directory;
    directory.finish(&directory_frame);

    format::CoreMetadata core;
    core.index_config = shape.tier == format::IndexTier::kT1 ? format::IndexConfig::kDocsOnly
                        : shape.tier == format::IndexTier::kT2
                                ? format::IndexConfig::kDocsPositions
                                : format::IndexConfig::kDocsPositionsScoring;
    core.stats = shape.stats;
    core.section_refs = refs;
    if (shape.common_grams_metadata.has_value()) {
        core.common_grams_metadata = *shape.common_grams_metadata;
    } else if (shape.has_common_grams_metadata) {
        inverted_index::CommonGramsSegmentMetadata metadata;
        metadata.plain_term_key_version = inverted_index::PlainTermKeyVersion::kRawNoInternal;
        core.common_grams_metadata = metadata;
    }
    if (shape.common_grams_posting_policy != format::CommonGramsPostingPolicy::kNone) {
        core.common_grams_posting_policy = shape.common_grams_posting_policy;
    }
    ByteSink core_frame;
    const Status core_status = format::encode_core_metadata(core, &core_frame);
    EXPECT_TRUE(core_status.ok()) << core_status.to_string();

    auto opened = std::make_unique<OpenedIndex>();
    opened->file = std::make_unique<BufferFileReader>(std::move(file_bytes));
    const Status open_status = reader::LogicalIndexReader::open(
            opened->file.get(), core_frame.view(), sampled_frame.view(), directory_frame.view(),
            &opened->reader);
    EXPECT_TRUE(open_status.ok()) << open_status.to_string();
    return opened;
}

std::unique_ptr<TabletIndex> make_index(std::map<std::string, std::string> properties,
                                        doris::IndexType type = doris::IndexType::INVERTED,
                                        int64_t index_id = 7, std::string index_suffix = "body") {
    TabletIndexPB pb;
    pb.set_index_id(index_id);
    pb.set_index_name("body_idx");
    pb.set_index_type(type);
    pb.set_index_suffix_name(std::move(index_suffix));
    for (auto& [key, value] : properties) {
        (*pb.mutable_properties())[key] = std::move(value);
    }
    auto index = std::make_unique<TabletIndex>();
    index->init_from_pb(pb);
    return index;
}

std::map<std::string, std::string> plain_properties() {
    return {{"lower_case", "true"}, {"parser", "standard"}, {"support_phrase", "true"}};
}

std::map<std::string, std::string> common_grams_properties() {
    return {{"analyzer", "common_grams_analyzer"}, {"support_phrase", "true"}};
}

inverted_index::CommonGramsQueryIdentity common_grams_identity(std::string dictionary = "dict-a") {
    return {.common_grams_dictionary_identity = std::move(dictionary),
            .base_analyzer_fingerprint = "base-a",
            .common_grams_fingerprint = "grams-a"};
}

inverted_index::CommonGramsSegmentMetadata complete_common_grams_metadata(
        const inverted_index::CommonGramsQueryIdentity& identity = common_grams_identity()) {
    auto metadata = inverted_index::make_common_grams_segment_metadata(identity);
    metadata.scoring_doc_count = 8;
    metadata.scoring_token_count = 0;
    return metadata;
}

inverted_index::CommonGramsSegmentMetadata hybrid_common_grams_metadata(
        const inverted_index::CommonGramsQueryIdentity& identity = common_grams_identity()) {
    auto metadata = complete_common_grams_metadata(identity);
    metadata.common_grams_coverage = inverted_index::CommonGramsCoverage::kMixed;
    return metadata;
}

compaction::PlainT2CompactionSource source(const OpenedIndex& index,
                                           const TabletIndex& index_meta) {
    return {.reader = std::cref(index.reader), .index_meta = std::cref(index_meta)};
}

void expect_rejected(const Status& status, std::string_view reason) {
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>()) << status.to_string();
    EXPECT_NE(status.to_string().find(reason), std::string::npos) << status.to_string();
}

class StubAnalyzerProvider final : public inverted_index::AnalyzerProvider {
public:
    explicit StubAnalyzerProvider(
            bool uses_common_grams,
            std::optional<inverted_index::CommonGramsQueryIdentity> identity = std::nullopt)
            : uses_common_grams_(uses_common_grams), identity_(std::move(identity)) {}

    std::shared_ptr<lucene::analysis::Analyzer> get_analyzer(
            inverted_index::AnalysisPurpose) const override {
        return nullptr;
    }
    bool uses_common_grams() const override { return uses_common_grams_; }
    const inverted_index::CommonGramsQueryIdentity* common_grams_identity() const override {
        return identity_ ? &*identity_ : nullptr;
    }

private:
    bool uses_common_grams_ = false;
    std::optional<inverted_index::CommonGramsQueryIdentity> identity_;
};

class CommonGramsBuildSwitchGuard {
public:
    CommonGramsBuildSwitchGuard() : saved_(doris::config::enable_common_grams_index_build) {}
    ~CommonGramsBuildSwitchGuard() { doris::config::enable_common_grams_index_build = saved_; }

private:
    bool saved_;
};

TEST(SniiCompactionEligibilityTest, AcceptsIdenticalPlainT2SourcesAndDestination) {
    auto first = open_index({});
    auto second = open_index({});
    auto first_meta = make_index(plain_properties());
    auto second_meta = make_index(plain_properties());
    auto destination = make_index(plain_properties());
    std::vector sources {source(*first, *first_meta), source(*second, *second_meta)};

    EXPECT_TRUE(compaction::validate_plain_t2_compaction_eligibility(sources, *destination).ok());
}

TEST(SniiCompactionEligibilityTest, AcceptsHomogeneousCompleteCommonGramsT3Sources) {
    CommonGramsBuildSwitchGuard guard;
    doris::config::enable_common_grams_index_build = true;
    const auto identity = common_grams_identity();
    IndexShape shape {.tier = format::IndexTier::kT3,
                      .has_norms = true,
                      .common_grams_metadata = complete_common_grams_metadata(identity)};
    auto first = open_index(shape);
    auto second = open_index(shape);
    auto first_meta = make_index(common_grams_properties());
    auto second_meta = make_index(common_grams_properties());
    auto destination = make_index(common_grams_properties());
    std::vector sources {source(*first, *first_meta), source(*second, *second_meta)};
    compaction::AnalyzerProviderFactory factory = [identity](const auto&) {
        return std::make_shared<StubAnalyzerProvider>(true, identity);
    };

    compaction::SniiCompactionEligibility eligibility;
    ASSERT_TRUE(compaction::validate_snii_compaction_eligibility(sources, *destination,
                                                                 &eligibility, factory)
                        .ok());
    EXPECT_EQ(eligibility.kind, compaction::SniiStreamedMergeKind::kCommonGramsT3);
    ASSERT_TRUE(eligibility.common_grams_metadata_seed.has_value());
    EXPECT_TRUE(inverted_index::common_grams_identity_matches(
            *eligibility.common_grams_metadata_seed, identity));
    EXPECT_EQ(eligibility.common_grams_metadata_seed->scoring_doc_count, 0U);
    EXPECT_EQ(eligibility.common_grams_metadata_seed->scoring_token_count, 0U);
    EXPECT_EQ(eligibility.common_grams_posting_policy, format::CommonGramsPostingPolicy::kNone);
}

TEST(SniiCompactionEligibilityTest, AcceptsHomogeneousHybridCommonGramsT3Sources) {
    CommonGramsBuildSwitchGuard guard;
    doris::config::enable_common_grams_index_build = true;
    const auto identity = common_grams_identity();
    IndexShape shape {.tier = format::IndexTier::kT3,
                      .has_norms = true,
                      .common_grams_metadata = hybrid_common_grams_metadata(identity),
                      .common_grams_posting_policy = format::CommonGramsPostingPolicy::kHybridV1};
    auto first = open_index(shape);
    auto second = open_index(shape);
    auto first_meta = make_index(common_grams_properties());
    auto second_meta = make_index(common_grams_properties());
    auto destination = make_index(common_grams_properties());
    const std::vector sources {source(*first, *first_meta), source(*second, *second_meta)};
    compaction::AnalyzerProviderFactory factory = [identity](const auto&) {
        return std::make_shared<StubAnalyzerProvider>(true, identity);
    };

    compaction::SniiCompactionEligibility eligibility;
    ASSERT_TRUE(compaction::validate_snii_compaction_eligibility(sources, *destination,
                                                                 &eligibility, factory)
                        .ok());
    EXPECT_EQ(eligibility.kind, compaction::SniiStreamedMergeKind::kCommonGramsT3);
    ASSERT_TRUE(eligibility.common_grams_metadata_seed.has_value());
    EXPECT_EQ(eligibility.common_grams_metadata_seed->common_grams_coverage,
              inverted_index::CommonGramsCoverage::kMixed);
    EXPECT_EQ(eligibility.common_grams_posting_policy, format::CommonGramsPostingPolicy::kHybridV1);
    EXPECT_TRUE(compaction::validate_snii_source_eligibility(first->reader,
                                                             /*source_ordinal=*/0, eligibility)
                        .ok());
}

TEST(SniiCompactionEligibilityTest, RejectsMixedMismatchedAndBuildDisabledCommonGramsSources) {
    CommonGramsBuildSwitchGuard guard;
    doris::config::enable_common_grams_index_build = true;
    const auto identity = common_grams_identity();
    IndexShape common_shape {.tier = format::IndexTier::kT3,
                             .has_norms = true,
                             .common_grams_metadata = complete_common_grams_metadata(identity)};
    auto common = open_index(common_shape);
    auto plain = open_index({});
    auto first_meta = make_index(common_grams_properties());
    auto second_meta = make_index(common_grams_properties());
    auto destination = make_index(common_grams_properties());
    compaction::AnalyzerProviderFactory factory = [identity](const auto&) {
        return std::make_shared<StubAnalyzerProvider>(true, identity);
    };
    compaction::SniiCompactionEligibility eligibility;

    std::vector mixed {source(*common, *first_meta), source(*plain, *second_meta)};
    expect_rejected(compaction::validate_snii_compaction_eligibility(mixed, *destination,
                                                                     &eligibility, factory),
                    "homogeneous");

    const auto other_identity = common_grams_identity("dict-b");
    common_shape.common_grams_metadata = complete_common_grams_metadata(other_identity);
    auto mismatched = open_index(common_shape);
    std::vector mismatch_sources {source(*common, *first_meta), source(*mismatched, *second_meta)};
    expect_rejected(compaction::validate_snii_compaction_eligibility(mismatch_sources, *destination,
                                                                     &eligibility, factory),
                    "identity");

    IndexShape hybrid_shape {
            .tier = format::IndexTier::kT3,
            .has_norms = true,
            .common_grams_metadata = hybrid_common_grams_metadata(identity),
            .common_grams_posting_policy = format::CommonGramsPostingPolicy::kHybridV1};
    auto hybrid = open_index(hybrid_shape);
    std::vector policy_mismatch_sources {source(*common, *first_meta),
                                         source(*hybrid, *second_meta)};
    expect_rejected(compaction::validate_snii_compaction_eligibility(
                            policy_mismatch_sources, *destination, &eligibility, factory),
                    "homogeneous");

    doris::config::enable_common_grams_index_build = false;
    std::vector disabled_sources {source(*common, *first_meta)};
    expect_rejected(compaction::validate_snii_compaction_eligibility(disabled_sources, *destination,
                                                                     &eligibility, factory),
                    "disabled");
}

TEST(SniiCompactionEligibilityTest, ExposesReusableSourceShapeValidation) {
    auto eligible = open_index({});
    EXPECT_TRUE(compaction::validate_plain_t2_source(eligible->reader, /*source_ordinal=*/3).ok());

    IndexShape invalid_shape;
    invalid_shape.stats.indexed_doc_count = 7;
    invalid_shape.stats.null_count = 2;
    auto invalid = open_index(invalid_shape);
    expect_rejected(compaction::validate_plain_t2_source(invalid->reader, /*source_ordinal=*/3),
                    "source 3");
}

TEST(SniiCompactionEligibilityTest, RejectsPhysicalShapesOutsidePlainT2) {
    struct Case {
        IndexShape shape;
        std::string_view reason;
    };
    std::vector<Case> cases;
    cases.push_back({IndexShape {.tier = format::IndexTier::kT1}, "T2"});
    cases.push_back({IndexShape {.has_norms = true}, "norms"});

    for (const auto& test_case : cases) {
        auto index = open_index(test_case.shape);
        auto source_meta = make_index(plain_properties());
        auto destination = make_index(plain_properties());
        std::vector sources {source(*index, *source_meta)};
        expect_rejected(compaction::validate_plain_t2_compaction_eligibility(sources, *destination),
                        test_case.reason);
    }
}

TEST(SniiCompactionEligibilityTest, RejectsCompanionMetadata) {
    auto index = open_index(IndexShape {.has_common_grams_metadata = true});
    auto source_meta = make_index(plain_properties());
    auto destination = make_index(plain_properties());
    std::vector sources {source(*index, *source_meta)};
    expect_rejected(compaction::validate_plain_t2_compaction_eligibility(sources, *destination),
                    "CommonGrams");
}

TEST(SniiCompactionEligibilityTest, RejectsLegacyBigramMarkerBeforeMergeExecution) {
    snii_test::MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 7;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 1;
    input.terms.push_back(
            snii_test::make_term(std::string(format::kPhraseBigramTermMarker) + "left\x1Fright",
                                 {{.docid = 0, .positions = {1}}}));
    writer::SniiCompoundWriter compound_writer(&file);
    ASSERT_TRUE(compound_writer.add_logical_index(input).ok());
    ASSERT_TRUE(compound_writer.finish().ok());
    reader::SniiSegmentReader segment;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&file, &segment).ok());
    reader::LogicalIndexReader index;
    ASSERT_TRUE(segment.open_index(7, "body", &index).ok());

    auto source_meta = make_index(plain_properties());
    auto destination = make_index(plain_properties());
    std::vector<compaction::PlainT2CompactionSource> sources {
            {.reader = std::cref(index), .index_meta = std::cref(*source_meta)}};
    expect_rejected(compaction::validate_plain_t2_compaction_eligibility(sources, *destination),
                    "legacy phrase-bigram");
}

TEST(SniiCompactionEligibilityTest, RejectsBrokenDocumentStatistics) {
    IndexShape shape;
    shape.stats.indexed_doc_count = 7;
    shape.stats.null_count = 2;
    auto index = open_index(shape);
    auto source_meta = make_index(plain_properties());
    auto destination = make_index(plain_properties());
    std::vector sources {source(*index, *source_meta)};

    expect_rejected(compaction::validate_plain_t2_compaction_eligibility(sources, *destination),
                    "statistics");
}

TEST(SniiCompactionEligibilityTest, RequiresExactSourceAndDestinationProperties) {
    auto first = open_index({});
    auto second = open_index({});
    auto first_meta = make_index(plain_properties());

    auto changed_properties = plain_properties();
    changed_properties["dict_compression"] = "true";
    auto changed_meta = make_index(changed_properties);
    auto destination = make_index(plain_properties());
    std::vector mismatched_sources {source(*first, *first_meta), source(*second, *changed_meta)};
    expect_rejected(
            compaction::validate_plain_t2_compaction_eligibility(mismatched_sources, *destination),
            "properties");

    auto matching_second_meta = make_index(plain_properties());
    std::vector matching_sources {source(*first, *first_meta),
                                  source(*second, *matching_second_meta)};
    expect_rejected(
            compaction::validate_plain_t2_compaction_eligibility(matching_sources, *changed_meta),
            "destination properties");
}

TEST(SniiCompactionEligibilityTest, RequiresDestinationLogicalIndexIdentity) {
    auto index = open_index({});
    auto destination = make_index(plain_properties());

    auto wrong_id = make_index(plain_properties(), doris::IndexType::INVERTED, /*index_id=*/8);
    std::vector wrong_id_sources {source(*index, *wrong_id)};
    expect_rejected(
            compaction::validate_plain_t2_compaction_eligibility(wrong_id_sources, *destination),
            "index id");

    auto wrong_suffix = make_index(plain_properties(), doris::IndexType::INVERTED,
                                   /*index_id=*/7, /*index_suffix=*/"other");
    std::vector wrong_suffix_sources {source(*index, *wrong_suffix)};
    expect_rejected(compaction::validate_plain_t2_compaction_eligibility(wrong_suffix_sources,
                                                                         *destination),
                    "index suffix");
}

TEST(SniiCompactionEligibilityTest, RequiresInvertedPhraseDestination) {
    auto index = open_index({});
    auto no_phrase_properties = plain_properties();
    no_phrase_properties["support_phrase"] = "false";
    auto no_phrase_source_meta = make_index(no_phrase_properties);
    auto no_phrase = make_index(no_phrase_properties);
    std::vector no_phrase_sources {source(*index, *no_phrase_source_meta)};
    expect_rejected(
            compaction::validate_plain_t2_compaction_eligibility(no_phrase_sources, *no_phrase),
            "phrase positions");

    auto source_meta = make_index(plain_properties());
    std::vector sources {source(*index, *source_meta)};
    auto non_inverted = make_index(plain_properties(), doris::IndexType::BITMAP);
    expect_rejected(compaction::validate_plain_t2_compaction_eligibility(sources, *non_inverted),
                    "inverted index");
}

TEST(SniiCompactionEligibilityTest, RejectsDestinationThatWouldBuildCommonGrams) {
    CommonGramsBuildSwitchGuard guard;
    doris::config::enable_common_grams_index_build = true;
    auto index = open_index({});
    auto properties = plain_properties();
    properties.erase("parser");
    properties["analyzer"] = "common_grams_analyzer";
    auto source_meta = make_index(properties);
    auto destination = make_index(properties);
    std::vector sources {source(*index, *source_meta)};

    compaction::AnalyzerProviderFactory factory = [](const doris::InvertedIndexAnalyzerConfig&) {
        return std::make_shared<StubAnalyzerProvider>(true);
    };
    expect_rejected(
            compaction::validate_plain_t2_compaction_eligibility(sources, *destination, factory),
            "CommonGrams");

    doris::config::enable_common_grams_index_build = false;
    EXPECT_TRUE(compaction::validate_plain_t2_compaction_eligibility(sources, *destination, factory)
                        .ok());
}

TEST(SniiCompactionEligibilityTest, ResolvesDestinationProviderFromWriterAnalyzerConfig) {
    auto index = open_index({});
    auto properties = plain_properties();
    properties["analyzer"] = "plain_custom_analyzer";
    properties["parser_mode"] = "fine_grained";
    properties["stopwords"] = "none";
    properties["char_filter_type"] = "char_replace";
    properties["char_filter_pattern"] = "_";
    properties["char_filter_replacement"] = " ";
    auto source_meta = make_index(properties);
    auto destination = make_index(properties);
    std::vector sources {source(*index, *source_meta)};
    std::optional<doris::InvertedIndexAnalyzerConfig> captured;

    compaction::AnalyzerProviderFactory factory =
            [&](const doris::InvertedIndexAnalyzerConfig& config) {
                captured = config;
                return std::make_shared<StubAnalyzerProvider>(false);
            };
    ASSERT_TRUE(compaction::validate_plain_t2_compaction_eligibility(sources, *destination, factory)
                        .ok());
    ASSERT_TRUE(captured.has_value());
    EXPECT_EQ(captured->analyzer_name, "plain_custom_analyzer");
    EXPECT_EQ(captured->parser_type, doris::InvertedIndexParserType::PARSER_STANDARD);
    EXPECT_EQ(captured->parser_mode, "fine_grained");
    EXPECT_EQ(captured->lower_case, "true");
    EXPECT_EQ(captured->stop_words, "none");
    EXPECT_EQ(captured->char_filter_map.at("char_filter_pattern"), "_");
    EXPECT_EQ(captured->char_filter_map.at("char_filter_replacement"), " ");
}

} // namespace
