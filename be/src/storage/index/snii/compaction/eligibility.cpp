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

#include "storage/index/snii/compaction/eligibility.h"

#include <cstddef>
#include <map>
#include <string>
#include <string_view>

#include "CLucene.h"
#include "common/check.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::snii::compaction {

namespace inverted_index = segment_v2::inverted_index;

namespace {

Status reject(std::string_view reason) {
    return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
            "SNII streamed compaction is not eligible: {}", reason);
}

InvertedIndexAnalyzerConfig analyzer_config_from_properties(
        const std::map<std::string, std::string>& properties) {
    InvertedIndexAnalyzerConfig config;
    config.analyzer_name = get_analyzer_name_from_properties(properties);
    config.parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(properties));
    config.parser_mode = get_parser_mode_string_from_properties(properties);
    config.char_filter_map = get_parser_char_filter_map_from_properties(properties);
    config.lower_case = get_parser_lowercase_from_properties<true>(properties);
    config.stop_words = get_parser_stopwords_from_properties(properties);
    return config;
}

Status validate_source_shape(const reader::LogicalIndexReader& source, size_t source_ordinal) {
    if (source.tier() != format::IndexTier::kT2) {
        return reject(fmt::format("source {} is not T2", source_ordinal));
    }
    if (!source.has_positions()) {
        return reject(fmt::format("source {} has no positions", source_ordinal));
    }

    const auto& stats = source.stats();
    if (stats.indexed_doc_count > stats.doc_count ||
        stats.null_count != stats.doc_count - stats.indexed_doc_count) {
        return reject(fmt::format("source {} document statistics violate indexed + null = doc",
                                  source_ordinal));
    }
    return Status::OK();
}

Status reject_legacy_bigram(const reader::LogicalIndexReader& source, size_t source_ordinal) {
    bool found_legacy_bigram = false;
    RETURN_IF_ERROR(source.visit_prefix_terms(
            format::kPhraseBigramTermMarker,
            [&found_legacy_bigram](reader::LogicalIndexReader::PrefixHit&&, bool* stop) {
                found_legacy_bigram = true;
                *stop = true;
                return Status::OK();
            }));
    if (found_legacy_bigram) {
        return reject(
                fmt::format("source {} contains a legacy phrase-bigram term", source_ordinal));
    }
    return Status::OK();
}

Status resolve_destination_analyzer(const TabletIndex& destination_index,
                                    const AnalyzerProviderFactory& analyzer_provider_factory,
                                    inverted_index::AnalyzerProviderPtr* analyzer_provider) {
    analyzer_provider->reset();
    const auto& properties = destination_index.properties();
    if (!inverted_index::InvertedIndexAnalyzer::should_analyzer(properties)) {
        return Status::OK();
    }
    const InvertedIndexAnalyzerConfig analyzer_config = analyzer_config_from_properties(properties);
    try {
        *analyzer_provider =
                analyzer_provider_factory
                        ? analyzer_provider_factory(analyzer_config)
                        : inverted_index::InvertedIndexAnalyzer::create_analyzer_provider(
                                  &analyzer_config);
    } catch (const CLuceneError& error) {
        return reject(fmt::format("destination analyzer resolution failed: {}", error.what()));
    } catch (const Exception& error) {
        return reject(fmt::format("destination analyzer resolution failed: {}", error.what()));
    }
    DORIS_CHECK(*analyzer_provider != nullptr);
    return Status::OK();
}

Status validate_destination_policy(const TabletIndex& destination_index,
                                   const AnalyzerProviderFactory& analyzer_provider_factory) {
    const auto& properties = destination_index.properties();
    if (get_parser_phrase_support_string_from_properties(properties) !=
        INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES) {
        return reject("destination does not request phrase positions");
    }
    if (!inverted_index::InvertedIndexAnalyzer::should_analyzer(properties)) {
        return Status::OK();
    }

    inverted_index::AnalyzerProviderPtr analyzer_provider;
    RETURN_IF_ERROR(resolve_destination_analyzer(destination_index, analyzer_provider_factory,
                                                 &analyzer_provider));
    DORIS_CHECK(analyzer_provider != nullptr);
    return Status::OK();
}

} // namespace

Status validate_plain_t2_source(const reader::LogicalIndexReader& source, size_t source_ordinal) {
    return validate_source_shape(source, source_ordinal);
}

Status validate_plain_t2_source_eligibility(const reader::LogicalIndexReader& source,
                                            size_t source_ordinal) {
    RETURN_IF_ERROR(validate_source_shape(source, source_ordinal));
    return reject_legacy_bigram(source, source_ordinal);
}

Status validate_snii_source_eligibility(const reader::LogicalIndexReader& source,
                                        size_t source_ordinal,
                                        const SniiCompactionEligibility& /*eligibility*/) {
    return validate_plain_t2_source_eligibility(source, source_ordinal);
}

Status validate_plain_t2_compaction_eligibility(
        std::span<const PlainT2CompactionSource> sources, const TabletIndex& destination_index,
        const AnalyzerProviderFactory& analyzer_provider_factory) {
    if (sources.empty()) {
        return reject("no source logical indexes");
    }
    if (!destination_index.is_inverted_index()) {
        return reject("destination is not an inverted index");
    }

    const TabletIndex& first_index_meta = sources.front().index_meta.get();
    if (!first_index_meta.is_inverted_index()) {
        return reject("source 0 is not an inverted index");
    }
    const auto& source_properties = first_index_meta.properties();
    for (size_t source_ordinal = 0; source_ordinal < sources.size(); ++source_ordinal) {
        const TabletIndex& index_meta = sources[source_ordinal].index_meta.get();
        if (!index_meta.is_inverted_index()) {
            return reject(fmt::format("source {} is not an inverted index", source_ordinal));
        }
        if (index_meta.properties() != source_properties) {
            return reject(fmt::format("source {} properties differ from source 0", source_ordinal));
        }
        if (index_meta.index_id() != destination_index.index_id()) {
            return reject(
                    fmt::format("source {} index id differs from destination", source_ordinal));
        }
        if (index_meta.get_index_suffix() != destination_index.get_index_suffix()) {
            return reject(
                    fmt::format("source {} index suffix differs from destination", source_ordinal));
        }
    }
    if (destination_index.properties() != source_properties) {
        return reject("destination properties differ from source properties");
    }

    for (size_t source_ordinal = 0; source_ordinal < sources.size(); ++source_ordinal) {
        RETURN_IF_ERROR(validate_plain_t2_source_eligibility(sources[source_ordinal].reader.get(),
                                                             source_ordinal));
    }
    return validate_destination_policy(destination_index, analyzer_provider_factory);
}

Status validate_snii_compaction_eligibility(
        std::span<const PlainT2CompactionSource> sources, const TabletIndex& destination_index,
        SniiCompactionEligibility* out, const AnalyzerProviderFactory& analyzer_provider_factory) {
    if (out == nullptr) {
        return Status::InvalidArgument("SNII compaction eligibility has null output");
    }
    *out = SniiCompactionEligibility {};
    if (sources.empty()) {
        return reject("no source logical indexes");
    }
    if (!destination_index.is_inverted_index()) {
        return reject("destination is not an inverted index");
    }
    if (get_parser_phrase_support_string_from_properties(destination_index.properties()) !=
        INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES) {
        return reject("destination does not request phrase positions");
    }

    const TabletIndex& first_index_meta = sources.front().index_meta.get();
    if (!first_index_meta.is_inverted_index()) {
        return reject("source 0 is not an inverted index");
    }
    const auto& source_properties = first_index_meta.properties();
    for (size_t source_ordinal = 0; source_ordinal < sources.size(); ++source_ordinal) {
        const TabletIndex& index_meta = sources[source_ordinal].index_meta.get();
        if (!index_meta.is_inverted_index()) {
            return reject(fmt::format("source {} is not an inverted index", source_ordinal));
        }
        if (index_meta.properties() != source_properties) {
            return reject(fmt::format("source {} properties differ from source 0", source_ordinal));
        }
        if (index_meta.index_id() != destination_index.index_id()) {
            return reject(
                    fmt::format("source {} index id differs from destination", source_ordinal));
        }
        if (index_meta.get_index_suffix() != destination_index.get_index_suffix()) {
            return reject(
                    fmt::format("source {} index suffix differs from destination", source_ordinal));
        }
    }
    if (destination_index.properties() != source_properties) {
        return reject("destination properties differ from source properties");
    }

    for (size_t source_ordinal = 0; source_ordinal < sources.size(); ++source_ordinal) {
        if (sources[source_ordinal].reader.get().tier() != format::IndexTier::kT2) {
            return reject(fmt::format("source {} is not a positional (T2) index", source_ordinal));
        }
        RETURN_IF_ERROR(validate_plain_t2_source_eligibility(sources[source_ordinal].reader.get(),
                                                             source_ordinal));
    }
    RETURN_IF_ERROR(validate_destination_policy(destination_index, analyzer_provider_factory));
    out->destination_writes_norms =
            inverted_index::InvertedIndexAnalyzer::should_analyzer(destination_index.properties());
    return Status::OK();
}

} // namespace doris::snii::compaction
