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

#include "storage/index/inverted/analyzer/segment_analyzer_context.h"

#include <CLucene.h>

#include "common/exception.h"
#include "runtime/index_policy/index_policy_mgr.h"

namespace doris::segment_v2::inverted_index {
namespace {

Result<std::optional<InvertedIndexAnalyzerCtx>> analyzer_bypass(std::string_view reason) {
    return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
            "segment analyzer unavailable: {}", reason));
}

} // namespace

Result<std::optional<InvertedIndexAnalyzerCtx>> maybe_rebuild_segment_analyzer_context(
        const InvertedIndexAnalyzerCtx* request_context,
        const std::optional<CommonGramsSegmentMetadata>& segment_metadata,
        const std::map<std::string, std::string>& physical_index_properties,
        IndexPolicyMgr* index_policy_mgr) {
    return maybe_rebuild_segment_analyzer_context(request_context,
                                                  segment_metadata ? &*segment_metadata : nullptr,
                                                  physical_index_properties, index_policy_mgr);
}

Result<std::optional<InvertedIndexAnalyzerCtx>> maybe_rebuild_segment_analyzer_context(
        const InvertedIndexAnalyzerCtx* request_context,
        const CommonGramsSegmentMetadata* segment_metadata,
        const std::map<std::string, std::string>& physical_index_properties,
        IndexPolicyMgr* index_policy_mgr) {
    if (segment_metadata == nullptr) {
        return std::optional<InvertedIndexAnalyzerCtx> {};
    }
    if (segment_metadata->base_analyzer_fingerprint.empty()) {
        return analyzer_bypass("typed metadata has no base analyzer fingerprint");
    }
    return maybe_rebuild_segment_analyzer_context(request_context,
                                                  segment_metadata->base_analyzer_fingerprint,
                                                  physical_index_properties, index_policy_mgr);
}

Result<std::optional<InvertedIndexAnalyzerCtx>> maybe_rebuild_segment_analyzer_context(
        const InvertedIndexAnalyzerCtx* request_context, std::string_view segment_base_fingerprint,
        const std::map<std::string, std::string>& physical_index_properties,
        IndexPolicyMgr* index_policy_mgr) {
    DORIS_CHECK(!segment_base_fingerprint.empty());
    if (request_context == nullptr || request_context->analyzer_provider == nullptr ||
        !request_context->should_tokenize()) {
        return analyzer_bypass("query context cannot reconstruct the physical token stream");
    }

    std::string_view request_base_fingerprint =
            request_context->analyzer_provider->base_analyzer_fingerprint();
    if (request_base_fingerprint.empty()) {
        const auto* identity = request_context->get_common_grams_identity();
        if (identity != nullptr) {
            request_base_fingerprint = identity->base_analyzer_fingerprint;
        }
    }
    if (request_base_fingerprint == segment_base_fingerprint) {
        return std::optional<InvertedIndexAnalyzerCtx> {};
    }
    if (index_policy_mgr == nullptr) {
        return analyzer_bypass("index policy manager is not initialized");
    }

    const CharFilterMap physical_char_filter_map =
            get_parser_char_filter_map_from_properties(physical_index_properties);
    AnalyzerProviderPtr provider;
    try {
        provider = index_policy_mgr->get_analyzer_provider_by_base_fingerprint(
                segment_base_fingerprint, physical_char_filter_map);
    } catch (const CLuceneError& error) {
        return analyzer_bypass(error.what());
    } catch (const Exception& error) {
        return analyzer_bypass(error.what());
    }
    if (provider == nullptr) {
        return analyzer_bypass("no installed policy matches the segment base fingerprint");
    }
    DORIS_CHECK(provider->base_analyzer_fingerprint() == segment_base_fingerprint);

    InvertedIndexAnalyzerCtx effective_context = *request_context;
    effective_context.char_filter_map = physical_char_filter_map;
    effective_context.analyzer.reset();
    effective_context.analyzer_provider = std::move(provider);
    effective_context.common_grams_identity.reset();
    return std::optional<InvertedIndexAnalyzerCtx>(std::move(effective_context));
}

} // namespace doris::segment_v2::inverted_index
