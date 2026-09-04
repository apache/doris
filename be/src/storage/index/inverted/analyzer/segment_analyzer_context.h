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

#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/inverted/inverted_index_parser.h"

namespace doris {

class IndexPolicyMgr;

namespace segment_v2::inverted_index {

// A missing metadata record is a legacy segment and keeps the request analyzer. A present record
// must identify its base analyzer; otherwise the caller must bypass the inverted index.
Result<std::optional<InvertedIndexAnalyzerCtx>> maybe_rebuild_segment_analyzer_context(
        const InvertedIndexAnalyzerCtx* request_context,
        const std::optional<CommonGramsSegmentMetadata>& segment_metadata,
        const std::map<std::string, std::string>& physical_index_properties,
        IndexPolicyMgr* index_policy_mgr);

Result<std::optional<InvertedIndexAnalyzerCtx>> maybe_rebuild_segment_analyzer_context(
        const InvertedIndexAnalyzerCtx* request_context,
        const CommonGramsSegmentMetadata* segment_metadata,
        const std::map<std::string, std::string>& physical_index_properties,
        IndexPolicyMgr* index_policy_mgr);

// Returns nullopt when the request analyzer already matches the persisted segment analyzer.
// A returned context owns a fresh provider and must remain local to one query execution.
Result<std::optional<InvertedIndexAnalyzerCtx>> maybe_rebuild_segment_analyzer_context(
        const InvertedIndexAnalyzerCtx* request_context, std::string_view segment_base_fingerprint,
        const std::map<std::string, std::string>& physical_index_properties,
        IndexPolicyMgr* index_policy_mgr);

} // namespace segment_v2::inverted_index
} // namespace doris
