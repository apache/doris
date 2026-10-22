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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_state.h"
#include "storage/field_type.h"
#include "storage/index/inverted/common/inverted_index_query_type.h"
#include "storage/index/inverted/common/query_info.h"

namespace doris {

struct InvertedIndexAnalyzerCtx;

// Build the analyzer context of an index, converting a failure to build the analyzer provider
// into a Status instead of letting the exception escape a Status-returning caller.
Result<InvertedIndexAnalyzerCtx> analyzer_context_from_properties(
        const std::map<std::string, std::string>& properties);

class VSlotRef;
class TabletIndex;
class TabletColumn;
class TabletSchema;
using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

struct LogicalScoringClause {
    uint32_t df_slot = 0;
    int32_t position = 0;
};

struct LogicalScoringLeaf {
    std::vector<LogicalScoringClause> clauses;
};

struct CollectInfo {
    std::vector<std::string> unique_terms;
    std::unordered_map<std::string, uint32_t> unique_term_slots;
    std::vector<LogicalScoringLeaf> logical_scoring_leaves;
    std::shared_ptr<const TabletIndex> owned_index_meta;
    const TabletIndex* index_meta = nullptr;
    std::shared_ptr<const TabletColumn> read_column = nullptr;
};
using CollectInfoMap = std::unordered_map<std::wstring, CollectInfo>;

namespace scoring_collection_detail {

struct ScoringIndexCandidates {
    FieldType field_type = FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    std::string index_suffix_path;
    std::vector<const TabletIndex*> index_metas;
    std::vector<std::shared_ptr<const TabletIndex>> owned_index_metas;
    std::shared_ptr<const TabletColumn> read_column = nullptr;
};

Result<std::vector<segment_v2::TermInfo>> analyze_plain_query(
        const std::string& value, const InvertedIndexAnalyzerCtx& analyzer_ctx);
Status append_scoring_leaf(CollectInfo* collect_info,
                           const std::vector<segment_v2::TermInfo>& term_infos);
Result<const TabletIndex*> select_index_meta(const std::vector<const TabletIndex*>& index_metas,
                                             FieldType field_type,
                                             segment_v2::InvertedIndexQueryType query_type,
                                             std::string_view analyzer_key,
                                             std::string_view legacy_analyzer_key = {});
Status validate_same_physical_index(const CollectInfo& collect_info,
                                    const TabletIndex& selected_index);
FieldType scoring_leaf_type(const TabletColumn& column);
ScoringIndexCandidates resolve_text_scoring_index_candidates(const TabletSchemaSPtr& tablet_schema,
                                                             const TabletColumn& column);
Status validate_scoring_leaf_type(const ScoringIndexCandidates& candidates,
                                  std::string_view field_name);
void preserve_selected_index_metadata(const ScoringIndexCandidates& candidates,
                                      const TabletIndex* selected_index, CollectInfo* collect_info);

} // namespace scoring_collection_detail

class PredicateCollector {
public:
    virtual ~PredicateCollector() = default;

    virtual Status collect(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                           const VExprSPtr& expr, CollectInfoMap* collect_infos) = 0;

protected:
    VSlotRef* find_slot_ref(const VExprSPtr& expr) const;
    std::string build_field_name(int32_t col_unique_id, const std::string& suffix_path) const;
};

class MatchPredicateCollector : public PredicateCollector {
public:
    Status collect(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                   const VExprSPtr& expr, CollectInfoMap* collect_infos) override;
};

using PredicateCollectorPtr = std::unique_ptr<PredicateCollector>;

} // namespace doris
