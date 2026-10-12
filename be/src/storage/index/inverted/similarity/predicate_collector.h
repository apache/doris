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
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exprs/vexpr_fwd.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/runtime_state.h"
#include "storage/index/inverted/query/query_info.h"

namespace doris {

struct InvertedIndexAnalyzerCtx;

// Build the analyzer context of an index, converting a failure to build the analyzer provider
// into a Status instead of letting the exception escape a Status-returning caller.
Result<InvertedIndexAnalyzerCtx> analyzer_context_from_properties(
        const std::map<std::string, std::string>& properties);

class VSlotRef;
class TabletIndex;
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
};
using CollectInfoMap = std::unordered_map<std::wstring, CollectInfo>;

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

class SearchPredicateCollector : public PredicateCollector {
public:
    Status collect(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                   const VExprSPtr& expr, CollectInfoMap* collect_infos) override;

private:
    enum class ClauseTypeCategory { NON_TOKENIZED, TOKENIZED, COMPOUND };
    using FieldBindingMap = std::unordered_map<std::string, const TSearchFieldBinding*>;

    Status collect_from_clause(const TSearchClause& clause, RuntimeState* state,
                               const TabletSchemaSPtr& tablet_schema,
                               const FieldBindingMap& field_bindings,
                               CollectInfoMap* collect_infos);
    Status collect_from_leaf(const TSearchClause& clause, RuntimeState* state,
                             const TabletSchemaSPtr& tablet_schema,
                             const FieldBindingMap& field_bindings, CollectInfoMap* collect_infos);
    bool is_score_query_type(const std::string& clause_type) const;
    ClauseTypeCategory get_clause_type_category(const std::string& clause_type) const;
};

using PredicateCollectorPtr = std::unique_ptr<PredicateCollector>;

} // namespace doris
