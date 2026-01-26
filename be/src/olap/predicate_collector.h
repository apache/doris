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

#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "runtime/runtime_state.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {

namespace vectorized {
class VSlotRef;
}

class TabletIndex;
class TabletSchema;
using TabletSchemaSPtr = std::shared_ptr<TabletSchema>;

struct TermInfoComparer {
    bool operator()(const segment_v2::TermInfo& lhs, const segment_v2::TermInfo& rhs) const {
        return lhs.term < rhs.term;
    }
};

struct CollectInfo {
    std::set<segment_v2::TermInfo, TermInfoComparer> term_infos;
    const TabletIndex* index_meta = nullptr;
};
using CollectInfoMap = std::unordered_map<std::wstring, CollectInfo>;

class PredicateCollector {
public:
    virtual ~PredicateCollector() = default;

    virtual Status collect(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                           const vectorized::VExprSPtr& expr, CollectInfoMap* collect_infos) = 0;

protected:
    vectorized::VSlotRef* find_slot_ref(const vectorized::VExprSPtr& expr) const;
    std::string build_field_name(int32_t col_unique_id, const std::string& suffix_path) const;
};

class MatchPredicateCollector : public PredicateCollector {
public:
    Status collect(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                   const vectorized::VExprSPtr& expr, CollectInfoMap* collect_infos) override;
};

class SearchPredicateCollector : public PredicateCollector {
public:
    Status collect(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                   const vectorized::VExprSPtr& expr, CollectInfoMap* collect_infos) override;

private:
    enum class ClauseTypeCategory { NON_TOKENIZED, TOKENIZED, COMPOUND };

    Status collect_from_clause(const TSearchClause& clause, RuntimeState* state,
                               const TabletSchemaSPtr& tablet_schema,
                               CollectInfoMap* collect_infos);
    Status collect_from_leaf(const TSearchClause& clause, RuntimeState* state,
                             const TabletSchemaSPtr& tablet_schema, CollectInfoMap* collect_infos);
    bool is_score_query_type(const std::string& clause_type) const;
    ClauseTypeCategory get_clause_type_category(const std::string& clause_type) const;
};

using PredicateCollectorPtr = std::unique_ptr<PredicateCollector>;

} // namespace doris