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

#include "olap/predicate_collector.h"

#include <glog/logging.h>

#include "gen_cpp/Exprs_types.h"
#include "olap/rowset/segment_v2/index_reader_helper.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"
#include "olap/tablet_schema.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vsearch.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {

using namespace segment_v2;

vectorized::VSlotRef* PredicateCollector::find_slot_ref(const vectorized::VExprSPtr& expr) const {
    if (!expr) {
        return nullptr;
    }

    auto cur = vectorized::VExpr::expr_without_cast(expr);
    if (cur->node_type() == TExprNodeType::SLOT_REF) {
        return static_cast<vectorized::VSlotRef*>(cur.get());
    }

    for (const auto& ch : cur->children()) {
        if (auto* s = find_slot_ref(ch)) {
            return s;
        }
    }

    return nullptr;
}

std::string PredicateCollector::build_field_name(int32_t col_unique_id,
                                                 const std::string& suffix_path) const {
    std::string field_name = std::to_string(col_unique_id);
    if (!suffix_path.empty()) {
        field_name += "." + suffix_path;
    }
    return field_name;
}

Status MatchPredicateCollector::collect(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                                        const vectorized::VExprSPtr& expr,
                                        CollectInfoMap* collect_infos) {
    DCHECK(collect_infos != nullptr);

    auto* left_slot_ref = find_slot_ref(expr->children()[0]);
    if (left_slot_ref == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Cannot find slot reference in match predicate "
                "left expression");
    }

    auto* right_literal = static_cast<vectorized::VLiteral*>(expr->children()[1].get());
    DCHECK(right_literal != nullptr);

    const auto* sd = state->desc_tbl().get_slot_descriptor(left_slot_ref->slot_id());
    if (sd == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Cannot find slot descriptor for slot_id={}",
                left_slot_ref->slot_id());
    }

    int32_t col_idx = tablet_schema->field_index(left_slot_ref->column_name());
    if (col_idx == -1) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Cannot find column index for column={}",
                left_slot_ref->column_name());
    }

    const auto& column = tablet_schema->column(col_idx);
    auto index_metas = tablet_schema->inverted_indexs(sd->col_unique_id(), column.suffix_path());

#ifndef BE_TEST
    if (index_metas.empty()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Score query is not supported without inverted "
                "index for column={}",
                left_slot_ref->column_name());
    }
#endif

    for (const auto* index_meta : index_metas) {
        if (!InvertedIndexAnalyzer::should_analyzer(index_meta->properties())) {
            continue;
        }

        if (!IndexReaderHelper::is_need_similarity_score(expr->op(), index_meta)) {
            continue;
        }

        auto term_infos = InvertedIndexAnalyzer::get_analyse_result(right_literal->value(),
                                                                    index_meta->properties());

        std::string field_name =
                build_field_name(index_meta->col_unique_ids()[0], column.suffix_path());
        std::wstring ws_field_name = StringHelper::to_wstring(field_name);

        auto iter = collect_infos->find(ws_field_name);
        if (iter == collect_infos->end()) {
            CollectInfo collect_info;
            collect_info.term_infos.insert(term_infos.begin(), term_infos.end());
            collect_info.index_meta = index_meta;
            (*collect_infos)[ws_field_name] = std::move(collect_info);
        } else {
            iter->second.term_infos.insert(term_infos.begin(), term_infos.end());
        }
    }

    return Status::OK();
}

Status SearchPredicateCollector::collect(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                                         const vectorized::VExprSPtr& expr,
                                         CollectInfoMap* collect_infos) {
    DCHECK(collect_infos != nullptr);

    auto* search_expr = dynamic_cast<vectorized::VSearchExpr*>(expr.get());
    if (search_expr == nullptr) {
        return Status::InternalError("SearchPredicateCollector: expr is not VSearchExpr type");
    }

    const TSearchParam& search_param = search_expr->get_search_param();

    RETURN_IF_ERROR(collect_from_clause(search_param.root, state, tablet_schema, collect_infos));

    return Status::OK();
}

Status SearchPredicateCollector::collect_from_clause(const TSearchClause& clause,
                                                     RuntimeState* state,
                                                     const TabletSchemaSPtr& tablet_schema,
                                                     CollectInfoMap* collect_infos) {
    const std::string& clause_type = clause.clause_type;
    ClauseTypeCategory category = get_clause_type_category(clause_type);

    if (category == ClauseTypeCategory::COMPOUND) {
        if (clause.__isset.children) {
            for (const auto& child_clause : clause.children) {
                RETURN_IF_ERROR(
                        collect_from_clause(child_clause, state, tablet_schema, collect_infos));
            }
        }
        return Status::OK();
    }

    return collect_from_leaf(clause, state, tablet_schema, collect_infos);
}

Status SearchPredicateCollector::collect_from_leaf(const TSearchClause& clause, RuntimeState* state,
                                                   const TabletSchemaSPtr& tablet_schema,
                                                   CollectInfoMap* collect_infos) {
    if (!clause.__isset.field_name || !clause.__isset.value) {
        return Status::InvalidArgument("Search clause missing field_name or value");
    }

    const std::string& field_name = clause.field_name;
    const std::string& value = clause.value;
    const std::string& clause_type = clause.clause_type;

    if (!is_score_query_type(clause_type)) {
        return Status::OK();
    }

    int32_t col_idx = tablet_schema->field_index(field_name);
    if (col_idx == -1) {
        return Status::OK();
    }

    const auto& column = tablet_schema->column(col_idx);

    auto index_metas = tablet_schema->inverted_indexs(column.unique_id(), column.suffix_path());
    if (index_metas.empty()) {
        return Status::OK();
    }

    ClauseTypeCategory category = get_clause_type_category(clause_type);
    for (const auto* index_meta : index_metas) {
        std::set<TermInfo, TermInfoComparer> term_infos;

        if (category == ClauseTypeCategory::TOKENIZED) {
            if (InvertedIndexAnalyzer::should_analyzer(index_meta->properties())) {
                auto analyzed_terms =
                        InvertedIndexAnalyzer::get_analyse_result(value, index_meta->properties());
                term_infos.insert(analyzed_terms.begin(), analyzed_terms.end());
            } else {
                term_infos.insert(TermInfo(value));
            }
        } else if (category == ClauseTypeCategory::NON_TOKENIZED) {
            if (clause_type == "TERM" &&
                InvertedIndexAnalyzer::should_analyzer(index_meta->properties())) {
                auto analyzed_terms =
                        InvertedIndexAnalyzer::get_analyse_result(value, index_meta->properties());
                term_infos.insert(analyzed_terms.begin(), analyzed_terms.end());
            } else {
                term_infos.insert(TermInfo(value));
            }
        }

        std::string lucene_field_name =
                build_field_name(index_meta->col_unique_ids()[0], column.suffix_path());
        std::wstring ws_field_name = StringHelper::to_wstring(lucene_field_name);

        auto iter = collect_infos->find(ws_field_name);
        if (iter == collect_infos->end()) {
            CollectInfo collect_info;
            collect_info.term_infos = std::move(term_infos);
            collect_info.index_meta = index_meta;
            (*collect_infos)[ws_field_name] = std::move(collect_info);
        } else {
            iter->second.term_infos.insert(term_infos.begin(), term_infos.end());
        }
    }

    return Status::OK();
}

bool SearchPredicateCollector::is_score_query_type(const std::string& clause_type) const {
    return clause_type == "TERM" || clause_type == "EXACT" || clause_type == "PHRASE" ||
           clause_type == "MATCH" || clause_type == "ANY" || clause_type == "ALL";
}

SearchPredicateCollector::ClauseTypeCategory SearchPredicateCollector::get_clause_type_category(
        const std::string& clause_type) const {
    if (clause_type == "AND" || clause_type == "OR" || clause_type == "NOT") {
        return ClauseTypeCategory::COMPOUND;
    } else if (clause_type == "TERM" || clause_type == "EXACT") {
        return ClauseTypeCategory::NON_TOKENIZED;
    } else if (clause_type == "PHRASE" || clause_type == "MATCH" || clause_type == "ANY" ||
               clause_type == "ALL") {
        return ClauseTypeCategory::TOKENIZED;
    } else {
        LOG(WARNING) << "Unknown clause type '" << clause_type
                     << "', defaulting to NON_TOKENIZED category";
        return ClauseTypeCategory::NON_TOKENIZED;
    }
}

} // namespace doris