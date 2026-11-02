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

#include "exprs/function/search/search_predicate_collector.h"

#include <glog/logging.h>

#include <optional>
#include <string_view>
#include <vector>

#include "exec/common/variant_util.h"
#include "exprs/vsearch.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/common/string_helper.h"
#include "storage/index/inverted/reader/inverted_index_selector.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

using namespace segment_v2;
using namespace scoring_collection_detail;

namespace {

InvertedIndexQueryType search_query_type(std::string_view clause_type) {
    if (clause_type == "EXACT") {
        return InvertedIndexQueryType::EQUAL_QUERY;
    }
    if (clause_type == "PHRASE") {
        return InvertedIndexQueryType::MATCH_PHRASE_QUERY;
    }
    if (clause_type == "ALL") {
        return InvertedIndexQueryType::MATCH_ALL_QUERY;
    }
    return InvertedIndexQueryType::MATCH_ANY_QUERY;
}

Result<ScoringIndexCandidates> resolve_search_scoring_index_candidates(
        const TabletSchemaSPtr& tablet_schema, const std::string& field_name,
        const TSearchFieldBinding* field_binding) {
    const int32_t column_index = tablet_schema->field_index(field_name);
    if (column_index >= 0) {
        return resolve_text_scoring_index_candidates(tablet_schema,
                                                     tablet_schema->column(column_index));
    }

    if (field_binding == nullptr || !field_binding->__isset.is_variant_subcolumn ||
        !field_binding->is_variant_subcolumn || !field_binding->__isset.parent_field_name ||
        field_binding->parent_field_name.empty() || !field_binding->__isset.subcolumn_path ||
        field_binding->subcolumn_path.empty()) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Cannot resolve search field={}", field_name));
    }

    const int32_t parent_column_index =
            tablet_schema->field_index(field_binding->parent_field_name);
    if (parent_column_index < 0) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Cannot resolve parent={} for search field={}",
                field_binding->parent_field_name, field_name));
    }
    const auto& parent_column = tablet_schema->column(parent_column_index);
    if (!parent_column.is_variant_type()) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Search field={} parent={} is not VARIANT",
                field_name, field_binding->parent_field_name));
    }

    TabletColumn dynamic_column;
    dynamic_column.set_unique_id(-1);
    dynamic_column.set_name(field_name);
    dynamic_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    dynamic_column.set_parent_unique_id(parent_column.unique_id());
    dynamic_column.set_path_info(
            PathInData(field_binding->parent_field_name + "." + field_binding->subcolumn_path));
    return resolve_text_scoring_index_candidates(tablet_schema, dynamic_column);
}

// The terms a SEARCH leaf scores: MATCH's value as given, a tokenized clause's value and a TERM's
// cut by the index's analyzer when it has one, and any other value as given; none for a clause
// of neither kind.
Result<std::vector<TermInfo>> leaf_scoring_terms(
        const std::string& clause_type, bool tokenized, bool non_tokenized,
        const std::string& value, const std::optional<InvertedIndexAnalyzerCtx>& analyzer_ctx) {
    std::vector<TermInfo> terms;
    if (clause_type == "MATCH") {
        terms.emplace_back(value);
        return terms;
    }
    const bool analyzed = tokenized || (non_tokenized && clause_type == "TERM");
    if (analyzed && analyzer_ctx.has_value()) {
        return analyze_plain_query(value, *analyzer_ctx);
    }
    if (tokenized || non_tokenized) {
        terms.emplace_back(value);
    }
    return terms;
}

} // namespace

Status SearchPredicateCollector::collect(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                                         const VExprSPtr& expr, CollectInfoMap* collect_infos) {
    DCHECK(collect_infos != nullptr);

    auto* search_expr = dynamic_cast<VSearchExpr*>(expr.get());
    if (search_expr == nullptr) {
        return Status::InternalError("SearchPredicateCollector: expr is not VSearchExpr type");
    }

    const TSearchParam& search_param = search_expr->get_search_param();
    FieldBindingMap field_bindings;
    field_bindings.reserve(search_param.field_bindings.size());
    for (const auto& field_binding : search_param.field_bindings) {
        field_bindings[field_binding.field_name] = &field_binding;
    }

    RETURN_IF_ERROR(collect_from_clause(search_param.root, state, tablet_schema, field_bindings,
                                        collect_infos));

    return Status::OK();
}

Status SearchPredicateCollector::collect_from_clause(const TSearchClause& clause,
                                                     RuntimeState* state,
                                                     const TabletSchemaSPtr& tablet_schema,
                                                     const FieldBindingMap& field_bindings,
                                                     CollectInfoMap* collect_infos) {
    const std::string& clause_type = clause.clause_type;
    // Lucene pure NOT injects a fieldless constant-score match-all child.
    if (clause_type == "MATCH_ALL_DOCS") {
        return Status::OK();
    }

    if (clause_type == "NESTED") {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Scoring nested search clauses is not supported");
    }
    ClauseTypeCategory category = get_clause_type_category(clause_type);

    if (category == ClauseTypeCategory::COMPOUND) {
        if (clause.__isset.children) {
            for (const auto& child_clause : clause.children) {
                RETURN_IF_ERROR(collect_from_clause(child_clause, state, tablet_schema,
                                                    field_bindings, collect_infos));
            }
        }
        return Status::OK();
    }

    return collect_from_leaf(clause, state, tablet_schema, field_bindings, collect_infos);
}

Status SearchPredicateCollector::collect_from_leaf(const TSearchClause& clause, RuntimeState* state,
                                                   const TabletSchemaSPtr& tablet_schema,
                                                   const FieldBindingMap& field_bindings,
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

    const auto field_binding_iter = field_bindings.find(field_name);
    const auto* field_binding =
            field_binding_iter == field_bindings.end() ? nullptr : field_binding_iter->second;
    auto candidates = DORIS_TRY(
            resolve_search_scoring_index_candidates(tablet_schema, field_name, field_binding));
    RETURN_IF_ERROR(validate_scoring_leaf_type(candidates, field_name));
    if (candidates.index_metas.empty()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Score query is not supported without "
                "inverted index for search field={}",
                field_name);
    }

    ClauseTypeCategory category = get_clause_type_category(clause_type);
    auto query_type = search_query_type(clause_type);
    std::string analyzer_key;
    if (query_type != InvertedIndexQueryType::EQUAL_QUERY && field_binding != nullptr &&
        field_binding->__isset.index_properties && !field_binding->index_properties.empty() &&
        is_string_type(candidates.field_type)) {
        analyzer_key = build_analyzer_key_from_properties(field_binding->index_properties);
    }

    auto selected_index = select_index_meta(candidates.index_metas, candidates.field_type,
                                            query_type, analyzer_key);
    if (!selected_index.has_value()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Cannot select scoring index for search "
                "field={}: {}",
                field_name, selected_index.error().to_string());
    }
    const auto* index_meta = *selected_index;
    if (infer_inverted_index_reader_type(candidates.field_type, index_meta->properties()) ==
        InvertedIndexReaderType::BKD) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: BM25 scoring does not support numeric BKD "
                "search field={}",
                field_name);
    }

    const auto& analysis_properties = index_meta->properties();

    std::optional<InvertedIndexAnalyzerCtx> analyzer_ctx;
    if (inverted_index::InvertedIndexAnalyzer::should_analyzer(analysis_properties)) {
        auto built_ctx = DORIS_TRY(analyzer_context_from_properties(analysis_properties));
        analyzer_ctx.emplace(std::move(built_ctx));
    }
    auto term_infos = DORIS_TRY(
            leaf_scoring_terms(clause_type, category == ClauseTypeCategory::TOKENIZED,
                               category == ClauseTypeCategory::NON_TOKENIZED, value, analyzer_ctx));

    std::string lucene_field_name =
            build_field_name(index_meta->col_unique_ids()[0], candidates.index_suffix_path);
    std::wstring ws_field_name = inverted_index::StringHelper::to_wstring(lucene_field_name);

    auto iter = collect_infos->find(ws_field_name);
    if (iter == collect_infos->end()) {
        CollectInfo collect_info;
        RETURN_IF_ERROR(append_scoring_leaf(&collect_info, term_infos));
        preserve_selected_index_metadata(candidates, index_meta, &collect_info);
        (*collect_infos)[ws_field_name] = std::move(collect_info);
    } else {
        RETURN_IF_ERROR(validate_same_physical_index(iter->second, *index_meta));
        RETURN_IF_ERROR(append_scoring_leaf(&iter->second, term_infos));
    }

    return Status::OK();
}

bool SearchPredicateCollector::is_score_query_type(const std::string& clause_type) const {
    return clause_type == "TERM" || clause_type == "EXACT" || clause_type == "PHRASE" ||
           clause_type == "MATCH" || clause_type == "ANY" || clause_type == "ALL";
}

SearchPredicateCollector::ClauseTypeCategory SearchPredicateCollector::get_clause_type_category(
        const std::string& clause_type) const {
    if (clause_type == "AND" || clause_type == "OR" || clause_type == "NOT" ||
        clause_type == "OCCUR_BOOLEAN") {
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
