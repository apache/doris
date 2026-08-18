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

#include "storage/index/inverted/similarity/predicate_collector.h"

#include <glog/logging.h>

#include <vector>

#include "exec/common/variant_util.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vsearch.h"
#include "exprs/vslot_ref.h"
#include "gen_cpp/Exprs_types.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_selector.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

using namespace segment_v2;

namespace {

InvertedIndexAnalyzerCtx analyzer_context_from_properties(
        const std::map<std::string, std::string>& properties) {
    InvertedIndexAnalyzerConfig config;
    config.analyzer_name = get_analyzer_name_from_properties(properties);
    config.parser_type = get_inverted_index_parser_type_from_string(
            get_parser_string_from_properties(properties));
    config.parser_mode = get_parser_mode_string_from_properties(properties);
    config.lower_case = get_parser_lowercase_from_properties(properties);
    config.stop_words = get_parser_stopwords_from_properties(properties);
    config.char_filter_map = get_parser_char_filter_map_from_properties(properties);

    InvertedIndexAnalyzerCtx analyzer_ctx;
    analyzer_ctx.analyzer_name = config.analyzer_name;
    analyzer_ctx.parser_type = config.parser_type;
    analyzer_ctx.char_filter_map = config.char_filter_map;
    analyzer_ctx.analyzer_provider =
            inverted_index::InvertedIndexAnalyzer::create_analyzer_provider(&config);
    return analyzer_ctx;
}

std::vector<TermInfo> analyze_plain_query(const std::string& value,
                                          const InvertedIndexAnalyzerCtx& analyzer_ctx) {
    DORIS_CHECK(analyzer_ctx.analyzer_provider != nullptr);
    auto analyzer = analyzer_ctx.analyzer_provider->get_analyzer(
            inverted_index::AnalysisPurpose::kPlainQuery);
    auto reader =
            inverted_index::InvertedIndexAnalyzer::create_reader(analyzer_ctx.char_filter_map);
    reader->init(value.data(), static_cast<int32_t>(value.size()), true);
    return inverted_index::InvertedIndexAnalyzer::get_analyse_result(reader, analyzer.get());
}

Status append_scoring_leaf(CollectInfo* collect_info, const std::vector<TermInfo>& term_infos,
                           std::string_view base_analyzer_fingerprint) {
    DORIS_CHECK(collect_info != nullptr);
    if (!collect_info->logical_scoring_leaves.empty() &&
        collect_info->expected_base_analyzer_fingerprint != base_analyzer_fingerprint) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Scoring predicates for one field use different base analyzers");
    }
    if (collect_info->logical_scoring_leaves.empty()) {
        collect_info->expected_base_analyzer_fingerprint = base_analyzer_fingerprint;
    }

    LogicalScoringLeaf leaf;
    leaf.clauses.reserve(term_infos.size());
    for (const auto& term_info : term_infos) {
        DORIS_CHECK(term_info.is_single_term());
        DORIS_CHECK(term_info.key_kind == TermKeyKind::kPlain);
        const auto& term = term_info.get_single_term();
        auto [slot, inserted] = collect_info->unique_term_slots.try_emplace(
                term, static_cast<uint32_t>(collect_info->unique_terms.size()));
        if (inserted) {
            collect_info->unique_terms.push_back(term);
        }
        leaf.clauses.emplace_back(
                LogicalScoringClause {.df_slot = slot->second, .position = term_info.position});
    }
    collect_info->logical_scoring_leaves.emplace_back(std::move(leaf));
    return Status::OK();
}

InvertedIndexQueryType match_query_type(TExprOpcode::type opcode) {
    switch (opcode) {
    case TExprOpcode::MATCH_ANY:
        return InvertedIndexQueryType::MATCH_ANY_QUERY;
    case TExprOpcode::MATCH_ALL:
        return InvertedIndexQueryType::MATCH_ALL_QUERY;
    case TExprOpcode::MATCH_PHRASE:
        return InvertedIndexQueryType::MATCH_PHRASE_QUERY;
    case TExprOpcode::MATCH_PHRASE_PREFIX:
        return InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY;
    case TExprOpcode::MATCH_REGEXP:
        return InvertedIndexQueryType::MATCH_REGEXP_QUERY;
    case TExprOpcode::MATCH_PHRASE_EDGE:
        return InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY;
    default:
        return InvertedIndexQueryType::UNKNOWN_QUERY;
    }
}

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

Result<const TabletIndex*> select_index_meta(const std::vector<const TabletIndex*>& index_metas,
                                             FieldType field_type,
                                             InvertedIndexQueryType query_type,
                                             std::string_view analyzer_key) {
    std::vector<InvertedIndexSelectionCandidate> candidates;
    candidates.reserve(index_metas.size());
    InvertedIndexSelectionKeyIndex key_index;
    for (const auto* index_meta : index_metas) {
        auto status = add_inverted_index_selection_candidate(
                InvertedIndexSelectionCandidate {.index_id = index_meta->index_id(),
                                                 .reader_type = infer_inverted_index_reader_type(
                                                         field_type, index_meta->properties()),
                                                 .analyzer_key = build_analyzer_key_from_properties(
                                                         index_meta->properties())},
                &candidates, &key_index);
        if (!status.ok()) {
            return ResultError(std::move(status));
        }
    }

    auto selection = select_best_inverted_index_candidate(
            candidates, key_index, field_type, query_type, normalize_analyzer_key(analyzer_key));
    if (!selection.has_value()) {
        return ResultError(std::move(selection.error()));
    }
    const size_t selected = *selection;
    DORIS_CHECK(selected < index_metas.size());
    return index_metas[selected];
}

Status validate_same_physical_index(const CollectInfo& collect_info,
                                    const TabletIndex& selected_index) {
    DORIS_CHECK(collect_info.index_meta != nullptr);
    if (collect_info.index_meta->index_id() != selected_index.index_id() ||
        collect_info.index_meta->get_index_suffix() != selected_index.get_index_suffix()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Scoring predicates for one field select different inverted indexes: {} and {}",
                collect_info.index_meta->index_id(), selected_index.index_id());
    }
    return Status::OK();
}

struct ScoringIndexCandidates {
    FieldType field_type = FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    std::string index_suffix_path;
    std::vector<const TabletIndex*> index_metas;
    std::vector<std::shared_ptr<const TabletIndex>> owned_index_metas;
};

FieldType scoring_leaf_type(const TabletColumn& column) {
    const TabletColumn* leaf = &column;
    while (leaf->is_array_type()) {
        DORIS_CHECK_EQ(leaf->get_subtype_count(), 1);
        leaf = &leaf->get_sub_column(0);
    }
    return leaf->type();
}

ScoringIndexCandidates resolve_text_scoring_index_candidates(const TabletSchemaSPtr& tablet_schema,
                                                             const TabletColumn& column) {
    ScoringIndexCandidates candidates {.field_type = scoring_leaf_type(column),
                                       .index_suffix_path = column.suffix_path(),
                                       .index_metas = tablet_schema->inverted_indexs(column),
                                       .owned_index_metas = {}};

    // The collector has tablet-schema context but no segment-side variant
    // inference. Resolve only shapes that are deterministic from schema:
    // typed/materialized paths, field-pattern templates, and a plain parent
    // index inherited by the dynamic VARIANT placeholder.
    if (!candidates.index_metas.empty() || !column.is_extracted_column()) {
        return candidates;
    }

    TabletSchema::SubColumnInfo sub_column_info;
    const std::string relative_path = column.path_info_ptr()->copy_pop_front().get_path();
    if (variant_util::generate_sub_column_info(*tablet_schema, column.parent_unique_id(),
                                               relative_path, &sub_column_info) &&
        !sub_column_info.indexes.empty()) {
        candidates.field_type = scoring_leaf_type(sub_column_info.column);
        candidates.index_suffix_path = sub_column_info.column.suffix_path();
        for (auto& index : sub_column_info.indexes) {
            candidates.index_metas.push_back(index.get());
            candidates.owned_index_metas.emplace_back(std::move(index));
        }
        return candidates;
    }

    if (!column.is_variant_type()) {
        return candidates;
    }

    // MATCH and score-bearing SEARCH clauses have text semantics. When a dynamic
    // VARIANT path has no materialized type, those semantics provide the missing
    // leaf-type proof for selecting its plain parent full-text index. Typed paths
    // returned above keep their schema type, so numeric BKD leaves remain rejected.
    candidates.field_type = FieldType::OLAP_FIELD_TYPE_STRING;
    const auto parent_indexes = tablet_schema->inverted_indexs(column.parent_unique_id());
    for (const auto* index : parent_indexes) {
        if (!index->field_pattern().empty()) {
            continue;
        }
        auto owned_index = std::make_shared<TabletIndex>(*index);
        owned_index->set_escaped_escaped_index_suffix_path(column.path_info_ptr()->get_path());
        candidates.index_metas.push_back(owned_index.get());
        candidates.owned_index_metas.emplace_back(std::move(owned_index));
    }
    return candidates;
}

Status validate_scoring_leaf_type(const ScoringIndexCandidates& candidates,
                                  std::string_view field_name) {
    if (candidates.field_type == FieldType::OLAP_FIELD_TYPE_VARIANT) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Cannot prove scoring leaf type for field={}",
                field_name);
    }
    return Status::OK();
}

void preserve_selected_index_metadata(const ScoringIndexCandidates& candidates,
                                      const TabletIndex* selected_index,
                                      CollectInfo* collect_info) {
    DORIS_CHECK(selected_index != nullptr);
    DORIS_CHECK(collect_info != nullptr);
    collect_info->index_meta = selected_index;
    for (const auto& owned_index : candidates.owned_index_metas) {
        if (owned_index.get() == selected_index) {
            collect_info->owned_index_meta = owned_index;
            return;
        }
    }
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

} // namespace

VSlotRef* PredicateCollector::find_slot_ref(const VExprSPtr& expr) const {
    if (!expr) {
        return nullptr;
    }

    auto cur = VExpr::expr_without_cast(expr);
    if (cur->node_type() == TExprNodeType::SLOT_REF) {
        return static_cast<VSlotRef*>(cur.get());
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
                                        const VExprSPtr& expr, CollectInfoMap* collect_infos) {
    DCHECK(collect_infos != nullptr);

    auto* left_slot_ref = find_slot_ref(expr->children()[0]);
    if (left_slot_ref == nullptr) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Cannot find slot reference in match predicate "
                "left expression");
    }

    auto* right_literal = static_cast<VLiteral*>(expr->children()[1].get());
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
    auto candidates = resolve_text_scoring_index_candidates(tablet_schema, column);
    RETURN_IF_ERROR(validate_scoring_leaf_type(candidates, left_slot_ref->column_name()));

#ifndef BE_TEST
    if (candidates.index_metas.empty()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Score query is not supported without inverted "
                "index for column={}",
                left_slot_ref->column_name());
    }
#else
    if (candidates.index_metas.empty()) {
        return Status::OK();
    }
#endif

    const auto* analyzer_ctx = expr->query_analyzer_ctx();
    DORIS_CHECK(analyzer_ctx != nullptr);
    const auto query_type = match_query_type(expr->op());
    DORIS_CHECK(query_type != InvertedIndexQueryType::UNKNOWN_QUERY);
    const auto* index_meta = DORIS_TRY(select_index_meta(
            candidates.index_metas, candidates.field_type, query_type, analyzer_ctx->analyzer_key));
    if (!InvertedIndexAnalyzer::should_analyzer(index_meta->properties()) ||
        !IndexReaderHelper::is_need_similarity_score(expr->op(), index_meta)) {
        return Status::OK();
    }

    DORIS_CHECK(analyzer_ctx->analyzer_provider != nullptr);
    auto options = DataTypeSerDe::get_default_format_options();
    options.timezone = &state->timezone_obj();
    auto term_infos = analyze_plain_query(right_literal->value(options), *analyzer_ctx);
    if (expr->op() == TExprOpcode::MATCH_PHRASE_PREFIX && !term_infos.empty()) {
        term_infos.pop_back();
    }
    const auto base_analyzer_fingerprint =
            analyzer_ctx->analyzer_provider->base_analyzer_fingerprint();

    std::string field_name =
            build_field_name(index_meta->col_unique_ids()[0], candidates.index_suffix_path);
    std::wstring ws_field_name = StringHelper::to_wstring(field_name);

    auto iter = collect_infos->find(ws_field_name);
    if (iter == collect_infos->end()) {
        CollectInfo collect_info;
        RETURN_IF_ERROR(append_scoring_leaf(&collect_info, term_infos, base_analyzer_fingerprint));
        preserve_selected_index_metadata(candidates, index_meta, &collect_info);
        (*collect_infos)[ws_field_name] = std::move(collect_info);
    } else {
        RETURN_IF_ERROR(validate_same_physical_index(iter->second, *index_meta));
        RETURN_IF_ERROR(append_scoring_leaf(&iter->second, term_infos, base_analyzer_fingerprint));
    }

    return Status::OK();
}

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

    std::vector<TermInfo> term_infos;
    std::string_view base_analyzer_fingerprint;
    std::optional<InvertedIndexAnalyzerCtx> analyzer_ctx;
    if (InvertedIndexAnalyzer::should_analyzer(analysis_properties)) {
        analyzer_ctx.emplace(analyzer_context_from_properties(analysis_properties));
        base_analyzer_fingerprint = analyzer_ctx->analyzer_provider->base_analyzer_fingerprint();
    }

    if (clause_type == "MATCH") {
        term_infos.emplace_back(value);
    } else if (category == ClauseTypeCategory::TOKENIZED) {
        if (analyzer_ctx.has_value()) {
            term_infos = analyze_plain_query(value, *analyzer_ctx);
        } else {
            term_infos.emplace_back(value);
        }
    } else if (category == ClauseTypeCategory::NON_TOKENIZED) {
        if (clause_type == "TERM" && analyzer_ctx.has_value()) {
            term_infos = analyze_plain_query(value, *analyzer_ctx);
        } else {
            term_infos.emplace_back(value);
        }
    }

    std::string lucene_field_name =
            build_field_name(index_meta->col_unique_ids()[0], candidates.index_suffix_path);
    std::wstring ws_field_name = StringHelper::to_wstring(lucene_field_name);

    auto iter = collect_infos->find(ws_field_name);
    if (iter == collect_infos->end()) {
        CollectInfo collect_info;
        RETURN_IF_ERROR(append_scoring_leaf(&collect_info, term_infos, base_analyzer_fingerprint));
        preserve_selected_index_metadata(candidates, index_meta, &collect_info);
        (*collect_infos)[ws_field_name] = std::move(collect_info);
    } else {
        RETURN_IF_ERROR(validate_same_physical_index(iter->second, *index_meta));
        RETURN_IF_ERROR(append_scoring_leaf(&iter->second, term_infos, base_analyzer_fingerprint));
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
