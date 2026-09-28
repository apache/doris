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

#include "storage/predicate_collector.h"

#include <glog/logging.h>

#include <vector>

#include "common/exception.h"
#include "exec/common/variant_util.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vmatch_predicate.h"
#include "exprs/vsearch.h"
#include "exprs/vslot_ref.h"
#include "gen_cpp/Exprs_types.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_selector.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

using namespace segment_v2;

namespace {

InvertedIndexAnalyzerCtx build_analyzer_context(
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

Result<const TabletIndex*> select_index_meta(const std::vector<const TabletIndex*>& index_metas,
                                             FieldType field_type,
                                             InvertedIndexQueryType query_type,
                                             std::string_view analyzer_key,
                                             std::string_view legacy_analyzer_key) {
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
            candidates, key_index, field_type, query_type, normalize_analyzer_key(analyzer_key),
            legacy_analyzer_key);
    if (!selection.has_value()) {
        return ResultError(std::move(selection.error()));
    }
    DORIS_CHECK(*selection < index_metas.size());
    return index_metas[*selection];
}

} // namespace

Result<InvertedIndexAnalyzerCtx> analyzer_context_from_properties(
        const std::map<std::string, std::string>& properties) {
    try {
        return build_analyzer_context(properties);
    } catch (const CLuceneError& error) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "Build scoring analyzer failed: {}", error.what()));
    } catch (const Exception& error) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "Build scoring analyzer failed: {}", error.what()));
    }
}

namespace {

Result<std::vector<TermInfo>> analyze_plain_query(const std::string& value,
                                                  const InvertedIndexAnalyzerCtx& analyzer_ctx) {
    DORIS_CHECK(analyzer_ctx.analyzer_provider != nullptr);
    try {
        auto analyzer = analyzer_ctx.analyzer_provider->get_analyzer();
        auto reader =
                inverted_index::InvertedIndexAnalyzer::create_reader(analyzer_ctx.char_filter_map);
        reader->init(value.data(), static_cast<int32_t>(value.size()), true);
        return inverted_index::InvertedIndexAnalyzer::get_analyse_result(reader, analyzer.get());
    } catch (const CLuceneError& error) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "Analyze scoring query failed: {}", error.what()));
    } catch (const Exception& error) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "Analyze scoring query failed: {}", error.what()));
    }
}

Result<std::vector<TermInfo>> analyze_plain_query(
        const std::string& value, const std::map<std::string, std::string>& properties) {
    auto context = analyzer_context_from_properties(properties);
    if (!context.has_value()) {
        return ResultError(std::move(context.error()));
    }
    return analyze_plain_query(value, context.value());
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
    auto index_metas = tablet_schema->inverted_indexs(column);
    std::vector<std::shared_ptr<const TabletIndex>> owned_index_metas;
    std::string index_suffix_path = column.suffix_path();

    // Schema-only fallback for variant sub-columns. Collector runs at tablet
    // level without segment context, so we cannot do nested-group inference
    // or inherit_index runtime-type dispatch. Two paths cover what is
    // resolvable from schema alone:
    //   1. field_pattern templates (MATCH_NAME / MATCH_NAME_GLOB) via
    //      generate_sub_column_info.
    //   2. Plain parent inverted index when the schema column is the dynamic
    //      path's VARIANT placeholder produced by _init_variant_columns. In
    //      that state inverted_indexs(column) misses because
    //      _path_set_info_map.subcolumn_indexes is only populated for typed
    //      paths / field_pattern outputs, not for plain parent indexes added
    //      by ALTER. Clone the parent's non-field-pattern indexes with the
    //      variant path as suffix so segment-side BM25 statistics can be
    //      collected.
    if (index_metas.empty() && column.is_extracted_column()) {
        TabletSchema::SubColumnInfo sub_column_info;
        const std::string relative_path = column.path_info_ptr()->copy_pop_front().get_path();
        if (variant_util::generate_sub_column_info(*tablet_schema, column.parent_unique_id(),
                                                   relative_path, &sub_column_info) &&
            !sub_column_info.indexes.empty()) {
            index_suffix_path = sub_column_info.column.suffix_path();
            for (auto& idx : sub_column_info.indexes) {
                index_metas.push_back(idx.get());
                owned_index_metas.emplace_back(std::move(idx));
            }
        } else if (column.is_variant_type()) {
            const auto parent_indexes = tablet_schema->inverted_indexs(column.parent_unique_id());
            for (const auto* index : parent_indexes) {
                if (!index->field_pattern().empty()) {
                    continue;
                }
                auto index_ptr = std::make_shared<TabletIndex>(*index);
                index_ptr->set_escaped_escaped_index_suffix_path(
                        column.path_info_ptr()->get_path());
                index_metas.push_back(index_ptr.get());
                owned_index_metas.emplace_back(std::move(index_ptr));
            }
        }
    }

#ifndef BE_TEST
    if (index_metas.empty()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "Index statistics collection failed: Score query is not supported without inverted "
                "index for column={}",
                left_slot_ref->column_name());
    }
#endif

    const InvertedIndexAnalyzerCtx* match_analyzer_ctx = nullptr;
    if (const auto* match = dynamic_cast<const VMatchPredicate*>(expr.get())) {
        match_analyzer_ctx = match->query_analyzer_ctx();
        DORIS_CHECK(match_analyzer_ctx != nullptr);
        const auto* selected = DORIS_TRY(select_index_meta(
                index_metas, column.type(), match_query_type(expr->op()),
                match_analyzer_ctx->analyzer_key, match_analyzer_ctx->legacy_analyzer_key));
        index_metas = {selected};
    }
    for (const auto* index_meta : index_metas) {
        if (!InvertedIndexAnalyzer::should_analyzer(index_meta->properties())) {
            continue;
        }

        if (!IndexReaderHelper::is_need_similarity_score(expr->op(), index_meta)) {
            continue;
        }

        auto options = DataTypeSerDe::get_default_format_options();
        options.timezone = &state->timezone_obj();
        const std::string value = right_literal->value(options);
        std::vector<TermInfo> term_infos;
        if (match_analyzer_ctx != nullptr) {
            term_infos = DORIS_TRY(analyze_plain_query(value, *match_analyzer_ctx));
        } else {
            term_infos = DORIS_TRY(analyze_plain_query(value, index_meta->properties()));
        }

        std::string field_name =
                build_field_name(index_meta->col_unique_ids()[0], index_suffix_path);
        std::wstring ws_field_name = StringHelper::to_wstring(field_name);

        auto iter = collect_infos->find(ws_field_name);
        if (iter == collect_infos->end()) {
            CollectInfo collect_info;
            collect_info.term_infos.insert(term_infos.begin(), term_infos.end());
            collect_info.index_meta = index_meta;
            for (const auto& owned_index_meta : owned_index_metas) {
                if (owned_index_meta.get() == index_meta) {
                    collect_info.owned_index_meta = owned_index_meta;
                    break;
                }
            }
            (*collect_infos)[ws_field_name] = std::move(collect_info);
        } else {
            iter->second.term_infos.insert(term_infos.begin(), term_infos.end());
        }
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
                        DORIS_TRY(analyze_plain_query(value, index_meta->properties()));
                term_infos.insert(analyzed_terms.begin(), analyzed_terms.end());
            } else {
                term_infos.insert(TermInfo(value));
            }
        } else if (category == ClauseTypeCategory::NON_TOKENIZED) {
            if (clause_type == "TERM" &&
                InvertedIndexAnalyzer::should_analyzer(index_meta->properties())) {
                auto analyzed_terms =
                        DORIS_TRY(analyze_plain_query(value, index_meta->properties()));
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
