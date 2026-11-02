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

#include "storage/index/inverted/reader/predicate_collector.h"

#include <glog/logging.h>

#include <algorithm>
#include <optional>
#include <vector>

#include "exec/common/variant_util.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "gen_cpp/Exprs_types.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/common/string_helper.h"
#include "storage/index/inverted/query/exec/term_pattern.h"
#include "storage/index/inverted/reader/inverted_index_selector.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

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

} // namespace

Result<InvertedIndexAnalyzerCtx> analyzer_context_from_properties(
        const std::map<std::string, std::string>& properties) {
    // Replayed components can collide across policy families, so building the provider throws.
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

namespace scoring_collection_detail {

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

Status append_scoring_leaf(CollectInfo* collect_info, const std::vector<TermInfo>& term_infos) {
    DORIS_CHECK(collect_info != nullptr);

    LogicalScoringLeaf leaf;
    leaf.clauses.reserve(term_infos.size());
    for (const auto& term_info : term_infos) {
        DORIS_CHECK(term_info.is_single_term());
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

void append_scoring_prefix(CollectInfo* collect_info, const TermInfo& prefix,
                           int32_t max_expansions) {
    DORIS_CHECK(collect_info != nullptr);
    DORIS_CHECK(prefix.is_single_term());
    const auto& text = prefix.get_single_term();
    const bool known = std::ranges::any_of(
            collect_info->scoring_prefixes, [&](const LogicalScoringPrefix& registered) {
                return registered.prefix == text && registered.max_expansions == max_expansions;
            });
    if (!known) {
        collect_info->scoring_prefixes.push_back(
                LogicalScoringPrefix {.prefix = text, .max_expansions = max_expansions});
    }
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

} // namespace scoring_collection_detail

using namespace scoring_collection_detail;

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
    const auto* index_meta = DORIS_TRY(
            select_index_meta(candidates.index_metas, candidates.field_type, query_type,
                              analyzer_ctx->analyzer_key, analyzer_ctx->legacy_analyzer_key));
    if (!inverted_index::InvertedIndexAnalyzer::should_analyzer(index_meta->properties()) ||
        !IndexReaderHelper::is_need_similarity_score(expr->op(), index_meta)) {
        return Status::OK();
    }
    // A regexp scores a constant and reads no statistics. Its value is a pattern, not text, so
    // analyzing it would collect statistics for made-up terms: 'ap.*' would become "ap".
    if (expr->op() == TExprOpcode::MATCH_REGEXP) {
        return Status::OK();
    }

    DORIS_CHECK(analyzer_ctx->analyzer_provider != nullptr);
    auto options = DataTypeSerDe::get_default_format_options();
    options.timezone = &state->timezone_obj();
    auto term_infos = DORIS_TRY(analyze_plain_query(right_literal->value(options), *analyzer_ctx));
    // The prefix of a phrase prefix is scored by the terms it expands to, so the statistics are
    // collected for them per segment; a prefix alone scores a constant and needs none.
    std::optional<TermInfo> scoring_prefix;
    if (expr->op() == TExprOpcode::MATCH_PHRASE_PREFIX && !term_infos.empty()) {
        if (term_infos.size() > 1) {
            scoring_prefix = term_infos.back();
        }
        term_infos.pop_back();
    }

    std::string field_name =
            build_field_name(index_meta->col_unique_ids()[0], candidates.index_suffix_path);
    std::wstring lucene_field = inverted_index::StringHelper::to_wstring(field_name);
    std::wstring ws_field_name =
            segment_v2::match_statistics_field(lucene_field, index_meta->index_id());

    auto iter = collect_infos->find(ws_field_name);
    if (iter == collect_infos->end()) {
        CollectInfo collect_info;
        collect_info.lucene_field = std::move(lucene_field);
        RETURN_IF_ERROR(append_scoring_leaf(&collect_info, term_infos));
        preserve_selected_index_metadata(candidates, index_meta, &collect_info);
        iter = collect_infos->emplace(ws_field_name, std::move(collect_info)).first;
    } else {
        RETURN_IF_ERROR(validate_same_physical_index(iter->second, *index_meta));
        RETURN_IF_ERROR(append_scoring_leaf(&iter->second, term_infos));
    }
    if (scoring_prefix.has_value()) {
        // The terms of a prefix are kept for the whole scan and copied to every scanner, so their
        // number is bounded. Under a session limit of 0, which expands a prefix to every term,
        // "quick b" would keep the whole vocabulary starting with b, and a sum of thousands of
        // idfs ranks nothing; the default limit scores it instead.
        const int32_t session_limit = index_query::max_expansions(state);
        append_scoring_prefix(&iter->second, *scoring_prefix,
                              session_limit > 0 ? session_limit : kDefaultScoringExpansions);
    }

    return Status::OK();
}

} // namespace doris
