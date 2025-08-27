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

#include "collection_statistics.h"

#include "common/exception.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_reader_helper.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
#include "common/compile_check_begin.h"

Status CollectionStatistics::collect(
        RuntimeState* state, const std::vector<RowSetSplits>& rs_splits,
        const TabletSchemaSPtr& tablet_schema,
        const vectorized::VExprContextSPtrs& common_expr_ctxs_push_down) {
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    RETURN_IF_ERROR(
            extract_collect_info(state, common_expr_ctxs_push_down, tablet_schema, &collect_infos));

    for (const auto& rs_split : rs_splits) {
        const auto& rs_reader = rs_split.rs_reader;
        auto rowset = rs_reader->rowset();
        auto rowset_meta = rowset->rowset_meta();

        auto num_segments = rowset->num_segments();
        for (int32_t seg_id = 0; seg_id < num_segments; ++seg_id) {
            auto seg_path = DORIS_TRY(rowset->segment_path(seg_id));
            auto status = process_segment(seg_path, rowset_meta->fs(), tablet_schema.get(),
                                          collect_infos);
            if (!status.ok()) {
                if (status.code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND ||
                    status.code() == ErrorCode::INVERTED_INDEX_BYPASS) {
                    LOG(ERROR) << "Index statistics collection failed: " << status.to_string();
                } else {
                    return status;
                }
            }
        }
    }

#ifndef NDEBUG
    LOG(INFO) << "term_num_docs: " << _total_num_docs;
    for (const auto& [ws_field_name, num_tokens] : _total_num_tokens) {
        LOG(INFO) << "field_name: " << StringHelper::to_string(ws_field_name)
                  << ", num_tokens: " << num_tokens;
        for (const auto& [term, doc_freq] : _term_doc_freqs.at(ws_field_name)) {
            LOG(INFO) << "term: " << StringHelper::to_string(term) << ", doc_freq: " << doc_freq;
        }
    }
    LOG(INFO) << "--------------------------------";
#endif

    return Status::OK();
}

vectorized::VSlotRef* find_slot_ref(const vectorized::VExprSPtr& expr) {
    if (!expr) return nullptr;
    auto cur = vectorized::VExpr::expr_without_cast(expr);
    if (cur->node_type() == TExprNodeType::SLOT_REF) {
        return static_cast<vectorized::VSlotRef*>(cur.get());
    }
    for (auto& ch : cur->children()) {
        if (auto* s = find_slot_ref(ch)) return s;
    }
    return nullptr;
}

Status handle_match_pred(RuntimeState* state, const TabletSchemaSPtr& tablet_schema,
                         const vectorized::VExprSPtr& expr,
                         std::unordered_map<std::wstring, CollectInfo>* collect_infos) {
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
        if (!segment_v2::IndexReaderHelper::is_need_similarity_score(expr->op(), index_meta)) {
            continue;
        }

        auto term_infos = InvertedIndexAnalyzer::get_analyse_result(right_literal->value(),
                                                                    index_meta->properties());

        std::string field_name = std::to_string(index_meta->col_unique_ids()[0]);
        if (!column.suffix_path().empty()) {
            field_name += "." + column.suffix_path();
        }
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

Status CollectionStatistics::extract_collect_info(
        RuntimeState* state, const vectorized::VExprContextSPtrs& common_expr_ctxs_push_down,
        const TabletSchemaSPtr& tablet_schema,
        std::unordered_map<std::wstring, CollectInfo>* collect_infos) {
    for (const auto& root_expr_ctx : common_expr_ctxs_push_down) {
        const auto& root_expr = root_expr_ctx->root();
        if (root_expr == nullptr) {
            continue;
        }

        std::stack<vectorized::VExprSPtr> stack;
        stack.emplace(root_expr);

        while (!stack.empty()) {
            const auto& expr = stack.top();
            stack.pop();

            if (expr->node_type() == TExprNodeType::MATCH_PRED) {
                RETURN_IF_ERROR(handle_match_pred(state, tablet_schema, expr, collect_infos));
            }

            const auto& children = expr->children();
            for (int32_t i = static_cast<int32_t>(children.size()) - 1; i >= 0; --i) {
                if (!children[i]->children().empty()) {
                    stack.emplace(children[i]);
                }
            }
        }
    }
    return Status::OK();
}

Status CollectionStatistics::process_segment(
        const std::string& seg_path, const io::FileSystemSPtr& fs,
        const TabletSchema* tablet_schema,
        const std::unordered_map<std::wstring, CollectInfo>& collect_infos) {
    auto idx_file_reader = std::make_unique<IndexFileReader>(
            fs, std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
            tablet_schema->get_inverted_index_storage_format());
    RETURN_IF_ERROR(idx_file_reader->init());

    int32_t total_seg_num_docs = 0;
    for (const auto& [ws_field_name, collect_info] : collect_infos) {
#ifdef BE_TEST
        auto compound_reader = DORIS_TRY(idx_file_reader->open(collect_info.index_meta, nullptr));
        auto* reader = lucene::index::IndexReader::open(compound_reader.get());
        auto index_searcher = std::make_shared<lucene::search::IndexSearcher>(reader, true);

        auto* index_reader = index_searcher->getReader();
#else
        InvertedIndexCacheHandle inverted_index_cache_handle;
        auto index_file_key = idx_file_reader->get_index_file_cache_key(collect_info.index_meta);
        InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);
        if (!InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                            &inverted_index_cache_handle)) {
            auto compound_reader =
                    DORIS_TRY(idx_file_reader->open(collect_info.index_meta, nullptr));
            auto* reader = lucene::index::IndexReader::open(compound_reader.get());
            size_t reader_size = reader->getTermInfosRAMUsed();
            auto index_searcher = std::make_shared<lucene::search::IndexSearcher>(reader, true);
            auto* cache_value = new InvertedIndexSearcherCache::CacheValue(
                    std::move(index_searcher), reader_size, UnixMillis());
            InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                           &inverted_index_cache_handle);
        }

        auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
        auto index_searcher = std::get<FulltextIndexSearcherPtr>(searcher_variant);
        auto* index_reader = index_searcher->getReader();
#endif

        total_seg_num_docs = std::max(total_seg_num_docs, index_reader->maxDoc());
        _total_num_tokens[ws_field_name] +=
                index_reader->sumTotalTermFreq(ws_field_name.c_str()).value_or(0);

        for (const auto& term_info : collect_info.term_infos) {
            auto iter = TermIterator::create(nullptr, false, index_reader, ws_field_name,
                                             term_info.get_single_term());
            _term_doc_freqs[ws_field_name][iter->term()] += iter->doc_freq();
        }
    }
    _total_num_docs += total_seg_num_docs;
    return Status::OK();
}

uint64_t CollectionStatistics::get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                                        const std::wstring& term) {
    if (!_term_doc_freqs.contains(lucene_col_name)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    if (!_term_doc_freqs[lucene_col_name].contains(term)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such term {}",
                        StringHelper::to_string(term));
    }

    return _term_doc_freqs[lucene_col_name][term];
}

uint64_t CollectionStatistics::get_total_term_cnt_by_col(const std::wstring& lucene_col_name) {
    if (!_total_num_tokens.contains(lucene_col_name)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    return _total_num_tokens[lucene_col_name];
}

uint64_t CollectionStatistics::get_doc_num() const {
    if (_total_num_docs == 0) {
        throw Exception(
                ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                "Index statistics collection failed: No data available for SimilarityCollector");
    }

    return _total_num_docs;
}

float CollectionStatistics::get_or_calculate_avg_dl(const std::wstring& lucene_col_name) {
    auto iter = _avg_dl_by_col.find(lucene_col_name);
    if (iter != _avg_dl_by_col.end()) {
        return iter->second;
    }

    const uint64_t total_term_cnt = get_total_term_cnt_by_col(lucene_col_name);
    const uint64_t total_doc_cnt = get_doc_num();
    float avg_dl = total_doc_cnt > 0 ? float((double)total_term_cnt / (double)total_doc_cnt) : 0.0F;
    _avg_dl_by_col[lucene_col_name] = avg_dl;
    return avg_dl;
}

float CollectionStatistics::get_or_calculate_idf(const std::wstring& lucene_col_name,
                                                 const std::wstring& term) {
    auto iter = _idf_by_col_term.find(lucene_col_name);
    if (iter != _idf_by_col_term.end()) {
        auto term_iter = iter->second.find(term);
        if (term_iter != iter->second.end()) {
            return term_iter->second;
        }
    }

    const uint64_t doc_num = get_doc_num();
    const uint64_t doc_freq = get_term_doc_freq_by_col(lucene_col_name, term);
    auto idf = (float)std::log(1 + ((double)doc_num - (double)doc_freq + (double)0.5) /
                                           ((double)doc_freq + (double)0.5));
    _idf_by_col_term[lucene_col_name][term] = idf;
    return idf;
}

#include "common/compile_check_end.h"
} // namespace doris