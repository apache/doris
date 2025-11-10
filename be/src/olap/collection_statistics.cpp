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

#include <stack>

#include "common/exception.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"
#include "olap/rowset/segment_v2/inverted_index/util/term_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
#include "common/compile_check_begin.h"

Status CollectionStatistics::collect(
        RuntimeState* state, const std::vector<RowSetSplits>& rs_splits,
        const TabletSchemaSPtr& tablet_schema,
        const vectorized::VExprContextSPtrs& common_expr_ctxs_push_down) {
    CollectInfoMap collect_infos;
    RETURN_IF_ERROR(
            extract_collect_info(state, common_expr_ctxs_push_down, tablet_schema, &collect_infos));

    if (collect_infos.empty()) {
        LOG(WARNING) << "No terms to collect for statistics";
        return Status::OK();
    }

    RETURN_IF_ERROR(collect_segment_statistics(rs_splits, tablet_schema, collect_infos));

    LOG(ERROR) << "BM25 Statistics Collection Summary:";
    LOG(ERROR) << "  Total documents: " << _total_num_docs;
    for (const auto& [ws_field_name, num_tokens] : _total_num_tokens) {
        LOG(ERROR) << "  Field: " << StringHelper::to_string(ws_field_name)
                   << ", Total tokens: " << num_tokens;
        for (const auto& [term, doc_freq] : _term_doc_freqs.at(ws_field_name)) {
            LOG(ERROR) << "    Term: " << StringHelper::to_string(term)
                       << ", Doc freq: " << doc_freq;
        }
    }
    LOG(ERROR) << "================================";

    return Status::OK();
}

Status CollectionStatistics::extract_collect_info(
        RuntimeState* state, const vectorized::VExprContextSPtrs& common_expr_ctxs_push_down,
        const TabletSchemaSPtr& tablet_schema, CollectInfoMap* collect_infos) {
    DCHECK(collect_infos != nullptr);

    std::unordered_map<TExprNodeType::type, PredicateCollectorPtr> collectors;
    collectors[TExprNodeType::MATCH_PRED] = std::make_unique<MatchPredicateCollector>();
    collectors[TExprNodeType::SEARCH_EXPR] = std::make_unique<SearchPredicateCollector>();

    for (const auto& root_expr_ctx : common_expr_ctxs_push_down) {
        const auto& root_expr = root_expr_ctx->root();
        if (root_expr == nullptr) {
            continue;
        }

        std::stack<vectorized::VExprSPtr> stack;
        stack.push(root_expr);

        while (!stack.empty()) {
            auto expr = stack.top();
            stack.pop();

            if (!expr) {
                continue;
            }

            auto collector_it = collectors.find(expr->node_type());
            if (collector_it != collectors.end()) {
                RETURN_IF_ERROR(
                        collector_it->second->collect(state, tablet_schema, expr, collect_infos));
            }

            const auto& children = expr->children();
            for (const auto& child : children) {
                stack.push(child);
            }
        }
    }

    LOG(INFO) << "Extracted collect info for " << collect_infos->size() << " fields";

    return Status::OK();
}

Status CollectionStatistics::collect_segment_statistics(const std::vector<RowSetSplits>& rs_splits,
                                                        const TabletSchemaSPtr& tablet_schema,
                                                        const CollectInfoMap& collect_infos) {
    int total_segments = 0;
    int processed_segments = 0;
    int failed_segments = 0;

    for (const auto& rs_split : rs_splits) {
        const auto& rs_reader = rs_split.rs_reader;
        auto rowset = rs_reader->rowset();
        auto rowset_meta = rowset->rowset_meta();

        auto num_segments = rowset->num_segments();
        total_segments += num_segments;

        for (int32_t seg_id = 0; seg_id < num_segments; ++seg_id) {
            auto seg_path = DORIS_TRY(rowset->segment_path(seg_id));

            auto status = process_segment(seg_path, rowset_meta->fs(), tablet_schema.get(),
                                          collect_infos);

            if (!status.ok()) {
                if (status.code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND ||
                    status.code() == ErrorCode::INVERTED_INDEX_BYPASS) {
                    LOG(WARNING) << "Segment " << seg_id
                                 << " index file not found: " << status.to_string();
                    failed_segments++;
                } else {
                    LOG(ERROR) << "Failed to process segment " << seg_id << ": "
                               << status.to_string();
                    return status;
                }
            } else {
                processed_segments++;
            }
        }
    }

    LOG(INFO) << "Segment statistics collection completed: "
              << "total=" << total_segments << ", processed=" << processed_segments
              << ", failed=" << failed_segments;

    return Status::OK();
}

Status CollectionStatistics::process_segment(const std::string& seg_path,
                                             const io::FileSystemSPtr& fs,
                                             const TabletSchema* tablet_schema,
                                             const CollectInfoMap& collect_infos) {
    auto idx_file_reader = std::make_unique<IndexFileReader>(
            fs, std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
            tablet_schema->get_inverted_index_storage_format());
    RETURN_IF_ERROR(idx_file_reader->init());

    int32_t total_seg_num_docs = 0;

    for (const auto& [ws_field_name, collect_info] : collect_infos) {
        lucene::search::IndexSearcher* index_searcher = nullptr;
        lucene::index::IndexReader* index_reader = nullptr;

#ifdef BE_TEST
        auto compound_reader = DORIS_TRY(idx_file_reader->open(collect_info.index_meta, nullptr));
        auto* reader = lucene::index::IndexReader::open(compound_reader.get());
        auto searcher_ptr = std::make_shared<lucene::search::IndexSearcher>(reader, true);
        index_searcher = searcher_ptr.get();
        index_reader = index_searcher->getReader();
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
            auto searcher_ptr = std::make_shared<lucene::search::IndexSearcher>(reader, true);
            auto* cache_value = new InvertedIndexSearcherCache::CacheValue(
                    std::move(searcher_ptr), reader_size, UnixMillis());
            InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                           &inverted_index_cache_handle);
        }

        auto searcher_variant = inverted_index_cache_handle.get_index_searcher();
        auto index_searcher_ptr = std::get<FulltextIndexSearcherPtr>(searcher_variant);
        index_searcher = index_searcher_ptr.get();
        index_reader = index_searcher->getReader();
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