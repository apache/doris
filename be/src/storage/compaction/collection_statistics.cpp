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

#include "storage/compaction/collection_statistics.h"

#include <mutex>
#include <set>
#include <sstream>
#include <utility>

#include "common/exception.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/util/string_helper.h"
#include "storage/index/inverted/util/term_iterator.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "util/uid_util.h"

namespace doris {

Status CollectionStatistics::_collect_rowset(const RowsetSharedPtr& rowset,
                                             const TabletSchemaSPtr& tablet_schema,
                                             const CollectInfoMap& collect_infos,
                                             io::IOContext* io_ctx) {
    const auto num_segments = rowset->num_segments();
    for (int32_t seg_id = 0; seg_id < num_segments; ++seg_id) {
        auto status = process_segment(rowset, seg_id, tablet_schema.get(), collect_infos, io_ctx);
        if (!status.ok()) {
            // A missing or bypassed index means this segment contributes nothing; the collection is
            // still usable, so log and keep going rather than failing the query.
            if (status.code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND ||
                status.code() == ErrorCode::INVERTED_INDEX_BYPASS) {
                LOG(ERROR) << "Index statistics collection failed: " << status.to_string();
            } else {
                return status;
            }
        }
    }
    return Status::OK();
}

Status CollectionStatistics::collect(RuntimeState* state,
                                     const std::vector<RowSetSplits>& rs_splits,
                                     const TabletSchemaSPtr& tablet_schema,
                                     const VExprContextSPtrs& common_expr_ctxs_push_down,
                                     io::IOContext* io_ctx) {
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    RETURN_IF_ERROR(
            extract_collect_info(state, common_expr_ctxs_push_down, tablet_schema, &collect_infos));
    if (collect_infos.empty()) {
        LOG(WARNING) << "Index statistics collection: no collect info extracted.";
        return Status::OK();
    }

    for (const auto& rs_split : rs_splits) {
        RETURN_IF_ERROR(_collect_rowset(rs_split.rs_reader->rowset(), tablet_schema, collect_infos,
                                        io_ctx));
    }

    // Build a single-line log with query_id, tablet_ids, and per-field term statistics
    if (VLOG_IS_ON(1)) {
        std::set<int64_t> tablet_ids;
        for (const auto& rs_split : rs_splits) {
            if (rs_split.rs_reader && rs_split.rs_reader->rowset()) {
                tablet_ids.insert(rs_split.rs_reader->rowset()->rowset_meta()->tablet_id());
            }
        }

        std::ostringstream oss;
        oss << "CollectionStatistics: query_id=" << print_id(state->query_id());

        oss << ", tablet_ids=[";
        bool first_tablet = true;
        for (int64_t tid : tablet_ids) {
            if (!first_tablet) oss << ",";
            oss << tid;
            first_tablet = false;
        }
        oss << "]";

        oss << ", total_num_docs=" << _total_num_docs;

        for (const auto& [ws_field_name, num_tokens] : _total_num_tokens) {
            oss << ", {field=" << StringHelper::to_string(ws_field_name)
                << ", num_tokens=" << num_tokens << ", terms=[";

            bool first_term = true;
            for (const auto& [term, doc_freq] : _term_doc_freqs.at(ws_field_name)) {
                if (!first_term) oss << ", ";
                oss << "(" << StringHelper::to_string(term) << ":" << doc_freq << ")";
                first_term = false;
            }
            oss << "]}";
        }

        VLOG(1) << oss.str();
    }

    return Status::OK();
}

Status CollectionStatistics::extract_collect_info(
        RuntimeState* state, const VExprContextSPtrs& common_expr_ctxs_push_down,
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

        std::stack<VExprSPtr> stack;
        stack.emplace(root_expr);

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

Status CollectionStatistics::process_segment(const RowsetSharedPtr& rowset, int32_t seg_id,
                                             const TabletSchema* tablet_schema,
                                             const CollectInfoMap& collect_infos,
                                             io::IOContext* io_ctx) {
    auto seg_path = DORIS_TRY(rowset->segment_path(seg_id));
    auto rowset_meta = rowset->rowset_meta();

    auto idx_file_reader = std::make_unique<IndexFileReader>(
            rowset_meta->fs(),
            std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
            tablet_schema->get_inverted_index_storage_format(),
            rowset_meta->inverted_index_file_info(seg_id), rowset_meta->tablet_id());
    RETURN_IF_ERROR(idx_file_reader->init(config::inverted_index_read_buffer_size, io_ctx));

    int32_t total_seg_num_docs = 0;

    for (const auto& [ws_field_name, collect_info] : collect_infos) {
        lucene::search::IndexSearcher* index_searcher = nullptr;
        lucene::index::IndexReader* index_reader = nullptr;

#ifdef BE_TEST
        auto compound_reader = DORIS_TRY(idx_file_reader->open(collect_info.index_meta, io_ctx));
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
                    DORIS_TRY(idx_file_reader->open(collect_info.index_meta, io_ctx));
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
            auto iter = TermIterator::create(io_ctx, false, index_reader, ws_field_name,
                                             term_info.get_single_term());
            _term_doc_freqs[ws_field_name][iter->term()] += iter->doc_freq();
        }
    }

    _total_num_docs += total_seg_num_docs;

    return Status::OK();
}

Status CollectionStatistics::collect_full_collection(
        RuntimeState* state, const std::vector<RowsetSharedPtr>& rowsets,
        const TabletSchemaSPtr& tablet_schema,
        const VExprContextSPtrs& common_expr_ctxs_push_down, io::IOContext* io_ctx) {
    CollectInfoMap collect_infos;
    RETURN_IF_ERROR(
            extract_collect_info(state, common_expr_ctxs_push_down, tablet_schema, &collect_infos));
    if (collect_infos.empty()) {
        LOG(WARNING) << "Index statistics collection: no collect info extracted.";
        return Status::OK();
    }

    for (const auto& rowset : rowsets) {
        RETURN_IF_ERROR(_collect_rowset(rowset, tablet_schema, collect_infos, io_ctx));
    }
    return Status::OK();
}

CollectionStatisticsPtr CollectionStatistics::clone_for_scanner() const {
    DCHECK(_shared_base != nullptr);
    auto clone = std::make_shared<CollectionStatistics>();
    clone->_shared_base = _shared_base;
    // Derived BM25 caches stay empty so every scanner owns its lazy mutations.
    return clone;
}

void CollectionStatistics::freeze_for_scanners() {
    DCHECK(_shared_base == nullptr);
    auto base = std::make_shared<SharedBaseStatistics>();
    base->total_num_docs = _total_num_docs;
    base->total_num_tokens = std::move(_total_num_tokens);
    base->term_doc_freqs = std::move(_term_doc_freqs);
    _shared_base = std::move(base);
}

Status CollectionStatisticsBuildState::get_or_build(const Builder& builder,
                                                    CollectionStatisticsPtr* statistics) {
    DCHECK(static_cast<bool>(builder));
    DCHECK(statistics != nullptr);
    *statistics = nullptr;

    bool build = false;
    CollectionStatisticsPtr ready_prototype;
    {
        std::unique_lock lock(_mutex);
        if (_state == State::EMPTY) {
            _state = State::BUILDING;
            build = true;
        } else if (_state == State::BUILDING) {
            _condition.wait(lock,
                            [this] { return _state == State::READY || _state == State::FAILED; });
        }

        if (!build) {
            if (_state == State::FAILED) {
                return _build_status;
            }
            DCHECK(_state == State::READY);
            DCHECK(_prototype != nullptr);
            ready_prototype = _prototype;
        }
    }

    if (!build) {
        *statistics = ready_prototype->clone_for_scanner();
        return Status::OK();
    }

    CollectionStatisticsPtr prototype;
    Status status;
    try {
        prototype = std::make_shared<CollectionStatistics>();
        status = builder(prototype.get(), _rowsets);
        if (status.ok()) {
            prototype->freeze_for_scanners();
        }
    } catch (const Exception& exception) {
        // collect() throws on malformed statistics; a throwing builder must still release the
        // waiters below instead of leaving them blocked in BUILDING forever.
        status = exception.to_status();
    } catch (const std::exception& exception) {
        status = Status::InternalError("Collection statistics build failed: {}", exception.what());
    } catch (...) {
        status = Status::InternalError("Collection statistics build failed with unknown error");
    }

    {
        std::lock_guard lock(_mutex);
        DCHECK(_state == State::BUILDING);
        if (status.ok()) {
            _prototype = std::move(prototype);
            ready_prototype = _prototype;
            _state = State::READY;
        } else {
            _build_status = status;
            _state = State::FAILED;
        }
    }
    _condition.notify_all();

    RETURN_IF_ERROR(status);
    DCHECK(ready_prototype != nullptr);
    *statistics = ready_prototype->clone_for_scanner();
    return Status::OK();
}

uint64_t CollectionStatistics::get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                                        const std::wstring& term) {
    const auto& term_doc_freqs =
            _shared_base == nullptr ? _term_doc_freqs : _shared_base->term_doc_freqs;
    if (!term_doc_freqs.contains(lucene_col_name)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    if (!term_doc_freqs.at(lucene_col_name).contains(term)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such term {}",
                        StringHelper::to_string(term));
    }

    return term_doc_freqs.at(lucene_col_name).at(term);
}

uint64_t CollectionStatistics::get_total_term_cnt_by_col(const std::wstring& lucene_col_name) {
    const auto& total_num_tokens =
            _shared_base == nullptr ? _total_num_tokens : _shared_base->total_num_tokens;
    if (!total_num_tokens.contains(lucene_col_name)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    return total_num_tokens.at(lucene_col_name);
}

uint64_t CollectionStatistics::get_doc_num() const {
    const uint64_t total_num_docs =
            _shared_base == nullptr ? _total_num_docs : _shared_base->total_num_docs;
    if (total_num_docs == 0) {
        throw Exception(
                ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                "Index statistics collection failed: No data available for SimilarityCollector");
    }

    return total_num_docs;
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

} // namespace doris