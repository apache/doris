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

#include <string_view>

#include "common/exception.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "olap/rowset/segment_v2/inverted_index/util/string_helper.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {

using namespace segment_v2::inverted_index;
using namespace segment_v2;

Status CollectionStatistics::collect(
        const std::vector<RowSetSplits>& rs_splits, const TabletSchemaSPtr& tablet_schema,
        const vectorized::VExprContextSPtrs& common_expr_ctxs_push_down) {
    class CollectInfo {
    public:
        std::vector<TermInfo> term_infos;
        const TabletIndex* index_meta = nullptr;
    };

    std::unordered_map<std::wstring, CollectInfo> collect_infos;

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
                auto* left_slot_ref = static_cast<vectorized::VSlotRef*>(expr->children()[0].get());
                auto* right_slot_ref =
                        static_cast<vectorized::VLiteral*>(expr->children()[1].get());
                auto column_idx = tablet_schema->field_index(left_slot_ref->column_name());
                auto column = tablet_schema->column(column_idx);
                const auto* index_meta = tablet_schema->inverted_index(column);

                std::string field_name = std::to_string(column.unique_id());
                std::wstring ws_field_name = StringHelper::to_wstring(field_name);
                auto iter = collect_infos.find(ws_field_name);
                if (iter == collect_infos.end()) {
                    CollectInfo collect_info;
                    collect_info.term_infos = InvertedIndexAnalyzer::get_analyse_result(
                            right_slot_ref->value(), index_meta->properties());
                    collect_info.index_meta = index_meta;
                    collect_infos[ws_field_name] = std::move(collect_info);
                } else {
                    auto term_infos = InvertedIndexAnalyzer::get_analyse_result(
                            right_slot_ref->value(), index_meta->properties());
                    iter->second.term_infos.insert(iter->second.term_infos.end(),
                                                   std::make_move_iterator(term_infos.begin()),
                                                   std::make_move_iterator(term_infos.end()));
                }
            }

            const auto& children = expr->children();
            for (int32_t i = children.size() - 1; i >= 0; --i) {
                if (!children[i]->children().empty()) {
                    stack.emplace(children[i]);
                }
            }
        }
    }

    for (const auto& rs_split : rs_splits) {
        const auto& rs_reader = rs_split.rs_reader;
        auto rowset = rs_reader->rowset();
        auto rowset_meta = rowset->rowset_meta();

        auto num_segments = rowset->num_segments();
        for (int32_t seg_id = 0; seg_id < num_segments; ++seg_id) {
            auto seg_path = DORIS_TRY(rowset->segment_path(seg_id));
            auto idx_file_reader = std::make_unique<IndexFileReader>(
                    rowset_meta->fs(),
                    std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
                    tablet_schema->get_inverted_index_storage_format());
            RETURN_IF_ERROR(idx_file_reader->init());
            for (const auto& [ws_field_name, collect_info] : collect_infos) {
                auto compound_reader =
                        DORIS_TRY(idx_file_reader->open(collect_info.index_meta, nullptr));
                auto* dir = compound_reader.release();
                lucene::index::IndexReader* reader = nullptr;
                ErrorContext error_context;
                try {
                    reader = lucene::index::IndexReader::open(dir);

                    _total_num_docs += reader->maxDoc();
                    _total_num_tokens[ws_field_name] +=
                            reader->sumTotalTermFreq(ws_field_name.c_str()).value_or(0);

                    for (const auto& term_info : collect_info.term_infos) {
                        auto iter = TermIterator::create(nullptr, reader, ws_field_name,
                                                         term_info.get_single_term());
                        _term_doc_freqs[ws_field_name][iter->term()] += iter->doc_freq();
                    }

                } catch (const CLuceneError& e) {
                    error_context.eptr = std::current_exception();
                    error_context.err_msg.append("clucene reader open error: ");
                    error_context.err_msg.append(e.what());
                    LOG(ERROR) << error_context.err_msg;
                }
                FINALLY({
                    FINALLY_CLOSE(reader);
                    _CLLDELETE(reader);
                    _CLDECDELETE(dir)
                })
            }
        }
    }

    LOG(ERROR) << "term_num_docs: " << _total_num_docs;
    for (const auto& [ws_field_name, num_tokens] : _total_num_tokens) {
        LOG(ERROR) << "field_name: " << StringHelper::to_string(ws_field_name)
                   << ", num_tokens: " << num_tokens;
        for (const auto& [term, doc_freq] : _term_doc_freqs[ws_field_name]) {
            LOG(ERROR) << "term: " << StringHelper::to_string(term) << ", doc_freq: " << doc_freq;
        }
    }
    LOG(ERROR) << "--------------------------------";

    return Status::OK();
}

uint64_t CollectionStatistics::get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                                        const std::wstring& term) {
    if (!_term_doc_freqs.contains(lucene_col_name)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR, "Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    if (!_term_doc_freqs[lucene_col_name].contains(term)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR, "Not such term {}",
                        StringHelper::to_string(term));
    }

    return _term_doc_freqs[lucene_col_name][term];
}

uint64_t CollectionStatistics::get_total_term_cnt_by_col(const std::wstring& lucene_col_name) {
    if (!_total_num_tokens.contains(lucene_col_name)) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR, "Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    return _total_num_tokens[lucene_col_name];
}

uint64_t CollectionStatistics::get_doc_num() const {
    if (_total_num_docs == 0) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "No data available for SimilarityCollector");
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
    float avg_dl = total_doc_cnt > 0 ? (1.0 * total_term_cnt / total_doc_cnt) : 0;
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
    float idf = std::log(1 + (doc_num - doc_freq + (double)0.5) / (doc_freq + (double)0.5));
    _idf_by_col_term[lucene_col_name][term] = idf;
    return idf;
}

} // namespace doris