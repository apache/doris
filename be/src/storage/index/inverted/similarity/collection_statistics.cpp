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

#include "storage/index/inverted/similarity/collection_statistics.h"

#include <exception>
#include <set>
#include <sstream>

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
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/reader/dict_block_cache.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "util/uid_util.h"

namespace doris {
namespace {

struct SniiScoringSegmentStats {
    uint64_t doc_count = 0;
    uint64_t token_count = 0;
    segment_v2::inverted_index::PlainTermKeyVersion plain_term_key_version =
            segment_v2::inverted_index::PlainTermKeyVersion::kLegacyRaw;
    std::string base_analyzer_fingerprint;
};

Result<SniiScoringSegmentStats> resolve_snii_scoring_segment(
        const std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata>& metadata,
        uint64_t index_doc_count, uint64_t physical_sum_total_term_freq, bool has_scoring_tier,
        bool has_positions, bool has_semantic_norms) {
    using namespace segment_v2::inverted_index;
    const auto* metadata_ptr = metadata ? &*metadata : nullptr;
    auto validation_status = validate_snii_scoring_metadata(
            metadata_ptr, index_doc_count, physical_sum_total_term_freq, has_scoring_tier,
            has_positions, has_semantic_norms);
    if (!validation_status.ok()) {
        return ResultError(std::move(validation_status));
    }
    DORIS_CHECK(metadata_ptr != nullptr);

    return SniiScoringSegmentStats {
            .doc_count = metadata_ptr->scoring_doc_count,
            .token_count = metadata_ptr->scoring_token_count,
            .plain_term_key_version = metadata_ptr->plain_term_key_version,
            .base_analyzer_fingerprint = metadata_ptr->base_analyzer_fingerprint};
}

void add_term_doc_frequency(
        std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>>*
                logical_frequencies,
        const std::wstring& field, const std::wstring& logical_term, uint64_t doc_frequency) {
    DORIS_CHECK(logical_frequencies != nullptr);
    (*logical_frequencies)[field][logical_term] += doc_frequency;
}

} // namespace

Status CollectionStatistics::collect(RuntimeState* state,
                                     const std::vector<RowSetSplits>& rs_splits,
                                     const TabletSchemaSPtr& tablet_schema,
                                     const VExprContextSPtrs& common_expr_ctxs_push_down,
                                     io::IOContext* io_ctx) {
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(rs_splits.size());
    for (const auto& rs_split : rs_splits) {
        DORIS_CHECK(rs_split.rs_reader != nullptr);
        auto rowset = rs_split.rs_reader->rowset();
        DORIS_CHECK(rowset != nullptr);
        rowsets.emplace_back(std::move(rowset));
    }
    return collect_full_collection(state, rowsets, tablet_schema, common_expr_ctxs_push_down,
                                   io_ctx);
}

Status CollectionStatistics::collect_full_collection(
        RuntimeState* state, const std::vector<RowsetSharedPtr>& rowsets,
        const TabletSchemaSPtr& tablet_schema, const VExprContextSPtrs& common_expr_ctxs_push_down,
        io::IOContext* io_ctx) {
    clear();
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    RETURN_IF_ERROR(
            extract_collect_info(state, common_expr_ctxs_push_down, tablet_schema, &collect_infos));
    if (collect_infos.empty()) {
        LOG(WARNING) << "Index statistics collection: no collect info extracted.";
        return Status::OK();
    }

    for (const auto& rowset : rowsets) {
        DORIS_CHECK(rowset != nullptr);
        const auto num_segments = rowset->num_segments();
        for (int64_t seg_id = 0; seg_id < num_segments; ++seg_id) {
            auto status =
                    process_segment(rowset, seg_id, tablet_schema.get(), collect_infos, io_ctx);
            if (!status.ok()) {
                if (tablet_schema->get_inverted_index_storage_format() !=
                            InvertedIndexStorageFormatPB::SNII &&
                    (status.code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND ||
                     status.code() == ErrorCode::INVERTED_INDEX_BYPASS)) {
                    LOG(ERROR) << "Index statistics collection failed: " << status.to_string();
                    continue;
                }
                clear();
                return status;
            }
        }
    }

    // Build a single-line log with query_id, tablet_ids, and per-field term statistics
    if (VLOG_IS_ON(1)) {
        std::set<int64_t> tablet_ids;
        for (const auto& rowset : rowsets) {
            DORIS_CHECK(rowset != nullptr);
            tablet_ids.insert(rowset->rowset_meta()->tablet_id());
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

            auto field_term_doc_freqs = _term_doc_freqs.find(ws_field_name);
            if (field_term_doc_freqs != _term_doc_freqs.end()) {
                bool first_term = true;
                for (const auto& [term, doc_freq] : field_term_doc_freqs->second) {
                    if (!first_term) oss << ", ";
                    oss << "(" << StringHelper::to_string(term) << ":" << doc_freq << ")";
                    first_term = false;
                }
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
            for (auto child = children.rbegin(); child != children.rend(); ++child) {
                stack.push(*child);
            }
        }
    }

    LOG(INFO) << "Extracted collect info for " << collect_infos->size() << " fields";

    return Status::OK();
}

Status CollectionStatistics::process_segment(const RowsetSharedPtr& rowset, int64_t seg_id,
                                             const TabletSchema* tablet_schema,
                                             const CollectInfoMap& collect_infos,
                                             io::IOContext* io_ctx) {
    auto seg_path = DORIS_TRY(rowset->segment_path(seg_id));
    auto rowset_meta = rowset->rowset_meta();

    auto idx_file_reader = std::make_unique<IndexFileReader>(
            rowset_meta->fs(),
            std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
            tablet_schema->get_inverted_index_storage_format(),
            rowset_meta->inverted_index_file_info(static_cast<int>(seg_id)),
            rowset_meta->tablet_id());
    const bool is_snii =
            idx_file_reader->get_storage_format() == InvertedIndexStorageFormatPB::SNII;
    auto init_status = idx_file_reader->init(config::inverted_index_read_buffer_size, io_ctx);
    if (!init_status.ok()) {
        if (is_snii && (init_status.code() == ErrorCode::NOT_FOUND ||
                        init_status.code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND ||
                        init_status.code() == ErrorCode::INVERTED_INDEX_BYPASS)) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "SNII scoring requires every collection segment: {}", init_status.msg());
        }
        return init_status;
    }

    if (is_snii) {
        segment_v2::snii_doris::DorisSniiFileReader::ScopedIOContext io_context_scope(io_ctx);
        SniiScoringSegmentAccumulator segment_accumulator;
        for (const auto& [ws_field_name, collect_info] : collect_infos) {
            auto logical_reader_result =
                    idx_file_reader->open_snii_index(collect_info.index_meta, io_ctx);
            if (!logical_reader_result.has_value()) {
                auto status = std::move(logical_reader_result.error());
                if (status.code() == ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND ||
                    status.code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND ||
                    status.code() == ErrorCode::INVERTED_INDEX_BYPASS) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                            "SNII scoring requires every logical index in the collection: {}",
                            status.msg());
                }
                return status;
            }
            auto logical_reader = std::move(logical_reader_result.value());
            const auto* common_grams_metadata = logical_reader->common_grams_metadata();
            segment_v2::inverted_index::PlainTermKeyVersion key_version;
            RETURN_IF_ERROR(admit_snii_scoring_segment(
                    ws_field_name,
                    common_grams_metadata == nullptr
                            ? std::nullopt
                            : std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata>(
                                      *common_grams_metadata),
                    collect_info.expected_base_analyzer_fingerprint,
                    logical_reader->stats().doc_count, logical_reader->stats().sum_total_term_freq,
                    logical_reader->tier() == ::doris::snii::format::IndexTier::kT3,
                    logical_reader->has_positions(),
                    logical_reader->section_refs().norms.length != 0, &key_version,
                    &segment_accumulator));
            DORIS_CHECK(common_grams_metadata != nullptr);

            ::doris::snii::reader::DictBlockCache dict_block_cache;
            for (const auto& logical_term_bytes : collect_info.unique_terms) {
                std::string physical_term;
                const bool term_present =
                        DORIS_TRY(segment_v2::inverted_index::try_encode_plain_term(
                                logical_term_bytes, key_version, &physical_term));
                const auto logical_term =
                        segment_v2::inverted_index::StringHelper::to_wstring(logical_term_bytes);
                if (!term_present) {
                    add_term_doc_frequency(&segment_accumulator.term_doc_freqs, ws_field_name,
                                           logical_term, 0);
                    continue;
                }

                bool found = false;
                ::doris::snii::format::DictEntry entry;
                uint64_t frq_base = 0;
                uint64_t prx_base = 0;
                RETURN_IF_ERROR(logical_reader->lookup(physical_term, &found, &entry, &frq_base,
                                                       &prx_base, &dict_block_cache));
                if (found && entry.df > common_grams_metadata->scoring_doc_count) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                            "SNII term document frequency {} exceeds scoring document count {}",
                            entry.df, common_grams_metadata->scoring_doc_count);
                }
                add_term_doc_frequency(&segment_accumulator.term_doc_freqs, ws_field_name,
                                       logical_term, found ? entry.df : 0);
            }
        }

        commit_snii_scoring_segment(std::move(segment_accumulator));
        return Status::OK();
    }

    int32_t total_segment_docs = 0;

    for (const auto& [ws_field_name, collect_info] : collect_infos) {
        lucene::search::IndexSearcher* index_searcher = nullptr;
        lucene::index::IndexReader* index_reader = nullptr;

#ifdef BE_TEST
        auto compound_reader = DORIS_TRY(idx_file_reader->open(collect_info.index_meta, io_ctx));
        auto* reader = lucene::index::IndexReader::open(compound_reader.get());
        auto owned_index_searcher = std::make_shared<lucene::search::IndexSearcher>(reader, true);
        index_searcher = owned_index_searcher.get();
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
        total_segment_docs = std::max(total_segment_docs, index_reader->maxDoc());
        _total_num_tokens[ws_field_name] +=
                index_reader->sumTotalTermFreq(ws_field_name.c_str()).value_or(0);

        for (const auto& logical_term_bytes : collect_info.unique_terms) {
            const auto logical_term =
                    segment_v2::inverted_index::StringHelper::to_wstring(logical_term_bytes);
            auto iter = TermIterator::create(io_ctx, false, index_reader, ws_field_name,
                                             logical_term_bytes);
            add_term_doc_frequency(&_term_doc_freqs, ws_field_name, logical_term, iter->doc_freq());
        }
    }

    _total_num_docs += static_cast<uint64_t>(total_segment_docs);
    _avg_dl_by_col.clear();
    _idf_by_col_term.clear();

    return Status::OK();
}

Status CollectionStatistics::admit_snii_scoring_segment(
        const std::wstring& field_name,
        const std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata>& metadata,
        std::string_view expected_base_analyzer_fingerprint, uint64_t index_doc_count,
        uint64_t physical_sum_total_term_freq, bool has_scoring_tier, bool has_positions,
        bool has_semantic_norms,
        segment_v2::inverted_index::PlainTermKeyVersion* plain_term_key_version,
        SniiScoringSegmentAccumulator* segment_accumulator) {
    DORIS_CHECK(plain_term_key_version != nullptr);
    DORIS_CHECK(segment_accumulator != nullptr);
    auto segment_stats =
            resolve_snii_scoring_segment(metadata, index_doc_count, physical_sum_total_term_freq,
                                         has_scoring_tier, has_positions, has_semantic_norms);
    if (!segment_stats.has_value()) {
        clear();
        return segment_stats.error();
    }

    const auto& base_analyzer_fingerprint = segment_stats->base_analyzer_fingerprint;
    if (base_analyzer_fingerprint != expected_base_analyzer_fingerprint) {
        clear();
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII scoring segment base analyzer does not match the request analyzer for field "
                "{}",
                StringHelper::to_string(field_name));
    }
    auto staged_fingerprint = segment_accumulator->base_analyzer_fingerprints.find(field_name);
    if (staged_fingerprint != segment_accumulator->base_analyzer_fingerprints.end() &&
        staged_fingerprint->second != base_analyzer_fingerprint) {
        clear();
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII scoring cannot combine segments with different base analyzers for field {}",
                StringHelper::to_string(field_name));
    }
    auto collected_fingerprint = _snii_base_analyzer_fingerprints.find(field_name);
    if (collected_fingerprint != _snii_base_analyzer_fingerprints.end() &&
        collected_fingerprint->second != base_analyzer_fingerprint) {
        clear();
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII scoring cannot combine segments with different base analyzers for field {}",
                StringHelper::to_string(field_name));
    }
    segment_accumulator->base_analyzer_fingerprints.insert_or_assign(field_name,
                                                                     base_analyzer_fingerprint);

    if (!segment_accumulator->token_counts.empty() &&
        segment_accumulator->doc_count != segment_stats->doc_count) {
        clear();
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII scoring fields in one segment have different document counts: {} and {}",
                segment_accumulator->doc_count, segment_stats->doc_count);
    }
    segment_accumulator->doc_count = segment_stats->doc_count;
    segment_accumulator->token_counts[field_name] += segment_stats->token_count;
    *plain_term_key_version = segment_stats->plain_term_key_version;
    return Status::OK();
}

void CollectionStatistics::commit_snii_scoring_segment(
        SniiScoringSegmentAccumulator&& segment_accumulator) {
    for (const auto& [field_name, base_analyzer_fingerprint] :
         segment_accumulator.base_analyzer_fingerprints) {
        auto [collected_fingerprint, inserted] =
                _snii_base_analyzer_fingerprints.try_emplace(field_name, base_analyzer_fingerprint);
        DORIS_CHECK(inserted || collected_fingerprint->second == base_analyzer_fingerprint);
    }
    for (const auto& [field_name, token_count] : segment_accumulator.token_counts) {
        _total_num_tokens[field_name] += token_count;
    }
    for (const auto& [field_name, term_doc_freqs] : segment_accumulator.term_doc_freqs) {
        for (const auto& [term, doc_freq] : term_doc_freqs) {
            _term_doc_freqs[field_name][term] += doc_freq;
        }
    }
    _total_num_docs += segment_accumulator.doc_count;
    _avg_dl_by_col.clear();
    _idf_by_col_term.clear();
}

void CollectionStatistics::clear() {
    _total_num_docs = 0;
    _total_num_tokens.clear();
    _term_doc_freqs.clear();
    _snii_base_analyzer_fingerprints.clear();
    _avg_dl_by_col.clear();
    _idf_by_col_term.clear();
}

uint64_t CollectionStatistics::get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                                        const std::wstring& term) {
    const auto field = _term_doc_freqs.find(lucene_col_name);
    if (field == _term_doc_freqs.end()) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    const auto term_frequency = field->second.find(term);
    if (term_frequency == field->second.end()) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such term {}",
                        StringHelper::to_string(term));
    }

    return term_frequency->second;
}

uint64_t CollectionStatistics::get_total_term_cnt_by_col(const std::wstring& lucene_col_name) {
    const auto token_count = _total_num_tokens.find(lucene_col_name);
    if (token_count == _total_num_tokens.end()) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        StringHelper::to_string(lucene_col_name));
    }

    return token_count->second;
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

} // namespace doris
