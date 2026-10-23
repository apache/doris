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

#include "storage/index/inverted/reader/collection_statistics.h"

#include <algorithm>
#include <exception>
#include <iterator>
#include <set>
#include <sstream>

#include "common/exception.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analysis/analyzer/analyzer.h"
#include "storage/index/inverted/clucene/clucene_index_reader.h"
#include "storage/index/inverted/clucene/searcher_cache_value.h"
#include "storage/index/inverted/clucene/spi/clucene_index_source.h"
#include "storage/index/inverted/common/inverted_index_cache.h"
#include "storage/index/inverted/common/string_helper.h"
#include "storage/index/inverted/snii/reader/snii_index_source.h"
#include "storage/index/inverted/snii/snii_doris_adapter.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/tablet_meta.h"
#include "util/uid_util.h"

namespace doris {
namespace {

Result<std::shared_ptr<lucene::search::IndexSearcher>> open_optional_clucene_stats_searcher(
        const segment_v2::IndexFileReader& file_reader, const TabletIndex* index,
        const io::IOContext* io_ctx) {
    auto compound = file_reader.open(index, io_ctx);
    if (!compound.has_value()) {
        // Variant rowsets can contain only `a` or only `b`. Missing `a` must not discard
        // this segment's available `b` statistics when scoring SEARCH('a:hit OR b:hit').
        const auto& status = compound.error();
        if (status.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>() ||
            status.is<ErrorCode::INVERTED_INDEX_BYPASS>()) {
            return std::shared_ptr<lucene::search::IndexSearcher> {};
        }
        return ResultError(status);
    }
    lucene::index::IndexReader* reader = nullptr;
    try {
        reader = lucene::index::IndexReader::open(compound.value().get());
    } catch (const CLuceneError& e) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                "CLuceneError occurred opening the index for BM25 statistics: {}", e.what()));
    }
    // As the query path does once the index meta is read: the searcher may be cached and read
    // by later queries, which must not use this query's I/O context, and later reads are of
    // index data, not meta.
    auto* stream = compound.value()->getDorisIndexInput();
    stream->setIoContext(nullptr);
    stream->setIndexFile(false);
    return std::make_shared<lucene::search::IndexSearcher>(reader, true);
}

} // namespace

namespace collection_statistics_detail {

Result<SniiScoringSegmentStats> resolve_snii_scoring_segment(uint64_t index_doc_count,
                                                             uint64_t sum_total_term_freq,
                                                             bool has_positions, bool has_norms,
                                                             bool is_analyzed,
                                                             bool single_term_normalizer) {
    // Keywords and lowercase-only normalizers can omit both sections: a matched value
    // contributes one term with unit frequency and length. Other analyzed sources need both.
    if (is_analyzed && !single_term_normalizer && (!has_positions || !has_norms)) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "SNII scoring requires positions and norms; this segment was written without "
                "positions or without norms. Norms are left out when the index sets \"norms\" = "
                "\"false\" or, for a variant path, when inverted_index_skip_norms_for_variant is "
                "on"));
    }
    return SniiScoringSegmentStats {.doc_count = index_doc_count,
                                    .token_count = sum_total_term_freq};
}

void add_term_doc_frequency(
        std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>>*
                logical_frequencies,
        const std::wstring& field, const std::wstring& logical_term, uint64_t doc_frequency) {
    DORIS_CHECK(logical_frequencies != nullptr);
    (*logical_frequencies)[field][logical_term] += doc_frequency;
}

Status sum_non_null_norm_lengths(index_query::IndexSource& source,
                                 std::span<const uint32_t> null_docs, uint64_t* out) {
    if (!null_docs.empty() && null_docs.back() >= source.doc_count()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "NULL row {} is beyond the {} documents of the index", null_docs.back(),
                source.doc_count());
    }
    uint64_t all_lengths = 0;
    RETURN_IF_ERROR(source.sum_norm_lengths(&all_lengths));
    uint64_t null_lengths = 0;
    RETURN_IF_ERROR(source.sum_norm_lengths_of(null_docs, &null_lengths));
    DORIS_CHECK(null_lengths <= all_lengths);
    *out = all_lengths - null_lengths;
    return Status::OK();
}

Status exclude_hidden_docs(index_query::IndexSource& source, std::span<const uint32_t> hidden_docs,
                           const roaring::Roaring& null_docs, uint64_t non_null_length_sum,
                           index_query::IndexScoringStats* stats) {
    DORIS_CHECK(stats != nullptr);
    if (!hidden_docs.empty() && hidden_docs.back() >= stats->doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "hidden row {} is beyond the {} documents of the index", hidden_docs.back(),
                stats->doc_count);
    }
    stats->doc_count -= hidden_docs.size();
    if (!stats->has_norms) {
        // Without norms every document stands for one token.
        stats->token_count = stats->doc_count;
        return Status::OK();
    }
    // The exact token count of the hidden rows is not stored anywhere, and norms hold lengths
    // only approximately, so count the lengths the norms of the returned rows stand for. A
    // replica that has compacted the hidden rows away counts the same rows with the same norms.
    std::vector<uint32_t> hidden_non_null_docs;
    hidden_non_null_docs.reserve(hidden_docs.size());
    std::ranges::copy_if(hidden_docs, std::back_inserter(hidden_non_null_docs),
                         [&null_docs](uint32_t doc) { return !null_docs.contains(doc); });
    uint64_t hidden_lengths = 0;
    RETURN_IF_ERROR(source.sum_norm_lengths_of(hidden_non_null_docs, &hidden_lengths));
    DORIS_CHECK(hidden_lengths <= non_null_length_sum);
    stats->token_count = non_null_length_sum - hidden_lengths;
    return Status::OK();
}

Status exclude_hidden_term_docs(index_query::IndexSource& source, std::string_view term,
                                std::span<const uint32_t> hidden_docs, uint64_t* doc_frequency) {
    DORIS_CHECK(doc_frequency != nullptr);
    uint64_t hidden = 0;
    RETURN_IF_ERROR(source.scoring_doc_freq_among(term, hidden_docs, &hidden));
    if (hidden > *doc_frequency) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                "term lists {} hidden rows but only {} documents", hidden, *doc_frequency);
    }
    *doc_frequency -= hidden;
    return Status::OK();
}

} // namespace collection_statistics_detail

namespace {

// The rows of a segment that a scan with `hidden_rows` does not return, ascending.
Status collect_hidden_docs(const RowsetSharedPtr& rowset, const RowsetSegmentView& seg,
                           const TabletSchema& tablet_schema, const MowHiddenRows& hidden_rows,
                           const io::IOContext* io_ctx, std::vector<uint32_t>* hidden_docs) {
    DORIS_CHECK(hidden_rows.delete_bitmap != nullptr);
    DORIS_CHECK(hidden_rows.version >= 0);
    roaring::Roaring hidden =
            *hidden_rows.delete_bitmap->get_agg(seg.delete_bitmap_key(hidden_rows.version));

    // A row written with the delete sign stays readable until base compaction removes it, while
    // queries filter it out by predicate.
    if (tablet_schema.delete_sign_idx() >= 0) {
        OlapReaderStatistics stats;
        SegmentCacheHandle cache_handle;
        // The delete sign is column data, read as the query reads its columns.
        io::IOContext data_io_ctx;
        if (io_ctx != nullptr) {
            data_io_ctx = *io_ctx;
        }
        data_io_ctx.is_inverted_index = false;
        RETURN_IF_ERROR(SegmentLoader::instance()->load_segment(
                std::static_pointer_cast<BetaRowset>(rowset), seg.ref(), &cache_handle,
                hidden_rows.use_segment_cache, false, &stats, &data_io_ctx));
        DORIS_CHECK(cache_handle.get_segments().size() == 1);
        const auto& segment = cache_handle.get_segments()[0];
        const TabletColumn& delete_sign = tablet_schema.column(tablet_schema.delete_sign_idx());

        StorageReadOptions read_options(stats);
        read_options.version = rowset->version();
        read_options.io_ctx = data_io_ctx;
        std::shared_ptr<segment_v2::ColumnReader> column_reader;
        RETURN_IF_ERROR(
                segment->get_column_reader_for_pruning(delete_sign, read_options, &column_reader));
        bool may_hold_deletes = true;
        if (column_reader->has_zone_map()) {
            segment_v2::ZoneMap zone_map;
            RETURN_IF_ERROR(column_reader->get_segment_zone_map(&zone_map));
            // Any sign other than 0 deletes the row. Bounds the zone map cannot vouch for say
            // nothing.
            may_hold_deletes = zone_map.pass_all || (zone_map.has_not_null &&
                                                     (zone_map.min_value.get<TYPE_TINYINT>() != 0 ||
                                                      zone_map.max_value.get<TYPE_TINYINT>() != 0));
        }
        if (may_hold_deletes) {
            std::unique_ptr<segment_v2::ColumnIterator> iterator;
            RETURN_IF_ERROR(segment->new_column_iterator(delete_sign, &iterator, &read_options));
            segment_v2::ColumnIteratorOptions iterator_options {
                    .use_page_cache =
                            hidden_rows.use_page_cache && !config::disable_storage_page_cache,
                    .file_reader = segment->file_reader().get(),
                    .stats = &stats,
                    .io_ctx = read_options.io_ctx,
            };
            RETURN_IF_ERROR(iterator->init(iterator_options));
            RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
            auto data_type = DataTypeFactory::instance().create_data_type(delete_sign);
            constexpr size_t kBatch = 4096;
            for (uint32_t begin = 0; begin < segment->num_rows();) {
                size_t rows = std::min<size_t>(kBatch, segment->num_rows() - begin);
                MutableColumnPtr column = data_type->create_column();
                bool has_null = false;
                RETURN_IF_ERROR(iterator->next_batch(&rows, column, &has_null));
                if (rows == 0) {
                    return Status::Corruption("segment {} ended after {} of its {} rows",
                                              segment->id(), begin, segment->num_rows());
                }
                const auto& signs = assert_cast<const ColumnInt8&>(*column).get_data();
                for (size_t i = 0; i < rows; ++i) {
                    if (signs[i] != 0) {
                        hidden.add(begin + static_cast<uint32_t>(i));
                    }
                }
                begin += static_cast<uint32_t>(rows);
            }
        }
    }

    hidden_docs->resize(hidden.cardinality());
    hidden.toUint32Array(hidden_docs->data());
    return Status::OK();
}

} // namespace

Status CollectionStatistics::collect(RuntimeState* state,
                                     const std::vector<RowSetSplits>& rs_splits,
                                     const TabletSchemaSPtr& tablet_schema,
                                     const VExprContextSPtrs& common_expr_ctxs_push_down,
                                     io::IOContext* io_ctx, const MowHiddenRows* hidden_rows) {
    std::vector<RowsetSharedPtr> rowsets;
    rowsets.reserve(rs_splits.size());
    for (const auto& rs_split : rs_splits) {
        DORIS_CHECK(rs_split.rs_reader != nullptr);
        auto rowset = rs_split.rs_reader->rowset();
        DORIS_CHECK(rowset != nullptr);
        rowsets.emplace_back(std::move(rowset));
    }
    return collect_full_collection(state, rowsets, tablet_schema, common_expr_ctxs_push_down,
                                   io_ctx, hidden_rows);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) Statistics collection spans rowsets and formats.
Status CollectionStatistics::collect_full_collection(
        RuntimeState* state, const std::vector<RowsetSharedPtr>& rowsets,
        const TabletSchemaSPtr& tablet_schema, const VExprContextSPtrs& common_expr_ctxs_push_down,
        io::IOContext* io_ctx, const MowHiddenRows* hidden_rows) {
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
        for (auto seg : rowset->segments()) {
            auto status = process_segment(rowset, seg, tablet_schema.get(), collect_infos,
                                          hidden_rows, io_ctx);
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
            if (!first_tablet) {
                oss << ",";
            }
            oss << tid;
            first_tablet = false;
        }
        oss << "]";

        oss << ", total_num_docs=" << _total_num_docs;

        for (const auto& [ws_field_name, num_tokens] : _total_num_tokens) {
            oss << ", {field=" << segment_v2::inverted_index::StringHelper::to_string(ws_field_name)
                << ", num_tokens=" << num_tokens << ", terms=[";

            auto field_term_doc_freqs = _term_doc_freqs.find(ws_field_name);
            if (field_term_doc_freqs != _term_doc_freqs.end()) {
                bool first_term = true;
                for (const auto& [term, doc_freq] : field_term_doc_freqs->second) {
                    if (!first_term) {
                        oss << ", ";
                    }
                    oss << "(" << segment_v2::inverted_index::StringHelper::to_string(term) << ":"
                        << doc_freq << ")";
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

    MatchPredicateCollector match_collector;
    std::unordered_map<TExprNodeType::type, PredicateCollector*> collectors;
    collectors[TExprNodeType::MATCH_PRED] = &match_collector;
    if (_search_collector != nullptr) {
        collectors[TExprNodeType::SEARCH_EXPR] = _search_collector.get();
    }

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
            // NOLINTNEXTLINE(modernize-loop-convert) Stack traversal needs reverse child order.
            for (auto child = children.rbegin(); child != children.rend(); ++child) {
                stack.push(*child);
            }
        }
    }

    LOG(INFO) << "Extracted collect info for " << collect_infos->size() << " fields";

    return Status::OK();
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size) Segment collection handles both formats.
Status CollectionStatistics::process_segment(const RowsetSharedPtr& rowset,
                                             const RowsetSegmentView& seg,
                                             const TabletSchema* tablet_schema,
                                             const CollectInfoMap& collect_infos,
                                             const MowHiddenRows* hidden_rows,
                                             io::IOContext* io_ctx) {
    auto seg_path = DORIS_TRY(seg.path());
    auto rowset_meta = rowset->rowset_meta();

    auto idx_file_reader = std::make_unique<IndexFileReader>(
            rowset_meta->fs(),
            std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path)},
            tablet_schema->get_inverted_index_storage_format(), seg.inverted_index_file_info(),
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

    std::vector<uint32_t> hidden_docs;
    if (hidden_rows != nullptr) {
        RETURN_IF_ERROR(collect_hidden_docs(rowset, seg, *tablet_schema, *hidden_rows, io_ctx,
                                            &hidden_docs));
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
            ::doris::snii::reader::SniiIndexSource source(*logical_reader);
            index_query::IndexScoringStats scoring_stats;
            RETURN_IF_ERROR(source.scoring_stats(&scoring_stats));
            if (hidden_rows != nullptr) {
                roaring::Roaring null_docs;
                uint64_t non_null_length_sum = 0;
                if (scoring_stats.has_norms) {
                    std::vector<uint32_t> null_doc_ids;
                    RETURN_IF_ERROR(logical_reader->read_null_docids(&null_doc_ids));
                    null_docs.addMany(null_doc_ids.size(), null_doc_ids.data());
                    RETURN_IF_ERROR(collection_statistics_detail::sum_non_null_norm_lengths(
                            source, null_doc_ids, &non_null_length_sum));
                }
                RETURN_IF_ERROR(collection_statistics_detail::exclude_hidden_docs(
                        source, hidden_docs, null_docs, non_null_length_sum, &scoring_stats));
            }
            const uint64_t segment_doc_count = scoring_stats.doc_count;
            RETURN_IF_ERROR(admit_snii_scoring_segment(
                    ws_field_name, segment_doc_count, scoring_stats.token_count,
                    scoring_stats.has_positions, scoring_stats.has_norms, &segment_accumulator,
                    segment_v2::inverted_index::InvertedIndexAnalyzer::should_analyzer(
                            collect_info.index_meta->properties()),
                    segment_v2::IndexReaderHelper::is_normalizer_only(
                            collect_info.index_meta->properties())));

            for (const auto& logical_term_bytes : collect_info.unique_terms) {
                const auto logical_term =
                        segment_v2::inverted_index::StringHelper::to_wstring(logical_term_bytes);
                uint64_t doc_frequency = 0;
                RETURN_IF_ERROR(source.scoring_doc_freq(logical_term_bytes, &doc_frequency));
                if (!hidden_docs.empty()) {
                    RETURN_IF_ERROR(collection_statistics_detail::exclude_hidden_term_docs(
                            source, logical_term_bytes, hidden_docs, &doc_frequency));
                }
                if (doc_frequency > segment_doc_count) {
                    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>(
                            "SNII term document frequency {} exceeds segment document count {}",
                            doc_frequency, segment_doc_count);
                }
                collection_statistics_detail::add_term_doc_frequency(
                        &segment_accumulator.term_doc_freqs, ws_field_name, logical_term,
                        doc_frequency);
            }
        }

        commit_snii_scoring_segment(std::move(segment_accumulator));
        _counted_hidden_docs = _counted_hidden_docs || !hidden_docs.empty();
        return Status::OK();
    }

    int32_t total_segment_docs = 0;

    for (const auto& [ws_field_name, collect_info] : collect_infos) {
        lucene::search::IndexSearcher* index_searcher = nullptr;
        lucene::index::IndexReader* index_reader = nullptr;
#ifdef BE_TEST
        std::shared_ptr<lucene::search::IndexSearcher> owned_index_searcher;
#else
        InvertedIndexCacheHandle inverted_index_cache_handle;
#endif
#ifdef BE_TEST
        owned_index_searcher = DORIS_TRY(open_optional_clucene_stats_searcher(
                *idx_file_reader, collect_info.index_meta, io_ctx));
        if (owned_index_searcher == nullptr) {
            continue; // This field has no index in this segment; collect the other fields.
        }
        index_searcher = owned_index_searcher.get();
        index_reader = index_searcher->getReader();
#else
        auto index_file_key = idx_file_reader->get_index_file_cache_key(collect_info.index_meta);
        InvertedIndexSearcherCache::CacheKey searcher_cache_key(index_file_key);

        if (!InvertedIndexSearcherCache::instance()->lookup(searcher_cache_key,
                                                            &inverted_index_cache_handle)) {
            auto searcher_ptr = DORIS_TRY(open_optional_clucene_stats_searcher(
                    *idx_file_reader, collect_info.index_meta, io_ctx));
            if (searcher_ptr == nullptr) {
                continue; // This field has no index in this segment; collect the other fields.
            }
            index_reader = searcher_ptr->getReader();
            size_t reader_size = index_reader->getTermInfosRAMUsed();
            auto* cache_value = new CluceneSearcherCacheValue(std::move(searcher_ptr), reader_size,
                                                              UnixMillis());
            InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                                           &inverted_index_cache_handle);
        }

        auto searcher_variant =
                inverted_index_cache_handle.get<CluceneSearcherCacheValue>()->searcher;
        auto index_searcher_ptr = std::get<FulltextIndexSearcherPtr>(searcher_variant);
        index_searcher = index_searcher_ptr.get();
        index_reader = index_searcher->getReader();
#endif
        auto source = segment_v2::clucene_index_source(segment_v2::non_owning_reader(index_reader),
                                                       ws_field_name, io_ctx);
        index_query::IndexScoringStats scoring_stats;
        RETURN_IF_ERROR(source->scoring_stats(&scoring_stats));
        if (hidden_rows != nullptr) {
            std::shared_ptr<roaring::Roaring> null_docs = std::make_shared<roaring::Roaring>();
            uint64_t non_null_length_sum = 0;
            if (scoring_stats.has_norms) {
                segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
                RETURN_IF_ERROR(segment_v2::read_clucene_null_bitmap(
                        *idx_file_reader, *collect_info.index_meta, io_ctx,
                        &null_bitmap_cache_handle));
                null_docs = null_bitmap_cache_handle.get_bitmap();
                DORIS_CHECK(null_docs != nullptr);
                auto sum_lengths = [&source, &null_docs](uint64_t* out) {
                    std::vector<uint32_t> null_doc_ids(null_docs->cardinality());
                    null_docs->toUint32Array(null_doc_ids.data());
                    return collection_statistics_detail::sum_non_null_norm_lengths(
                            *source, null_doc_ids, out);
                };
#ifdef BE_TEST
                RETURN_IF_ERROR(sum_lengths(&non_null_length_sum));
#else
                RETURN_IF_ERROR(inverted_index_cache_handle.get<CluceneSearcherCacheValue>()
                                        ->non_null_norm_length_sum(ws_field_name, sum_lengths,
                                                                   &non_null_length_sum));
#endif
            }
            RETURN_IF_ERROR(collection_statistics_detail::exclude_hidden_docs(
                    *source, hidden_docs, *null_docs, non_null_length_sum, &scoring_stats));
        }
        total_segment_docs =
                std::max(total_segment_docs, static_cast<int32_t>(scoring_stats.doc_count));
        // BM25 on an analyzed index needs the record length of every row, and CLucene keeps
        // them, together with the field's token count, in the norms. A segment written without
        // norms would feed a zero avgdl, or rank its rows as zero-length documents next to the
        // segments that have norms, so refuse to score the collection, as SNII does.
        // Keywords and lowercase-only normalizers instead use unit document length.
        if (!scoring_stats.has_norms &&
            !segment_v2::IndexReaderHelper::is_normalizer_only(
                    collect_info.index_meta->properties()) &&
            segment_v2::inverted_index::InvertedIndexAnalyzer::should_analyzer(
                    collect_info.index_meta->properties())) {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "BM25 scoring requires norms, but segment {} was written without norms for "
                    "field {}. Norms are left out when the index sets \"norms\" = \"false\" or, "
                    "for a variant path, when inverted_index_skip_norms_for_variant is on",
                    seg_path, segment_v2::inverted_index::StringHelper::to_string(ws_field_name));
        }
        _total_num_tokens[ws_field_name] +=
                scoring_stats.has_norms ? scoring_stats.token_count : scoring_stats.doc_count;

        for (const auto& logical_term_bytes : collect_info.unique_terms) {
            const auto logical_term =
                    segment_v2::inverted_index::StringHelper::to_wstring(logical_term_bytes);
            uint64_t doc_frequency = 0;
            RETURN_IF_ERROR(source->scoring_doc_freq(logical_term_bytes, &doc_frequency));
            if (!hidden_docs.empty()) {
                RETURN_IF_ERROR(collection_statistics_detail::exclude_hidden_term_docs(
                        *source, logical_term_bytes, hidden_docs, &doc_frequency));
            }
            collection_statistics_detail::add_term_doc_frequency(&_term_doc_freqs, ws_field_name,
                                                                 logical_term, doc_frequency);
        }
    }

    _total_num_docs += static_cast<uint64_t>(total_segment_docs);
    _counted_hidden_docs = _counted_hidden_docs || !hidden_docs.empty();
    _avg_dl_by_col.clear();
    _idf_by_col_term.clear();

    return Status::OK();
}

Status CollectionStatistics::admit_snii_scoring_segment(
        const std::wstring& field_name, uint64_t index_doc_count, uint64_t sum_total_term_freq,
        bool has_positions, bool has_norms, SniiScoringSegmentAccumulator* segment_accumulator,
        bool is_analyzed, bool single_term_normalizer) {
    DORIS_CHECK(segment_accumulator != nullptr);
    auto segment_stats = collection_statistics_detail::resolve_snii_scoring_segment(
            index_doc_count, sum_total_term_freq, has_positions, has_norms, is_analyzed,
            single_term_normalizer);
    if (!segment_stats.has_value()) {
        clear();
        return segment_stats.error();
    }
    if (!segment_accumulator->token_counts.empty() &&
        segment_accumulator->doc_count != segment_stats->doc_count) {
        clear();
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                "SNII scoring fields in one segment have different document counts: {} and {}",
                segment_accumulator->doc_count, segment_stats->doc_count);
    }
    segment_accumulator->doc_count = segment_stats->doc_count;
    segment_accumulator->token_counts[field_name] += segment_stats->token_count;
    return Status::OK();
}

void CollectionStatistics::commit_snii_scoring_segment(
        SniiScoringSegmentAccumulator&& segment_accumulator) {
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
    _counted_hidden_docs = false;
    _total_num_tokens.clear();
    _term_doc_freqs.clear();
    _avg_dl_by_col.clear();
    _idf_by_col_term.clear();
}

void CollectionStatistics::copy_collected_from(const CollectionStatistics& other) {
    _total_num_docs = other._total_num_docs;
    _counted_hidden_docs = other._counted_hidden_docs;
    _total_num_tokens = other._total_num_tokens;
    _term_doc_freqs = other._term_doc_freqs;
    _avg_dl_by_col.clear();
    _idf_by_col_term.clear();
}

Status TabletCollectionStatistics::copy_to(RuntimeState* state,
                                           const TabletSchemaSPtr& tablet_schema,
                                           const VExprContextSPtrs& common_expr_ctxs_push_down,
                                           io::IOContext* io_ctx, const MowHiddenRows* hidden_rows,
                                           CollectionStatistics* statistics) {
    DORIS_CHECK(statistics != nullptr);
    std::lock_guard lock(_mutex);
    if (!_collected) {
        // Every scanner of the tablet reads with the same delete bitmap and version.
        _status = _statistics.collect_full_collection(
                state, _rowsets, tablet_schema, common_expr_ctxs_push_down, io_ctx, hidden_rows);
        _collected = true;
        // Do not hold the rowsets any longer than needed.
        _rowsets.clear();
    }
    RETURN_IF_ERROR(_status);
    statistics->copy_collected_from(_statistics);
    return Status::OK();
}

uint64_t CollectionStatistics::get_term_doc_freq_by_col(const std::wstring& lucene_col_name,
                                                        const std::wstring& term) {
    const auto field = _term_doc_freqs.find(lucene_col_name);
    if (field == _term_doc_freqs.end()) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        segment_v2::inverted_index::StringHelper::to_string(lucene_col_name));
    }

    const auto term_frequency = field->second.find(term);
    if (term_frequency == field->second.end()) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such term {}",
                        segment_v2::inverted_index::StringHelper::to_string(term));
    }

    return term_frequency->second;
}

uint64_t CollectionStatistics::get_total_term_cnt_by_col(const std::wstring& lucene_col_name) {
    const auto token_count = _total_num_tokens.find(lucene_col_name);
    if (token_count == _total_num_tokens.end()) {
        throw Exception(ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                        "Index statistics collection failed: Not such column {}",
                        segment_v2::inverted_index::StringHelper::to_string(lucene_col_name));
    }

    return token_count->second;
}

uint64_t CollectionStatistics::get_doc_num() const {
    // A tablet whose rows are all hidden has no document a query can match, so its statistics
    // are empty rather than missing.
    if (_total_num_docs == 0 && !_counted_hidden_docs) {
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
    float avg_dl = total_doc_cnt > 0 ? float((double)total_term_cnt / (double)total_doc_cnt) : 1.0F;
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
    auto idf = (float)std::log(1 + ((double)doc_num - (double)doc_freq + 0.5) /
                                           ((double)doc_freq + 0.5));
    _idf_by_col_term[lucene_col_name][term] = idf;
    return idf;
}

} // namespace doris
